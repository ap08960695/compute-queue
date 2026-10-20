"""Scheduler and job-lifecycle behaviour from docs/SPEC.md (model.py only)."""
import os
import tempfile
import unittest

from cq import model

GiB = 2**30
GB = 10**9


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = model.connect(os.path.join(self.tmp.name, 'cq.db'))
        self.addCleanup(self.db.close)

    def node(self, name='ai', avail=20 * GiB, vram_used=0, outside=False,
             status='active', floor=4 * GiB, vram_cap=19.5 * GB, allocs=()):
        """Register a node (once), set its status and send one heartbeat."""
        if not self.db.execute('select 1 from nodes where name=?',
                               (name,)).fetchone():
            model.register_node(self.db, name, 'linux',
                                {'ram_total': 64 * GiB, 'vram_total': 24 * GB},
                                status='pending', ram_floor=floor,
                                vram_cap=vram_cap)
        model.set_node_status(self.db, name, status)
        self.beat(name, avail, vram_used, outside, allocs)

    def beat(self, name='ai', avail=20 * GiB, vram_used=0, outside=False,
             allocs=()):
        model.record_heartbeat(self.db, name, {
            'mem_available': avail, 'vram_used': vram_used,
            'outside_gpu_busy': outside, 'allocs': list(allocs)}, 0)

    def job(self, **kw):
        kw.setdefault('submitter', 'alice')
        kw.setdefault('argv', ['true'])
        return model.create_job(self.db, **kw)

    def row(self, jid):
        return self.db.execute('select * from jobs where id=?', (jid,)).fetchone()

    def run_job(self, jid, node='ai'):
        """Force a job into running on a node (as the heartbeat would)."""
        self.db.execute("update jobs set state='running', node=?, started=0 "
                        "where id=?", (node, jid))
        self.db.commit()

    def plan(self, node='ai', now=0):
        return model.plan_for_node(self.db, node, now)

    def started(self, node='ai', now=0):
        return [j['id'] for j in self.plan(node, now)['start']]


class QueueOrderTest(Base):
    def test_order_urgent_priority_position_id(self):
        self.node()
        a = self.job(priority=0)
        b = self.job(priority=5)
        c = self.job(priority=0, urgent=True)
        d = self.job(priority=5)
        self.assertEqual(self.started(), [c, b, d, a])

    def test_position_beats_id(self):
        self.node()
        a, b = self.job(), self.job()
        self.db.execute('update jobs set position=-1 where id=?', (b,))
        self.db.commit()
        self.assertEqual(self.started(), [b, a])

    def test_node_pref(self):
        self.node('ai')
        self.node('mac')
        a = self.job(node_pref='mac')
        b = self.job(node_pref='ai')
        c = self.job(node_pref='any')
        self.assertEqual(sorted(self.started('ai')), sorted([b, c]))
        self.assertEqual(self.started('mac'), [a])

    def test_inactive_node_gets_nothing(self):
        for status in ('pending', 'faulted', 'disabled'):
            with self.subTest(status=status):
                self.node(status=status)
                self.job()
                p = self.plan()
                self.assertEqual((p['start'], p['preempt']), ([], []))

    def test_start_marks_running_on_node(self):
        self.node()
        a = self.job()
        self.assertEqual(self.started(), [a])
        r = self.row(a)
        self.assertEqual((r['state'], r['node']), ('running', 'ai'))
        self.assertEqual(self.started(), [])


class DepsTest(Base):
    def test_dep_not_done_waits(self):
        self.node()
        a = self.job()
        b = self.job(deps=[a])
        self.assertEqual(self.started(), [a])
        self.assertEqual(self.row(b)['state'], 'queued')

    def test_dep_done_runs(self):
        self.node()
        a = self.job()
        b = self.job(deps=[a])
        self.db.execute("update jobs set state='done' where id=?", (a,))
        self.db.commit()
        self.assertEqual(self.started(), [b])

    def test_dep_failed_cancelled_blocked(self):
        self.node()
        for bad in ('failed', 'cancelled', 'blocked'):
            with self.subTest(dep=bad):
                a = self.job()
                self.db.execute("update jobs set state=? where id=?", (bad, a))
                b = self.job(deps=[a])
                self.db.commit()
                p = self.plan()
                self.assertIn(b, p['blocked'])
                r = self.row(b)
                self.assertEqual(r['state'], 'blocked')
                self.assertIn(str(a), r['state_note'])

    def test_blocked_dep_does_not_stop_others(self):
        self.node()
        a = self.job()
        self.db.execute("update jobs set state='failed' where id=?", (a,))
        b = self.job(deps=[a])
        c = self.job()
        self.db.commit()
        self.assertEqual(self.started(), [c])
        self.assertEqual(self.row(b)['state'], 'blocked')


class HeadroomTest(Base):
    def test_ram_floor(self):
        # avail 20, floor 4 -> 16 GiB free; effective = 1.3 * req
        self.node(avail=20 * GiB)
        fits = self.job(ram_req=12 * GiB)      # 15.6
        self.assertEqual(self.started(), [fits])

    def test_ram_too_big(self):
        self.node(avail=20 * GiB)
        big = self.job(ram_req=13 * GiB)       # 16.9 > 16
        self.assertEqual(self.started(), [])
        self.assertEqual(self.row(big)['state'], 'queued')

    def test_ram_custom_floor(self):
        self.node(avail=20 * GiB, floor=12 * GiB)   # 8 GiB free
        a = self.job(ram_req=7 * GiB)               # 9.1 > 8
        b = self.job(ram_req=6 * GiB)               # 7.8 <= 8
        # b fits but would leave 0.2 GiB, less than head a's 9.1 -> no start
        self.assertEqual(self.started(), [])
        self.assertEqual(self.row(a)['state'], 'queued')
        self.assertEqual(self.row(b)['state'], 'queued')

    def test_deducts_between_starts(self):
        self.node(avail=20 * GiB)                   # 16 free
        a = self.job(ram_req=8 * GiB)               # 10.4
        b = self.job(ram_req=8 * GiB)               # would be 20.8 total
        self.assertEqual(self.started(), [a])
        self.assertEqual(self.row(b)['state'], 'queued')

    def test_pending_growth(self):
        run = self.job(ram_req=10 * GiB)            # eff 13
        self.run_job(run)
        # running alloc has used 3 GiB so far -> 10 GiB growth still pending
        self.node(avail=20 * GiB, allocs=[
            {'job_id': run, 'state': 'running',
             'current_ram': 3 * GiB, 'current_vram': 0}])
        a = self.job(ram_req=6 * GiB)               # 7.8 > 16-10=6
        self.assertEqual(self.started(), [])
        self.assertEqual(self.row(a)['state'], 'queued')

    def test_no_pending_growth_when_at_effective(self):
        run = self.job(ram_req=10 * GiB)
        self.run_job(run)
        self.node(avail=20 * GiB, allocs=[
            {'job_id': run, 'state': 'running',
             'current_ram': 13 * GiB, 'current_vram': 0}])
        a = self.job(ram_req=6 * GiB)
        self.assertEqual(self.started(), [a])


class GpuTest(Base):
    def test_one_gpu_alloc_per_node(self):
        self.node()
        a = self.job(gpu=True, vram_req=1 * GB)
        b = self.job(gpu=True, vram_req=1 * GB)
        self.assertEqual(self.started(), [a])
        self.assertEqual(self.row(b)['state'], 'queued')

    def test_running_gpu_alloc_blocks_gpu(self):
        run = self.job(gpu=True, vram_req=1 * GB)
        self.run_job(run)
        self.node()
        g = self.job(gpu=True, vram_req=1 * GB)
        c = self.job()
        self.assertEqual(self.started(), [c])
        self.assertEqual(self.row(g)['state'], 'queued')

    def test_preempting_gpu_alloc_blocks_gpu(self):
        run = self.job(gpu=True, vram_req=1 * GB)
        self.run_job(run)
        self.db.execute("update jobs set state='preempting' where id=?", (run,))
        self.db.commit()
        self.node()
        g = self.job(gpu=True, vram_req=1 * GB)
        self.assertNotIn(g, self.started())

    def test_gpu_and_cpu_job_together(self):
        self.node()
        g = self.job(gpu=True, vram_req=1 * GB)
        c = self.job()
        self.assertEqual(sorted(self.started()), sorted([g, c]))

    def test_outside_gpu_busy_blocks_gpu_only(self):
        self.node(outside=True)
        g = self.job(gpu=True, vram_req=1 * GB)
        c = self.job()
        self.assertEqual(self.started(), [c])
        self.assertEqual(self.row(g)['state'], 'queued')

    def test_faulted_node_blocks_gpu(self):
        self.node(status='faulted')
        g = self.job(gpu=True, vram_req=1 * GB)
        self.assertNotIn(g, self.started())

    def test_vram_cap(self):
        self.node(vram_used=0)
        big = self.job(gpu=True, vram_req=20 * GB)          # > 19.5 cap
        self.assertEqual(self.started(), [])
        self.assertEqual(self.row(big)['state'], 'queued')

    def test_vram_free_accounts_used(self):
        self.node(vram_used=15 * GB)                        # 4.5 GB free
        big = self.job(gpu=True, vram_req=6 * GB)
        self.assertEqual(self.started(), [])
        self.assertEqual(self.row(big)['state'], 'queued')
        # fits, but is a GPU job behind a GPU head with unknown est_s and
        # not leaseable: neither 6(a) nor 6(b) applies
        ok = self.job(gpu=True, vram_req=2 * GB)
        self.assertEqual(self.started(), [])
        self.assertEqual(self.row(ok)['state'], 'queued')

    def test_non_gpu_job_behind_gpu_blocked_head_starts(self):
        self.node(outside=True)                             # GPU not free
        g = self.job(gpu=True, vram_req=1 * GB)             # head, blocked
        c = self.job(ram_req=1 * GiB)                       # est_s unknown
        self.assertEqual(self.started(), [c])
        self.assertEqual(self.row(g)['state'], 'queued')

    def test_small_ram_job_starts_when_ram_head_still_covered(self):
        self.node(avail=20 * GiB, outside=True)             # 16 GiB free
        g = self.job(gpu=True, vram_req=1 * GB, ram_req=8 * GiB)  # head 10.4
        c = self.job(ram_req=2 * GiB)                       # 2.6 -> 13.4 left
        self.assertEqual(self.started(), [c])
        self.assertEqual(self.row(g)['state'], 'queued')


class BackfillTest(Base):
    def setUp(self):
        super().setUp()
        self.node(avail=20 * GiB)                           # 16 GiB free
        self.head = self.job(ram_req=14 * GiB, est_s=7200)  # 18.2 > 16

    def test_short_job_backfills(self):
        s = self.job(ram_req=1 * GiB, est_s=1800)
        self.assertEqual(self.started(), [s])
        self.assertEqual(self.row(self.head)['state'], 'queued')

    def test_long_job_not_backfilled(self):
        self.job(ram_req=1 * GiB, est_s=1801)
        self.assertEqual(self.started(), [])

    def test_leaseable_long_job_backfills(self):
        s = self.job(ram_req=1 * GiB, est_s=7200, leaseable=True)
        self.assertEqual(self.started(), [s])

    def test_jobs_before_head_start_regardless_of_est(self):
        self.db.execute("update jobs set state='cancelled' where id=?",
                        (self.head,))
        self.db.commit()
        s = self.job(ram_req=1 * GiB, est_s=99999)
        self.assertEqual(self.started(), [s])


class PreemptTest(Base):
    def running(self, **kw):
        kw.setdefault('ram_req', 10 * GiB)                  # eff 13
        jid = self.job(**kw)
        self.run_job(jid)
        return jid

    def beat_with(self, *ids, avail=20 * GiB, cur=13 * GiB):
        self.beat(avail=avail, allocs=[
            {'job_id': i, 'state': 'running',
             'current_ram': cur, 'current_vram': 0} for i in ids])

    def test_urgent_head_preempts_leaseable(self):
        self.node()
        low = self.running(leaseable=True)
        self.beat_with(low)
        head = self.job(ram_req=18 * GiB, urgent=True)      # 23.4 > 16
        p = self.plan()
        self.assertEqual(p['start'], [])
        self.assertEqual(p['preempt'], [low])
        self.assertEqual(self.row(head)['state'], 'queued')

    def test_never_preempt_non_leaseable(self):
        self.node()
        low = self.running(leaseable=False)
        self.beat_with(low)
        self.job(ram_req=18 * GiB, urgent=True)
        self.assertEqual(self.plan()['preempt'], [])

    def test_never_preempt_hold(self):
        self.node()
        h = self.running(argv=(), hold=True, leaseable=True, hold_until=10**6)
        self.beat_with(h)
        self.job(ram_req=18 * GiB, urgent=True)
        self.assertEqual(self.plan()['preempt'], [])

    def test_only_lower_urgent_priority(self):
        self.node()
        peer = self.running(leaseable=True, urgent=True, priority=5)
        self.beat_with(peer)
        self.job(ram_req=18 * GiB, urgent=True, priority=0)
        self.assertEqual(self.plan()['preempt'], [])

    def test_non_urgent_head_does_not_preempt(self):
        self.node()
        low = self.running(leaseable=True)
        self.beat_with(low)
        self.job(ram_req=18 * GiB, priority=9)
        self.assertEqual(self.plan(now=60)['preempt'], [])

    def test_preempts_only_enough(self):
        self.node()
        ids = [self.running(leaseable=True, ram_req=4 * GiB)   # eff 5.2 each
               for _ in range(3)]
        self.beat_with(*ids, cur=int(5.2 * GiB))
        self.job(ram_req=14 * GiB, urgent=True)             # 18.2; free 16
        p = self.plan()
        self.assertEqual(len(p['preempt']), 1)
        self.assertTrue(set(p['preempt']) <= set(ids))

    def test_preempt_is_request_only(self):
        self.node()
        low = self.running(leaseable=True)
        self.beat_with(low)
        self.job(ram_req=18 * GiB, urgent=True)
        self.plan()
        self.assertIn(self.row(low)['state'], ('running', 'preempting'))


class ProfileTest(Base):
    def finish(self, kind, peak_ram, peak_vram=0, wall_s=100, rc=0):
        jid = self.job(kind=kind)
        self.run_job(jid)
        model.report(self.db, jid, rc,
                     {'peak_ram': peak_ram, 'peak_vram': peak_vram},
                     wall_s, 10)
        return jid

    def test_no_profile_uses_request(self):
        j = self.job(kind='k', ram_req=10 * GiB, est_s=60)
        ram, _, est = model.effective(self.db, self.row(j))
        self.assertAlmostEqual(ram, 1.3 * 10 * GiB, delta=GiB // 100)
        self.assertEqual(est, 60)

    def test_profile_raises_above_small_request(self):
        for peak in (4, 6, 5):
            self.finish('k', peak * GiB, 2 * GB, wall_s=50 + peak)
        j = self.job(kind='k', ram_req=1 * GiB)
        ram, vram, est = model.effective(self.db, self.row(j))
        self.assertAlmostEqual(ram, 1.3 * 6 * GiB, delta=GiB // 100)
        self.assertGreaterEqual(vram, 2 * GB)
        self.assertLessEqual(vram, 2 * GB + 0.5 * GB + 1)
        self.assertEqual(est, 56)

    def test_profile_never_lowers_below_request(self):
        for peak in (4, 6, 5):
            self.finish('k', peak * GiB)
        j = self.job(kind='k', ram_req=40 * GiB)
        ram, _, _ = model.effective(self.db, self.row(j))
        self.assertAlmostEqual(ram, 1.3 * 40 * GiB, delta=GiB // 100)

    def test_ram_floor_256_mib(self):
        j = self.job(kind='none', ram_req=1)
        ram, _, _ = model.effective(self.db, self.row(j))
        self.assertEqual(ram, 256 * 2**20)
        self.finish('tiny', 1000)
        j = self.job(kind='tiny', ram_req=0)
        ram, _, _ = model.effective(self.db, self.row(j))
        self.assertEqual(ram, 256 * 2**20)

    def test_zero_profile_peaks_ignored(self):
        self.finish('z', 0)
        self.finish('z', 0)
        j = self.job(kind='z', ram_req=10 * GiB)
        ram, _, _ = model.effective(self.db, self.row(j))
        self.assertAlmostEqual(ram, 1.3 * 10 * GiB, delta=GiB // 100)
        self.finish('z', 3 * GiB)
        j2 = self.job(kind='z', ram_req=1 * GiB)
        ram, _, _ = model.effective(self.db, self.row(j2))
        self.assertAlmostEqual(ram, 1.3 * 3 * GiB, delta=GiB // 100)

    def test_profile_keeps_last_five_runs(self):
        self.finish('k', 30 * GiB)                 # oldest, falls out
        for _ in range(5):
            self.finish('k', 2 * GiB)
        j = self.job(kind='k')
        ram, _, _ = model.effective(self.db, self.row(j))
        self.assertAlmostEqual(ram, 1.3 * 2 * GiB, delta=GiB // 100)

    def test_profile_drives_scheduling(self):
        self.finish('k', 2 * GiB)
        self.node(avail=20 * GiB)                  # 16 free
        j = self.job(kind='k', ram_req=1 * GiB)    # profile 2.6 GiB fits
        self.assertEqual(self.started(), [j])

    def test_profile_does_not_lower_request_for_scheduling(self):
        self.finish('k', 2 * GiB)
        self.node(avail=20 * GiB)                  # 16 free
        j = self.job(kind='k', ram_req=40 * GiB)   # 52 GiB cap, never fits
        self.assertEqual(self.started(), [])
        self.assertEqual(self.row(j)['state'], 'queued')

    def test_rc6_writes_profile(self):
        self.finish('k', 8 * GiB, rc=6)
        j = self.job(kind='k')
        ram, _, _ = model.effective(self.db, self.row(j))
        self.assertAlmostEqual(ram, 1.3 * 8 * GiB, delta=GiB // 100)


class ReportTest(Base):
    def test_rc0_done(self):
        j = self.job()
        self.run_job(j)
        model.report(self.db, j, 0, {'peak_ram': 1, 'peak_vram': 0}, 5, 9)
        self.assertEqual(self.row(j)['state'], 'done')

    def test_other_rc_failed(self):
        j = self.job()
        self.run_job(j)
        model.report(self.db, j, 1, {'peak_ram': 1, 'peak_vram': 0}, 5, 9)
        r = self.row(j)
        self.assertEqual((r['state'], r['rc']), ('failed', 1))

    def test_cancelled(self):
        j = self.job()
        self.run_job(j)
        self.db.execute("update jobs set state='cancelling' where id=?", (j,))
        self.db.commit()
        model.report(self.db, j, -15, {}, 5, 9, cancelled=True)
        self.assertEqual(self.row(j)['state'], 'cancelled')

    def test_cancelled_flag_on_running_job_uses_rc(self):
        for rc, state in ((-15, 'failed'), (0, 'done')):
            with self.subTest(rc=rc):
                j = self.job()
                self.run_job(j)
                model.report(self.db, j, rc, {}, 5, 9, cancelled=True)
                self.assertEqual(self.row(j)['state'], state)

    def test_rc6_requeues_same_position(self):
        a, b = self.job(), self.job()
        self.run_job(a)
        before = self.row(a)
        model.report(self.db, a, 6, {'peak_ram': 1, 'peak_vram': 0}, 5, 9)
        after = self.row(a)
        self.assertEqual(after['state'], 'queued')
        self.assertEqual(after['position'], before['position'])
        self.assertEqual(after['attempt'], before['attempt'] + 1)
        self.node()
        self.assertEqual(self.started(), [a, b])

    def test_fault_sets_node_faulted(self):
        self.node()
        j = self.job(gpu=True)
        self.run_job(j)
        model.report(self.db, j, 1, {}, 5, 9, fault='vram')
        st = self.db.execute("select status from nodes where name='ai'"
                             ).fetchone()['status']
        self.assertEqual(st, 'faulted')


class HoldTest(Base):
    def hold(self, until=1000, **kw):
        j = self.job(argv=(), hold=True, hold_until=until, **kw)
        self.run_job(j)
        return j

    def test_not_expired_within_grace(self):
        j = self.hold(1000, gpu=True)
        self.assertEqual(model.expire_holds(self.db, 1000 + 1800), [])
        self.assertEqual(self.row(j)['state'], 'running')

    def test_expired_after_grace(self):
        j = self.hold(1000, gpu=True)
        self.assertEqual(model.expire_holds(self.db, 1000 + 1801), [j])
        r = self.row(j)
        self.assertEqual((r['state'], r['state_note']), ('done', 'hold expired'))
        self.assertEqual(model.expire_holds(self.db, 99999), [])

    def test_non_hold_jobs_never_expire(self):
        j = self.job(hold_until=1)
        self.run_job(j)
        self.assertEqual(model.expire_holds(self.db, 10**6), [])

    def test_hold_occupies_gpu_slot(self):
        self.hold(10**6, gpu=True)
        self.node()
        g = self.job(gpu=True, vram_req=1 * GB)
        self.assertEqual(self.started(), [])
        self.assertEqual(self.row(g)['state'], 'queued')

    def test_expiry_frees_slot(self):
        self.hold(1000, gpu=True)
        self.node()
        g = self.job(gpu=True, vram_req=1 * GB)
        model.expire_holds(self.db, 5000)
        self.assertEqual(self.started(now=5000), [g])


if __name__ == '__main__':
    unittest.main()
