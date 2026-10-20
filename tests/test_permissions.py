"""Permissions table from docs/SPEC.md, exercised through model.allowed()."""
import unittest

from cq import model

ME, OTHER = 'alice', 'bob'
OWN = {'submitter': ME, 'node': None}
THEIRS = {'submitter': OTHER, 'node': None}
ROLES = ('owner', 'operator', 'submitter', 'node')


def can(role, action, job=None):
    return model.allowed(role, ME, action, job)


class PermissionsTest(unittest.TestCase):
    def check(self, action, job, expected):
        for role, want in zip(ROLES, expected):
            with self.subTest(action=action, role=role):
                self.assertIs(bool(can(role, action, job)), want)

    def test_submit_view(self):
        for action in ('submit', 'view'):
            self.check(action, OWN, (True, True, True, False))

    def test_cancel_own(self):
        self.check('cancel', OWN, (True, True, True, False))

    def test_cancel_others(self):
        self.check('cancel', THEIRS, (True, False, False, False))

    def test_move_priority_own(self):
        for action in ('move', 'priority'):
            self.check(action, OWN, (True, True, True, False))

    def test_move_priority_others(self):
        for action in ('move', 'priority'):
            self.check(action, THEIRS, (True, True, False, False))

    def test_urgent_preempt(self):
        for action in ('urgent', 'preempt'):
            for job in (OWN, THEIRS):
                self.check(action, job, (True, True, False, False))

    def test_node_admin(self):
        self.check('node_admin', None, (True, False, False, False))

    def test_heartbeat_own_node_only(self):
        job = {'submitter': None, 'node': ME}
        self.assertTrue(model.allowed('node', ME, 'heartbeat', job))
        self.assertFalse(model.allowed('node', ME, 'heartbeat',
                                       {'submitter': None, 'node': 'mac'}))
        for role in ('owner', 'operator', 'submitter'):
            with self.subTest(role=role):
                self.assertFalse(model.allowed(role, ME, 'heartbeat', job))

    def test_unknown_role_denied(self):
        for action in ('submit', 'view', 'cancel', 'node_admin', 'heartbeat'):
            self.assertFalse(model.allowed('guest', ME, action, OWN))


if __name__ == '__main__':
    unittest.main()
