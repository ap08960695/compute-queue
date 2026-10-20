"""CPU-only live probe: the launcher exits while a memory-using child remains.

Invoke explicitly through cq; unittest discovery does not run this file.
Receipts must be outside the allocation's signal directory so they survive cleanup.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def record(root, name, **extra):
    data = {"at": time.time(), "pid": os.getpid(), "pgid": os.getpgrp(),
            "job_id": os.environ.get("CQ_JOB_ID"), **extra}
    path = root / f"{name}.json"
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(data))
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receipt_dir", type=Path)
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--seconds", type=int, default=30)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 60:
        parser.error("--seconds must be between 1 and 60")
    args.receipt_dir.mkdir(parents=True, exist_ok=True)
    if args.child:
        data = bytearray(b"\x5a") * (32 * 2**20)
        record(args.receipt_dir, "child-start", bytes=len(data))
        time.sleep(args.seconds)
        record(args.receipt_dir, "child-done", sha256=hashlib.sha256(data).hexdigest())
        return
    if (args.receipt_dir / "parent.json").exists():
        raise RuntimeError("existing probe receipt; choose a new directory")
    record(args.receipt_dir, "parent")
    subprocess.Popen([sys.executable, str(Path(__file__).resolve()),
                      str(args.receipt_dir), "--child", "--seconds", str(args.seconds)])


if __name__ == "__main__":
    main()
