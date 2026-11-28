"""Run a browser script under hard limits, so a hung engine cannot eat the Mac.

Usage:
    python guard.py script.py [args...]        run script.py with the limits below
    python guard.py /abs/path/binary [args…]  run a probe binary with the same limits
    from guard import watch                   watch(cmd) -> dict with the run's result

The limits cover the whole process tree: a script that wraps a binary is measured and killed together with it.

Limits (environment):
    BO_TIMEOUT       seconds per run before the process tree is killed   (default 20)
    BO_MEM_MB        RSS ceiling in MB before the process tree is killed (default 600)
    BO_SAMPLE        optional file path: when a run is still alive after BO_SAMPLE_AFTER seconds,
                     its stack is written there with the macOS `sample` tool
    BO_SAMPLE_AFTER                                                              (default 8)

The engine footprint measured so far is 110-195 MB, so the ceiling leaves about 3x headroom.
"""
import os
import subprocess
import sys
import threading
import time

DEFAULT_TIMEOUT = float(os.environ.get("BO_TIMEOUT", 20))
DEFAULT_MEM_MB = float(os.environ.get("BO_MEM_MB", 600))
DEFAULT_SAMPLE_AFTER = float(os.environ.get("BO_SAMPLE_AFTER", 8))
POLL = 0.02  # the engine can allocate GBs per second, so poll fast


def _tree_mb(root_pid):
    """RSS in MB of a process plus every descendant (a script may wrap a binary)."""
    try:
        out = subprocess.run(["ps", "-axo", "pid=,ppid=,rss="],
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                             text=True).stdout
    except Exception:
        return 0, []
    rows = [line.split() for line in out.splitlines() if len(line.split()) == 3]
    children, rss = {}, {}
    for pid, ppid, kb in rows:
        pid, ppid = int(pid), int(ppid)
        children.setdefault(ppid, []).append(pid)
        rss[pid] = int(kb)
    seen, stack = [], [root_pid]
    while stack:
        pid = stack.pop()
        if pid in seen:
            continue
        seen.append(pid)
        stack.extend(children.get(pid, []))
    return sum(rss.get(pid, 0) for pid in seen) // 1024, seen


def _kill_tree(pids):
    for pid in pids:
        try:
            os.kill(pid, 9)
        except Exception:
            pass


def watch(cmd, timeout=DEFAULT_TIMEOUT, mem_mb=DEFAULT_MEM_MB,
          sample_path=None, sample_after=DEFAULT_SAMPLE_AFTER, stream=False):
    """Run cmd and kill it when it passes the time limit or the memory ceiling.

    Returns {code, reason, peak_mb, stdout, stderr}. reason is None, "time limit" or "memory ceiling".
    """
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                            errors="replace", bufsize=1)
    # Drain both pipes continuously: a chatty child (oxide logs a lot to stderr) would otherwise
    # fill the 64 KB pipe buffer and block forever. stream=True forwards lines live instead of
    # collecting them, so long runs can be followed through a pipe.
    bufs = {"out": [], "err": []}
    def _pump(src, key, sink):
        for line in src:
            if stream:
                sink.write(line); sink.flush()
            else:
                bufs[key].append(line)
    readers = [threading.Thread(target=_pump, args=(proc.stdout, "out", sys.stdout), daemon=True),
               threading.Thread(target=_pump, args=(proc.stderr, "err", sys.stderr), daemon=True)]
    for r in readers:
        r.start()
    t0 = time.time()
    peak = 0
    reason = None
    sampled = False
    while proc.poll() is None:
        elapsed = time.time() - t0
        used, tree = _tree_mb(proc.pid)
        peak = max(peak, used)
        if elapsed > timeout:
            reason = "time limit"
        elif mem_mb and peak > mem_mb:
            reason = "memory ceiling"
        if reason is None and sample_path and not sampled and elapsed >= sample_after:
            sampled = True
            try:
                subprocess.run(["sample", str(proc.pid), "2", "-file", sample_path],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
            except Exception:
                pass
        if reason:
            _kill_tree(tree)
            try:
                proc.kill()
                proc.wait(timeout=5)
            except Exception:
                pass
            break
        time.sleep(POLL)
    try:
        proc.wait(timeout=5)
    except Exception:
        pass
    for r in readers:
        r.join(timeout=5)
    out, err = "".join(bufs["out"]), "".join(bufs["err"])
    return {"code": proc.returncode, "reason": reason, "peak_mb": peak, "stdout": out, "stderr": err}


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    first = sys.argv[1]
    # a python script runs under this interpreter; anything else (a probe binary) runs as is
    python_script = first.endswith(".py")
    cmd = sys.argv[1:] if not python_script else [sys.executable] + sys.argv[1:]
    res = watch(cmd, sample_path=os.environ.get("BO_SAMPLE") or None, stream=True)
    sys.stdout.write(res["stdout"])
    sys.stderr.write(res["stderr"])
    note = f"guard: peak {res['peak_mb']} MB"
    if res["reason"]:
        note += f"; KILLED ({res['reason']})"
    print(note, file=sys.stderr, flush=True)
    sys.exit(res["code"] if res["code"] is not None else 1)
