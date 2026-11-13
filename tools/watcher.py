#!/usr/bin/env python3
"""
watcher.py -- watch a running Dagster job, end it on EVIDENCE, and record what happened.

  watcher.py --logs DIR --components COMPONENTS.json --raw RAW.cnf --sid SESSION_ID
             --t0 EPOCH --out RESULT.json [--stall 60] [--kill-stall 0] [--poll 0.25]
             [--no-kill]

Why. With -e 0, Dagster sets its terminate trigger on the first terminal solution but
exits only after EVERY worker has polled, and a mode-4 worker cannot poll mid-solve.
On the campaign workstation a race found its solution at 1.6 s and exited at 205.8 s. Dagster also writes
-o only at exit. So the harness must not wait for Dagster's shutdown.

What it does, reading the per-rank V4 glog files incrementally:
  * On the first terminal-node emission ("Solution message mapped to: message about
    node T", T a terminal), it writes the logged literals as a model, VERIFIES it with
    tools/verify_preimage.py, and only if VALID records the time and ends the job
    (TERM to every process in the job's session, then KILL). The logged literals equal
    Dagster's own -o output on the reporting variables (checked on real runs), so
    nothing is lost by not waiting. Invalid solutions are recorded and never trigger a
    kill; the runsheet treats them as fatal.
  * Flags a STALL: no terminal dispatch for --stall seconds while sources keep emitting
    (the refinement lock / sticky-message starvation). With --kill-stall 1 it ends the
    run as censored.
  * Records REPLICAS: the largest number of workers dispatched the identical terminal
    message (identical deterministic searches: the workstation race had 16 on one message).
  * Ends by itself when the job's session has no processes left.

Times: t_wall is seconds since --t0 (the runner's launch time); t_log is seconds since
the first glog line of the job (Dagster's own clock). Both are recorded.
"""
import os, re, sys, glob, json, time, signal, subprocess, argparse
from datetime import datetime
from collections import defaultdict

LINE = re.compile(r'^[IWEF]\d{4} (\d\d:\d\d:\d\d\.\d+)\s+\d+\s+\S+\] (.*)$')
EMIT = re.compile(r'WORKER (\d+): Solution message mapped to: message about node (-?\d+) assignments:([-\d ]*)')
DISP = re.compile(r'sending message to worker (\d+) message about node (-?\d+) assignments:([-\d ]*)')
DONE = re.compile(r'received MPI_TAG_(REQUEST_FOR_ASSIGNMENT|SOLUTION) from worker (\d+)')
TOOLS = os.path.dirname(os.path.abspath(__file__))


def session_pids(sid):
    pids = []
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        try:
            with open("/proc/%s/stat" % d) as f:
                st = f.read()
            fields = st[st.rindex(")") + 2:].split()
            if int(fields[3]) == sid:            # field 6 of stat: session id
                pids.append(int(d))
        except (OSError, ValueError, IndexError):
            pass
    return [p for p in pids if p != os.getpid()]


def kill_session(sid, grace=3.0):
    for sig in (signal.SIGTERM, signal.SIGKILL):
        for p in session_pids(sid):
            try:
                os.kill(p, sig)
            except OSError:
                pass
        deadline = time.time() + grace
        while time.time() < deadline and session_pids(sid):
            time.sleep(0.1)
        if not session_pids(sid):
            return True
    return not session_pids(sid)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--logs", required=True); ap.add_argument("--components", required=True)
    ap.add_argument("--raw", required=True); ap.add_argument("--sid", type=int, required=True)
    ap.add_argument("--t0", type=float, required=True); ap.add_argument("--out", required=True)
    ap.add_argument("--stall", type=float, default=60.0); ap.add_argument("--kill-stall", type=int, default=0)
    ap.add_argument("--poll", type=float, default=0.25); ap.add_argument("--no-kill", action="store_true")
    a = ap.parse_args()

    C = json.load(open(a.components))
    role = {int(k): v for k, v in C["node_roles"].items()}
    cname = {c["id"]: c["name"] for c in C["components"]}
    ncomp = {int(k): v for k, v in C["node_component"].items()}
    terminals = {n for n, r in role.items() if r.endswith("TERMINAL")}
    comp_of = lambda n: cname.get(ncomp.get(n))

    offsets, first_log = {}, None
    last_term_dispatch, last_src_emit = None, None
    on_terminal = {}          # worker -> terminal node it was dispatched, until it reports back
    replicas = defaultdict(set)
    res = {"status": "UNSOLVED", "t0": a.t0, "stall_flagged_at_wall": None,
           "invalid_solutions": [], "terminal_emissions_seen": 0}
    ending = False
    saw_log = False                       # becomes true at the first parsed glog line
    t_start = time.time()
    GRACE = 15.0                          # don't declare the job gone before logs appear
    model_dir = os.path.splitext(a.out)[0] + ".models"
    os.makedirs(model_dir, exist_ok=True)

    def finish(status=None):
        if status:
            res["status"] = status
        res["replicas_max_workers_on_one_terminal_message"] = max((len(v) for v in replicas.values()), default=0)
        res["t_end_wall"] = time.time() - a.t0
        json.dump(res, open(a.out, "w"), indent=1)

    def on_term(signum, frame):
        finish("WATCHER_TERMINATED"); sys.exit(0)
    signal.signal(signal.SIGTERM, on_term)

    while True:
        for path in glob.glob(os.path.join(a.logs, "*")):
            if os.path.isdir(path) or os.path.islink(path):   # glog symlink would double-read a rank
                continue
            try:
                with open(path, "rb") as f:            # byte offsets: exact and restartable
                    f.seek(offsets.get(path, 0))
                    chunk = f.read()
                cut = chunk.rfind(b"\n") + 1           # consume complete lines only
                offsets[path] = offsets.get(path, 0) + cut
                lines = chunk[:cut].decode("utf-8", "replace").splitlines()
            except OSError:
                continue
            for line in lines:
                m = LINE.match(line)
                if not m:
                    continue
                ts = datetime.strptime(m.group(1), "%H:%M:%S.%f")
                if first_log is None or ts < first_log:
                    first_log = ts
                saw_log = True
                body = m.group(2)
                d = DISP.search(body)
                if d:
                    w, node = int(d.group(1)), int(d.group(2))
                    if node in terminals:
                        last_term_dispatch = time.time()
                        replicas[(node, d.group(3).strip())].add(w)
                        on_terminal[w] = node
                    else:
                        on_terminal.pop(w, None)
                    continue
                dn = DONE.search(body)
                if dn:
                    on_terminal.pop(int(dn.group(2)), None)   # finished (UNSAT) or reported a solution
                    continue
                e = EMIT.search(body)
                if not e:
                    continue
                node = int(e.group(2))
                if node not in terminals:
                    last_src_emit = time.time()
                    continue
                res["terminal_emissions_seen"] += 1
                if res["status"] == "SOLVED":
                    continue
                toks = e.group(3).split()
                if not toks or not all(re.fullmatch(r'-?\d+', t) for t in toks):
                    continue
                mp = os.path.join(model_dir, "node%d_%d.model" % (node, res["terminal_emissions_seen"]))
                open(mp, "w").write(" ".join(toks) + " \n")
                v = subprocess.run([sys.executable, os.path.join(TOOLS, "verify_preimage.py"), a.raw, mp],
                                   capture_output=True, text=True)
                ok = v.returncode == 0 and "VALID PREIMAGE" in v.stdout
                if not ok:
                    res["invalid_solutions"].append({"node": node, "model": mp})
                    continue
                res.update({"status": "SOLVED", "node": node, "component": comp_of(node),
                            "model": mp, "t_wall": time.time() - a.t0,
                            "t_log": (ts - first_log).total_seconds() if first_log else None})
                if not a.no_kill:
                    res["killed_after_solution"] = kill_session(a.sid)
                    finish(); return
        # stall: sources active, no terminal dispatch for too long
        now = time.time()
        ref = last_term_dispatch or a.t0
        res["terminal_jobs_in_flight"] = len(on_terminal)
        if res["stall_flagged_at_wall"] is None and last_src_emit and last_src_emit > ref \
                and now - ref > a.stall and not on_terminal:
            res["stall_flagged_at_wall"] = now - a.t0
            if a.kill_stall and not a.no_kill:
                res["killed_on_stall"] = kill_session(a.sid)
                finish("STALLED"); return
        if ending:
            finish(); return                    # this pass was the final read after exit
        if not session_pids(a.sid):
            if not saw_log and time.time() - t_start < GRACE:
                time.sleep(a.poll); continue    # job not visible yet; wait for logs to appear
            ending = True                       # one more full pass to read the last lines
            continue
        time.sleep(a.poll)


if __name__ == "__main__":
    main()
