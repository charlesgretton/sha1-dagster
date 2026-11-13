#!/usr/bin/env python3
"""
runner.py -- drive the SHA-1 preimage decomposition experiment under Dagster.

For every (instance, strategy) pair it repairs the DIMACS header, emits a DAG
file, runs Dagster under mpirun with a wall-clock timer, verifies the reported
solution is a genuine reduced-round SHA-1 preimage, and appends a CSV row.

Stdlib only -- no pysat needed here (that is only for tools/calib.py).

    python3 scripts/runner.py --dagster /path/to/dagster \
        --instances instances --cores 48 --bits 7 --out results.csv

Strategies compared
-------------------
  onenode        no decomposition (baseline, == utilities/onenodedag.sh)
  single         one backward-frontier splitter on a[r-1]
  naive-message  one splitter on independent message bits w[0]
  dual-frontier  forward a[5] + backward a[r-1] splitters joined at a terminal
                 node holding the full formula (historical; not recommended by default)

Dagster flags used
------------------
  -e 0   stop at the first solution. The default is 1 (enumerate ALL
         solutions); a preimage instance has on the order of 2^352 solutions,
         so the default never terminates.
  -b 0   depth-first node allocation (the default) so workers push toward the
         terminal node rather than breadth-filling the frontier.
"""

import argparse
import csv
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
TOOLS = os.path.join(ROOT, "tools")
sys.path.insert(0, TOOLS)

STRATEGIES = ["onenode", "single", "naive-message", "dual-frontier"]


def run(cmd, timeout=None, **kw):
    t0 = time.time()
    try:
        p = subprocess.run(cmd, capture_output=True, text=True,
                           timeout=timeout, **kw)
        return p.returncode, p.stdout, p.stderr, time.time() - t0, False
    except subprocess.TimeoutExpired:
        return None, "", "", time.time() - t0, True


def prepare(cnf, outdir, strategy, bits):
    """Repair header + emit DAG. Returns (cnf_path, dag_path, info)."""
    base = os.path.basename(cnf)[:-4]
    out_cnf = os.path.join(outdir, base + ".cnf")
    out_dag = os.path.join(outdir, "%s.%s.dag" % (base, strategy))
    rc, out, err, _, _ = run([sys.executable,
                              os.path.join(TOOLS, "sha1_dagify.py"), cnf,
                              "--out-cnf", out_cnf, "--out-dag", out_dag,
                              "--strategy", strategy, "--bits", str(bits)])
    if rc != 0:
        raise SystemExit("sha1_dagify failed on %s/%s:\n%s%s"
                         % (cnf, strategy, out, err))
    info = {}
    for line in out.splitlines():
        if "job count" in line:
            info["jobs"] = line.split("up to")[1].split()[0]
        if line.startswith("validation"):
            info["validation"] = line.split(":", 1)[1].strip()
        if "VALIDATION ERRORS" in line:
            info["validation"] = "ERRORS"
    return out_cnf, out_dag, info


def verify(orig_cnf, sol_path):
    if not os.path.exists(sol_path) or os.path.getsize(sol_path) == 0:
        return "no-solution"
    rc, out, err, _, _ = run([sys.executable,
                              os.path.join(TOOLS, "verify_preimage.py"),
                              orig_cnf, sol_path])
    if rc == 0 and "VALID PREIMAGE" in out:
        return "VALID"
    return "INVALID"


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dagster", required=True, help="path to the dagster binary")
    ap.add_argument("--instances", default="instances",
                    help="directory of generated .cnf files")
    ap.add_argument("--cores", type=int, default=16,
                    help="total MPI ranks; one is the master")
    ap.add_argument("--bits", type=int, default=7,
                    help="branch bits per splitter (dual-frontier jobs = 2^(2*bits))")
    ap.add_argument("--timeout", type=int, default=600, help="seconds per run")
    ap.add_argument("--strategies", default=",".join(STRATEGIES))
    ap.add_argument("--workdir", default="work")
    ap.add_argument("--out", default="results.csv")
    ap.add_argument("--mpirun", default="mpirun")
    ap.add_argument("--oversubscribe", action="store_true",
                    help="pass --oversubscribe (needed when ranks > physical cores)")
    ap.add_argument("--mpirun-args", default="",
                    help="extra args passed through to mpirun, space separated "
                         "(e.g. --allow-run-as-root, --bind-to none, --map-by node)")
    a = ap.parse_args()

    if not os.path.exists(a.dagster):
        raise SystemExit("dagster binary not found: %s" % a.dagster)
    os.makedirs(a.workdir, exist_ok=True)

    cnfs = sorted(os.path.join(a.instances, f)
                  for f in os.listdir(a.instances) if f.endswith(".cnf"))
    if not cnfs:
        raise SystemExit("no .cnf files in %s -- run scripts/generate.sh first"
                         % a.instances)

    strategies = [s.strip() for s in a.strategies.split(",") if s.strip()]
    fields = ["instance", "rounds", "strategy", "bits", "cores", "nodes",
              "jobs", "validation", "status", "wall_s", "solution"]

    with open(a.out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, lineterminator="\n")
        w.writeheader()

        for cnf in cnfs:
            base = os.path.basename(cnf)[:-4]
            rounds = ""
            with open(cnf) as f:
                for line in f:
                    if "nr_rounds" in line:
                        rounds = line.strip().split("=")[-1].strip()
                        break
                    if not line.startswith("c") and not line.startswith("p"):
                        break

            for strat in strategies:
                out_cnf, out_dag, info = prepare(cnf, a.workdir, strat, a.bits)
                sol = os.path.join(a.workdir, "%s.%s.sol" % (base, strat))
                if os.path.exists(sol):
                    os.remove(sol)

                cmd = [a.mpirun]
                if a.oversubscribe:
                    cmd.append("--oversubscribe")
                if a.mpirun_args:
                    cmd += a.mpirun_args.split()
                cmd += ["-n", str(a.cores), a.dagster,
                        "-e", "0", "-o", sol, out_dag, out_cnf]

                rc, out, err, wall, timed_out = run(cmd, timeout=a.timeout)
                if timed_out:
                    status, ver = "TIMEOUT", "-"
                elif rc != 0:
                    status, ver = "ERROR(rc=%s)" % rc, "-"
                    sys.stderr.write(err[-800:] + "\n")
                else:
                    status = "ok"
                    ver = verify(cnf, sol)

                nodes = 3 if strat == "dual-frontier" else (1 if strat == "onenode" else 2)
                row = dict(instance=base, rounds=rounds, strategy=strat,
                           bits=(a.bits if strat != "onenode" else 0),
                           cores=a.cores, nodes=nodes,
                           jobs=info.get("jobs", "1"),
                           validation=info.get("validation", "?"),
                           status=status, wall_s=round(wall, 2),
                           solution=ver)
                w.writerow(row)
                fh.flush()
                print("%-22s %-14s cores=%-4d jobs=%-8s %-12s %8.2fs  %s"
                      % (base, strat, a.cores, row["jobs"], status, wall, ver),
                      flush=True)

    print("\nwrote %s" % a.out)


if __name__ == "__main__":
    main()
