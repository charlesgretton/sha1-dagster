#!/usr/bin/env python3
"""
portfolio_race.py -- one OBSERVED K-worker portfolio race (arm Br).

  portfolio_race.py --solver ENGINE --cnf REPAIRED.cnf --raw RAW.cnf --k 64 --seed-start S
                    --cap 1800 --out RESULT.json [--pin] [--workdir DIR]

Launches K seeded trajectories of the full formula at once (seeds S..S+K-1, MiniSat -rnd-init),
optionally pinned one per core (taskset -c 0..K-1). The race ends at the first trajectory whose
model VERIFIES (tools/verify_preimage.py against the raw sha1gen CNF); every other trajectory is
then terminated. Only this script's own child processes are ever signalled.

This is the matched counterpart of a Dagster arm-C run: same cores, same cap, one observed
first-verified-preimage time per race. The census arm B (portfolio harness) remains useful but
gives a MODEL-DERIVED race estimate (order statistic of one trajectory census), not observed races.
"""
import os, sys, json, time, signal, shutil, argparse, subprocess

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--solver", required=True); ap.add_argument("--cnf", required=True)
    ap.add_argument("--raw", required=True); ap.add_argument("--k", type=int, required=True)
    ap.add_argument("--seed-start", type=int, required=True); ap.add_argument("--cap", type=float, required=True)
    ap.add_argument("--out", required=True); ap.add_argument("--pin", action="store_true")
    ap.add_argument("--workdir", default=None)
    a = ap.parse_args()
    wd = a.workdir or os.path.splitext(a.out)[0] + ".race"
    os.makedirs(wd, exist_ok=True)
    taskset = shutil.which("taskset") if a.pin else None
    procs = {}
    t0 = time.time()
    for i in range(a.k):
        seed = a.seed_start + i
        model = os.path.join(wd, "seed%d.model" % seed)
        cmd = [a.solver, "-rnd-init", "-rnd-seed=%d" % seed, a.cnf, model]
        if taskset:
            cmd = [taskset, "-c", str(i)] + cmd
        procs[seed] = (subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL), model)
    res = {"status": "UNSOLVED", "k": a.k, "seed_block": [a.seed_start, a.seed_start + a.k - 1],
           "cap": a.cap, "pinned": bool(taskset), "invalid": []}

    def stop_all():
        for p, _ in procs.values():
            if p.poll() is None:
                p.terminate()
        deadline = time.time() + 3
        for p, _ in procs.values():
            try:
                p.wait(timeout=max(0.0, deadline - time.time()))
            except subprocess.TimeoutExpired:
                p.kill()

    def on_term(signum, frame):
        res["status"] = "TERMINATED"; stop_all(); res["t_end"] = time.time() - t0
        json.dump(res, open(a.out, "w"), indent=1); sys.exit(0)
    signal.signal(signal.SIGTERM, on_term)

    done = set()
    while time.time() - t0 < a.cap:
        for seed, (p, model) in procs.items():
            if seed in done or p.poll() is None:
                continue
            done.add(seed)
            if p.returncode == 10 and os.path.exists(model):
                v = subprocess.run([sys.executable, os.path.join(HERE, "verify_preimage.py"), a.raw, model],
                                   capture_output=True, text=True)
                if v.returncode == 0 and "VALID PREIMAGE" in v.stdout:
                    res.update({"status": "SOLVED", "t_wall": time.time() - t0, "seed": seed, "model": model})
                    stop_all(); res["t_end"] = time.time() - t0
                    json.dump(res, open(a.out, "w"), indent=1); return
                res["invalid"].append(seed)
        if len(done) == a.k:
            break
        time.sleep(0.05)
    stop_all()
    res["t_end"] = time.time() - t0
    json.dump(res, open(a.out, "w"), indent=1)


if __name__ == "__main__":
    main()
