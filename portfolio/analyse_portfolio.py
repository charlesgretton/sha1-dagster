#!/usr/bin/env python3
"""
analyse_portfolio.py RESULTS_DIR [RESULTS_DIR ...] [--metric wall_seconds|user_seconds]

Several directories (shards of one campaign, e.g. a PBS job array) are merged
after checking they share solver, generator, timeout and worker settings and
contain no duplicate run_id.

Reads manifest.jsonl + campaign.json written by portfolio_harness.sh.

Definitions
  success      status == SAT_VERIFIED only. Exit code 10 alone never counts.
  censored     TIMEOUT / TIMEOUT_KILLED: right-censored at the limit T.
               Never replaced by T in a mean.
  failure      any other non-success (errors, invalid models): no success,
               reported separately because it indicates a harness or solver
               problem rather than search difficulty.

With a single common limit T (type-I censoring), the Kaplan-Meier estimate
equals the empirical CDF for t < T, so F(t) below is both.

Portfolio forecast
  A k-worker seed race finishes at T_(1) = min(T_1..T_k). Resampling the n
  observed trajectories with replacement, the minimum has the EXACT law
      P(T_(1) > t) = (1 - F_hat(t))^k,
  so quantiles are computed directly, not by Monte Carlo. Outer bootstrap over
  the n observations gives a 95% interval on the median.

  Saturation: once k > ~0.69 n, the resampled minimum is usually the smallest
  time already observed. Those rows describe the sample, not the unobserved
  left tail, and are flagged.

Restart control
  Seeded restarts with fixed cutoff c <= T, assuming independent restarts:
      E[time to success] = E[min(T, c)] / F(c)
  Compared on equal CPU against one long run. This is a screening estimate.
"""
import json, math, os, random, sys
from collections import defaultdict

THRESH = [5, 10, 30, 100, 300]
KS = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000, 2000, 4000, 8000]
CENSOR = {"TIMEOUT", "TIMEOUT_KILLED"}


def wilson(x, n, z=1.96):
    if n == 0:
        return (0.0, 1.0)
    p = x / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, c - h), min(1.0, c + h))


def fmt_p(x, n):
    lo, hi = wilson(x, n)
    if x == 0:
        return "0/%d  (95%% upper ~%.3f)" % (n, min(1.0, 3.0 / n))
    return "%.3f [%.3f, %.3f]" % (x / n, lo, hi)


def min_quantile(times_sorted, n, k, q, T):
    """q-quantile of min of k draws (with replacement) from n obs whose
    successes are times_sorted (all <= T); non-successes never finish."""
    m = len(times_sorted)
    for j in range(1, m + 1):
        if 1.0 - (1.0 - j / n) ** k >= q:
            return times_sorted[j - 1]
    return None      # quantile lies beyond T


def analyse_target(rows, T, metric, rng):
    n = len(rows)
    succ = sorted(r[metric] for r in rows if r["status"] == "SAT_VERIFIED" and r[metric] is not None)
    cens = sum(1 for r in rows if r["status"] in CENSOR)
    fail = n - len(succ) - cens
    out = {"n": n, "successes": len(succ), "censored": cens, "failures": fail}
    out["F"] = {}
    for t in THRESH:
        if t <= T:
            x = sum(1 for s in succ if s <= t)
            out["F"][t] = (x, n)
    out["median"] = succ[(n + 1) // 2 - 1] if len(succ) >= (n + 1) // 2 else None
    out["rmst"] = (sum(succ) + (n - len(succ)) * T) / n     # restricted mean to T
    out["min_obs"] = succ[0] if succ else None

    port = []
    for k in KS:
        med = min_quantile(succ, n, k, 0.5, T)
        p90 = min_quantile(succ, n, k, 0.9, T)
        p99 = min_quantile(succ, n, k, 0.99, T)
        pT = 1 - (1 - len(succ) / n) ** k
        lo, hi = wilson(len(succ), n)
        # outer bootstrap on the median of the minimum
        meds = []
        allv = [(r[metric] if r["status"] == "SAT_VERIFIED" else None) for r in rows]
        for _ in range(400):
            bs = [allv[rng.randrange(n)] for _ in range(n)]
            bsucc = sorted(v for v in bs if v is not None)
            meds.append(min_quantile(bsucc, n, k, 0.5, T))
        finite = sorted(v for v in meds if v is not None)
        ci = None
        if len(finite) >= 0.975 * len(meds):
            ci = (finite[int(0.025 * len(meds))], finite[min(len(finite) - 1, int(0.975 * len(meds)))])
        port.append(dict(k=k, median=med, p90=p90, p99=p99, med_ci=ci,
                         p_by_T=(pT, 1 - (1 - lo) ** k, 1 - (1 - hi) ** k),
                         saturated=k > 0.69 * n))
    out["portfolio"] = port

    rest = []
    for c in [t for t in [1, 2, 5, 10, 30, 100, 300] if t <= T]:
        Fc = sum(1 for s in succ if s <= c) / n
        if Fc == 0:
            rest.append((c, None)); continue
        emin = (sum(min(s, c) for s in succ) + (n - len(succ)) * c) / n
        rest.append((c, emin / Fc))
    out["restart"] = rest
    return out


def show(v):
    return "  >T  " if v is None else "%7.2f" % v


def main():
    args = sys.argv[1:]
    metric = "wall_seconds"
    if "--metric" in args:
        i = args.index("--metric"); metric = args[i + 1]; del args[i:i + 2]
    dirs = args
    camps = [json.load(open(os.path.join(x, "campaign.json"))) for x in dirs]
    keys = ("solver_sha256", "generator_sha256", "solver_args_template",
            "timeout_sec", "workers", "kill_after_sec")
    for c in camps[1:]:
        for k in keys:
            if c.get(k) != camps[0].get(k):
                sys.exit("refusing to merge: shards differ in %s (%r vs %r)"
                         % (k, camps[0].get(k), c.get(k)))
    for x in dirs:
        if not os.path.exists(os.path.join(x, "completion.json")):
            print("!! %s has no completion.json: shard incomplete, using records present" % x)
    camp = camps[0]
    T = camp["timeout_sec"]
    rows, seen = [], set()
    for x in dirs:
        src = os.path.join(x, "manifest.jsonl")
        lines = open(src) if os.path.exists(src) else (
            open(os.path.join(x, "records", f)) for f in sorted(os.listdir(os.path.join(x, "records")))
            if f.endswith(".json"))
        for l in lines:
            l = l.strip() if isinstance(l, str) else l.read().strip()
            if not l:
                continue
            r = json.loads(l)
            if r["run_id"] in seen:
                sys.exit("duplicate run_id across shards: %s" % r["run_id"])
            seen.add(r["run_id"]); rows.append(r)
    d = dirs[0]
    rng = random.Random(12345)

    if len(dirs) > 1:
        print("merged %d shards: %s" % (len(dirs), ", ".join(os.path.basename(x) for x in dirs)))
    print("campaign %s | host %s | %d logical cpus | %d workers | T=%ss | metric=%s"
          % (camp["experiment_id"], camp["hostname"], camp["logical_cpus"],
             camp["workers"], T, metric))
    if camp["workers"] > camp["logical_cpus"]:
        print("!! workers exceed logical CPUs: wall times include contention")

    status = defaultdict(int)
    for r in rows:
        status[r["status"]] += 1
    print("status counts:", dict(status))
    for bad in ("UNSAT", "INVALID_MODEL", "SAT_NO_MODEL", "HARNESS_ERROR", "SOLVER_ERROR"):
        if status.get(bad):
            print("!! integrity: %d x %s" % (status[bad], bad))

    groups = defaultdict(list)
    for r in rows:
        groups[(r["rounds"], r["generator_seed"])].append(r)

    summary = {}
    by_round = defaultdict(list)
    for (rd, gs), rs in sorted(groups.items()):
        a = analyse_target(rs, T, metric, rng)
        summary["r%d_g%d" % (rd, gs)] = a
        by_round[rd].append((gs, a))
        print("\n=== round %d, target g%d: n=%d, verified=%d, censored=%d, failures=%d"
              % (rd, gs, a["n"], a["successes"], a["censored"], a["failures"]))
        for t, (x, n) in a["F"].items():
            print("   F(%3ds) = %s" % (t, fmt_p(x, n)))
        print("   median %s | RMST(<=T) %.2fs | fastest observed %s"
              % (show(a["median"]).strip(), a["rmst"], show(a["min_obs"]).strip()))
        print("   portfolio (exact resampled minimum):")
        print("       k   median   [95% CI]            p90      p99    P(success<=T) [95% CI]")
        for p in a["portfolio"]:
            ci = ("[%6.2f,%6.2f]" % p["med_ci"]) if p["med_ci"] else "[ undefined ]  "
            pt = p["p_by_T"]
            print("   %5d  %s   %s  %s  %s   %.3f [%.3f, %.3f]%s"
                  % (p["k"], show(p["median"]), ci, show(p["p90"]), show(p["p99"]),
                     pt[0], pt[1], pt[2], "   (saturated)" if p["saturated"] else ""))
        print("   fixed-cutoff restarts, one core, E[time to success]:")
        for c, e in a["restart"]:
            print("       cutoff %4ds -> %s" % (c, "never within data" if e is None else "%.2fs" % e))

    for rd, lst in sorted(by_round.items()):
        if len(lst) > 1:
            print("\n=== round %d across %d targets (target-level spread; not pooled)" % (rd, len(lst)))
            for gs, a in lst:
                x, n = a["F"][max(a["F"])]
                print("   g%-6d F(T)=%.2f  median %s  fastest %s"
                      % (gs, x / n, show(a["median"]).strip(), show(a["min_obs"]).strip()))

    with open(os.path.join(d, "analysis.json"), "w") as f:
        json.dump(summary, f, indent=1, default=str)
    print("\nwrote %s" % os.path.join(d, "analysis.json"))


if __name__ == "__main__":
    main()
