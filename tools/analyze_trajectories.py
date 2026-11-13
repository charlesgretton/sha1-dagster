#!/usr/bin/env python3
"""
analyze_trajectories.py -- turn an Experiment 0 trajectory sample into a
portfolio go/no-go decision.

    python3 tools/analyze_trajectories.py trajectories.csv [--thresholds 10,30,100,300]

Input: the CSV written by scripts/sample_trajectories.sh, one row per
INDEPENDENT UNCENSORED trajectory (no first-solution cancellation -- a racing
harness censors every loser and destroys the distribution).

What it computes
----------------
1. F(t) = P(one trajectory solves within t), as the empirical CDF.
   All runs share one timeout, so censoring is administrative and identical,
   and the Kaplan-Meier estimator reduces exactly to the empirical CDF for
   t <= timeout.  (KM machinery is only needed if batches use different
   timeouts; this script refuses to pool those silently.)

2. Clopper-Pearson exact binomial confidence intervals on F(t).  Used rather
   than a bootstrap because F(t) is a binomial proportion and the interesting
   regime has very few events, where the bootstrap is unreliable.

3. Portfolio success probability for k independent workers:
       P(k, t) = 1 - (1 - F(t))^k
   with the CI propagated from the F(t) interval.  This is valid at any k and
   does not saturate.

4. min-of-k bootstrap, reported ONLY for k <= n/5.  Resampling the minimum of k
   draws from n observations saturates at the sample minimum once k >> n, so a
   "predicted 8000-core time" from 100 runs is an artefact, not a measurement.
   The script refuses to print it outside its valid range.

Statistical power note
----------------------
The 8000-core decision threshold is F(t) ~ 1e-4.  Distinguishing that from zero
needs on the order of 1e4 trajectories: with n=100 and zero successes the exact
95% upper bound is only about 0.03.  Design the run so that roughly 10-30% of
trajectories succeed -- that needs >= ~10 events to pin F(t) to within a factor
of about two.  The script reports achieved power so this cannot be overlooked.

Stdlib only.
"""

import argparse
import csv
import math
import random
import sys
from collections import defaultdict


# ----------------------------------------------------------------- statistics
def _log_binom(n, k):
    return math.lgamma(n + 1) - math.lgamma(k + 1) - math.lgamma(n - k + 1)


def _binom_cdf(k, n, p):
    """P(X <= k) for X ~ Bin(n, p)."""
    if p <= 0.0:
        return 1.0
    if p >= 1.0:
        return 1.0 if k >= n else 0.0
    total = 0.0
    for i in range(0, k + 1):
        total += math.exp(_log_binom(n, i) + i * math.log(p) + (n - i) * math.log1p(-p))
    return min(1.0, total)


def clopper_pearson(x, n, alpha=0.05):
    """Exact two-sided CI for a binomial proportion, by bisection."""
    if n == 0:
        return (0.0, 1.0)
    lo = 0.0
    if x > 0:
        a, b = 0.0, 1.0
        for _ in range(200):
            m = (a + b) / 2
            # upper tail P(X >= x) = 1 - P(X <= x-1)
            if 1.0 - _binom_cdf(x - 1, n, m) < alpha / 2:
                a = m
            else:
                b = m
        lo = a
    hi = 1.0
    if x < n:
        a, b = 0.0, 1.0
        for _ in range(200):
            m = (a + b) / 2
            if _binom_cdf(x, n, m) > alpha / 2:
                a = m
            else:
                b = m
        hi = a
    return (lo, hi)


def portfolio_prob(f, k):
    if f <= 0.0:
        return 0.0
    if f >= 1.0:
        return 1.0
    return 1.0 - math.exp(k * math.log1p(-f))


def min_of_k_bootstrap(times, k, trials=10000, rng=None):
    """Median and 95th percentile of min(k draws). Valid only while k << n."""
    rng = rng or random.Random(12345)
    n = len(times)
    out = []
    for _ in range(trials):
        m = min(times[rng.randrange(n)] for _ in range(k))
        out.append(m)
    out.sort()
    return out[len(out) // 2], out[int(0.95 * (len(out) - 1))]


# ----------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv")
    ap.add_argument("--thresholds", default="10,30,100,300")
    ap.add_argument("--cores", default="1,16,64,256,1024,8000")
    ap.add_argument("--alpha", type=float, default=0.05)
    a = ap.parse_args()

    rows = list(csv.DictReader(open(a.csv)))
    if not rows:
        raise SystemExit("no rows in %s" % a.csv)

    groups = defaultdict(list)
    for r in rows:
        key = (r.get("rounds", "?"), r.get("hash_bits", "?"), r.get("solver", "?"))
        groups[key].append(r)

    thresholds = [float(x) for x in a.thresholds.split(",")]
    cores = [int(x) for x in a.cores.split(",")]

    for key, rs in sorted(groups.items()):
        rounds, hb, solver = key
        timeouts = {float(r["timeout_s"]) for r in rs}
        if len(timeouts) > 1:
            print("!! r=%s hb=%s: mixed timeouts %s -- refusing to pool; "
                  "re-run with one timeout per group" % (rounds, hb, sorted(timeouts)))
            continue
        T = timeouts.pop()
        n = len(rs)

        solved = [float(r["wall_s"]) for r in rs if r["status"] == "SAT_VERIFIED"]
        bad = [r for r in rs if r["status"] in ("INVALID_MODEL", "ERROR")]
        unsat = [r for r in rs if r["status"] == "UNSAT"]
        solved.sort()

        print("=" * 72)
        print("rounds=%s  hash_bits=%s  solver=%s" % (rounds, hb, solver))
        print("trajectories=%d  timeout=%.0fs  solved=%d  censored=%d"
              % (n, T, len(solved), n - len(solved)))
        if unsat:
            print("!! %d trajectories returned UNSAT on an unrestricted instance "
                  "-- investigate before trusting anything below" % len(unsat))
        if bad:
            print("!! %d trajectories ERROR/INVALID_MODEL" % len(bad))

        if solved:
            q = lambda p: solved[min(len(solved) - 1, int(p * len(solved)))]
            print("solved-run quantiles (s): min=%.2f p10=%.2f p25=%.2f med=%.2f "
                  "p75=%.2f p90=%.2f max=%.2f"
                  % (solved[0], q(.10), q(.25), q(.50), q(.75), q(.90), solved[-1]))
        else:
            print("no trajectory solved -- only an upper bound on F(t) is available")

        print("\n  F(t) = P(single trajectory solves within t), exact %d%% CI"
              % round((1 - a.alpha) * 100))
        print("  %-8s %-7s %-12s %s" % ("t (s)", "events", "F(t)", "CI"))
        ft = {}
        for t in thresholds:
            if t > T:
                print("  %-8.0f  -- beyond timeout, not estimable" % t)
                continue
            x = sum(1 for s in solved if s <= t)
            f = x / n
            lo, hi = clopper_pearson(x, n, a.alpha)
            ft[t] = (f, lo, hi)
            print("  %-8.0f %-7d %-12.3g [%.3g, %.3g]" % (t, x, f, lo, hi))

        # achieved power
        best_t = max(ft) if ft else None
        if best_t is not None:
            x = sum(1 for s in solved if s <= best_t)
            print("\n  power: %d events at t=%.0fs." % (x, best_t), end=" ")
            if x == 0:
                print("Zero events -- you learn only F <= %.3g. To resolve "
                      "F~1e-4 you need ~1e4-1e5 trajectories, or a longer timeout."
                      % ft[best_t][2])
            elif x < 10:
                print("Fewer than 10 events: F(t) is uncertain to well over a "
                      "factor of 2. Increase n or the timeout.")
            else:
                print("Adequate for a factor-~2 estimate.")

        print("\n  portfolio success  P(k,t) = 1-(1-F(t))^k   [CI from F(t) CI]")
        header = "  %-8s" % "t (s)" + "".join("%-22s" % ("k=%d" % k) for k in cores)
        print(header)
        for t in sorted(ft):
            f, lo, hi = ft[t]
            line = "  %-8.0f" % t
            for k in cores:
                line += "%-22s" % ("%.3f [%.3f,%.3f]"
                                   % (portfolio_prob(f, k), portfolio_prob(lo, k),
                                      portfolio_prob(hi, k)))
            print(line)

        if solved:
            print("\n  min-of-k bootstrap (median, p95) -- only where k <= n/5 = %d"
                  % max(1, n // 5))
            shown = False
            for k in cores:
                if k <= max(1, n // 5):
                    med, p95 = min_of_k_bootstrap(solved, k)
                    print("    k=%-6d median=%8.2fs  p95=%8.2fs" % (k, med, p95))
                    shown = True
            if not shown:
                print("    (none: n=%d is too small for any requested k)" % n)
            print("    larger k omitted: min-of-k saturates at the sample minimum "
                  "(%.2fs) once k >> n, so it would report an artefact." % solved[0])
            print("    Use the P(k,t) table above for large k instead.")
        print()

    print("=" * 72)
    print("Decision rule (from the review's go/no-go criteria):")
    print("  Continue with the portfolio if P(k=8000, t) is high at an")
    print("  operationally useful t -- i.e. F(t) >= ~1e-4.")
    print("  If the CI on F(t) spans that threshold, the run was underpowered;")
    print("  increase trajectories or timeout before deciding.")


if __name__ == "__main__":
    main()
