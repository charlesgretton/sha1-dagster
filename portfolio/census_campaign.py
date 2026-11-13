#!/usr/bin/env python3
"""
census_campaign.py CUBE_MANIFEST [--workers 63] [--bits 7]

Exact K-worker first-solution campaign quantities from a complete cube census
(see census_lib.py): index order, best and worst first batches, and the EXACT
distribution over uniformly random first batches (finite-population formula,
no simulation). Requires all 2^bits cubes per target; an incomplete census is
reported and skipped, never silently analysed.

Also reports MARGINAL cube-bit effects (Fisher exact per bit, Bonferroni).
These test only P(solved | bit j = 0) vs P(solved | bit j = 1). They do not
detect interactions (e.g. b0 XOR b1), clustering, or runtime correlations, and
say nothing about statistical independence between concurrent jobs.
"""
import math, sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from census_lib import load, campaign

a = sys.argv[1:]
K = int(a[a.index('--workers') + 1]) if '--workers' in a else 63
bits = int(a[a.index('--bits') + 1]) if '--bits' in a else 7
C = 1 << bits
by, problems = load(a[0], bits)
for p in problems: print("!! " + p)
f = lambda v: ">T" if v is None or v == math.inf else "%.2f" % v
print("round target  cubes solved | K=%d EXACT: index-order  best   worst | uniformly random first batch: median [p10, p90]  P(solve<=T)" % K)
complete = {}
for (r, g), cubes in sorted(by.items()):
    if len(cubes) < C:
        print("r%-4d g%-6d %3d  INCOMPLETE census (%d of %d cubes) -- skipped" % (r, g, len(cubes), len(cubes), C)); continue
    ts = [cubes[i] for i in range(C)]; complete[(r, g)] = ts
    c = campaign(ts, K)
    print("r%-4d g%-6d %3d  %4d   |     %8s  %6s  %6s |          %8s [%s, %s]      %.4f"
          % (r, g, c['cubes'], c['solved'], f(c['index_order']), f(c['best']), f(c['worst']),
             f(c['median']), f(c['p10']), f(c['p90']), c['p_success']))

def fisher(a_, b, c, d):
    n1, n2, k, n = a_ + b, c + d, a_ + c, a_ + b + c + d
    hp = lambda x: math.comb(n1, x) * math.comb(n2, k - x) / math.comb(n, k)
    p0 = hp(a_); lo, hi = max(0, k - n2), min(k, n1)
    return min(1.0, sum(hp(x) for x in range(lo, hi + 1) if hp(x) <= p0 * (1 + 1e-9)))
ntests = len(complete) * bits
print("\nMARGINAL CUBE-BIT EFFECTS on solved-within-cutoff (Fisher exact, Bonferroni over %d tests)" % ntests)
flagged = 0
for (r, g), ts in sorted(complete.items()):
    row = []
    for j in range(bits):
        s1 = sum(1 for i in range(C) if (i >> j) & 1 and ts[i] != math.inf)
        s0 = sum(1 for i in range(C) if not (i >> j) & 1 and ts[i] != math.inf)
        n = C // 2; padj = min(1.0, fisher(s1, n - s1, s0, n - s0) * max(1, ntests))
        flagged += padj < 0.05
        row.append("b%d:%d/%d|%d/%d%s" % (j, s0, n, s1, n, "*" if padj < 0.05 else ""))
    print("  r%d g%d  %s" % (r, g, "  ".join(row)))
print("  -> %d significant marginal bit effects (* marked). This tests main effects only;" % flagged)
print("     it does not establish or refute independence between cells.")
