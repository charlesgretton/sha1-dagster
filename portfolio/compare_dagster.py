#!/usr/bin/env python3
"""
compare_dagster.py MANIFEST DAGSTER_CSV [--round 21] [--workers 63]

Places Dagster runs against a SAME-TARGET, SAME-SIZE seed-portfolio race.

 1. Per row: how many of the target's portfolio trajectories beat the Dagster
    time; the point probability a K-worker race beats it, 1-(1-F(t))^K; and a
    conservative version using the 95% Clopper-Pearson upper bound on F(t).
    (A bootstrap is uninformative when zero trajectories beat t, so it is not
    used: resampling a zero count returns zero.)
 2. Realised races: portfolio seeds 1..K as one actual K-worker race.
 3. Per-strategy tallies. Strategies are scored separately, because picking
    the best of several strategies per target after seeing the results is
    optimistic selection.
 4. Pooled binomial test at fixed thresholds (strategies on a target are
    correlated, so the effective sample size is closer to the target count).
"""
import csv, json, math, sys
from collections import defaultdict

a = sys.argv[1:]
R = int(a[a.index('--round') + 1]) if '--round' in a else 21
K = int(a[a.index('--workers') + 1]) if '--workers' in a else 63
M = [json.loads(l) for l in open(a[0]) if l.strip()]
D = [r for r in csv.DictReader(open(a[1]))]
T = max((m.get('wall_seconds') or 0) for m in M)          # informational only

def cp_upper(x, n, alpha=0.05):
    if x >= n: return 1.0
    lo, hi = x / n, 1.0
    for _ in range(60):
        mid = (lo + hi) / 2
        cdf = sum(math.comb(n, i) * mid**i * (1 - mid)**(n - i) for i in range(x + 1))
        lo, hi = (mid, hi) if cdf > alpha else (lo, mid)
    return hi

port = defaultdict(dict)
for m in M:
    if m['rounds'] != R: continue
    port[m['generator_seed']][m['solver_seed']] = (m['wall_seconds'] if m['status'] == 'SAT_VERIFIED' else math.inf)

rows = []
for r in D:
    if r['status'] != 'ok' or r['strategy'] == 'onenode': continue
    g = int(r['instance'].split('_g')[1]); t = float(r['wall_s'])
    ts = list(port[g].values()); n = len(ts); x = sum(1 for v in ts if v <= t)
    F, U = x / n, cp_upper(x, n)
    race63 = min(port[g][s] for s in range(1, K + 1) if s in port[g])
    rows.append(dict(g=g, strat=r['strategy'], t=t, x=x, n=n,
                     p=1 - (1 - F)**K, pu=1 - (1 - U)**K, race=race63))

print("1. PER ROW (round %d, %d-worker race)" % (R, K))
print("   target  strategy        dagster_s  beat_by  P(race<=t) point  [<= conservative]  realised_race")
for w in rows:
    band = ("beyond all %d observed" % w['n']) if w['x'] == 0 else ("lower 10%%" if w['p'] < 0.10 else ("within normal variation" if w['p'] < 0.9 else "portfolio usually faster"))
    rr = ">T" if w['race'] == math.inf else "%.2f" % w['race']
    print("   g%-6d %-14s %9.2f  %3d/%-3d     %.3f          [%.3f]        %7s   %s"
          % (w['g'], w['strat'], w['t'], w['x'], w['n'], w['p'], w['pu'], rr, band))

print("\n2./3. PER STRATEGY vs the realised %d-worker race (strategy fixed in advance)" % K)
by = defaultdict(list)
for w in rows: by[w['strat']].append(w)
for s, ws in sorted(by.items()):
    wins = [w for w in ws if w['t'] < w['race']]
    sp = sorted(w['race'] / w['t'] for w in wins if w['race'] != math.inf)
    print("   %-14s wins %d/%d   speedup where it won: %s" % (s, len(wins), len(ws),
          ("%.1f-%.1fx" % (sp[0], sp[-1])) if sp else "-"))
best = defaultdict(lambda: math.inf)
for w in rows: best[w['g']] = min(best[w['g']], w['t'])
bw = sum(1 for g, t in best.items() if t < min(port[g][s] for s in range(1, K + 1) if s in port[g]))
print("   (best-of-%d per target, SELECTION-BIASED: wins %d/%d)" % (len(by), bw, len(best)))

print("\n4. POOLED across targets")
succ = [v for g in port for v in port[g].values() if v != math.inf]
N = sum(len(port[g]) for g in port)
dts = [w['t'] for w in rows]
for t in (10, 20, 30, 60):
    x = sum(1 for v in succ if v <= t); F = x / N; U = cp_upper(x, N)
    q, qU = 1 - (1 - F)**K, 1 - (1 - U)**K
    k = sum(1 for v in dts if v <= t); nn = len(dts)
    tail = lambda qq: sum(math.comb(nn, i) * qq**i * (1 - qq)**(nn - i) for i in range(k, nn + 1))
    print("   t=%3ds portfolio %d/%d -> race P=%.2f [<=%.2f] | Dagster %d/%d <= t | p(null) %.1e [conservative %.3f]"
          % (t, x, N, q, qU, k, nn, tail(q), tail(qU)))
