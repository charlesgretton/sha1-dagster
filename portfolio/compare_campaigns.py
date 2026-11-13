#!/usr/bin/env python3
"""
compare_campaigns.py LABEL=MANIFEST [LABEL=MANIFEST ...] [--workers 63]

Side-by-side portfolio comparison of several campaigns on the same targets,
e.g. TiniSAT vs Kissat vs CaDiCaL. Per round and target: verified solve rate
at 30/60/120/300 s, and the REALISED K-worker race (seeds 1..K, no resampling).

Only meaningful when the campaigns ran under matched execution context: same
machine, same worker count and pinning, same timeout. Identical deterministic
work was measured to vary ~2x in wall time with context on the campaign workstation.
"""
import json, math, sys
from collections import defaultdict
a = sys.argv[1:]
K = int(a[a.index('--workers') + 1]) if '--workers' in a else 63
camps = [x.split('=', 1) for x in a if '=' in x and not x.startswith('--')]
data = {}
for label, path in camps:
    d = defaultdict(dict)
    for l in open(path):
        if not l.strip(): continue
        m = json.loads(l)
        d[(m['rounds'], m['generator_seed'])][m['solver_seed']] = (m['wall_seconds'] if m['status'] == 'SAT_VERIFIED' else math.inf)
    data[label] = d
keys = sorted(set().union(*[set(d) for d in data.values()]))
TS = (30, 60, 120, 300)
f = lambda v: ">T" if v == math.inf else "%.1f" % v
for r in sorted({k[0] for k in keys}):
    print("\n=== round %d   (solve rate by t | realised %d-worker race)" % (r, K))
    print("target   campaign    " + "  ".join("F(%3ds)" % t for t in TS) + "   race")
    for k in [k for k in keys if k[0] == r]:
        for label in data:
            runs = data[label].get(k)
            if not runs: continue
            v = list(runs.values()); n = len(v)
            race = min(runs[s] for s in runs if s <= K) if any(s <= K for s in runs) else math.inf
            print("g%-6d %-10s  " % (k[1], label) + "  ".join("%5.1f%% " % (100 * sum(1 for x in v if x <= t) / n) for t in TS) + "  %s  (n=%d)" % (f(race), n))
    for label in data:
        v = [x for k in keys if k[0] == r for x in data[label].get(k, {}).values()]
        if v: print("pooled   %-10s  " % label + "  ".join("%5.1f%% " % (100 * sum(1 for x in v if x <= t) / len(v)) for t in TS) + "        (n=%d)" % len(v))
