#!/usr/bin/env python3
"""
config_differs.py MANIFEST_A MANIFEST_B [--workers 63] [--bits 7] [--need 3]

Implements the cube-census step 3b's pre-registered trigger. For each target present in
BOTH complete censuses: the target DIFFERS if F(cutoff) (solved/C) or the exact
random-first-batch campaign median differs by more than 2x between A and B.
Censoring: both medians beyond the cutoff -> not a difference; one beyond the
cutoff and the other at or below cutoff/2 -> a difference (ratio certainly > 2);
otherwise inconclusive -> not counted. F: both 0 -> equal; one 0, other > 0 ->
a difference (ratio unbounded; the event counts are printed so you can judge).

exit 0 = rule met (run 3b), 1 = not met, 2 = cannot evaluate.
"""
import math, sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from census_lib import load, campaign
a = sys.argv[1:]
K = int(a[a.index('--workers') + 1]) if '--workers' in a else 63
bits = int(a[a.index('--bits') + 1]) if '--bits' in a else 7
need = int(a[a.index('--need') + 1]) if '--need' in a else 3
T = float(a[a.index('--cutoff') + 1]) if '--cutoff' in a else 300.0
C = 1 << bits
A, pa = load(a[0], bits); B, pb = load(a[1], bits)
if pa or pb: print("cannot evaluate: census problems:", pa + pb); sys.exit(2)
allk = sorted(set(A) | set(B))
bad = [k for k in allk if len(A.get(k, {})) != C or len(B.get(k, {})) != C]
if bad:
    print("cannot evaluate: census incomplete or unpaired for %s" % ", ".join("r%d g%d" % k for k in bad)); sys.exit(2)
keys = allk
def ratio_diff(x, y):
    if x == 0 and y == 0: return False
    if x == 0 or y == 0: return True
    return max(x, y) / min(x, y) > 2
def med_diff(x, y):
    if x is None and y is None: return False
    if x is None: return y <= T / 2
    if y is None: return x <= T / 2
    return max(x, y) / min(x, y) > 2
n_diff = 0
print("target   A solved  B solved  A median  B median   differs")
for k in keys:
    ta = [A[k][i] for i in range(C)]; tb = [B[k][i] for i in range(C)]
    ca, cb = campaign(ta, K), campaign(tb, K)
    d = ratio_diff(ca['solved'], cb['solved']) or med_diff(ca['median'], cb['median'])
    n_diff += d
    fm = lambda v: ">T" if v is None else "%.1f" % v
    print("r%d g%d   %4d/%d   %4d/%d   %7s   %7s    %s" % (k[0], k[1], ca['solved'], C, cb['solved'], C, fm(ca['median']), fm(cb['median']), "YES" if d else "no"))
met = n_diff >= need
print("rule: >2x difference on >= %d targets -> %d of %d differ -> %s" % (need, n_diff, len(keys), "RUN 3b" if met else "skip 3b"))
sys.exit(0 if met else 1)
