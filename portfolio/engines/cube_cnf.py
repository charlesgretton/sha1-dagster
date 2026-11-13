#!/usr/bin/env python3
"""cube_cnf.py IN OUT CUBE_INDEX BITS VARS PERMUTE_SEED
Append a cube as unit clauses AFTER the formula (Dagster terminal-job layout,
verified against CNFs dumped from Dagster workers): bit j of CUBE_INDEX sets VARS[j].
VARS is "1-7" style or comma list. PERMUTE_SEED=0 keeps clause order (as a
Dagster worker sees it); >0 applies a seeded clause-order permutation.
Units only ADD constraints, so any model verifies against the original CNF."""
import random, sys
src, dst, idx, bits, vs, pseed = sys.argv[1:7]
idx, bits, pseed = int(idx), int(bits), int(pseed)
vars_ = []
for part in vs.split(","):
    if "-" in part:
        a, b = map(int, part.split("-")); vars_ += list(range(a, b + 1))
    else:
        vars_.append(int(part))
vars_ = vars_[:bits]
assert len(vars_) == bits, "need %d cube variables, got %d" % (bits, len(vars_))
nv = nc = None; body = []
for l in open(src):
    if l.startswith("p "):
        _, _, nv, nc = l.split(); nv, nc = int(nv), int(nc)
    elif l.strip() and l[0] not in "c%":
        body.append(l if l.endswith("\n") else l + "\n")
units = ["%d 0\n" % (v if (idx >> j) & 1 else -v) for j, v in enumerate(vars_)]
if pseed > 0:
    random.Random(pseed).shuffle(body)
with open(dst, "w") as f:
    f.write("p cnf %d %d\n" % (nv, len(body) + len(units)))
    f.writelines(body); f.writelines(units)   # units LAST, exactly as a Dagster terminal job
