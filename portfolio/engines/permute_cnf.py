#!/usr/bin/env python3
"""permute_cnf.py IN OUT SEED -- seeded clause-order permutation.
Variables are NOT renamed, so any model of OUT is a model of IN and verifies
against the original symbol-table CNF unchanged."""
import random, sys
src, dst, seed = sys.argv[1], sys.argv[2], int(sys.argv[3])
hdr, body = None, []
for l in open(src):
    if l.startswith("p "): hdr = l
    elif l.strip() and l[0] not in "c%": body.append(l if l.endswith("\n") else l + "\n")
random.Random(seed).shuffle(body)
with open(dst, "w") as f:
    f.write(hdr); f.writelines(body)
