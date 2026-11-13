#!/usr/bin/env python3
"""
split_join_probe.py -- does a round-boundary split of the low-16 relaxation filter?

  split_join_probe.py RAW.cnf [--cut 11] [--widths 4,6,8] [--samples 200] [--seed 1] [--out F]

Split the low-16 trace relaxation R16 (tools/trace_relaxation.relaxation) at round
boundary CUT:
  early half:  CONST + all EXPAND + R16 clauses of ROUND:0 .. ROUND:CUT-1
  late half:   CONST + all EXPAND + R16 clauses of ROUND:CUT .. ROUND:r-1 + FINAL + FIX
Boundary: the low b bits of a[CUT-1] and a[CUT] (state words touched by both halves).
For random boundary assignments, test satisfiability of each half separately (pysat
CaDiCaL 1.5.3). A consistency join at the boundary can filter only if some boundary
values are rejected by a half.

Reporting: with zero rejections in N samples, the one-sided 95% upper bound on the
rejection probability is 1 - 0.05**(1/N) (about 1.49% for N=200). The result supports
"no filtration OBSERVED, rejection rate below the bound", not "no filtration".
"""
import os, sys, json, random
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sha1_cnf import Sha1Cnf
import trace_relaxation as tr


def main(raw, cut, widths, n, seed, outp):
    from pysat_compat import Solver as Cadical153, SOLVER_NAME
    import pysat
    c = Sha1Cnf(raw); r = c.rounds
    keep = set(tr.relaxation(c, 16)[0])
    stage_of = {}
    for name, (lo, hi) in c.stages.items():
        for ci in range(lo, hi + 1):
            stage_of[ci] = name
    def rnd(ci):
        s = stage_of[ci]
        return int(s.split(":")[1]) if s.startswith("ROUND:") else None
    shared = [ci for ci in keep if stage_of[ci] == "CONST" or stage_of[ci].startswith("EXPAND")]
    early = shared + [ci for ci in keep if rnd(ci) is not None and rnd(ci) < cut]
    late = shared + [ci for ci in keep if (rnd(ci) is not None and rnd(ci) >= cut)
                     or stage_of[ci] in ("FINAL",) or stage_of[ci].startswith("FIX")]
    se = Cadical153(bootstrap_with=[c.clauses[i] for i in early])
    sl = Cadical153(bootstrap_with=[c.clauses[i] for i in late])
    rng = random.Random(seed)
    res = {"raw": os.path.abspath(raw), "rounds": r, "cut": cut, "samples": n, "seed": seed,
           "solver": "pysat %s (pysat %s)" % (SOLVER_NAME, getattr(pysat, "__version__", "?")),
           "early_clauses": len(early), "late_clauses": len(late), "widths": []}
    for b in widths:
        bvars = [v for i in (cut - 1, cut) for k, v in enumerate(c.a(i)) if k < b]
        acc_e = acc_l = both = 0
        for _ in range(n):
            val = [(v if rng.random() < 0.5 else -v) for v in bvars]
            e = se.solve(assumptions=val); l = sl.solve(assumptions=val)
            acc_e += e; acc_l += l; both += (e and l)
        rej = n - both
        ub = 1 - 0.05 ** (1.0 / n) if rej == 0 else None
        res["widths"].append({"low_bits_per_word": b, "boundary_vars": len(bvars),
                              "accepted_early": acc_e, "accepted_late": acc_l,
                              "accepted_both": both, "rejected": rej,
                              "rejection_rate_upper95_if_zero": ub})
        print("b=%d (%d boundary vars): early %d/%d, late %d/%d, both %d/%d%s" % (
            b, len(bvars), acc_e, n, acc_l, n, both, n,
            ("  -> no rejection observed; rejection rate < %.2f%% (95%%)" % (100 * ub)) if ub else ""))
    if outp:
        json.dump(res, open(outp, "w"), indent=1)
    return res


if __name__ == "__main__":
    a = sys.argv[1:]
    if not a:
        sys.exit(__doc__)
    opt = {"cut": "11", "widths": "4,6,8", "samples": "200", "seed": "1", "out": None}
    for i, x in enumerate(a):
        if x.startswith("--"):
            opt[x[2:]] = a[i + 1]
    main(a[0], int(opt["cut"]), [int(x) for x in opt["widths"].split(",")],
         int(opt["samples"]), int(opt["seed"]), opt["out"])
