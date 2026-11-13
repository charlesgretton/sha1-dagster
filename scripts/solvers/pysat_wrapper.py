#!/usr/bin/env python3
"""
Reference solver wrapper.  Fixed interface:

    solver-wrapper SEED CNF MODEL

Writes a DIMACS-style model (space-separated literals) to MODEL and exits
  10  SAT      20  UNSAT      1  error
matching the MiniSat convention, so portfolio.sh is engine-neutral.

Diversification here is initial-phase randomisation seeded by SEED, because
PySAT does not expose the solver's own RNG.  For the strict null control, use a
wrapper around the SAME engine and configuration Dagster uses (TiniSAT or the
bundled MiniSat with -rnd-seed/-rnd-init/-rnd-freq), varying only the seed.

Needs: pip install python-sat
"""
import random
import sys


def main():
    if len(sys.argv) != 4:
        sys.stderr.write("usage: pysat_wrapper.py SEED CNF MODEL\n")
        return 1
    seed, cnf_path, model_path = int(sys.argv[1]), sys.argv[2], sys.argv[3]
    from pysat.formula import CNF
    from pysat.solvers import Minisat22

    f = CNF(from_file=cnf_path)
    s = Minisat22(bootstrap_with=f.clauses)
    rng = random.Random(seed)
    nv = f.nv
    s.set_phases([(v if rng.random() < 0.5 else -v) for v in range(1, nv + 1)])
    res = s.solve()
    if res:
        with open(model_path, "w") as g:
            g.write(" ".join(str(x) for x in s.get_model()) + " 0\n")
        return 10
    return 20 if res is False else 1


if __name__ == "__main__":
    sys.exit(main())
