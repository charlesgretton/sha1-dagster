"""
pysat_compat.py -- choose a complete SAT solver class from whatever python-sat is installed.

Older python-sat releases (e.g. on Python 3.8) lack Cadical153. Every use in this bundle
needs only a COMPLETE solver with assumptions and, for the pipeline, conf_budget +
solve_limited; SAT/UNSAT answers do not depend on which complete solver computes them.
Set PYSAT_SOLVER=<ClassName> to force a choice (used by the tests).
"""
import os
import pysat.solvers as _ps

PREFERENCE = ("Cadical195", "Cadical153", "Cadical103", "Cadical", "Glucose4", "Minisat22")


def solver_class():
    forced = os.environ.get("PYSAT_SOLVER")
    if forced:
        if not hasattr(_ps, forced):
            raise SystemExit("PYSAT_SOLVER=%s is not available in this python-sat" % forced)
        return getattr(_ps, forced), forced
    for name in PREFERENCE:
        if hasattr(_ps, name):
            return getattr(_ps, name), name
    raise SystemExit("no usable complete solver found in python-sat")


Solver, SOLVER_NAME = solver_class()
