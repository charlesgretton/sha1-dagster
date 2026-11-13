"""
Calibration harness for SHA-1 preimage decompositions under Dagster semantics.

Dagster mechanics that this models (verified against the source):
  * a node's CNF is exactly its listed clauses, no inheritance
    (CnfHolder::get_Cnf -> Cnf(master_cnf, clause_indices_for_each_node[node]))
  * a node's solutions are projected onto the union of its outgoing edge
    variables and deduplicated (SolutionsInterface::register_completion,
    TableSolutions::add_message)
  * the blocking no-good is added over that projection only, so a root node
    performs projected model enumeration = cube generation
  * a root node (no predecessors) receives exactly one seed message, so its
    enumeration is SERIAL and sits on the critical path
    (Dag::generate_from_minimal)
  * downstream, each cube becomes an independent job whose CNF is the node's
    clauses plus the cube as unit clauses (CnfHolder::compile_Cnf_from_Message)

So predicted walltime on N cores is:
    root_enumeration_time  +  makespan(cube job costs, N cores)

Cost is reported in CDCL conflicts (machine independent) as well as seconds.
"""

import sys, time, json
from sha1_cnf import Sha1Cnf
import os, sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pysat_compat import Solver as Cadical153


def solve_full(clauses, assumptions=(), conf_budget=None):
    """Solve, returning (status, conflicts, seconds). status in {SAT,UNSAT,UNKNOWN}."""
    s = Cadical153(bootstrap_with=clauses)
    t0 = time.time()
    if conf_budget:
        s.conf_budget(conf_budget)
        res = s.solve_limited(assumptions=list(assumptions))
    else:
        res = s.solve(assumptions=list(assumptions))
    dt = time.time() - t0
    stats = s.accum_stats()
    conf = stats.get('conflicts', -1)
    model = s.get_model() if res else None
    s.delete()
    status = "SAT" if res else ("UNSAT" if res is False else "UNKNOWN")
    return status, conf, dt, model


def enumerate_projections(clauses, proj_vars, cap=200000, time_cap=120.0):
    """
    Projected model enumeration, exactly as a Dagster root node performs it:
    solve, project onto proj_vars, emit, add the projected no-good, repeat.
    Returns (cubes, seconds, total_conflicts, hit_cap).
    """
    s = Cadical153(bootstrap_with=clauses)
    proj = list(proj_vars)
    cubes = []
    t0 = time.time()
    hit_cap = False
    while True:
        if len(cubes) >= cap or (time.time() - t0) > time_cap:
            hit_cap = True
            break
        if not s.solve():
            break
        m = s.get_model()
        sign = {abs(l): (l > 0) for l in m}
        cube = [(v if sign.get(v, False) else -v) for v in proj]
        cubes.append(cube)
        s.add_clause([-l for l in cube])          # projected blocking clause
    dt = time.time() - t0
    conf = s.accum_stats().get('conflicts', -1)
    s.delete()
    return cubes, dt, conf, hit_cap


def makespan(costs, ncores):
    """Longest-processing-time-first schedule of independent jobs on ncores."""
    loads = [0.0] * ncores
    for c in sorted(costs, reverse=True):
        i = min(range(ncores), key=lambda j: loads[j])
        loads[i] += c
    return max(loads) if loads else 0.0


def load(path):
    return Sha1Cnf(path)


def stages_matching(c, pred):
    return [k for k in c.stages if pred(k)]
