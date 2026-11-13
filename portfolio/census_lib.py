"""census_lib -- exact first-solution campaign quantities from a cube census.

A census runs every cube of a k-bit split (C = 2^k cubes per target) to
completion or timeout, with deterministic TiniSAT (CUBE_PERMUTE=0), so each
cube's outcome is a fixed number: its solve time, or censored (> cutoff).

With K workers and satisfiable cubes, no worker frees up before the first
solution, so a campaign's time to first solution is the minimum over the K
cubes dispatched first. For a UNIFORMLY RANDOM first batch of K distinct cubes
the distribution is exact (finite population, sampling without replacement):

    P(min <= t) = 1 - C(C - S(t), K) / C(C, K),   S(t) = #cubes solved by t

No simulation is needed. Index order, best and worst first batches are also
exact. What is NOT covered: Dagster's actual dispatch order (unknown unless
recorded with the worker-CNF dump), and runtime interactions between workers.
"""
import json, math
from collections import defaultdict

CENSORED = {"TIMEOUT", "TIMEOUT_KILLED"}
SOLVED = "SAT_VERIFIED"

def load(manifest, bits):
    """-> {(round, target): {cube_index: time_or_inf}} plus a list of problems."""
    C = 1 << bits
    by = defaultdict(dict); problems = []
    for line in open(manifest):
        if not line.strip(): continue
        m = json.loads(line)
        st = m["status"]
        if st == SOLVED:
            t = m["wall_seconds"]
        elif st in CENSORED:
            t = math.inf
        else:
            problems.append("%s: status %s (neither solved nor censored)" % (m["run_id"], st)); continue
        cube = (m["solver_seed"] - 1) % C
        key = (m["rounds"], m["generator_seed"])
        if cube in by[key]:
            problems.append("%s: duplicate cube %d" % (m["run_id"], cube)); continue
        by[key][cube] = t
    return by, problems

def p_min_le(times, K, t):
    """exact P(min over a uniformly random K-subset <= t)"""
    C = len(times); S = sum(1 for v in times if v <= t)
    if K > C: raise ValueError("K=%d exceeds census size %d" % (K, C))
    return 1.0 - math.comb(C - S, K) / math.comb(C, K)

def min_quantile(times, K, q):
    """exact q-quantile of the random-first-batch minimum; None means beyond the cutoff"""
    for t in sorted(v for v in times if v != math.inf):
        if p_min_le(times, K, t) >= q - 1e-12:
            return t
    return None

def campaign(times, K):
    """exact campaign summary for one complete census"""
    C = len(times); srt = sorted(times)
    return dict(
        cubes=C,
        solved=sum(1 for v in times if v != math.inf),
        index_order=min(times[:K]),                 # cubes 0..K-1 dispatched first
        best=srt[0],                                # the best possible first batch
        worst=srt[C - K],                           # the worst possible first batch
        median=min_quantile(times, K, 0.5),
        p10=min_quantile(times, K, 0.1),
        p90=min_quantile(times, K, 0.9),
        p_success=p_min_le(times, K, math.inf - 1) if C else 0.0,
    )
