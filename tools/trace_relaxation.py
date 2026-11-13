#!/usr/bin/env python3
"""
trace_relaxation.py -- carry-aligned trans-round trace relaxation for sha1-sat CNFs.

  build     RAW.cnf --bits K --out-dag D.dag --out-cnf F.cnf
            Two-node Dagster DAG:
              node 0  relaxation: CONST + all EXPAND + FIX + every ROUND/FINAL adder
                      clause computing bit positions 0..K-1   (a clause subset)
              node 1  the full formula
              edge    low K bits of every FREE state word a[5..r-1]   (the trace)
  pipeline  RAW.cnf --bits K --seconds S [--budget C]
            Standalone equivalent: enumerate relaxation models (projected onto the
            trace, blocked one by one), check each on the exact formula with the
            trace as ASSUMPTIONS in one persistent incremental solver (the same
            mechanism as Dagster MiniSat mode with -q), verify every solution.

Why low bits.  A slice starting at bit 0 has a known carry-in (0), so the kept
adder clauses are exact for those positions. Measured on r=21, g10003:
  planted low-16 trace  -> full solve in 14,609 conflicts vs 866,048 unrestricted
  planted TRMA rotating 8-bit block -> harder than unrestricted (interior segments,
                         unknown carry-in)
  random low-16 traces  -> refuted instantly (the trace space filters strongly)
  relaxation traces     -> ~0.4% extend; 3 verified preimages, first at 13.9 s vs
                         64 s unrestricted with the same solver
  r=22                  -> 0/6,871 (16-bit) and 0/43,030 (24-bit) extended
  REAL DAGSTER (-m 4 -q 2 -e 0 -i 1), 3 ranks, one oversubscribed core:
                           4 of 5 r=21 targets solved, VERIFIED (4.1-39 s); the same
                           engine unrestricted solved 0 of 5 in 48 s

Properties by construction (the defects reported for the earlier prototype):
  * cumulative: node 1 holds the ENTIRE formula (Dagster nodes do not inherit
    clauses, so nothing is left to "earlier levels")
  * sound relaxation: node 0 is a clause subset, so every real solution's trace
    satisfies it; the planted trace is checked with a SOLVER, not propagation
  * interface is only the trace (K bits x free words); carries stay internal
  * B, C, D, E are rotated aliases of earlier a-words in this encoding, so the
    trace is K bits per a-word, not 5x that
"""
import os, re, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sha1_cnf import Sha1Cnf
from sha1_dagify import write_dag, validate


def relaxation(c, bits):
    info = {}
    for (s, n, lab, _inst) in c.var_blocks:
        for off in range(n):
            info[s + off] = (lab, off)
    def position(cl, stage):
        if stage.startswith('ROUND:'):
            i = int(stage.split(':')[1])
            names = ('carry', 't0', 't1', 't2', 'f[%d]' % i, 'a[%d]' % (i + 5))
            offs = [info[abs(l)][1] for l in cl if abs(l) in info and info[abs(l)][0] in names]
        else:  # FINAL: the h_out adders
            offs = [info[abs(l)][1] for l in cl if abs(l) in info and
                    (info[abs(l)][0] in ('carry', 't0', 't1', 't2') or info[abs(l)][0].startswith('h_out'))]
        return max(offs) if offs else 0
    keep = []
    for name, (lo, hi) in c.stages.items():
        for ci in range(lo, hi + 1):
            if not name.startswith(('ROUND', 'FINAL')) or position(c.clauses[ci], name) < bits:
                keep.append(ci)
    trace = [v for i in range(5, c.rounds) for b, v in enumerate(c.a(i)) if b < bits]
    return keep, trace


def planted_check(c, keep, trace, raw_path):
    """SAT check: relaxation + planted trace (requires SHA1GEN_PLANTED_OUT words next to RAW)."""
    pf = raw_path + '.planted'
    if not os.path.exists(pf):
        return None
    from pysat_compat import Solver as Cadical153, SOLVER_NAME
    w = [int(l, 16) for l in open(pf) if l.strip()]
    s = Cadical153(bootstrap_with=c.clauses)
    msg = [(v if (w[i] >> b) & 1 else -v) for i in range(16) for b, v in enumerate(c.w(i))]
    assert s.solve(assumptions=msg)
    m = {abs(l): l > 0 for l in s.get_model()}
    z = [(v if m[v] else -v) for v in trace]
    r = Cadical153(bootstrap_with=[c.clauses[i] for i in keep])
    return r.solve(assumptions=z)


def cmd_build(a):
    c = Sha1Cnf(a['raw'])
    keep, trace = relaxation(c, a['bits'])
    c.write_repaired(a['out-cnf'])
    nodes = [keep, list(range(c.true_cc))]
    edges = {(0, 1): trace}
    reporting = [v for i in range(16) for v in c.w(i)]
    errs, _ = validate(c, nodes, edges, reporting)
    write_dag(a['out-dag'], nodes, edges, reporting)
    pc = planted_check(c, keep, trace, a['raw'])
    print("r=%d: relaxation %d of %d clauses; trace %d bits over a[5..%d]"
          % (c.rounds, len(keep), c.true_cc, len(trace), c.rounds - 1))
    print("planted trace satisfies relaxation: %s" % ({True: 'yes', False: 'NO', None: 'not checked (no .planted file)'}[pc]))
    print("DAG validation: %s" % ("OK" if not errs else "; ".join(errs)))
    print("wrote %s %s" % (a['out-cnf'], a['out-dag']))
    print("run:   mpirun -n N dagster -m 4 -q 2 -e 0 -i 1 -o sol.txt %s %s" % (a['out-dag'], a['out-cnf']))
    print("       -i 1 is REQUIRED: without it Dagster finds a solution, then waits for the")
    print("       relaxation worker to reach its 100th solution before exiting, and can hang.")


def cmd_pipeline(a):
    from pysat_compat import Solver as Cadical153, SOLVER_NAME
    from verify_preimage import sha1_rounds
    c = Sha1Cnf(a['raw'])
    keep, trace = relaxation(c, a['bits'])
    gen = Cadical153(bootstrap_with=[c.clauses[i] for i in keep])
    chk = Cadical153(bootstrap_with=c.clauses)
    units = {abs(cl[0]): cl[0] > 0 for cl in c.clauses if len(cl) == 1}
    target = [sum((1 << b) for b, v in enumerate(c.h_out(j)) if units.get(v)) for j in range(5)]
    t0 = time.time(); n = 0; out = {'SAT': 0, 'UNSAT': 0, 'UNKNOWN': 0}
    while time.time() - t0 < a['seconds']:
        if not gen.solve():
            print("relaxation exhausted"); break
        m = {abs(l): l > 0 for l in gen.get_model()}
        z = [(v if m.get(v) else -v) for v in trace]
        gen.add_clause([-x for x in z]); n += 1
        chk.conf_budget(a['budget']); res = chk.solve_limited(assumptions=z)
        out['SAT' if res else ('UNSAT' if res is False else 'UNKNOWN')] += 1
        if res:
            mm = {abs(l): l > 0 for l in chk.get_model()}
            w16 = [sum((1 << b) for b, v in enumerate(c.w(i)) if mm.get(v)) for i in range(16)]
            ok = sha1_rounds(c.rounds, w16) == target      # independent verification
            print("  sample %d at %.1fs: preimage %s  %s" % (n, time.time() - t0, "VERIFIED" if ok else "*** FAILED VERIFICATION ***",
                  " ".join("%08x" % x for x in w16[:4]) + " ..."), flush=True)
    print("r=%d, %d-bit trace: %d traces in %.0fs -> %s" % (c.rounds, a['bits'], n, time.time() - t0, out))


if __name__ == '__main__':
    args = sys.argv[1:]
    if len(args) < 2 or args[0] not in ('build', 'pipeline'):
        sys.exit(__doc__)
    a = {'raw': args[1], 'bits': 16, 'seconds': 120, 'budget': 20000}
    for i, x in enumerate(args):
        if x.startswith('--'):
            k = x[2:]; val = args[i + 1]
            a[k] = int(val) if k in ('bits', 'seconds', 'budget') else val
    (cmd_build if args[0] == 'build' else cmd_pipeline)(a)
