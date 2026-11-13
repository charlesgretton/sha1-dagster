#!/usr/bin/env python3
"""
solver_bench.py -- one single-thread solver run on one instance, verified (the report's Table of
single solvers, Section 'Performance against CryptoMiniSat and other single solvers').

  solver_bench.py SOLVER INSTANCE CAP_SECONDS OUT.jsonl
     SOLVER: minisat22 | cadical195 | cms     (cms = CryptoMiniSat via pycryptosat)
     INSTANCE: a raw sha1gen CNF (plain, or with --xor native XOR lines for cms)

Appends one JSON line {solver, instance, cap, status, wall, valid}. A model is accepted only if
tools/verify_preimage.py recomputes reduced-round SHA-1 and matches the target digest.
Requires: pip install python-sat pycryptosat
"""
import sys, time, json, subprocess, os, threading
HERE = os.path.dirname(os.path.abspath(__file__))

def parse(path):
    cl, xs = [], []
    for l in open(path):
        if not l.strip() or l[0] in 'cp':
            continue
        if l[0] == 'x':                                 # x l1 l2 ... 0 : odd number of literals true
            lits = [int(a) for a in l[1:].split()][:-1]; rhs = True; vs = []
            for a in lits:
                if a < 0: rhs = not rhs
                vs.append(abs(a))
            xs.append((vs, rhs))
        else:
            cl.append([int(a) for a in l.split()][:-1])
    return cl, xs

def run(solver, path, cap, out):
    cl, xs = parse(path); t0 = time.time(); model = None; status = 'TIMEOUT'
    if solver == 'cms':
        import pycryptosat
        s = pycryptosat.Solver(threads=1, time_limit=cap)
        s.add_clauses(cl)
        for vs, rhs in xs: s.add_xor_clause(vs, rhs)
        sat, sol = s.solve()
        if sat is True: status = 'SAT'; model = [i if sol[i] else -i for i in range(1, len(sol))]
        elif sat is False: status = 'UNSAT'
    else:
        if xs: sys.exit("ERROR: %s cannot read native XOR lines; use the plain CNF" % solver)
        from pysat.solvers import Solver
        s = Solver(name=solver, bootstrap_with=cl)
        timer = threading.Timer(cap, s.interrupt); timer.start()
        r = s.solve_limited(expect_interrupt=True); timer.cancel()
        if r is True: status = 'SAT'; model = s.get_model()
        elif r is False: status = 'UNSAT'
        s.delete()
    wall = time.time() - t0; valid = None
    if model:
        mp = out + '.model.%d' % os.getpid()
        open(mp, 'w').write('SAT\n' + ' '.join(map(str, model)) + ' 0\n')
        v = subprocess.run([sys.executable, os.path.join(HERE, 'verify_preimage.py'), path, mp], capture_output=True, text=True)
        valid = 'VALID PREIMAGE' in v.stdout
        os.remove(mp)
    rec = dict(solver=solver, instance=os.path.basename(path), cap=cap, status=status, wall=round(wall, 1), valid=valid)
    open(out, 'a').write(json.dumps(rec) + '\n'); print(rec, flush=True)

if __name__ == '__main__':
    if len(sys.argv) != 5: sys.exit(__doc__)
    need = 'pycryptosat' if sys.argv[1] == 'cms' else 'pysat'
    try:
        __import__(need)
    except ImportError:
        sys.exit("ERROR: Python package for %s missing. Install with:\n"
                 "    pip install python-sat pycryptosat" % sys.argv[1])
    run(sys.argv[1], sys.argv[2], float(sys.argv[3]), sys.argv[4])
