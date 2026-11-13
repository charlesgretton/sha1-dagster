#!/usr/bin/env python3
"""
report_stats.py -- recompute the report's statistics from a results prefix.

  report_stats.py PREFIX TARGETS [BR_PREFIX]
     e.g. report_stats.py results/eval_r22 "10001 10002 10003"           (Phase C)
          report_stats.py results/dev_L8_B0 "10004 10005" results/dev_Br  (Phase B: races kept separately)

Reads PREFIX.C_g*_r*.watch.json, PREFIX.Cp_g*_r*.watch.json and PREFIX.Br_g*_r*.race.json and prints,
per target and pooled: solved counts; min/median/max of solved times; paired outcomes with censoring
(a pair is decisive when one side solved before the other solved or was censored); two-sided sign
test; exact 95% Clopper-Pearson interval for the win probability; paired ratios T_Br/T_C (censored
Br races give lower bounds 1800/T_C); and yields per allocated PHYSICAL core-hour (64 cores x wall
time for every arm). The definitions match the report's protocol section.
"""
import sys, json, glob, re, math, statistics as st

CORES = 64

def load(prefix, arm):
    D = {}
    pat = '%s.%s_g*_r*.%s' % (prefix, arm, 'race.json' if arm == 'Br' else 'watch.json')
    for p in glob.glob(pat):
        m = re.search(r'_g(\d+)_r(\d+)\.', p); r = json.load(open(p))
        solved = r['status'] == 'SOLVED'
        end = r.get('t_end', r.get('t_end_wall')) or 0
        D[(int(m[1]), int(m[2]))] = (r.get('t_wall') if solved else None, end)
    return D

def sign_test(w, l):
    n = w + l
    return 1.0 if n == 0 else min(1.0, 2 * sum(math.comb(n, j) for j in range(0, min(w, l) + 1)) / 2 ** n)

def cp_interval(k, n, a=0.05):
    cdf = lambda k, p: sum(math.comb(n, j) * p**j * (1 - p)**(n - j) for j in range(0, k + 1))
    def solve(f, rising):
        lo, hi = 0.0, 1.0
        for _ in range(200):
            mid = (lo + hi) / 2
            if (f(mid) > 0) == rising: hi = mid
            else: lo = mid
        return (lo + hi) / 2
    lower = 0.0 if k == 0 else solve(lambda p: (1 - cdf(k - 1, p)) - a / 2, True)
    upper = 1.0 if k == n else solve(lambda p: cdf(k, p) - a / 2, False)
    return lower, upper

def pair(X, Y):
    xw = yw = und = 0
    for k in sorted(set(X) & set(Y)):
        (x, xe), (y, ye) = X[k], Y[k]
        if x is not None and (y is None or x < y): xw += 1
        elif y is not None and ((x is None and y < xe) or (x is not None and y < x)): yw += 1
        else: und += 1
    return xw, yw, und

def describe(name, D):
    s = sorted(v[0] for v in D.values() if v[0] is not None)
    ch = sum(CORES * v[1] for v in D.values()) / 3600
    if not D: return
    print("  %-4s solved %2d/%-2d  min %7s  median %7s  max %7s  | %.3f verified per physical core-hour"
          % (name, len(s), len(D), ('%.1f' % s[0]) if s else '-', ('%.1f' % st.median(s)) if s else '-',
             ('%.1f' % s[-1]) if s else '-', len(s) / ch if ch else 0))

def report(prefix, targets, label, br_prefix=None):
    A = {a: load(prefix, a) for a in ('C', 'Cp')}
    A['Br'] = load(br_prefix or prefix, 'Br')
    sub = lambda D: {k: v for k, v in D.items() if k[0] in targets}
    A = {a: sub(D) for a, D in A.items()}
    print("== %s" % label)
    for a in ('C', 'Cp', 'Br'): describe(a, A[a])
    for x, y in (('C', 'Br'), ('Cp', 'Br'), ('C', 'Cp')):
        if A[x] and A[y]:
            w, l, u = pair(A[x], A[y]); lo, hi = cp_interval(w, w + l) if w + l else (0, 1)
            print("  %-2s vs %-2s: %d-%d (undecided %d)  sign test p=%.2g  95%% CP win prob [%.3f, %.3f]"
                  % (x, y, w, l, u, sign_test(w, l), lo, hi))
    if A['C'] and A['Br']:
        R = []; nlb = 0
        for k in sorted(set(A['C']) & set(A['Br'])):
            c = A['C'][k][0]; b = A['Br'][k][0]
            if c is None: continue
            if b is None: nlb += 1; b = 1800.0
            R.append(b / c)
        if R:
            gm = math.exp(sum(math.log(r) for r in R) / len(R))
            print("  paired ratio T_Br/T_C: min %.1f median %.1f geometric mean %.1f max %.1f (%d lower bounds)"
                  % (min(R), st.median(R), gm, max(R), nlb))

if __name__ == '__main__':
    if len(sys.argv) < 3: sys.exit(__doc__)
    prefix = sys.argv[1]; targets = [int(t) for t in sys.argv[2].split()]
    br = sys.argv[3] if len(sys.argv) > 3 else None
    for t in targets: report(prefix, [t], 'g%d' % t, br)
    report(prefix, targets, 'pooled ' + ' '.join('g%d' % t for t in targets), br)
