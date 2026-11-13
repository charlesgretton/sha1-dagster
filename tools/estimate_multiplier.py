#!/usr/bin/env python3
"""
estimate_multiplier.py MANIFEST [MANIFEST ...] [--metric wall_seconds|conflicts]
                       [--cutoff 300] [--allow-drop]

Per-round difficulty multipliers with per-run RIGHT-CENSORING.

Observations
  solved (SAT_VERIFIED)          exact value
  censored (TIMEOUT, TIMEOUT_KILLED)
                                 the run's own recorded value is its censoring
                                 threshold: the true value is GREATER than it.
                                 wall: the run's wall time (fallback --cutoff).
                                 conflicts: the conflicts reached at the timeout.
  other statuses                 excluded, with a warning

A censored run with no recorded threshold (e.g. TIMEOUT_KILLED, or runs from
before conflict recording) cannot be used: dropping it biases the estimate
towards "easy". Such a round is REFUSED for that metric unless --allow-drop.

Timeouts are wall-clock, so censoring thresholds in conflicts differ run to run.
The methods below handle per-run thresholds; none assumes a common cutoff.

Estimands (each is a different number; none is "the" multiplier)
  E1 median ratio               only when every run in both rounds is solved
  E2 KM lower-quantile ratio    Kaplan-Meier with per-run censoring, at quantile q.
                                Racing-relevant: a K-worker race sits near q ~ 0.69/K.
  E3 lognormal scale ratio      censored MLE, shared sigma per round, per-target
                                median, each censored run at its own threshold.
                                Flagged when the fitted median is extrapolated
                                beyond the largest value seen in that round.
  E4 zero-success lower bound   round with no solves: smallest scale ratio such
                                that P(every run exceeds its own threshold) >= 5%,
                                holding the previous round's sigma and per-target
                                medians. Model-based, 95%.
"""
import json, math, sys
from collections import defaultdict

args = sys.argv[1:]
def opt(name, default):
    return args[args.index(name) + 1] if name in args else default
metric = opt('--metric', 'wall_seconds'); CUT = float(opt('--cutoff', 300))
allow_drop = '--allow-drop' in args
files = [x for i, x in enumerate(args) if not x.startswith('--') and (i == 0 or args[i - 1] not in ('--metric', '--cutoff'))]
CENS = {"TIMEOUT", "TIMEOUT_KILLED"}

R = defaultdict(lambda: defaultdict(list))   # round -> target -> [(value, event)]
dropped = defaultdict(int); excluded = defaultdict(int); seen = set()
for f in files:
    for line in open(f):
        if not line.strip(): continue
        m = json.loads(line); r = m['rounds']; g = m['generator_seed']; st = m['status']; seen.add(r)
        v = m.get(metric)
        if st == 'SAT_VERIFIED':
            if v is None: excluded[r] += 1; continue
            R[r][g].append((float(v), True))
        elif st in CENS:
            if v is None and metric == 'wall_seconds': v = CUT
            if v is None: dropped[r] += 1; continue
            R[r][g].append((float(v), False))
        else:
            excluded[r] += 1

Phi = lambda z: 0.5 * math.erfc(-z / math.sqrt(2))
def logS(z):                                  # log(1 - Phi(z)), stable
    return math.log(max(1e-300, 0.5 * math.erfc(z / math.sqrt(2))))

def km(obs):
    """Kaplan-Meier with per-run censoring: [(value, F(value))] at event values.
    At tied values, events are counted before censorings (standard convention)."""
    pts = sorted(obs, key=lambda x: (x[0], not x[1]))
    groups = []; i = 0
    while i < len(pts):
        v = pts[i][0]; d = c = 0
        while i < len(pts) and pts[i][0] == v:
            d += pts[i][1]; c += (not pts[i][1]); i += 1
        groups.append((v, d, c))
    S, at_risk, res = 1.0, len(pts), []
    for v, d, c in groups:
        if d:
            S *= 1 - d / at_risk; res.append((v, 1 - S))
        at_risk -= d + c
    return res
def km_quantile(obs, q):
    for v, F in km(obs):
        if F >= q - 1e-12: return v
    return None

def ll_target(mu, s, obs):
    L = 0.0
    for v, ev in obs:
        z = (math.log(v) - mu) / s
        L += (-math.log(s * v) - 0.5 * z * z) if ev else logS(z)
    return L
def golden(fn, lo, hi, it=60):
    g = (math.sqrt(5) - 1) / 2; a, b = lo, hi
    c, d = b - g * (b - a), a + g * (b - a); fc, fd = fn(c), fn(d)
    for _ in range(it):
        if fc > fd: b, d, fd = d, c, fc; c = b - g * (b - a); fc = fn(c)
        else:       a, c, fc = c, d, fd; d = a + g * (b - a); fd = fn(d)
    return (a + b) / 2
def fit_round(tg):
    """shared sigma, per-target mu; each censored run at its own threshold"""
    allv = [v for obs in tg.values() for v, _ in obs]
    lo, hi = math.log(min(allv)) - 5, math.log(max(allv)) + 15
    def prof(s):
        tot = 0.0; mus = {}
        for g, obs in tg.items():
            mu = golden(lambda m: ll_target(m, s, obs), lo, hi); mus[g] = mu; tot += ll_target(mu, s, obs)
        return tot, mus
    s = golden(lambda s: prof(s)[0], 0.1, 5.0, it=40)
    return s, prof(s)[1], max(allv)

rounds = sorted(seen)
print("metric=%s   (censored runs use their own recorded %s as threshold)" % (metric, metric))
usable = {}
for r in rounds:
    obs = [x for g in R[r] for x in R[r][g]]; ev = sum(1 for _, e in obs if e)
    note = ""
    if not obs:
        print("  r=%d: NO usable runs for metric %s (%d censored without threshold, %d solved without value)"
              % (r, metric, dropped[r], excluded[r]))
        usable[r] = False; continue
    if dropped[r]: note += "  !! %d censored runs WITHOUT a %s threshold" % (dropped[r], metric)
    if excluded[r]: note += "  (%d runs excluded: other status or missing value)" % excluded[r]
    print("  r=%d: %d usable runs, %d solved, %d censored%s" % (r, len(obs), ev, len(obs) - ev, note))
    usable[r] = not dropped[r] or allow_drop
    if dropped[r] and not allow_drop:
        print("     -> round REFUSED for this metric: dropping censored runs biases towards easy (use --allow-drop to override)")

print("\nconsecutive rounds")
fits = {}
for r in rounds:
    if usable[r] and any(e for g in R[r] for _, e in R[r][g]): fits[r] = fit_round(R[r])
if not any(usable.values()):
    print("\nNOTHING TO ESTIMATE for metric %s: every round refused." % metric); sys.exit(3)
for r0, r1 in zip(rounds, rounds[1:]):
    if not (usable[r0] and usable[r1]): print("  r%d->r%d: skipped (refused round)" % (r0, r1)); continue
    o0 = [x for g in R[r0] for x in R[r0][g]]; o1 = [x for g in R[r1] for x in R[r1][g]]
    all0 = all(e for _, e in o0); all1 = all(e for _, e in o1)
    print("  r%d -> r%d" % (r0, r1))
    if all0 and all1:
        m0 = sorted(v for v, _ in o0)[len(o0) // 2]; m1 = sorted(v for v, _ in o1)[len(o1) // 2]
        print("     E1 median ratio                         x%.2f" % (m1 / m0))
    for q in (0.004, 0.011, 0.02, 0.05):
        a0, a1 = km_quantile(o0, q), km_quantile(o1, q)
        if a0 and a1:
            print("     E2 KM %4.1f%% quantile ratio (~%4d-worker race)  x%.1f" % (100 * q, round(0.69 / q), a1 / a0))
    if r0 in fits and r1 in fits:
        (s0, mu0, mx0), (s1, mu1, mx1) = fits[r0], fits[r1]
        common = [g for g in mu0 if g in mu1]
        ratio = math.exp(sum(mu1[g] - mu0[g] for g in common) / len(common))
        extrap = [r for r, (s, mu, mx) in ((r0, fits[r0]), (r1, fits[r1])) if math.exp(sum(mu.values()) / len(mu)) > mx]
        print("     E3 lognormal scale ratio (sigma %.2f -> %.2f)   x%.0f%s" % (s0, s1, ratio,
              ("   [!! fitted median extrapolated beyond observed range in r=%s]" % ",".join(map(str, extrap))) if extrap else ""))
    if r0 in fits and not any(e for _, e in o1):
        s0, mu0, _ = fits[r0]; base = sum(mu0.values()) / len(mu0)
        def logPall(delta):
            return sum(logS((math.log(v) - (mu0.get(g, base) + delta)) / s0) for g in R[r1] for v, _ in R[r1][g])
        lo, hi = -30.0, 30.0
        for _ in range(100):
            mid = (lo + hi) / 2
            if logPall(mid) >= math.log(0.05): hi = mid
            else: lo = mid
        print("     E4 zero-success bound (sigma %.2f held)        multiplier >= x%.1f  (95%%, model-based)" % (s0, math.exp(hi)))
