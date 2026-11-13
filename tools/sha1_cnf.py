"""
Structural parser for sha1-sat generated DIMACS CNF (preimage and collision).

sha1-sat emits a symbol table ("c var <start>/<count> <label>") plus structural
markers interleaved with the clause stream, and emits clauses in strict
topological order.  Every structural stage is therefore a *contiguous* range of
clause indices, which maps directly onto Dagster's DAG "a-b" range syntax.

Collision instances contain two SHA-1 encodings back to back.  sha1-sat
namespaces the message/hash blocks (w0[i], h0_out0, w1[i], h1_out0) but does
NOT namespace a[i], f[i] or the adder temporaries, so lookups are resolved by
declaration occurrence rather than by label alone.

Stage names:
    [S<k>:]EXPAND:<i>   message expansion of w[i], i = 16 .. r-1
    [S<k>:]CONST        k[0..3] / h_in[0..4] unit clauses
    [S<k>:]ROUND:<i>    f[i] clauses + the add5 defining a[i+5]
    [S<k>:]FINAL        rotate-back + five add2 producing h_out[0..4]
    LINK                cross-block constraints (collision only)
    FIX                 unit clauses fixing message / hash bits
"""

import re
from collections import OrderedDict


class Sha1Cnf:
    def __init__(self, path):
        self.path = path
        self.header_vc = self.header_cc = None
        self.clauses = []
        self.var_blocks = []      # (start, count, label, instance)
        self.events = []          # (clause_index, kind, payload) in file order
        self._load()
        self.n_instances = max(1, sum(1 for e in self.events if e[1] == 'sha1'))
        self.rounds = self._infer_rounds()
        self.stages = self._attribute()

    # ---------------------------------------------------------------- loading
    def _load(self):
        ci = 0
        inst = -1
        with open(self.path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if line[0] == 'p':
                    m = re.match(r'p\s+cnf\s+(\d+)\s+(\d+)', line)
                    self.header_vc, self.header_cc = int(m.group(1)), int(m.group(2))
                    continue
                if line[0] == 'd':
                    continue
                if line[0] == 'c':
                    body = line[1:].strip()
                    m = re.match(r'var (\d+)/(\d+) (.*)', body)
                    if m:
                        lab = m.group(3).strip()
                        self.var_blocks.append(
                            (int(m.group(1)), int(m.group(2)), lab, max(inst, 0)))
                        self.events.append((ci, 'var', body))
                    else:
                        if body == 'sha1':
                            inst += 1
                        self.events.append((ci, body.split(' ')[0] if body else '', body))
                    continue
                if line.startswith('x'):        # native XOR constraint (sha1gen --xor): not a CNF clause;
                    continue                    # label-based verification recomputes SHA-1 and never needs it
                lits = [int(x) for x in line.split()]
                assert lits[-1] == 0
                self.clauses.append(lits[:-1])
                ci += 1

    @property
    def true_cc(self):
        return len(self.clauses)

    @property
    def true_vc(self):
        return max(abs(l) for c in self.clauses for l in c)

    # ------------------------------------------------------------- symbol map
    def vars_of(self, label, instance=0):
        for (s, n, lab, inst) in self.var_blocks:
            if lab == label and inst == instance:
                return list(range(s, s + n))
        raise KeyError("%s (instance %d)" % (label, instance))

    def has(self, label, instance=0):
        try:
            self.vars_of(label, instance)
            return True
        except KeyError:
            return False

    def w(self, i, instance=0):
        """Message word i. Collision instances label these w0[i] / w1[i]."""
        for cand in ("w[%d]" % i, "w%d[%d]" % (instance, i)):
            if self.has(cand, instance):
                return self.vars_of(cand, instance)
        raise KeyError("message word %d" % i)

    def a(self, i, instance=0):
        return self.vars_of("a[%d]" % i, instance)

    def h_out(self, j, instance=0):
        for cand in ("h_out%d" % j, "h%d_out%d" % (instance, j)):
            if self.has(cand, instance):
                return self.vars_of(cand, instance)
        raise KeyError("h_out%d" % j)

    def _infer_rounds(self):
        for (_, kind, body) in self.events:
            m = re.match(r'parameter nr_rounds = (\d+)', body)
            if m:
                return int(m.group(1))
        raise RuntimeError("nr_rounds not found")

    # -------------------------------------------------------- stage attribution
    def _attribute(self):
        r, n = self.rounds, self.true_cc
        multi = self.n_instances > 1

        # Segment the event stream by SHA-1 instance.
        seg_starts = [i for i, e in enumerate(self.events) if e[1] == 'sha1']
        if not seg_starts:
            seg_starts = [0]
        seg_starts.append(len(self.events))

        stages = OrderedDict()
        last_end = 0
        for k in range(len(seg_starts) - 1):
            ev = self.events[seg_starts[k]:seg_starts[k + 1]]
            pre = ("S%d:" % k) if multi else ""

            f_start, xor4_at, c32_at = {}, [], []
            for (ci, kind, body) in ev:
                m = re.match(r'var \d+/\d+ f\[(\d+)\]', body)
                if m:
                    f_start.setdefault(int(m.group(1)), ci)
                elif body == 'xor4':
                    xor4_at.append(ci)
                elif body.startswith('constant32'):
                    c32_at.append(ci)
            if not f_start:
                continue

            # FINAL: first clause mentioning an h_out var of this instance,
            # backed up to the head of its enclosing add2 block.
            hset = set()
            for j in range(5):
                hset.update(self.h_out(j, k))
            lo_bound = ev[0][0]
            final_start = None
            for i in range(lo_bound, n):
                if any(abs(l) in hset for l in self.clauses[i]):
                    final_start = i
                    break
            add2 = [ci for (ci, _, body) in ev if body == 'add2' and ci <= final_start]
            if add2:
                final_start = add2[-1]

            # end of this instance's encoding = start of next instance, else
            # the first "Fix"/"eq" marker after it
            seg_end = self.events[seg_starts[k + 1]][0] if k + 1 < len(seg_starts) - 1 else None
            if seg_end is None:
                fix_at = None
                for (ci, _, body) in self.events:
                    if ci >= final_start and (body.startswith('Fix') or body.startswith('eq')):
                        fix_at = ci
                        break
                seg_end = fix_at if fix_at is not None else n

            const_lo = c32_at[0] if c32_at else f_start[0]
            for idx, start in enumerate(xor4_at):
                end = (xor4_at[idx + 1] - 1) if idx + 1 < len(xor4_at) else const_lo - 1
                stages[pre + "EXPAND:%d" % (16 + idx)] = (start, end)
            stages[pre + "CONST"] = (const_lo, f_start[0] - 1)
            for i in range(r):
                lo = f_start[i]
                hi = (f_start[i + 1] - 1) if (i + 1) in f_start else final_start - 1
                stages[pre + "ROUND:%d" % i] = (lo, hi)
            stages[pre + "FINAL"] = (final_start, seg_end - 1)
            last_end = seg_end

        if last_end < n:
            stages["FIX"] = (last_end, n - 1)

        return OrderedDict((k, v) for k, v in stages.items() if v[1] >= v[0])

    # ------------------------------------------------------------- validation
    def validate(self):
        problems = []
        spans = sorted(self.stages.values())
        expect = 0
        for (lo, hi) in spans:
            if lo != expect:
                problems.append("coverage break at clause %d (next stage starts %d)" % (expect, lo))
            expect = hi + 1
        if expect != self.true_cc:
            problems.append("coverage ends at %d, expected %d" % (expect, self.true_cc))
        if self.header_cc != self.true_cc:
            problems.append("HEADER MISMATCH: declares %d, body has %d (delta %d)"
                            % (self.header_cc, self.true_cc, self.header_cc - self.true_cc))
        return problems

    def clause_range(self, *stage_names):
        """Union of clause indices for the named stages, as a sorted index list."""
        out = []
        for s in stage_names:
            lo, hi = self.stages[s]
            out.extend(range(lo, hi + 1))
        return sorted(set(out))

    def write_repaired(self, out_path):
        with open(out_path, 'w') as g:
            g.write("p cnf %d %d\n" % (self.true_vc, self.true_cc))
            for c in self.clauses:
                g.write(" ".join(map(str, c)) + " 0\n")
        return out_path
