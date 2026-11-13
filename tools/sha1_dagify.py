#!/usr/bin/env python3
"""
sha1_dagify.py -- build Dagster DAG files for sha1-sat preimage/collision CNFs.

    python3 sha1_dagify.py IN.cnf --out-cnf fixed.cnf --out-dag fixed.dag \
            [--strategy dual-frontier] [--bits 8] [--rounds-per-splitter 1]

Why this exists
---------------
Two things must happen before a sha1-sat instance will run under Dagster:

 1. HEADER REPAIR.  sha1-sat double-counts clauses in constant32() -- it bumps
    nr_clauses once itself and once per inner constant() call -- so the DIMACS
    header overshoots by 32 * (number of constant32 calls): 288 for preimage,
    576 for collision.  Dagster's Cnf::load_DIMACS_Cnf reads until
    cc == header_cc and throws ParsingException at EOF.  This tool rewrites the
    header from the true clause count.

 2. A DECOMPOSITION.  See the strategy notes below.

Strategy status -- no strategy is recommended by default
--------------------------------------------------------
Cut points here were originally chosen by maximising unit propagation. That
ranking did NOT predict measured runtime: the strategy built as a low-
propagation control ('naive-message') beat the recommended frontier cut
('single') on two of three measured seeds. No strategy is fastest across all
seeds, and with one observation per cell nothing about ranking is established.

Projected enumeration did not filter any tested instance, so a root node here
acts as a splitter rather than a filter, and its enumeration is serial
(Dag::generate_from_minimal gives a predecessor-free node one seed message).

See README.md sections 3-8 for the current evidence and the experiment plan.
Numbers are deliberately NOT duplicated here; this docstring carries only the
operational warning so it cannot go stale.

Strategies
----------
  dual-frontier  two splitters, forward + backward, joined into a terminal node
                 holding the full clause set. No longer recommended by default.
  single         one backward splitter + terminal.
  naive-message  one splitter on message bits. Built as a low-propagation
                 control; beats 'single' on two of three measured seeds. Name
                 kept so existing result CSVs stay comparable.
  onenode        no decomposition; baseline equivalent to utilities/onenodedag.sh

Run Dagster with  -e 0  (stop at first solution; the default 1 enumerates ALL
solutions, which for a preimage means never terminating) and leave -b at 0
(depth-first allocation, so workers push toward the terminal node).
"""

import argparse, sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sha1_cnf import Sha1Cnf


# ----------------------------------------------------------------- formatting
def compress(ids):
    """[1,2,3,7,9,10] -> '1-3,7,9-10'.  Dagster requires end>begin for a range
    in the CLAUSES and REPORTING blocks, so singletons are emitted bare."""
    ids = sorted(set(ids))
    if not ids:
        return ""
    out, lo, prev = [], ids[0], ids[0]
    for v in ids[1:] + [None]:
        if v is not None and v == prev + 1:
            prev = v
            continue
        if prev > lo:
            out.append("%d-%d" % (lo, prev))
        else:
            out.append("%d" % lo)
        if v is not None:
            lo = prev = v
    return ",".join(out)


def write_dag(path, nodes_clauses, edges, reporting):
    """
    nodes_clauses : list of clause-index lists, one per node (0-based)
    edges         : dict (src,dst) -> list of 1-based variable ids
    reporting     : list of 1-based variable ids

    Dagster's Dag.cpp matches the section headers with strcmp against
    "DAG-FILE\\n", "GRAPH:\\n", "CLAUSES:\\n", "REPORTING:\\n", so the file must
    use Unix line endings with no trailing spaces, and must end immediately
    after the reporting line (a trailing blank line makes the REPORTING loop
    read a non-digit and throw).
    """
    with open(path, "w", newline="\n") as f:
        f.write("DAG-FILE\n")
        f.write("NODES:%d\n" % len(nodes_clauses))
        f.write("GRAPH:\n")
        for (src, dst) in sorted(edges):
            f.write("%d->%d:%s\n" % (src, dst, compress(edges[(src, dst)])))
        f.write("CLAUSES:\n")
        for i, cls in enumerate(nodes_clauses):
            f.write("%d:%s\n" % (i, compress(cls)))
        f.write("REPORTING:\n")
        f.write("%s\n" % compress(reporting))


# ------------------------------------------------------------------ validation
def validate(c, nodes_clauses, edges, reporting):
    """Re-implements the soundness conditions Dagster relies on but never checks."""
    errs, warns = [], []

    covered = set()
    for cls in nodes_clauses:
        covered.update(cls)
    missing = c.true_cc - len(covered)
    if missing:
        errs.append("%d clauses appear in NO node -- they would never be enforced" % missing)

    # every edge variable must actually occur in the clauses of both endpoints,
    # otherwise the node's vc excludes it and the value cannot be carried
    # (Cnf::populate_from_clauses derives vc from the clauses present)
    for (src, dst), vs in edges.items():
        for nd in (src, dst):
            present = set()
            for ci in nodes_clauses[nd]:
                for l in c.clauses[ci]:
                    present.add(abs(l))
            absent = [v for v in vs if v not in present]
            if absent:
                errs.append("edge %d->%d carries %d var(s) absent from node %d's clauses "
                            "(e.g. %s)" % (src, dst, len(absent), nd, absent[:4]))

    # the reported solution is filtered to the TERMINAL node's variables
    terminals = [i for i in range(len(nodes_clauses))
                 if not any(s == i for (s, _d) in edges)]
    for t in terminals:
        present = set()
        for ci in nodes_clauses[t]:
            for l in c.clauses[ci]:
                present.add(abs(l))
        missing_rep = [v for v in reporting if v not in present]
        if missing_rep:
            errs.append("terminal node %d does not contain %d REPORTING vars -- "
                        "they cannot be recovered" % (t, len(missing_rep)))

    # acyclicity
    adj = {}
    for (s, d) in edges:
        adj.setdefault(s, []).append(d)
    seen, stack = set(), set()

    def dfs(n):
        if n in stack:
            errs.append("DAG is cyclic at node %d" % n)
            return
        if n in seen:
            return
        seen.add(n); stack.add(n)
        for m in adj.get(n, []):
            dfs(m)
        stack.discard(n)

    for n in range(len(nodes_clauses)):
        dfs(n)

    return errs, warns


# ------------------------------------------------------------------ strategies
def build(c, strategy, bits, rps):
    r = c.rounds
    allc = list(range(c.true_cc))

    def rng(*names):
        return c.clause_range(*[n for n in names if n in c.stages])

    const = ["CONST"] if "CONST" in c.stages else []
    fwd_stages = const + ["ROUND:%d" % i for i in range(rps)]
    bwd_stages = const + ["ROUND:%d" % i for i in range(r - rps, r)] + ["FINAL", "FIX"]

    if strategy == "onenode":
        return [allc], {}, c.w(0) and [v for i in range(16) for v in c.w(i)]

    reporting = [v for i in range(16) for v in c.w(i)]

    if strategy == "dual-frontier":
        n0 = rng(*fwd_stages)                 # forward splitter
        n1 = rng(*bwd_stages)                 # backward splitter
        n2 = allc                             # terminal: full formula
        e = {(0, 2): c.a(5)[:bits],
             (1, 2): c.a(r - 1)[:bits]}
        return [n0, n1, n2], e, reporting

    if strategy == "single":
        n0 = rng(*bwd_stages)
        n1 = allc
        return [n0, n1], {(0, 1): c.a(r - 1)[:bits]}, reporting

    if strategy == "naive-message":
        n0 = rng(*fwd_stages)
        n1 = allc
        return [n0, n1], {(0, 1): c.w(0)[:bits]}, reporting

    raise SystemExit("unknown strategy %s" % strategy)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cnf")
    ap.add_argument("--out-cnf", required=True)
    ap.add_argument("--out-dag", required=True)
    ap.add_argument("--strategy", default="dual-frontier",
                    choices=["dual-frontier", "single", "naive-message", "onenode"])
    ap.add_argument("--bits", type=int, default=8,
                    help="branch bits per splitter; jobs = 2^bits per splitter, "
                         "multiplied across splitters")
    ap.add_argument("--rounds-per-splitter", type=int, default=1,
                    help="how many SHA-1 rounds each splitter node carries")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()

    c = Sha1Cnf(a.cnf)
    struct = [p for p in c.validate() if "HEADER" not in p]
    if struct:
        raise SystemExit("structural parse failed: %s" % struct)

    c.write_repaired(a.out_cnf)
    nodes, edges, reporting = build(c, a.strategy, a.bits, a.rounds_per_splitter)
    errs, warns = validate(c, nodes, edges, reporting)
    write_dag(a.out_dag, nodes, edges, reporting)

    if not a.quiet:
        hdr = [p for p in c.validate() if "HEADER" in p]
        print("input      : %s  (r=%d, %d clauses, %d vars)"
              % (a.cnf, c.rounds, c.true_cc, c.true_vc))
        if hdr:
            print("header     : REPAIRED -- %s" % hdr[0].split(":", 1)[1].strip())
        print("strategy   : %s, %d bit(s) per splitter" % (a.strategy, a.bits))
        for i, nd in enumerate(nodes):
            kind = "terminal" if not any(s == i for (s, _d) in edges) else "splitter"
            print("  node %d   : %7d clauses  (%s)" % (i, len(nd), kind))
        for (s, d), vs in sorted(edges.items()):
            print("  edge %d->%d: %d vars -> <=%d cubes" % (s, d, len(vs), 2 ** len(vs)))
        if edges:
            tot = 1
            for vs in edges.values():
                tot *= 2 ** len(vs)
            print("  job count : up to %d terminal-node jobs" % tot)
        print("wrote      : %s , %s" % (a.out_cnf, a.out_dag))
        if errs:
            print("VALIDATION ERRORS:")
            for e in errs:
                print("  ! %s" % e)
        else:
            print("validation : OK")


if __name__ == "__main__":
    main()
