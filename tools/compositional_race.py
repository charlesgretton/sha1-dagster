#!/usr/bin/env python3
"""
compositional_race.py -- build a multi-component DAG for reduced-round SHA-1 over ONE
shared clause database, for Dagster (v15).

Each component is a disjoint subgraph. Every reporting terminal contains the COMPLETE
exact formula, so any reported model satisfies the original formula (verify anyway).

  build RAW.cnf --out-prefix OUT [--components LIST] [--planted FILE]

Emits OUT.cnf, OUT.dag, OUT.components.json, OUT.manifest.json.
If a planted preimage is available (--planted, or RAW.planted next to RAW), every
component's nodes are checked on the planted path (see planted_path_check).

Components (default set):
  direct    [F]                                    exact control in the same allocation
  low12     R12 -> F|z12
  low16     R16 -> F|z16                           the measured positive decomposition
  low20     R20 -> F|z20
  staged    R8 -> R16|z8 -> F|z16                  cumulative R16 in the middle node
  part16    (R16 & D_ij) -> T_ij, four SEPARATE    partition units on the source only;
            two-node pipelines, one per partition   only the trace reaches T_ij
  actv16    R16 -> F|(z16 + carry-out of bit 15)   width FIXED at 16 (the name is the spec)

part16 topology (v15 fix): each partition source has its OWN exact terminal. In Dagster,
several incoming edges to one node define a JOIN (the master combines compatible parent
messages), not an OR of alternative parents; v14 wired all four sources into one
terminal and so built a four-way consistency join by mistake.

Clause order: the DAG format stores each node's clause indices as sorted ranges
(write_dag/compress), so Dagster sees a canonical, sorted clause list. clause_sha16 in
the manifest hashes that canonical list. Clause-ORDER variants therefore cannot be
expressed by listing indices differently; they would need duplicated clause blocks.
"""
import os, sys, json, hashlib, re
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sha1_cnf import Sha1Cnf
from sha1_dagify import write_dag, validate
import trace_relaxation as tr

ACTV_BITS = 16
DEFAULT = "direct,low12,low16,low20,staged,part16,actv16"


def sha16(seq):
    return hashlib.sha256((",".join(map(str, seq))).encode()).hexdigest()[:16]


def var_index(c):
    """var -> (label, offset, stage). Blocks declared after f[i] belong to ROUND:i. Every
    round's 5-operand addition uses the same number of carry chains (measured from round 0);
    carry chains beyond that count after the LAST round are the feed-forward adders: FINAL."""
    f_idx = [i for i, (s0, n, lab, _) in enumerate(c.var_blocks) if re.match(r'f\[\d+\]$', lab)]
    last_f = max(f_idx)
    # carry chains per round, from round 0 (between f[0] and f[1])
    per_round = sum(1 for i in range(f_idx[0], f_idx[1]) if c.var_blocks[i][2] == "carry")
    info, stage, carries_in_stage, final = {}, None, 0, False
    for i, (s0, n, lab, _inst) in enumerate(c.var_blocks):
        m = re.match(r'f\[(\d+)\]$', lab)
        if m:
            stage = "ROUND:%s" % m.group(1); carries_in_stage = 0
        if lab == "carry" and stage is not None:
            if i > last_f and carries_in_stage >= per_round:
                final = True
            carries_in_stage += 1
        st = "FINAL" if final else stage
        for off in range(n):
            info[s0 + off] = (lab, off, st)
    return info


def unit_propagate(clauses, extra):
    """queue-based unit propagation; returns assignment dict or None on conflict"""
    cls = clauses + [[l] for l in extra]
    occ = defaultdict(list)
    for i, cl in enumerate(cls):
        for l in cl:
            occ[-l].append(i)
    val = {}
    q = [cl[0] for cl in cls if len(cl) == 1]
    while q:
        l = q.pop(); v = abs(l)
        if v in val:
            if val[v] != (l > 0):
                return None
            continue
        val[v] = (l > 0)
        for i in occ[l]:
            if any(abs(x) in val and val[abs(x)] == (x > 0) for x in cls[i]):
                continue
            un = [x for x in cls[i] if abs(x) not in val]
            if not un:
                return None
            if len(un) == 1:
                q.append(un[0])
    return val


def build(raw, out_prefix, want, planted_path=None):
    try:
        c = Sha1Cnf(raw)
    except RuntimeError as e:
        sys.exit("ERROR: %s is not a raw sha1gen CNF (%s). Pass sha1gen's own output WITH its "
                 "comment lines (the harness's cnfs/raw/ copy), not a repaired cnfs/solver/ CNF." % (raw, e))
    r = c.rounds
    base_cc = c.true_cc
    full = list(range(base_cc))                 # the exact formula: ORIGINAL clauses only
    reporting = [v for i in range(16) for v in c.w(i)]
    info = var_index(c)

    nodes, edges, comps, roles, node_comp = [], {}, [], {}, {}

    def add_node(clauses, role, comp):
        nodes.append(list(clauses)); i = len(nodes) - 1
        roles[i] = role; node_comp[i] = comp
        return i

    def trace_bits(k):
        return [v for i in range(5, r) for b, v in enumerate(c.a(i)) if b < k]

    relax_cache = {}
    def relax(k):
        if k not in relax_cache:
            relax_cache[k] = tr.relaxation(c, k)[0]
        return relax_cache[k]

    def comp_add(rec):
        rec["id"] = len(comps); comps.append(rec); return rec["id"]

    if "direct" in want:
        cid = len(comps)
        t = add_node(full, "DIRECT_TERMINAL", cid)
        comp_add({"name": "direct", "nodes": {"terminal": t}, "edges": []})

    for name, k in (("low12", 12), ("low16", 16), ("low20", 20)):
        if name not in want:
            continue
        cid = len(comps)
        s = add_node(relax(k), "SOURCE", cid)
        t = add_node(full, "EXACT_TERMINAL", cid)
        edges[(s, t)] = trace_bits(k)
        comp_add({"name": name, "trace_bits": k, "nodes": {"source": s, "terminal": t},
                  "edges": [[s, t, len(edges[(s, t)])]]})

    if "staged" in want:
        cid = len(comps)
        s8 = add_node(relax(8), "SOURCE", cid)
        r16 = add_node(relax(16), "REFINEMENT", cid)          # CUMULATIVE low-16 relaxation
        t = add_node(full, "EXACT_TERMINAL", cid)
        edges[(s8, r16)] = trace_bits(8)
        edges[(r16, t)] = trace_bits(16)
        comp_add({"name": "staged", "nodes": {"source": s8, "refine": r16, "terminal": t},
                  "edges": [[s8, r16, len(edges[(s8, r16)])], [r16, t, len(edges[(r16, t)])]],
                  "note": "q8 = survival of z8 across the source->refine edge"})

    if "part16" in want:
        part_vars = [c.w(0)[0], c.w(1)[0]]                    # bit 0 of w[0] and of w[1]
        for combo in range(4):
            cid = len(comps)
            units = [(v if (combo >> b) & 1 else -v) for b, v in enumerate(part_vars)]
            extra = []
            for lit in units:                                  # real unit clauses, shared CNF
                c.clauses.append([lit]); extra.append(len(c.clauses) - 1)
            s = add_node(relax(16) + extra, "SOURCE", cid)
            t = add_node(full, "EXACT_TERMINAL", cid)         # its OWN terminal (v15 fix)
            edges[(s, t)] = trace_bits(16)
            comp_add({"name": "part16_%d%d" % (combo & 1, (combo >> 1) & 1), "family": "part16",
                      "partition_literals": units, "partition_unit_clauses": extra,
                      "nodes": {"source": s, "terminal": t},
                      "edges": [[s, t, len(edges[(s, t)])]],
                      "note": "partition steers generation only; terminal gets the trace"})

    carry_detail = []
    if "actv16" in want:
        cid = len(comps)
        k = ACTV_BITS
        s = add_node(relax(k), "SOURCE", cid)
        t = add_node(full, "EXACT_TERMINAL", cid)
        carry = sorted(v for v, (lab, off, st) in info.items() if lab == "carry" and off == k - 1)
        ordinal, seen = {}, defaultdict(int)
        for (s0, n, lab, _inst) in c.var_blocks:
            if lab == "carry":
                st = info[s0][2]
                ordinal[s0] = seen[st]; seen[st] += 1
        starts = sorted(b for (b, n, lab, _) in c.var_blocks if lab == "carry")
        for v in carry:
            s0 = max(b for b in starts if b <= v)
            carry_detail.append({"var": v, "stage": info[v][2], "carry_block": ordinal[s0],
                                 "offset": k - 1})
        edges[(s, t)] = trace_bits(k) + carry
        comp_add({"name": "actv16", "trace_bits": k, "carry_boundary_vars": len(carry),
                  "nodes": {"source": s, "terminal": t},
                  "edges": [[s, t, len(edges[(s, t)])]], "activation_corpus": [],
                  "note": "trace + carry-out of bit %d of every adder carry chain (label "
                          "'carry', offset %d); identities in carry_boundary" % (k - 1, k - 1)})

    # ---------- structural validation ------------------------------------------------
    errs, warns = validate(c, nodes, edges, reporting)
    node_vars = []
    for cls in nodes:
        vs = set()
        for ci in cls:
            vs.update(abs(l) for l in c.clauses[ci])
        node_vars.append(vs)
    carry_checks = {}
    if carry_detail:
        a = [x for x in comps if x["name"] == "actv16"][0]
        s, t = a["nodes"]["source"], a["nodes"]["terminal"]
        in_src = sum(1 for d in carry_detail if d["var"] in node_vars[s])
        in_term = sum(1 for d in carry_detail if d["var"] in node_vars[t])
        by_stage = defaultdict(int)
        for d in carry_detail:
            by_stage[d["stage"]] += 1
        carry_checks = {"count": len(carry_detail), "present_in_source": in_src,
                        "present_in_terminal": in_term, "per_stage": dict(by_stage),
                        "without_stage_identity": sum(1 for d in carry_detail if d["stage"] is None)}
        if in_src != len(carry_detail) or in_term != len(carry_detail):
            errs.append("actv16: %d/%d carry vars in source, %d/%d in terminal"
                        % (in_src, len(carry_detail), in_term, len(carry_detail)))
        if carry_checks["without_stage_identity"]:
            errs.append("actv16: carry vars without stage identity")

    # ---------- planted-path check, every node of every component ----------------------
    planted = None
    pp = planted_path or (raw + ".planted" if os.path.exists(raw + ".planted") else None)
    if pp:
        planted = planted_path_check(c, base_cc, nodes, edges, comps, pp, carry_detail)
        if "error" in planted:
            errs.append("planted path: " + planted["error"])
        else:
            for comp_name, res in planted["components"].items():
                st = res["status"]
                if st.startswith("UNCHECKED"):
                    warns.append("planted path: component %s -> %s (not fatal)" % (comp_name, st))
                elif st not in ("REACHABLE", "PLANTED_IN_OTHER_PARTITION"):
                    errs.append("planted path: component %s -> %s" % (comp_name, st))
            for comp in comps:
                if comp.get("family") == "part16" and comp.get("planted_partition") \
                        and planted["components"][comp["name"]]["status"] != "REACHABLE":
                    errs.append("planted path: %s holds the planted message but is not reachable"
                                % comp["name"])

    # ---------- write --------------------------------------------------------------------
    write_dag(out_prefix + ".dag", nodes, edges, reporting)
    c.write_repaired(out_prefix + ".cnf")
    node_recs = [{"node": i, "component": node_comp[i], "role": roles[i],
                  "clause_count": len(cls), "clause_sha16_canonical": sha16(sorted(cls)),
                  "var_count": len(node_vars[i]),
                  "is_full_formula": sorted(cls) == full}
                 for i, cls in enumerate(nodes)]
    edge_recs = [{"src": s, "dst": d, "vars": vs, "vars_sha16": sha16(vs)}
                 for (s, d), vs in sorted(edges.items())]
    json.dump({"components": comps, "nodes": node_recs, "edges": edge_recs,
               "node_component": node_comp, "node_roles": roles,
               "carry_boundary": carry_detail, "carry_checks": carry_checks,
               "trace16_vars": trace_bits(16),
               "planted_path": planted},
              open(out_prefix + ".components.json", "w"), indent=1)
    json.dump({"raw": os.path.abspath(raw), "rounds": r,
               "base_clauses": base_cc, "shared_clauses": c.true_cc,
               "repaired_cnf_sha256": hashlib.sha256(open(out_prefix + ".cnf", "rb").read()).hexdigest(),
               "dag_sha256": hashlib.sha256(open(out_prefix + ".dag", "rb").read()).hexdigest(),
               "n_nodes": len(nodes), "n_components": len(comps),
               "validation": {"errors": errs, "warnings": warns},
               "clause_order_note": "node clause lists are canonicalised (sorted ranges) by the DAG format",
               "launch_race": "mpirun -n N dagster -m 4 -q 2 -b 0 -e 0 -i 1 -o win.txt DAG CNF",
               "launch_calibrate_joint": "mpirun -n N dagster -m 4 -q 2 -b 0 -e 2 -i 1 -o cal.txt DAG CNF"},
              open(out_prefix + ".manifest.json", "w"), indent=1)

    print("components: %s" % ", ".join(x["name"] for x in comps))
    print("nodes: %d  edges: %d  shared clauses: %d (base %d)" % (len(nodes), len(edges), c.true_cc, base_cc))
    if carry_checks:
        print("actv16 carries: %(count)d, in source %(present_in_source)d, in terminal "
              "%(present_in_terminal)d" % carry_checks + ", per stage: " +
              ", ".join("%s=%d" % kv for kv in sorted(carry_checks["per_stage"].items(),
                        key=lambda kv: (kv[0] == "FINAL", int(kv[0].split(':')[1]) if ':' in kv[0] else 99))))
    if planted and "components" in planted:
        for n_, res in planted["components"].items():
            print("planted path %-10s %s%s" % (n_, res["status"], ("  (" + res["detail"] + ")") if res.get("detail") else ""))
    print("validation: %s" % ("OK" if not errs else "; ".join(errs)))
    for w in warns:
        print("  warning: %s" % w)
    print("wrote %s.{cnf,dag,components.json,manifest.json}" % out_prefix)
    return errs


def planted_path_check(c, base_cc, nodes, edges, comps, planted_file, carry_detail):
    """Compute the planted full execution P by unit propagation from the planted message,
    then for every node N with incoming edge vars I and outgoing edge vars O check with a
    SOLVER that clauses(N) & P|I & P|O is SAT (terminals: clauses(N) & P|I). A component is
    REACHABLE only if every node passes. part16 components whose partition disagrees with
    the planted message are reported as PLANTED_IN_OTHER_PARTITION (expected for 3 of 4)."""
    from pysat_compat import Solver as Cadical153, SOLVER_NAME
    words = [int(l, 16) for l in open(planted_file) if l.strip()]
    msg = [(v if (words[i] >> b) & 1 else -v) for i in range(16) for b, v in enumerate(c.w(i))]
    P = unit_propagate(c.clauses[:base_cc], msg)
    if P is None:
        return {"error": "planted message conflicts with the formula"}
    lit = lambda v: v if P.get(v) else -v
    incoming = defaultdict(list); outgoing = defaultdict(list)
    for (s, d), vs in edges.items():
        outgoing[s] += vs; incoming[d] += vs
    msg_vars = [v for i in range(16) for v in c.w(i)]          # the 512 message bits
    BUDGET = 200000                                            # backstop: never unbounded
    out = {}
    for comp in comps:
        ids = []
        for v in comp["nodes"].values():
            ids += v if isinstance(v, list) else [v]
        status, detail = "REACHABLE", ""
        for nd in ids:
            node_vars = {abs(l) for ci in nodes[nd] for l in c.clauses[ci]}
            # fix the node's edge variables AND whichever planted message bits it contains:
            # with the message fixed, the planted assignment is forced by propagation, so the
            # check is near-instant even for a full-formula terminal (was a full r=22 solve).
            fixset = (set(incoming[nd]) | set(outgoing[nd]) | set(msg_vars)) & node_vars
            assum = [lit(v) for v in sorted(fixset)]
            s = Cadical153(bootstrap_with=[c.clauses[i] for i in nodes[nd]])
            try:
                s.conf_budget(BUDGET); ok = s.solve_limited(assumptions=assum)
            except Exception:
                ok = s.solve(assumptions=assum)
            s.delete()
            if ok is None:                                     # hit the budget: report, don't hang
                status = "UNCHECKED_BUDGET_AT_NODE_%d" % nd; break
            if not ok:
                if comp.get("family") == "part16" and nd == comp["nodes"]["source"]:
                    agree = all(P.get(abs(l)) == (l > 0) for l in comp["partition_literals"])
                    status = "PLANTED_IN_OTHER_PARTITION" if not agree else "BLOCKED_AT_NODE_%d" % nd
                else:
                    status = "BLOCKED_AT_NODE_%d" % nd
                break
        if comp.get("family") == "part16":
            agree = all(P.get(abs(l)) == (l > 0) for l in comp["partition_literals"])
            comp["planted_partition"] = agree
            if status == "REACHABLE" and not agree:
                detail = "planted TRACE emittable here although the planted message is in another partition"
            if agree:
                detail = "holds the planted message"
        if comp["name"] == "actv16" and status == "REACHABLE":
            detail = "planted carry values consistent with the source relaxation"
        out[comp["name"]] = {"status": status, "detail": detail}
    return {"planted_file": os.path.abspath(planted_file), "solver": SOLVER_NAME, "components": out}


if __name__ == "__main__":
    a = sys.argv[1:]
    if len(a) < 2 or a[0] != "build":
        sys.exit(__doc__)
    opt = {"out-prefix": "compositional_race", "components": DEFAULT, "planted": None}
    for i, x in enumerate(a):
        if x.startswith("--"):
            opt[x[2:]] = a[i + 1]
    if "bits" in opt:
        print("note: --bits is ignored; each component's width is fixed by its name", file=sys.stderr)
    errs = build(a[1], opt["out-prefix"], set(opt["components"].split(",")), opt["planted"])
    sys.exit(1 if errs else 0)
