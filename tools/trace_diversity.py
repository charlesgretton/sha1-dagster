#!/usr/bin/env python3
"""
trace_diversity.py -- is the compositional race generating DIFFERENT exact-search work?

  trace_diversity.py events PREFIX.events.jsonl [--out FILE.json]
      From a real Dagster run (tools/dagster_events.py output):
        * per source node: emissions, unique traces, emission duplicate fraction D
        * per terminal: dispatches vs unique traces dispatched (balancer duplication)
        * pairwise Jaccard of emitted traces across components, on the COMMON low-16
          trace variables (only components whose edge contains all of them)
        * SAT-trace sets per component and their pairwise Jaccard (useful-trace overlap)

  trace_diversity.py offline RAW.cnf DAG CNF COMPONENTS.json [--per-source N] [--out F]
      Reproducible structural reference without Dagster: enumerate N models of each
      SOURCE node's clause set (pysat MiniSat22, blocking each projected trace, which is
      what Dagster's reset_solver_for_next_solution does), fingerprint them, and compute
      the same overlap measures. Also a REPLICA test: two independent, uncoordinated
      solver instances on one source -> overlap of their first N traces (the collision
      hazard Dagster's solution refresh is there to break).

Reading the numbers. Relaxations have astronomically many models, so traces from
different sources are expected to be almost always distinct: raw Jaccard near 0 is the
NULL expectation, not evidence of useful diversity. What matters is (a) emission
duplicate fraction within a source (wasted source work), (b) terminal dispatch
duplication (wasted terminal work), and (c) overlap and rate of SAT (useful) traces.
"""
import sys, json, hashlib
from collections import defaultdict
from itertools import combinations


def jaccard(a, b):
    return (len(a & b) / len(a | b)) if (a or b) else None


def from_events(path):
    ev = [json.loads(l) for l in open(path)]
    emit_fp = defaultdict(list); emit_t16 = defaultdict(set)
    disp = defaultdict(list); sat = defaultdict(set)
    for e in ev:
        if e.get("ev") == "EMIT" and e.get("fp"):
            emit_fp[(e["component"], e["node"])].append(e["fp"])
            if e.get("fp_t16"):
                emit_t16[e["component"]].add(e["fp_t16"])
        if e.get("ev") == "DISPATCH" and (e.get("role") or "").endswith("TERMINAL") and e.get("fp"):
            disp[(e["component"], e["node"])].append(e["fp"])
        if e.get("ev") == "EMIT" and (e.get("role") or "").endswith("TERMINAL") and e.get("solved_trace_fp_t16"):
            sat[e["component"]].add(e["solved_trace_fp_t16"])
    out = {"sources": {}, "terminals": {}, "jaccard_emitted_t16": {}, "jaccard_sat_t16": {},
           "sat_traces_by_component": {k: len(v) for k, v in sat.items()}}
    for (comp, node), fps in sorted(emit_fp.items(), key=lambda x: x[0][1]):
        out["sources"]["%s/node%d" % (comp, node)] = {
            "emitted": len(fps), "unique": len(set(fps)),
            "emission_duplicate_fraction": round(1 - len(set(fps)) / len(fps), 4)}
    for (comp, node), fps in sorted(disp.items(), key=lambda x: x[0][1]):
        out["terminals"]["%s/node%d" % (comp, node)] = {
            "dispatches": len(fps), "unique_traces": len(set(fps)),
            "workers_per_trace": round(len(fps) / len(set(fps)), 3)}
    for a, b in combinations(sorted(emit_t16), 2):
        out["jaccard_emitted_t16"]["%s|%s" % (a, b)] = jaccard(emit_t16[a], emit_t16[b])
    for a, b in combinations(sorted(sat), 2):
        out["jaccard_sat_t16"]["%s|%s" % (a, b)] = jaccard(sat[a], sat[b])
    return out


def read_dag_nodes(dag_path):
    nodes, sec = {}, None
    for line in open(dag_path):
        line = line.rstrip("\n")
        if line in ("GRAPH:", "CLAUSES:", "REPORTING:"):
            sec = line; continue
        if sec == "CLAUSES:" and ":" in line:
            i, rng = line.split(":", 1)
            idx = []
            for part in rng.split(","):
                if "-" in part:
                    lo, hi = part.split("-"); idx += list(range(int(lo), int(hi) + 1))
                elif part:
                    idx.append(int(part))
            nodes[int(i)] = idx
    return nodes


def offline(raw, dag, cnf, comps_path, per_source):
    from pysat.solvers import Minisat22
    C = json.load(open(comps_path))
    clauses = [[int(x) for x in l.split()[:-1]] for l in open(cnf) if l.strip() and l[0] not in "cp"]
    nodes = read_dag_nodes(dag)
    t16 = set(C["trace16_vars"])
    out_vars = defaultdict(list)
    for e in C["edges"]:
        out_vars[e["src"]] += e["vars"]
    role = {int(k): v for k, v in C["node_roles"].items()}
    cname = {c["id"]: c["name"] for c in C["components"]}
    ncomp = {int(k): v for k, v in C["node_component"].items()}

    def enumerate_source(nd, n):
        s = Minisat22(bootstrap_with=[clauses[i] for i in nodes[nd]])
        vs = out_vars[nd]; seq = []
        for _ in range(n):
            if not s.solve():
                break
            m = {abs(l): l > 0 for l in s.get_model()}
            z = [(v if m.get(v) else -v) for v in vs]
            seq.append(z); s.add_clause([-x for x in z])
        s.delete(); return seq

    fp = lambda lits: hashlib.sha256(",".join(map(str, sorted(lits, key=abs))).encode()).hexdigest()[:20]
    per = {}
    for nd, r in sorted(role.items()):
        if r != "SOURCE":
            continue
        seq = enumerate_source(nd, per_source)
        full = [fp(z) for z in seq]
        f16 = {fp([l for l in z if abs(l) in t16]) for z in seq
               if len([l for l in z if abs(l) in t16]) == len(t16)}
        per[nd] = {"component": cname[ncomp[nd]], "enumerated": len(seq),
                   "unique": len(set(full)), "t16": f16}
    res = {"per_source": {("%s/node%d" % (v["component"], k)): {"enumerated": v["enumerated"],
                          "unique": v["unique"], "has_t16": bool(v["t16"])} for k, v in per.items()},
           "jaccard_t16": {}}
    keys = [k for k in per if per[k]["t16"]]
    for a, b in combinations(keys, 2):
        res["jaccard_t16"]["%s|%s" % (per[a]["component"], per[b]["component"])] = jaccard(per[a]["t16"], per[b]["t16"])
    # replica test: two uncoordinated identical instances on the first low-16 source
    src = next((k for k in keys if per[k]["component"] == "low16"), keys[0] if keys else None)
    if src is not None:
        a = [fp(z) for z in enumerate_source(src, min(per_source, 100))]
        b = [fp(z) for z in enumerate_source(src, min(per_source, 100))]
        res["replica_test"] = {"source": per[src]["component"], "n": len(a),
                               "identical_sequences": a == b, "jaccard": jaccard(set(a), set(b))}
    return res


if __name__ == "__main__":
    a = sys.argv[1:]
    if not a or a[0] not in ("events", "offline"):
        sys.exit(__doc__)
    outp = a[a.index("--out") + 1] if "--out" in a else None
    if a[0] == "events":
        R = from_events(a[1])
    else:
        n = int(a[a.index("--per-source") + 1]) if "--per-source" in a else 200
        R = offline(a[1], a[2], a[3], a[4], n)
    print(json.dumps(R, indent=1))
    if outp:
        json.dump(R, open(outp, "w"), indent=1)
