#!/usr/bin/env python3
"""
portfolio_dag.py OUT_PREFIX COMPONENT [COMPONENT ...]

Build ONE Dagster DAG made of disconnected components (independent searchers)
over ONE shared CNF. Dagster assigns each disconnected component its own
subgraph_index (Dag.cpp), so:
    -e 0   components race; the first terminal solution anywhere ends the run
    -e 2   one solution per component (per subgraph), the rest keep going

COMPONENT is  RAW.cnf:STRATEGY   with STRATEGY one of
    direct      one node, the full formula
    traceK      two nodes: low-K adder-slice relaxation -> full formula,
                edge = low K bits of every free state word  (e.g. trace16)

Components naming the SAME raw CNF share its variables and clauses (a portfolio
of pipelines on one target). Different raw CNFs are placed in disjoint variable
ranges (a multi-target campaign; use -e 2).

Writes OUT_PREFIX.cnf, OUT_PREFIX.dag, and OUT_PREFIX.map.json, which records each
component's node range, target, variable offset and message variables, so every
reported solution can be attributed to its component and verified.
"""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sha1_cnf import Sha1Cnf
from sha1_dagify import write_dag
from trace_relaxation import relaxation

prefix, specs = sys.argv[1], sys.argv[2:]
targets, order = {}, []                      # raw path -> (Sha1Cnf, var offset, clause offset)
voff = coff = 0
for sp in specs:
    raw, _ = sp.rsplit(':', 1)
    if raw not in targets:
        c = Sha1Cnf(raw)
        targets[raw] = (c, voff, coff); order.append(raw)
        voff += c.true_vc; coff += c.true_cc

clauses = []
for raw in order:
    c, vo, _ = targets[raw]
    for cl in c.clauses:
        clauses.append([(l + vo) if l > 0 else (l - vo) for l in cl])

nodes, edges, comps, reporting = [], {}, [], []
for k, sp in enumerate(specs):
    raw, strat = sp.rsplit(':', 1)
    c, vo, co = targets[raw]
    full = list(range(co, co + c.true_cc))
    msg = [v + vo for i in range(16) for v in c.w(i)]
    first = len(nodes)
    if strat == 'direct':
        nodes.append(full)
    elif strat.startswith('trace'):
        bits = int(strat[5:])
        keep, trace = relaxation(c, bits)
        nodes.append([co + i for i in keep])
        nodes.append(full)
        edges[(first, first + 1)] = [v + vo for v in trace]
    else:
        sys.exit("unknown strategy %s" % strat)
    comps.append(dict(component=k, strategy=strat, target=raw, nodes=list(range(first, len(nodes))),
                      var_offset=vo, message_vars=[msg[0], msg[-1]]))
    for v in msg:
        if v not in reporting:
            reporting.append(v)

with open(prefix + '.cnf', 'w') as f:
    f.write("p cnf %d %d\n" % (voff, len(clauses)))
    for cl in clauses:
        f.write(" ".join(map(str, cl)) + " 0\n")
write_dag(prefix + '.dag', nodes, edges, sorted(reporting))
json.dump(dict(components=comps, total_vars=voff, total_clauses=len(clauses)),
          open(prefix + '.map.json', 'w'), indent=1)
for cm in comps:
    print("component %d: %-8s nodes %s  target %s" % (cm['component'], cm['strategy'], cm['nodes'], os.path.basename(cm['target'])))
print("shared CNF: %d vars, %d clauses; DAG: %d nodes, %d edges" % (voff, len(clauses), len(nodes), len(edges)))
