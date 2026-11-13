#!/usr/bin/env python3
"""
dagster_events.py -- structured events from a Dagster glog log (GLOG_v>=4,
GLOG_logtostderr=true), joined to a compositional_race components.json.

  dagster_events.py LOG_OR_DIR COMPONENTS.json --out PREFIX

LOG_OR_DIR is either one log file, or a directory of per-rank glog files. Prefer the
directory: run with GLOG_logtostderr=false GLOG_log_dir=DIR so every rank writes its own
file. With GLOG_logtostderr=true all ranks share one stderr pipe and long message lines
can be interleaved (observed: a literal truncated to a bare '-'); such lines are counted
as malformed and skipped, never trusted.

Writes PREFIX.events.jsonl and PREFIX.summary.json.

Event sources (all verified against the Dagster source):
  DISPATCH  Master.cpp:154 VLOG(2) "sending message to worker W message about node X
            assignments: ..."            -> worker W is assigned destination node X
  EMIT      Worker.cpp:101 VLOG(4) "WORKER W: Solution message mapped to: message about
            node X assignments: ..."     -> worker W emitted a projected solution
  SOLUTION  Master.cpp:190 VLOG(3) "received MPI_TAG_SOLUTION from worker W"
  REQUEST   Master.cpp:179 VLOG(2) "received MPI_TAG_REQUEST_FOR_ASSIGNMENT from worker W"
  POLL      Master.cpp:219 VLOG(3) "received MPI_TAG_POLL_FOR_REASSIGNMENT from worker W"
  TERMINATE Master.cpp:134/213 "terminate trigger true"

Attribution: a worker's CURRENT node is the destination of the last DISPATCH it received.
(The worker line "loading into SAT; node N" prints m->from, not the destination, so it is
NOT used for attribution.) A SOLUTION from a worker whose current node is a terminal is a
terminal solution; its component is that node's component.

Fingerprint: sha256 of the sorted signed literals in the message, restricted to the edge
variables of the emitting node's outgoing edge (or the receiving node's incoming edge).
"""
import re, sys, json, hashlib
from collections import defaultdict, Counter
from datetime import datetime

LINE = re.compile(r'^[IWEF](\d{4}) (\d\d:\d\d:\d\d\.\d+)\s+\d+\s+(\S+)\] (.*)$')
MSG = re.compile(r'message about node (-?\d+) assignments:([-\d ]*)')


def fp(lits):
    return hashlib.sha256((",".join(map(str, sorted(lits, key=abs)))).encode()).hexdigest()[:20]


def parse(log, comps_path):
    C = json.load(open(comps_path))
    role = {int(k): v for k, v in C["node_roles"].items()}
    ncomp = {int(k): v for k, v in C["node_component"].items()}
    cname = {c["id"]: c["name"] for c in C["components"]}
    out_vars, in_vars = defaultdict(set), defaultdict(set)
    for e in C["edges"]:
        out_vars[e["src"]].update(e["vars"]); in_vars[e["dst"]].update(e["vars"])

    import glob, os
    # skip glog's <program>.INFO symlink: it points at one rank's file and would double-count it
    paths = sorted(p for p in glob.glob(os.path.join(log, "*")) if not os.path.islink(p)) if os.path.isdir(log) else [log]
    recs, malformed = [], 0
    for path in paths:
        for raw in open(path, errors="replace"):
            m = LINE.match(raw.rstrip("\n"))
            if m:
                recs.append((datetime.strptime(m.group(2), "%H:%M:%S.%f"), m.group(4)))
    recs.sort(key=lambda x: x[0])
    t0 = recs[0][0] if recs else None

    def lits_of(text):
        msg = MSG.search(text)
        if not msg:
            return None, None
        toks = msg.group(2).split()
        if not all(re.fullmatch(r'-?\d+', x) and x not in ("-",) for x in toks):
            return int(msg.group(1)), False
        return int(msg.group(1)), [int(x) for x in toks]

    t16 = set(C.get("trace16_vars", []))
    def fp16(lits):
        tl = [l for l in lits if abs(l) in t16]
        return fp(tl) if t16 and len(tl) == len(t16) else None
    events, cur, cur_fp16 = [], {}, {}
    for t, body in recs:
        ts = (t - t0).total_seconds()
        mm = re.search(r'sending message to worker (\d+) ', body)
        if mm:
            w = int(mm.group(1)); node, lits = lits_of(body)
            if lits is False:
                malformed += 1; lits = []
            cur[w] = node
            vs = in_vars.get(node, set())
            tl = [l for l in (lits or []) if abs(l) in vs]
            f16 = fp16(lits or [])
            cur_fp16[w] = f16
            events.append({"t": ts, "ev": "DISPATCH", "worker": w, "node": node,
                           "role": role.get(node), "component": cname.get(ncomp.get(node)),
                           "trace_vars": len(tl), "fp": fp(tl) if tl and len(tl) == len(vs) else None,
                           "fp_t16": f16})
            continue
        mm = re.search(r'WORKER (\d+): Solution message mapped to: (.*)', body)
        if mm:
            w = int(mm.group(1)); logged, lits = lits_of(mm.group(2))
            if lits is False:
                malformed += 1; lits = []
            # GROUND TRUTH: Dagster names the node that produced this solution (m2->to,
            # set from the message the worker was solving). Dispatch-based attribution can
            # lag (a dispatch may be logged while the worker is still emitting from its
            # previous job), so it is recorded only as a cross-check.
            src = logged if logged is not None else cur.get(w)
            mismatch = (logged is not None and cur.get(w) is not None and cur.get(w) != logged)
            vs = out_vars.get(src, set())
            tl = [l for l in (lits or []) if abs(l) in vs]
            rec = {"t": ts, "ev": "EMIT", "worker": w, "node": src,
                   "role": role.get(src), "component": cname.get(ncomp.get(src)),
                   "dispatch_node": cur.get(w), "attribution_mismatch": mismatch,
                   "trace_vars": len(tl), "fp": fp(tl) if tl and len(tl) == len(vs) else None,
                   "fp_t16": fp16(lits or [])}
            if (role.get(src) or "").endswith("TERMINAL"):
                # a terminal solution; the trace it answered is the one dispatched to it,
                # valid only when the dispatch record agrees on the node
                rec["solved_trace_fp_t16"] = cur_fp16.get(w) if cur.get(w) == src else None
            events.append(rec)
            continue
        for ev, pat in (("SOLUTION", r'received MPI_TAG_SOLUTION from worker (\d+)'),
                        ("REQUEST", r'received MPI_TAG_REQUEST_FOR_ASSIGNMENT from worker (\d+)'),
                        ("POLL", r'received MPI_TAG_POLL_FOR_REASSIGNMENT from worker (\d+)')):
            mm = re.search(pat, body)
            if mm:
                w = int(mm.group(1)); nd = cur.get(w)
                rec = {"t": ts, "ev": ev, "worker": w, "node": nd, "role": role.get(nd),
                       "component": cname.get(ncomp.get(nd))}
                if ev == "SOLUTION" and (role.get(nd) or "").endswith("TERMINAL"):
                    rec["fp_t16"] = cur_fp16.get(w)
                events.append(rec)
                break
        else:
            if "terminate trigger true" in body:
                events.append({"t": ts, "ev": "TERMINATE"})
    events.append({"t": 0.0, "ev": "META", "malformed_lines": malformed, "log_files": len(paths)})
    return C, role, ncomp, cname, events


def summarise(C, role, ncomp, cname, events, sample=0.5):
    per_node = defaultdict(Counter)
    emitted = defaultdict(list)
    dispatched_fp = defaultdict(list)
    first_term, first_term_dispatch_based = {}, {}
    mismatches = sum(1 for e in events if e.get("attribution_mismatch"))
    for e in events:
        nd = e.get("node")
        if nd is not None:
            per_node[nd][e["ev"]] += 1
        if e["ev"] == "EMIT" and e["fp"]:
            emitted[nd].append(e["fp"])
        if e["ev"] == "DISPATCH" and e["fp"] and (e["role"] or "").endswith("TERMINAL"):
            dispatched_fp[nd].append(e["fp"])
        if e["ev"] == "EMIT" and (e["role"] or "").endswith("TERMINAL"):
            first_term.setdefault(e["component"], e["t"])
        if e["ev"] == "SOLUTION" and (e["role"] or "").endswith("TERMINAL"):
            first_term_dispatch_based.setdefault(e["component"], e["t"])
    nodes = {}
    for nd, cnt in sorted(per_node.items()):
        em = emitted.get(nd, [])
        nodes[nd] = {"role": role.get(nd), "component": cname.get(ncomp.get(nd)),
                     "counts": dict(cnt),
                     "emitted": len(em), "emitted_unique": len(set(em)),
                     "emit_duplicate_fraction": (1 - len(set(em)) / len(em)) if em else None,
                     "terminal_dispatches": len(dispatched_fp.get(nd, [])),
                     "terminal_dispatch_unique": len(set(dispatched_fp.get(nd, [])))}
    # worker occupancy by component, sampled
    tmax = max((e["t"] for e in events), default=0.0)
    timeline, cur, i = [], {}, 0
    evs = sorted((e for e in events if e["ev"] == "DISPATCH"), key=lambda e: e["t"])
    t = 0.0
    while t <= tmax + 1e-9:
        while i < len(evs) and evs[i]["t"] <= t:
            cur[evs[i]["worker"]] = evs[i]["component"]; i += 1
        timeline.append({"t": round(t, 3), "workers_by_component": dict(Counter(cur.values()))})
        t += sample
    terminals = [int(n) for n, r in role.items() if r.endswith("TERMINAL")]
    starved = [n for n in terminals if per_node.get(n, Counter())["DISPATCH"] == 0]
    meta = [e for e in events if e["ev"] == "META"][0]
    return {"events": len(events), "duration_s": tmax, "nodes": nodes,
            "malformed_lines": meta["malformed_lines"], "log_files": meta["log_files"],
            "first_terminal_solution_s_by_component": first_term,
            "first_terminal_solution_dispatch_based_UNRELIABLE": first_term_dispatch_based,
            "emit_attribution_mismatches": mismatches,
            "terminals_never_dispatched": starved,
            "terminals_never_dispatched_components": sorted({cname.get(ncomp.get(n)) for n in starved}),
            "worker_occupancy_timeline": timeline}


if __name__ == "__main__":
    a = sys.argv[1:]
    if len(a) < 2:
        sys.exit(__doc__)
    out = a[a.index("--out") + 1] if "--out" in a else "dagster"
    C, role, ncomp, cname, events = parse(a[0], a[1])
    with open(out + ".events.jsonl", "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    S = summarise(C, role, ncomp, cname, events)
    json.dump(S, open(out + ".summary.json", "w"), indent=1)
    print("events: %d over %.1f s from %d log file(s); malformed (skipped) lines: %d"
          % (S["events"], S["duration_s"], S["log_files"], S["malformed_lines"]))
    print("node  role              component    DISPATCH  EMIT(unique)  SOLUTION  term-dispatch(unique)")
    for nd, v in S["nodes"].items():
        c = v["counts"]
        print("%4s  %-16s  %-11s  %8d  %5d(%5d)  %8d  %6d(%6d)" % (
            nd, v["role"], v["component"], c.get("DISPATCH", 0), v["emitted"], v["emitted_unique"],
            c.get("SOLUTION", 0), v["terminal_dispatches"], v["terminal_dispatch_unique"]))
    print("first terminal solution by component:", S["first_terminal_solution_s_by_component"] or "none")
    print("emissions whose logged node differed from the last dispatch: %d" % S["emit_attribution_mismatches"])
    print("terminals never dispatched:", S["terminals_never_dispatched"],
          "(components: %s)" % ", ".join(S["terminals_never_dispatched_components"]))
