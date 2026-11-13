#!/usr/bin/env python3
"""
minisat_profile.py -- measure what the v3 controls actually did, from per-rank glog files.

  minisat_profile.py LOGDIR COMPONENTS.json [--out FILE.json]

Needs a patch-v3 binary and GLOG_v >= 4 (MINISAT_RESULT is VLOG(3); solution lines are VLOG(4)).

Reports
  * refinement descendants per PARENT message: emissions from intermediate nodes grouped by the
    parent message they answered (identified from the master's dispatch to that worker). This is
    the REALISED count; the configured L is only a worker-local retirement threshold, so
    concurrent workers can overshoot it.
  * per-role conflicts per completed job (result 0 or 1), summed over all quanta of that job:
    quantiles B50/B80/B95/B99 -- use these to choose budgets.
  * per-message service: quanta received, total conflicts, completion status (budget fairness).
  * budget-exhaustion counts by role, and REFINEMENT_RETIRE events.
"""
import re, sys, json, glob, os, hashlib
from datetime import datetime
from collections import defaultdict

LINE = re.compile(r'^[IWEF]\d{4} (\d\d:\d\d:\d\d\.\d+)\s+\d+\s+\S+\] (.*)$')
DISP = re.compile(r'sending message to worker (\d+) message about node (-?\d+) assignments:([-\d ]*)')
EMIT = re.compile(r'WORKER (\d+): Solution message mapped to: message about node (-?\d+) ')
RES = re.compile(r'WORKER (\d+): MINISAT_RESULT node=(\d+) role=(\w+) msg=([0-9a-f]+) result=(\d) '
                 r'conflicts_this_call=(\d+) conflicts_total=(\d+)')
EXH = re.compile(r'WORKER (\d+): MINISAT_BUDGET_EXHAUSTED node=(\d+) role=(\w+)')
RET = re.compile(r'WORKER (\d+): REFINEMENT_RETIRE node=(\d+) msg=([0-9a-f]+) emitted_by_this_worker=(\d+)')


def q(xs, p):
    if not xs:
        return None
    xs = sorted(xs); return xs[min(len(xs) - 1, int(p * len(xs)))]


def main(logdir, comps, outp):
    C = json.load(open(comps))
    role = {int(k): v for k, v in C["node_roles"].items()}
    recs = []
    for p in (q for q in glob.glob(os.path.join(logdir, "*")) if not os.path.islink(q)):
        for l in open(p, errors="replace"):
            m = LINE.match(l.rstrip("\n"))
            if m:
                recs.append((datetime.strptime(m.group(1), "%H:%M:%S.%f"), m.group(2)))
    recs.sort(key=lambda r: r[0])
    cur = {}                                   # worker -> (node, parent fingerprint)
    desc = defaultdict(int)                    # (node, parent fp) -> descendants emitted
    job = defaultdict(lambda: {"quanta": 0, "conflicts": 0, "result": None, "role": None, "node": None})
    exhausted = defaultdict(int); retire = []
    for _, body in recs:
        d = DISP.search(body)
        if d:
            w, node = int(d.group(1)), int(d.group(2))
            cur[w] = (node, hashlib.sha256(d.group(3).strip().encode()).hexdigest()[:16])
            continue
        e = EMIT.search(body)
        if e:
            w, node = int(e.group(1)), int(e.group(2))
            if role.get(node) == "REFINEMENT" and cur.get(w, (None,))[0] == node:
                desc[cur[w]] += 1
            continue
        r = RES.search(body)
        if r:
            w, node, rl, msg, res, cc = int(r.group(1)), int(r.group(2)), r.group(3), r.group(4), int(r.group(5)), int(r.group(6))
            j = job[(node, msg)]; j["quanta"] += 1; j["conflicts"] += cc; j["role"] = rl; j["node"] = node
            if res in (0, 1):
                j["result"] = res
            continue
        x = EXH.search(body)
        if x:
            exhausted[x.group(3)] += 1; continue
        t = RET.search(body)
        if t:
            retire.append(int(t.group(4)))
    counts = list(desc.values())
    by_role = defaultdict(list)
    for j in job.values():
        if j["result"] is not None:
            by_role[j["role"]].append(j["conflicts"])
    out = {
        "refinement": {"parents": len(counts), "descendants_total": sum(counts),
                       "descendants_per_parent_max": max(counts) if counts else 0,
                       "descendants_per_parent_mean": (sum(counts) / len(counts)) if counts else 0,
                       "histogram": {str(k): counts.count(k) for k in sorted(set(counts))},
                       "retire_events": len(retire)},
        "conflicts_per_completed_job": {rl: {"jobs": len(v), "B50": q(v, .5), "B80": q(v, .8), "B95": q(v, .95), "B99": q(v, .99)}
                                        for rl, v in by_role.items()},
        "budget_exhausted_by_role": dict(exhausted),
        "service": {"jobs": len(job), "completed": sum(1 for j in job.values() if j["result"] is not None),
                    "max_quanta_one_job": max((j["quanta"] for j in job.values()), default=0),
                    "unfinished_jobs_conflicts_top": sorted((j["conflicts"] for j in job.values() if j["result"] is None), reverse=True)[:5]},
    }
    print(json.dumps(out, indent=1))
    if outp:
        json.dump(out, open(outp, "w"), indent=1)


if __name__ == "__main__":
    a = sys.argv[1:]
    if len(a) < 2:
        sys.exit(__doc__)
    main(a[0], a[1], a[a.index("--out") + 1] if "--out" in a else None)
