#!/usr/bin/env python3
"""
recover_solutions.py -- recover terminal-node solutions from a Dagster run WITHOUT relying
on the -o output file (which Dagster writes only at exit, after every worker has polled).

  recover_solutions.py logs LOGDIR COMPONENTS.json --out-dir DIR
      Needs GLOG_v >= 4. For a terminal node the worker fills its solution message with
      exactly the DAG's REPORTING literals (Worker.cpp: "else terminal node, filter by dag
      reporting literals"), and logs it at VLOG(4) as
        "WORKER W: Solution message mapped to: message about node T assignments: ..."
      Each such line from a terminal node T is a candidate preimage. At V3 the master logs
      only "received MPI_TAG_SOLUTION", with no content, so V3 is NOT enough.

  recover_solutions.py checkpoint FILE.check COMPONENTS.json --out-dir DIR
      Needs -v SECONDS (checkpoint_frequency) and -z PREFIX. The master writes checkpoints
      from the top of its main loop, independent of the terminate trigger, so they keep
      being written while it waits for workers to poll after the first solution. Every
      accepted solution, terminal ones included, is stored as a no-good in its node's
      additional_clauses (the NEGATION of its literals); negating it back gives the model.

Every recovered candidate is written as DIR/<source>_<n>.model; verify each with
tools/verify_preimage.py RAW.cnf DIR/<file>. Nothing here is trusted until verified.
"""
import os, re, sys, glob, json

MSG = re.compile(r'Solution message mapped to: message about node (-?\d+) assignments:([-\d ]*)')


def terminals(comps_path):
    C = json.load(open(comps_path))
    return {int(k) for k, v in C["node_roles"].items() if v.endswith("TERMINAL")}, C


def from_logs(logdir, comps_path):
    term, _ = terminals(comps_path)
    found = []
    for path in sorted(p for p in glob.glob(os.path.join(logdir, "*")) if not os.path.islink(p)):
        for line in open(path, errors="replace"):
            m = MSG.search(line)
            if m and int(m.group(1)) in term:
                toks = m.group(2).split()
                if toks and all(re.fullmatch(r'-?\d+', t) for t in toks):
                    found.append((int(m.group(1)), [int(t) for t in toks]))
    return found


def from_checkpoint(path, comps_path):
    """Checkpoint layout (TableSolutions::dump_checkpoint): header '92529 N dumb_mode', the
    N*N message tables, then N DIMACS blocks ('p cnf V C' + clauses) of additional_clauses,
    one per node in node order, then completed combinations."""
    term, C = terminals(comps_path)
    reporting = set()
    text = open(path, errors="replace").read()
    blocks = [m.start() for m in re.finditer(r'(?m)^p cnf ', text)]
    n_nodes = len(C["nodes"])
    if len(blocks) < n_nodes:
        raise SystemExit("checkpoint has %d DIMACS blocks, expected >= %d" % (len(blocks), n_nodes))
    blocks = blocks[:n_nodes]
    found = []
    for node, start in enumerate(blocks):
        if node not in term:
            continue
        end = blocks[node + 1] if node + 1 < len(blocks) else len(text)
        body = text[start:end].split("\n", 1)[1] if "\n" in text[start:end] else ""
        cur = []
        for tok in body.split():
            if not re.fullmatch(r'-?\d+', tok):
                break                      # reached the next section of the checkpoint
            v = int(tok)
            if v == 0:
                if cur:
                    found.append((node, [-l for l in cur]))   # no-good -> model
                cur = []
            else:
                cur.append(v)
    return found


if __name__ == "__main__":
    a = sys.argv[1:]
    if len(a) < 3 or a[0] not in ("logs", "checkpoint"):
        sys.exit(__doc__)
    out = a[a.index("--out-dir") + 1] if "--out-dir" in a else "recovered"
    os.makedirs(out, exist_ok=True)
    found = from_logs(a[1], a[2]) if a[0] == "logs" else from_checkpoint(a[1], a[2])
    seen = set()
    for i, (node, lits) in enumerate(found):
        key = tuple(sorted(lits, key=abs))
        if key in seen:
            continue
        seen.add(key)
        p = os.path.join(out, "%s_node%d_%d.model" % (a[0], node, len(seen)))
        open(p, "w").write(" ".join(map(str, lits)) + " \n")
        print("%s: terminal node %d, %d literals -> %s" % (a[0], node, len(lits), p))
    if not found:
        print("%s: no terminal solutions found" % a[0])
