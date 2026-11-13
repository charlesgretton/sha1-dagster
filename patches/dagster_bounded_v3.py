#!/usr/bin/env python3
"""
dagster_bounded_v3.py -- patch a STOCK Dagster source tree with two opt-in controls, and write
the unified diff and a manifest beside it.

  python3 patches/dagster_bounded_v3.py /path/to/dagster          # directory with Worker.cpp

All controls are inert unless their environment variable is set to a positive integer. With every
variable unset or 0 the patch is intended to preserve stock search and scheduling SEMANTICS; it
still adds a startup log line, per-call result logging at VLOG(3) and negligible branch overhead,
so the binary and its logs are NOT byte-identical to stock.

1. Refinement retirement threshold   DAGSTER_MAX_INTERMEDIATE_SOLUTIONS = L
   A worker solving a message on an INTERMEDIATE node (a non-root with outgoing edges; Dagster
   marks roots with a self-loop reverse connection) requests retirement of that parent message
   (MPI_TAG_REQUEST_FOR_ASSIGNMENT) once IT has emitted L descendants for that node+assignment
   pair. The master then removes the message and frees every worker on it. This is a
   COOPERATIVE, WORKER-LOCAL threshold, not a strict global bound: concurrent workers on the
   same parent have private counters and may emit further descendants before removal takes
   effect, so the realised count per parent lies between L and about W*L for W workers.
   Measure the realised count (tools/refinement_stats.py), do not assume L.
   A finite L makes the staged component INCOMPLETE: refinements after the first L emissions of
   a parent are never explored. The whole DAG may still contain unbounded complete routes
   (e.g. a low16 source -> terminal component); a time-capped campaign is incomplete anyway.

2. Role-specific MiniSat conflict quanta
      DAGSTER_MINISAT_TERMINAL_CONFLICT_BUDGET      terminal nodes (no outgoing edges)
      DAGSTER_MINISAT_INTERMEDIATE_CONFLICT_BUDGET  non-root nodes with outgoing edges
      DAGSTER_MINISAT_SOURCE_CONFLICT_BUDGET        root nodes
      DAGSTER_MINISAT_CONFLICT_QUANTUM              default for any role not set above
   When a role's budget N>0, MinisatSolver::run uses solveLimited with a fresh quantum of N
   conflicts (setConfBudget is relative to the current conflict count) and returns 2 when it is
   exhausted, which reuses Dagster's poll/reassign path. A later quantum RE-ENTERS the same
   assumption-conditioned problem in the persistent (-q 2) solver, retaining learnt clauses and
   heuristic state; it is not a continuation of the interrupted search stack.
   Because status 2 also means "poll for reassignment", the worker logs the cause explicitly:
      MINISAT_BUDGET_EXHAUSTED node=<n> role=<r> msg=<digest> conflicts_this_call=<q> conflicts_total=<Q>
   and every call's outcome at VLOG(3):
      MINISAT_RESULT node=<n> role=<r> msg=<digest> result=<0|1|2> conflicts_this_call=<q> conflicts_total=<Q>
   The MINISAT_RESULT stream gives the conflict distribution needed to calibrate budgets.

Startup line (checked by the runsheets before relying on the controls):
   DAGSTER_PATCH v3 L=<L> budget_terminal=<..> budget_intermediate=<..> budget_source=<..>
Invalid values (non-integer, negative, or above 10^12) are FATAL at worker start-up.

Outputs written into the Dagster directory: dagster_bounded_v3.stock/ (stock copies, kept out of
the Makefile's object globs such as minisat_solver/*.o*), dagster_bounded_v3.diff (unified diff: the scientific artefact) and
dagster_bounded_v3.manifest.json (patch script SHA-256, file SHA-256s before and after).
Refuses to run on an already-patched tree, or if any anchor is missing or ambiguous.
"""
import os, sys, json, hashlib, difflib, time, shutil

MARK = "// DAGSTER_BOUNDED_PATCH v3"
sha = lambda p: hashlib.sha256(open(p, "rb").read()).hexdigest()


def apply(path, pairs):
    s = open(path).read()
    if "DAGSTER_BOUNDED_PATCH" in s:
        sys.exit("ERROR: %s already carries a DAGSTER_BOUNDED_PATCH; start from a stock tree" % path)
    for old, new in pairs:
        n = s.count(old)
        if n != 1:
            sys.exit("ERROR: anchor occurs %d times in %s (expected 1):\n%s" % (n, path, old[:200]))
        s = s.replace(old, new)
    return MARK + "\n" + s


WORKER_STATE = r'''void Worker::loop() {
  int solution_count = 0;
  // ---- DAGSTER_BOUNDED_PATCH v3: parse and validate controls (fatal on invalid input) ----
  auto env_int = [](const char* name) -> long long {
    const char* v = getenv(name);
    if (v == NULL || *v == '\0') return -1;                    // unset
    char* end = NULL; errno = 0;
    long long x = strtoll(v, &end, 10);
    if (errno != 0 || end == v || *end != '\0' || x < 0 || x > 1000000000000LL)
      LOG(FATAL) << "DAGSTER_PATCH: invalid value " << name << "=" << v << " (need integer 0..1e12)";
    return x;
  };
  const long long max_intermediate = std::max(0LL, env_int("DAGSTER_MAX_INTERMEDIATE_SOLUTIONS"));
  const long long q_default = std::max(0LL, env_int("DAGSTER_MINISAT_CONFLICT_QUANTUM"));
  auto role_budget = [&](const char* name) { long long v = env_int(name); return v < 0 ? q_default : v; };
  const long long budget_terminal = role_budget("DAGSTER_MINISAT_TERMINAL_CONFLICT_BUDGET");
  const long long budget_intermediate = role_budget("DAGSTER_MINISAT_INTERMEDIATE_CONFLICT_BUDGET");
  const long long budget_source = role_budget("DAGSTER_MINISAT_SOURCE_CONFLICT_BUDGET");
  LOG(INFO) << "WORKER " << comms->world_rank << ": DAGSTER_PATCH v3 L=" << max_intermediate
            << " budget_terminal=" << budget_terminal << " budget_intermediate=" << budget_intermediate
            << " budget_source=" << budget_source;
  long long msg_solutions = 0; std::string msg_key; size_t msg_digest = 0;'''

WORKER_KEY = r'''      message_index++;
      initialise_solver_from_message(m);
      { std::string key = std::to_string(m->to) + ":";
        for (size_t q = 0; q < m->assignments.size(); q++) key += std::to_string(m->assignments[q]) + ",";
        if (key != msg_key) { msg_key = key; msg_solutions = 0; msg_digest = std::hash<std::string>{}(key); } }
'''

WORKER_SOLVE = r'''generate_new_result:
      // role of the node being solved: root (self-loop reverse edge), terminal, or intermediate
      { int nd = m2->to;
        bool is_root = dag->reverse_connections[nd].size() == 1 && dag->reverse_connections[nd][0] == nd;
        bool is_terminal = dag->forward_connections[nd].empty();
        dagster_minisat_role = is_root ? "source" : (is_terminal ? "terminal" : "intermediate");
        dagster_minisat_budget = is_root ? budget_source : (is_terminal ? budget_terminal : budget_intermediate); }
      int result = solve_and_generate_message(m2, working_message);
      if (minisat_mode) VLOG(3) << "WORKER " << comms->world_rank << ": MINISAT_RESULT node=" << m2->to << " role=" << dagster_minisat_role
              << " msg=" << std::hex << msg_digest << std::dec << " result=" << result
              << " conflicts_this_call=" << dagster_minisat_conflicts_this_call
              << " conflicts_total=" << dagster_minisat_conflicts_total;
      if (minisat_mode && result == 2 && dagster_minisat_budget_exhausted)
        LOG(INFO) << "WORKER " << comms->world_rank << ": MINISAT_BUDGET_EXHAUSTED node=" << m2->to
                  << " role=" << dagster_minisat_role << " msg=" << std::hex << msg_digest << std::dec
                  << " conflicts_this_call=" << dagster_minisat_conflicts_this_call
                  << " conflicts_total=" << dagster_minisat_conflicts_total;'''

WORKER_BOUND = r'''        bool is_root2 = dag->reverse_connections[m2->to].size() == 1 && dag->reverse_connections[m2->to][0] == m2->to;
        bool intermediate = !is_root2 && !dag->forward_connections[m2->to].empty();
        bool retire = intermediate && max_intermediate > 0 && msg_solutions >= max_intermediate;
        if (retire) LOG(INFO) << "WORKER " << comms->world_rank << ": REFINEMENT_RETIRE node=" << m2->to
                              << " msg=" << std::hex << msg_digest << std::dec << " emitted_by_this_worker=" << msg_solutions;
        if (retire || (!reset_solver_for_next_solution(m2)) || (command_line_arguments.ENUMERATE_SOLUTIONS==3)) {'''

MINISAT_GLOBALS = r'''using namespace Minisat;
// ---- DAGSTER_BOUNDED_PATCH v3: per-call budget set by the Worker, and per-call statistics ----
long long dagster_minisat_budget = 0;
const char* dagster_minisat_role = "unset";
long long dagster_minisat_conflicts_this_call = 0;
long long dagster_minisat_conflicts_total = 0;
bool dagster_minisat_budget_exhausted = false;'''

MINISAT_RUN = r'''  long long c_entry = (long long) conflicts;
  dagster_minisat_budget_exhausted = false;
  int out;
  if (dagster_minisat_budget > 0) {            // fresh quantum, relative to the current count
    setConfBudget(dagster_minisat_budget);
    lbool r = solveLimited(lits, false, false);
    budgetOff();
    if (r == l_True) out = 1; else if (r == l_False) out = 0;
    else { out = 2; dagster_minisat_budget_exhausted = true; }
  } else {
    out = solve(lits, false, false) ? 1 : 0;
  }
  dagster_minisat_conflicts_this_call = (long long) conflicts - c_entry;
  dagster_minisat_conflicts_total = (long long) conflicts;
  return out;
'''


def main(d):
    w = os.path.join(d, "Worker.cpp"); ms = os.path.join(d, "minisat_solver", "minisat_solver.cc")
    for p in (w, ms):
        if not os.path.exists(p):
            sys.exit("ERROR: %s not found -- pass the Dagster source directory (with Worker.cpp)" % p)
    before = {p: sha(p) for p in (w, ms)}
    new_w = apply(w, [
        ("void Worker::loop() {\n  int solution_count = 0;", WORKER_STATE),
        ("      message_index++;\n      initialise_solver_from_message(m);\n", WORKER_KEY),
        ("generate_new_result:\n      int result = solve_and_generate_message(m2, working_message);", WORKER_SOLVE),
        ("        solution_count++;\n", "        solution_count++;\n        msg_solutions++;\n"),
        ("        if ((!reset_solver_for_next_solution(m2)) || (command_line_arguments.ENUMERATE_SOLUTIONS==3)) {", WORKER_BOUND),
    ])
    # declarations the Worker needs (defined in minisat_solver.cc)
    new_w = new_w.replace(MARK + "\n", MARK + "\n#include <cerrno>\n#include <cstdlib>\n#include <string>\n#include <functional>\n#include <algorithm>\n"
                          "extern long long dagster_minisat_budget; extern const char* dagster_minisat_role;\n"
                          "extern long long dagster_minisat_conflicts_this_call; extern long long dagster_minisat_conflicts_total;\n"
                          "extern bool dagster_minisat_budget_exhausted;\n", 1)
    new_m = apply(ms, [
        ("using namespace Minisat;", MINISAT_GLOBALS),
        ("  bool ret = solve(lits, false, false);\n  DB(printf(\"returning %i\\n\",ret);)\n  return ret;\n", MINISAT_RUN),
    ])
    stock = {w: open(w).read(), ms: open(ms).read()}
    diff = []
    stock_dir = os.path.join(d, "dagster_bounded_v3.stock")   # outside every build glob
    os.makedirs(stock_dir, exist_ok=True)
    for p, new in ((w, new_w), (ms, new_m)):
        shutil.copyfile(p, os.path.join(stock_dir, os.path.relpath(p, d).replace("/", "__") + ".stock"))
        rel = os.path.relpath(p, d)
        diff += difflib.unified_diff(stock[p].splitlines(True), new.splitlines(True),
                                     "a/" + rel, "b/" + rel)
        open(p, "w").write(new)
    open(os.path.join(d, "dagster_bounded_v3.diff"), "w").writelines(diff)
    json.dump({"patch": "dagster_bounded_v3", "patch_script_sha256": sha(os.path.abspath(__file__)),
               "applied_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "files": {os.path.relpath(p, d): {"stock_sha256": before[p], "patched_sha256": sha(p)} for p in (w, ms)},
               "diff_sha256": sha(os.path.join(d, "dagster_bounded_v3.diff"))},
              open(os.path.join(d, "dagster_bounded_v3.manifest.json"), "w"), indent=1)
    print("patched Worker.cpp and minisat_solver/minisat_solver.cc; wrote dagster_bounded_v3.diff (%d lines) "
          "and dagster_bounded_v3.manifest.json. Rebuild with make." % len(diff))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
