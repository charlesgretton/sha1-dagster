#!/usr/bin/env python3
"""dagster_dump_worker_cnf.py /path/to/dagster-main/dagster
Idempotently patches Worker.cpp so that, when DAGSTER_DUMP_CNF_DIR is set, every
TiniSAT job writes the exact CNF it is about to solve (node clause subset plus
the communicated unit clauses and any additional clauses) to
  $DAGSTER_DUMP_CNF_DIR/rank<R>_job<N>.cnf
Unset, behaviour is unchanged. Rebuild Dagster afterwards (make)."""
import sys, os
p = os.path.join(sys.argv[1], "Worker.cpp")
s = open(p, newline="").read()
if "DAGSTER_DUMP_CNF_DIR" in s:
    print("already patched"); sys.exit(0)
eol = "\r\n" if "\r\n" in s else "\n"
anchor = "generated_cnf = cnf_holder->compile_Cnf_from_Message(m);"
i = s.index(anchor); j = s.index(eol, i) + len(eol)
ins = eol.join([
 "  { // DAGSTER_DUMP_CNF_DIR: dump the exact CNF this worker solves (control experiment)",
 "    const char* dump_dir = getenv(\"DAGSTER_DUMP_CNF_DIR\");",
 "    if (dump_dir) { static int dump_n = 0; int dump_rank = 0; char dump_fn[4096];",
 "      MPI_Comm_rank(MPI_COMM_WORLD, &dump_rank);",
 "      snprintf(dump_fn, sizeof dump_fn, \"%s/rank%d_job%d.cnf\", dump_dir, dump_rank, dump_n++);",
 "      generated_cnf->output_dimacs(dump_fn); }",
 "  }", ""])
s = s[:j] + ins + s[j:]
open(p, "w", newline="").write(s); print("patched", p)
