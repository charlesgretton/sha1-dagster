// tinisat_seeded.cc -- Dagster's standalone TiniSAT with a CONTROLLED seed.
// Derived from dagster/standalone_tinisat/tinisat_standalone.cc (GPL-2+,
// Copyright 2020 Mark Burgess). Changes only:
//   * argv[4] = seed (required); the original seeds from wall-clock
//     microseconds, which is neither reproducible nor recordable
//   * seed echoed to stdout for the run log
//   * exit 10 on SAT, 20 on UNSAT (the portfolio harness contract)
// glibc rand() and random() share one generator, so srandom() here drives the
// rand() calls in TiniSAT's probabilistic restart policy.
// Dagster sources are included by name and resolved with -I$(DAGSTER_SRC);
// build with `make engines DAGSTER_SRC=/path/to/dagster-main/dagster`.
/*************************
Copyright 2020 Mark Burgess

This file is part of Dagster.

Dagster is free software; you can redistribute it 
and/or modify it under the terms of the GNU General 
Public License as published by the Free Software 
Foundation; either version 2 of the License, or
(at your option) any later version.

Dagster is distributed in the hope that it will be
useful, but WITHOUT ANY WARRANTY; without even the
implied warranty of MERCHANTABILITY or FITNESS FOR 
A PARTICULAR PURPOSE. See the GNU General Public 
License for more details.

You should have received a copy of the GNU General 
Public License along with Dagster.
If not, see <http://www.gnu.org/licenses/>.
*************************/

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cmath>
#include <cassert>
#include <cerrno>
#include <time.h>

#include "Message.h"
#include "Message.cpp"
#include "SatSolverInterface.h"
#include "SatSolverInterface.cpp"
#include "Cnf.h"
#include "Cnf.cpp"
#include "CnfManager.h"
#include "CnfManager.cpp"
#include "CnfHolder.h"
#include "CnfHolder.cpp"
#include "SatSolver.h"
#include "SatSolver.cpp"
#include "utilities.h"
#include "utilities.cpp"
#include "strengthener/Work.h"
#include "strengthener/Work.cc"
#include "strengthener/MpiBuffer.h"
#include "strengthener/MpiBuffer.cc"
#include "ReversableIntegerMap.h"
#include "ReversableIntegerMap.cc"
#include "Arguments.h"
#include "Arguments.cpp"

using namespace std;

Arguments command_line_arguments; // holder for parsed command line arguments
CnfHolder* cnf_holder; // the cnf_holder object for retrieving CNF components for a dag


#include <csignal>
#include <unistd.h>
/* Statistics in MiniSat's "name : value" format, so the portfolio harness
   parses them. Also printed from SIGTERM, so a run stopped by the timeout
   still reports the conflicts it reached (censored runs are most of r>=21). */
static SatSolver* g_stats_solver = NULL;
/* Async-signal-safe: no snprintf/printf, only write(); counters read through
   volatile. The SIGTERM snapshot is BEST-EFFORT: the interrupted solver may be
   mid-update, so the value can lag the true count by a few conflicts. That is
   safe for its one use, a right-censoring threshold ("needed more than c"),
   because a lagging c is still a valid, slightly weaker, lower bound. */
static size_t put_u(char* b, unsigned long v) {
  char t[24]; size_t n = 0, k = 0;
  do { t[n++] = (char)('0' + v % 10); v /= 10; } while (v && n < sizeof t);
  while (n) b[k++] = t[--n];
  return k;
}
static void put_line(int fd, const char* name, unsigned long v) {
  char b[64]; size_t k = 0;
  for (const char* c = name; *c; ++c) b[k++] = *c;
  b[k++] = ' '; b[k++] = ':'; b[k++] = ' ';
  k += put_u(b + k, v); b[k++] = '\n';
  ssize_t w = write(fd, b, k); (void)w;
}
static void print_stats_fd(int fd) {
  SatSolver* s = g_stats_solver;
  if (!s) return;
  unsigned long c = *(volatile unsigned*)&s->nConflicts;
  unsigned long d = (unsigned long)*(volatile int*)&s->decisions;
  put_line(fd, "conflicts            ", c);
  put_line(fd, "decisions            ", d);
}
static void on_term(int sig) { print_stats_fd(1); _exit(128 + sig); }

int main(int argc,char *argv[]) {
  if (argc < 1) {
    printf("must pass CNF file <filename> [FILE_OUT] [COUNTING]\n");
    return 1;
  }
  bool counting = false;
  if (argc>=4)
    counting = strcmp(argv[3],"true")==0;
  
  std::string str ("");
  clock_t tStart = clock();
  if (argc < 5) { fprintf(stderr, "usage: %s CNF MODEL counting(true|false) SEED\n", argv[0]); return 2; }
  unsigned int seed_arg = (unsigned int)strtoul(argv[4], NULL, 10);
  srandom(seed_arg);
  printf("c seed %u\n", seed_arg);
  
  // load the CNF into new solver
  Cnf* cnf = new Cnf((const char*)argv[1]);
  /* TINISAT_DAGSTER_CONFIG=1 reproduces Dagster's worker construction exactly
     (Worker.cpp: decision_interval=30, suggestion_size=30, pure literals NOT
     asserted). Default keeps the standalone's original settings, which the
     completed tinisat_r20-22 campaign used. */
  const char* dcfg = getenv("TINISAT_DAGSTER_CONFIG");
  bool as_dagster = dcfg && strcmp(dcfg, "1") == 0;
  SatSolver* solver = as_dagster
      ? new SatSolver(cnf, 30, 30, NULL, NULL, false, false, str, 0)
      : new SatSolver(cnf, 5, 5, NULL, NULL, true, false, str, 0);
  printf("c solver config: %s\n", as_dagster ? "dagster-worker (30,30,pure=false)" : "standalone (5,5,pure=true)");
  g_stats_solver = solver;
  signal(SIGTERM, on_term);
  
  int ret_code;
  Message* m = new Message();
  std::deque<int> conflicts;
  int solutions = 0;
  FILE *ofp;
  
  // if outputting to file, clear the file
  if(argc > 2)
    if ((ofp = fopen(argv[2], "w")) != NULL)
      fclose(ofp);
  
  // start a loop searching for all solutions (if counting)
  do {
    // run the solver while it is returning status code 2 (ie. continuing)
    ret_code = 2;
    while (ret_code==2) {ret_code=solver->run(NULL);}
    
    // if solution
    if (ret_code==1) {
      solutions++;
      // print to file or console
      if(argc > 2){ 
        if ((ofp = fopen(argv[2], "a")) != NULL){
          if (solutions==1) 
            fprintf(ofp, "SAT\n");
          solver->printSolution(ofp);
          fclose(ofp);
        }
      }else{
        if (solutions==1)
          printf("s SATISFIABLE\n");
        solver->printSolution(stdout);
      }
      
      // add the negation of the solution to the solver and refresh it
      conflicts.clear();
      solver->load_into_message(m);
      for (auto it = m->assignments.begin(); it!=m->assignments.end(); it++)
        conflicts.push_back(-*it);
      solver->solver_add_conflict_clause(conflicts);
      ret_code = solver->reset_solver();
    }
    // and repeat while it is SAT
  } while ((ret_code==1) && (counting==1));
  if ((argc<=2) && (solutions==0))
    printf("s UNSATISFIABLE\n");
  
  delete m;
  fflush(stdout); print_stats_fd(1); g_stats_solver = NULL;
  delete solver;
  delete cnf;
  printf("\nTime taken: %.5fs\n", (double)(clock() - tStart)/CLOCKS_PER_SEC);
  return (solutions > 0) ? 10 : 20;
}




