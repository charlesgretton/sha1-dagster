# Portfolio baseline

Seed-only portfolio measurement for the reduced-round SHA-1 preimage family:
same instance, same engine, only the trajectory seed varies. Its output,
F_r(t), is the denominator every decomposition, encoding or scheduling claim
has to beat.

Only the verified first-solution time counts as success. Exit code 10 on its
own never counts.

## Three engine facts measured while building this

These change how earlier results should be read.

1. **MiniSat's `-rnd-seed` is a no-op without `-rnd-init`.** Seeds 1 and 2
   produced identical trajectories (22312 conflicts each) until `-rnd-init`
   was added. The harness preflight refuses to launch if two seeds return the
   same model.

2. **Dagster's default engine is TiniSAT, not MiniSat.** `minisat_mode`
   defaults to false in `Worker.h`, so every Dagster run in this project so far
   used TiniSAT.

3. **TiniSAT is deterministic in Dagster's configuration.** Dagster's MPI
   workers never call `srand`, and even with an explicit `srandom(seed)` the
   standalone returned the identical model for seeds 1, 2 and 3. So in Dagster
   runs, **the cube each worker received was the only source of diversity
   between workers**. For TiniSAT this bundle diversifies by seeded
   clause-order permutation instead: distinct verified models, and exactly
   reproducible per seed. Variables are not renamed, so models verify against
   the original CNF unchanged.

## Engines

| engine | diversifier | template |
|---|---|---|
| `engines/minisat` | `-rnd-init -rnd-seed=S` | default |
| `engines/tinisat_perm.sh` | clause permutation, seed S | `"{seed} {cnf} {model}"` |

```sh
make engines DAGSTER_SRC=/path/to/dagster-main/dagster
```

The engine rules compile against the Dagster tree with the same include roots
as Dagster's own Makefile (`-I<dagster> -I<dagster>/minisat`, `-std=c++17`),
and they honour `CPPFLAGS`, `LDFLAGS` and `MPICXX`. Any fix you needed to build
Dagster carries over:

```sh
make engines DAGSTER_SRC=... CPPFLAGS="-I/usr/include/x86_64-linux-gnu"
make engines DAGSTER_SRC=... MPICXX=CC            # Cray
make engines DAGSTER_SRC=... LDFLAGS="-L/path/to/glog/lib"
make -n -B engines DAGSTER_SRC=...                 # show the exact commands
```

A wrong `DAGSTER_SRC` fails immediately rather than partway through the build.
If `tinisat_seeded` builds but will not start, `ldd` it: glog found at build
time via `LDFLAGS` may also need `LD_LIBRARY_PATH` at run time.

`tinisat_seeded.cc` is Dagster's standalone TiniSAT (GPL-2+, Mark Burgess)
with three changes: an explicit seed argument instead of a wall-clock seed,
the seed echoed to the log, and exit codes 10/20.

For comparisons with Dagster cube strategies, use the TiniSAT engine, since
that is the one Dagster ran.

## Running

```sh
# full campaign: 3 rounds x 5 paired targets x 100 trajectories
SOLVER=portfolio/engines/tinisat_perm.sh SOLVER_ARGS_TEMPLATE="{seed} {cnf} {model}" \
WORKERS=64 PIN=1 TIMEOUT_SEC=300 ROUNDS="20 21 22" \
  portfolio/portfolio_harness.sh

python3 portfolio/analyse_portfolio.py results/<experiment_id>
```

Keep `WORKERS` at or below physical cores. On a shared core, wall time
measures contention: in validation, 2 workers on 1 core ran at about 2.1x user
time. Shard across PBS job arrays with `SOLVER_SEED_START` (1, 51, ...) and
pass all shard directories to the analysis. It refuses to merge shards whose
solver, generator, template, timeout or worker settings differ, or which share
a `run_id`.

Worst-case budget for the full campaign on 64 cores is about 1500 x 300 s / 64,
roughly 2 hours per engine. r=22 cells will mostly censor.

## What the harness guarantees

- Process hierarchy `time -> timeout -> solver`: `timeout` owns the solver
  directly.
- Stragglers are swept by **session id**, not process group. GNU `timeout`
  calls `setpgid`, so solvers live in timeout's group, not the worker's
  (measured: solver pgid 979, worker session 968). Killing the worker's group
  would miss them.
- A campaign killed by TERM/INT/HUP kills every worker session and exits
  143/130/129, never 0.
- One immutable JSON record per trajectory, published atomically, assembled
  in sorted order at the end, and every record validated.
- Refuses to write into an existing campaign directory. Fails, rather than
  skips, on missing inputs.
- Targets are generated inside the campaign with generator seed, command,
  SHA-256 of raw and repaired CNF, and target hash recorded. Generator seeds are
  paired across rounds.
- Exhaustive status classes: `SAT_VERIFIED`, `SAT_NO_MODEL`, `INVALID_MODEL`,
  `UNSAT` (integrity warning on a planted target), `TIMEOUT`, `TIMEOUT_KILLED`,
  `TERMINATED`, `HARNESS_ERROR`, `SOLVER_ERROR`. Missing statistics are JSON
  `null`, never 0.

Validation performed: an 8-case fault injection (valid model, SAT with no
model, wrong model, UNSAT, timeout, TERM-ignoring solver, crash, orphaned
grandchild) classified every case correctly and left no processes; campaign
TERM cleanup; refuse-to-append; the negative preflight (template without
`-rnd-init` rejected); 8 real solves; and two 100-trajectory pilots.

## Analysis

- F(t) at 5/10/30/100/300 s with Wilson 95% intervals; for zero successes it
  reports the rule-of-three bound (about 3/n), not a point estimate. With one
  common limit, Kaplan-Meier equals the empirical CDF below T.
- Timeouts are right-censored. RMST is restricted to T. Nothing averages a
  timeout in as T.
- Portfolio forecast uses the exact law of the minimum under resampling,
  P(min > t) = (1 - F(t))^k, with an outer bootstrap interval on the median.
- **Saturation flag:** beyond k of roughly 0.69n, the forecast is mostly the
  smallest time already observed. It describes the sample, not the unobserved
  left tail. With n=100, nothing about k=8000 can be read from these rows.
- Fixed-cutoff restart control: E[time] = E[min(T,c)] / F(c), assuming
  independent restarts. It is a screening estimate.

## Pilot results (r=20, target g10001, n=100 per engine, CPU seconds, 1 core)

| | MiniSat `-rnd-init` | TiniSAT + permutation |
|---|---|---|
| verified | 100/100 | 100/100 |
| F(5 s) | 0.97 [0.92, 0.99] | 0.94 [0.88, 0.97] |
| median | 1.12 s | 2.02 s |
| fastest observed | 0.15 s | 0.20 s |
| k=16 median of min | 0.37 s (3.0x) | 0.69 s (2.9x) |
| k=64 median of min | 0.34 s (3.3x) | 0.27 s (7.5x) |
| best restart cutoff | none: long runs best | none: long runs best |

At r=20 the left tail is short. Portfolio gain is strongly **sublinear**
(about 3x at 16 workers), and restarts do not help. Nothing here supports a
heavy tail at r=20. Whether the tail lengthens at r=21/22, which is where the
Dagster results were, is exactly what the full campaign measures. This sandbox
has one core, so it could not.

`pilot_results/` holds the four shard manifests and campaign files. Re-run the
analysis with:

```sh
python3 portfolio/analyse_portfolio.py portfolio/pilot_results/pilot_s001 \
    portfolio/pilot_results/pilot_s051 --metric user_seconds
```

## Cube-without-Dagster control

`engines/tinisat_cube.sh` runs Dagster-style cube jobs *outside* Dagster: the
full formula plus one k-bit cube as unit clauses, solved by the same TiniSAT
the portfolio uses. Seed s gets cube index (s-1) mod 2^k, so seeds
1..2^k are distinct cubes. The defaults reproduce Dagster's `naive-message`
jobs at `--bits 7`: the low 7 bits of w[0], which are variables 1-7, with the
clause order left as a Dagster worker sees it.

```sh
SOLVER=portfolio/engines/tinisat_cube.sh SOLVER_ARGS_TEMPLATE="{seed} {cnf} {model}" \
ROUNDS=21 SOLVER_SEEDS=128 WORKERS=63 PIN=1 TIMEOUT_SEC=300 \
EXPERIMENT_ID=cube_r21 portfolio/portfolio_harness.sh
```

`CUBE_BITS`, `CUBE_VARS` and `CUBE_PERMUTE=1` (also permute clauses) change
the cube. Compare its per-trajectory F(t) with the permutation portfolio's on
the same targets.

### Exact equivalence with Dagster's workers

`patches/dagster_dump_worker_cnf.py /path/to/dagster` patches Dagster so that,
with `DAGSTER_DUMP_CNF_DIR` set, every TiniSAT job writes the exact CNF it
solves. A dumped terminal job is the full formula in original clause order
followed by the cube's unit clauses; `cube_cnf.py` builds the same layout.
Verified: a control CNF rebuilt for the same cube was identical clause for
clause, and `tinisat_seeded` with `TINISAT_DAGSTER_CONFIG=1` (Dagster's worker
settings: 30/30, no pure-literal assertion) reproduced Dagster's own answer on
512/512 message bits.

So a cube census run with `CUBE_PERMUTE=0 TINISAT_DAGSTER_CONFIG=1` is an exact
replica of Dagster's worker computations. `census_campaign.py` then computes
exact K-worker first-solution campaign quantities: index order, best and worst first
batches, and the exact distribution over uniformly random first batches
(finite-population formula, no simulation).
Run `SOLVER_SEEDS=128` (all 2^7 cubes) so the census is complete.

`compare_dagster.py MANIFEST DAGSTER_CSV --round R` places each Dagster run
against a same-target, same-size portfolio race: per-row placement with
Clopper-Pearson bounds, per-strategy tallies (not best-of-N), realised races,
and a pooled test.
