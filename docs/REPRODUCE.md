# Reproducing the 22-round SHA-1 / Dagster campaign

These steps reproduce every result in `proceedings/sha1_dagster_proceedings.pdf`. Each step names the
proceedings section it reproduces. Run everything from the repository root.

## 0. Requirements

- A Linux machine with 64 physical cores (the campaign used one workstation: 128 logical CPUs, SMT on).
- MPICH (with Hydra) or Open MPI; a C++17 compiler; `make`; Python 3.8+.
- Dagster's build dependencies: glog, CUDD, zlib.
- A **stock Dagster source tree**. Record its Git commit: `git -C <tree> rev-parse HEAD`.
- For the analysis and the single-solver benchmark: `pip install python-sat pycryptosat`.

Check the release first: `make selftest` must pass.

## 1. Build Dagster twice: stock (control) and patched

    cp -r /path/to/dagster-stock /path/to/dagster-v3
    python3 patches/dagster_bounded_v3.py /path/to/dagster-v3/dagster
    (cd /path/to/dagster-v3/dagster && make)
    strings /path/to/dagster-v3/dagster/dagster | grep -c 'DAGSTER_PATCH v3'    # must be >= 1

The patcher refuses an already-patched tree. It writes `dagster_bounded_v3.diff` and
`dagster_bounded_v3.manifest.json` beside the sources; keep both. Its script SHA-256 must be
`68549a28...` (proceedings, Appendix B; `make selftest` checks it).

Then: `cp env.example.sh ~/sha1_env.sh`, edit the paths, and `source ~/sha1_env.sh`.

## 2. Phase A, Semantic Equivalence Regression (proceedings section 7, ~30 min)

    scripts/phaseA_regression.sh          # last line must be: PHASE A: PASS

## 3. Phase B, Development-Target Tuning (proceedings section 8; targets g10004, g10005 only)

    export TARGETS="10004 10005"
    # B1 calibration, which is also the first L=8 measurement
    DAGSTER_MAX_INTERMEDIATE_SOLUTIONS=8 REPEATS=5 ARMS=C PREFIX=results/dev_cal scripts/runsheet_demo.sh
    # B2 observed portfolio races (independent of L, so run once)
    REPEATS=10 ARMS=Br PREFIX=results/dev_Br scripts/runsheet_demo.sh
    # B2 unbounded control (L=0 means no threshold, on the patched binary)
    DAGSTER_MAX_INTERMEDIATE_SOLUTIONS=0 REPEATS=10 ARMS=C PREFIX=results/dev_L0 scripts/runsheet_demo.sh
    # B3 conflict-budget sweep at L=8 (B=0 is the no-budget control)
    for B in 2000 20000 200000 0; do
      DAGSTER_MAX_INTERMEDIATE_SOLUTIONS=8 DAGSTER_MINISAT_TERMINAL_CONFLICT_BUDGET=$B REPEATS=10 ARMS=C \
        PREFIX=results/dev_L8_B$B scripts/runsheet_demo.sh || break
    done

## 4. Phase C, Locked Confirmatory Evaluation (proceedings section 9; untouched g10001-g10003)

The frozen configuration is the box at the start of proceedings section 9. Do not change it.

    export TARGETS="10001 10002 10003"
    DAGSTER_MAX_INTERMEDIATE_SOLUTIONS=8 DAGSTER_MINISAT_TERMINAL_CONFLICT_BUDGET=0 \
      REPEATS=10 ARMS="B Br C Cp Y" PREFIX=results/eval_r22 scripts/runsheet_demo.sh

Budget 12-20 hours. Run nothing else on the machine meanwhile.

## 5. Recompute the proceedings' statistics

    python3 tools/report_stats.py results/eval_r22 "10001 10002 10003"
    python3 tools/report_stats.py results/dev_L8_B0 "10004 10005" results/dev_Br

To check the analysis without rerunning anything, point it at the shipped run records:

    python3 tools/report_stats.py evidence/runs/eval_r22 "10001 10002 10003"

This must print, for the pooled targets: C 30/30 (median 26.9 s), C' 30/30 (40.8 s), Br 28/30
(552.6 s); C vs Br 30-0, p=1.9e-09, 95% interval [0.884, 1.000]; paired ratio median 23.6.

## 6. Single-solver benchmark (proceedings single-solver table; any one-core machine, about 1.5 h)

    scripts/run_solver_bench.sh            # results/solver_bench/bench.jsonl

One seed per cell: expect the outcomes to vary from run to run (heavy-tailed run times).

## 7. Building the instances by hand

Proceedings Appendix A gives the generator, header-repair, DAG-building, validation and verification
commands. Proceedings Appendix D lists the SHA-256 of every artefact and the planted blocks, so a
regenerated instance can be checked byte for byte.

## Provenance

Every campaign writes `PREFIX.provenance.txt` (CPU model and core numbering, kernel, MPI and
compiler versions, Dagster binary hash and commit, library versions), via
`scripts/capture_provenance.sh`. The reported campaign predates this; see proceedings item R14.
