# Compositional SAT search with Dagster — investigation repository

Can a sound, structure-following decomposition of a hard satisfiability problem, run by the Dagster
distributed SAT solver, find solutions faster than a conventional portfolio of the same solver on the
same cores? This repository holds the complete software and data record of an investigation of that
question on preimages of SHA-1 reduced to 22 rounds: the virtual proceedings, every tool that
produced a result in them, the opt-in Dagster patch the result depends on, and the derived evidence
behind every reported number.

Start with `proceedings/sha1_dagster_proceedings.pdf` (34 pages). Everything here is referenced from
it.

> On 22-round SHA-1 preimage instances, a Dagster DAG that races several pipelines built on a sound
> trans-round relaxation (the clauses for the low 16 bits of every addition) found an independently
> verified preimage in 30 of 30 runs on three targets withheld from all tuning (median 26.9 s on 64
> worker cores). Observed 64-process portfolios of the same MiniSat 2.2 solver solved 28 of 30
> (median 552.6 s). The decomposition was faster in all 30 paired runs (sign test p = 1.9 × 10⁻⁹).
> Reaching this required removing a scheduler pathology, the *refinement lock*, with a small, opt-in
> change to Dagster's worker. The comparison is against a MiniSat portfolio: one single-core
> CryptoMiniSat run solved a 22-round target in 397 s, so a portfolio of a stronger solver is a
> materially stronger, still untested, baseline. Nothing here bears on full 80-round SHA-1.

## Version

See `VERSION`. `make manifest` regenerates `MANIFEST.sha256`, which every released archive carries.

## Quick start

```bash
pip install python-sat            # used by the DAG builder's validation and by the self-test
make selftest                     # all checks must pass before trusting anything (about a minute)
make proceedings                  # rebuild proceedings/sha1_dagster_proceedings.pdf (pdflatex, three passes)
```

`make selftest` runs no Dagster experiment. It builds the instance generator and checks that it
regenerates all five campaign instances byte for byte; that each planted block hashes, under an
independent SHA-1 implementation, to its instance's digest; that the planted model verifies and the
arm-C DAG rebuilds to its recorded SHA-256; that the generator and patch scripts carry the hashes the
proceedings bind them to; that the evidence tables regenerate exactly from the shipped run records
and agree with the proceedings' headline numbers; and that the release carries its licences and no
private paths or host names.

## Results, and what produced them

### Locked confirmatory evaluation (Phase C)

Frozen configuration (retirement threshold L = 8, conflict budgets off), three untouched targets,
10 repeats each, 64 cores (AMD Ryzen Threadripper 3990X), 1800 s cap.

| arm | solved | median time to verified preimage | verified per physical core-hour |
|---|---|---|---|
| **C**, compositional race | **30/30** | **26.9 s** | **1.38** |
| C′, low16 pipeline alone | 30/30 | 40.8 s | 0.60 |
| Br, observed 64-process MiniSat portfolio race | 28/30 | 552.6 s | 0.069 |

Paired by target and repeat: C faster than Br in 30 of 30 (exact 95% interval for the win
probability [0.884, 1]); paired ratio T_Br / T_C from 2.5 to 234, median 23.6 (two lower bounds from
censored races). C against C′: 16 of 30, no detectable difference in speed; the full race is less
sensitive to the target. Produced by `scripts/runsheet_demo.sh`; tabulated in
`evidence/derived/summary.csv`; statistics by `tools/report_stats.py`.

### Development-target tuning (Phase B), and why the patch is needed

| configuration (two development targets, 20 runs) | solved | median | verified per physical core-hour |
|---|---|---|---|
| unbounded refinement (L = ∞) | 14/20 (6 killed) | 144.7 s | 0.22 |
| **L = 8, no budget** | **20/20** | **21.3 s** | **1.61** |
| L = 8, terminal budget 20,000 conflicts | 20/20 | 30.7 s | 1.21 |
| observed portfolio races | 19/20 | 499.6 s | 0.072 |

With unbounded refinement the staged pipeline's intermediate messages never complete, fill Dagster's
work buffer, and stop all new terminal work within the first second; six runs were killed at a fixed
refinement workload, the signature of memory exhaustion. The opt-in retirement threshold removes the
lock. Conflict budgets were measured to reduce performance and were disabled.
`evidence/derived/mechanism.csv` and `budget.csv` hold the evidence.

### Correctness

220 Phase B and C runs: 210 verified preimages, 10 censored (6 killed, 4 at the time cap), no
invalid answers. Every preimage was checked by recomputing reduced-round SHA-1 outside the solver;
the 60 preimages from arms C and C′ were re-audited against independently regenerated instances.
Phase A (semantic equivalence of the patched binary with its controls off) passed.
`evidence/derived/runs.csv` holds one record per run, including the censored ones.

## Where results come from

| in the proceedings | evidence | produced by |
|---|---|---|
| Phase A | `evidence/validation/phaseA_report.txt` | `scripts/phaseA_regression.sh` |
| Phases B and C, every run | `evidence/runs/`, `evidence/derived/runs.csv` | `scripts/runsheet_demo.sh` → `evidence/scripts/derive.py` |
| per-arm times and yields | `evidence/derived/summary.csv` | `evidence/scripts/derive.py` |
| paired tests, intervals, ratios | recomputed from `evidence/runs/` | `tools/report_stats.py` |
| refinement lock and its removal | `evidence/derived/mechanism.csv` | `derive.py` from raw event logs (hashes in `checksums.txt`) |
| census of MiniSat trajectories | `evidence/derived/census.csv` | `portfolio/portfolio_harness.sh` |
| instances, digests, planted blocks | `evidence/instances/` | `bin/sha1gen` (`third_party/sha1-sat`) |
| single-solver comparison | proceedings, single-solver table | `scripts/run_solver_bench.sh` |
| environment | `evidence/provenance.txt`, `evidence/checksums.txt` | — |

Run records were stripped of absolute paths and host names before release. The raw campaign
archives and Dagster event logs contain both and are not distributed; they are identified by SHA-256
in `evidence/checksums.txt`.

## Layout

```
proceedings/      the virtual proceedings: LaTeX source (self-contained, TikZ/pgfplots) and PDF
third_party/      sha1-sat, the instance generator (GPL-3.0-or-later; see MODIFICATIONS.md)
patches/          dagster_bounded_v3.py, the opt-in Dagster patch; development-phase Dagster patches
scripts/          campaign runsheets, Phase A regression, provenance capture, solver benchmark
tools/            DAG builder, relaxation, verifier, watcher, log parsers, statistics, self-test
portfolio/        portfolio harness and engines (the trajectory census and early baselines)
examples/         a trace-relaxation example instance and DAG
evidence/         run records, derived tables, validation outputs, instances, checksums, provenance
docs/             REPRODUCE.md, the step-by-step campaign guide
```

Development-phase scripts (`scripts/runsheet_compositional.sh`, `runsheet_2_3.sh`,
`runsheet_modern.sh`, `sweep.sh`, `sample_trajectories.sh`, `runner.py`, `runme.sh`, and
`patches/tinisat-seed.patch`, `dagster_dump_worker_cnf.py`) produced the background results the
proceedings summarise; they are kept for completeness and are not needed to reproduce Phases A–C.

## Software that is not bundled

| software | version used | where |
|---|---|---|
| Dagster | commit not recorded (stock and patched source files identified by SHA-256 in `evidence/checksums.txt`) | https://github.com/ANU-HPC/dagster |
| MiniSat 2.2 | as embedded in Dagster (`-m 4`), and built from it as the portfolio engine | part of Dagster |
| glog, CUDD, zlib | as required by Dagster; versions not recorded | their own repositories |
| MPICH (Hydra) | version not recorded | https://www.mpich.org |
| Python | 3.8 in the campaign; python-sat for validation; pycryptosat for the single-solver table | https://www.python.org |

`scripts/capture_provenance.sh` now records the items marked "not recorded" for any rerun.

## Reproducing the runs

`docs/REPRODUCE.md` gives every command. In outline, on a Linux machine with 64 physical cores:

```bash
cp -r /path/to/dagster /path/to/dagster-v3                     # keep the stock tree as the control
python3 patches/dagster_bounded_v3.py /path/to/dagster-v3/dagster && (cd /path/to/dagster-v3/dagster && make)
cp env.example.sh ~/sha1_env.sh   # edit the three paths, then: source ~/sha1_env.sh
scripts/phaseA_regression.sh                                  # must end PHASE A: PASS
# Phase B (development targets g10004, g10005) and Phase C (untouched g10001-g10003): docs/REPRODUCE.md
python3 tools/report_stats.py results/eval_r22 "10001 10002 10003"
```

## Discipline

Five conventions are load-bearing. Each was adopted after a failure in this project.

**Verify every answer outside the solver.** Each returned model is checked by recomputing reduced-round
SHA-1 from its 512 message-block bits. An exit code of 0 from Dagster once meant "no work left", not
"solved"; only the external check told the two apart.

**Fail closed.** A run whose Dagster master logs nothing is a launch failure, not an unsolved search; a
run that requests patch controls without the patched binary's start-up line stops. An earlier campaign
recorded 100 launches that never started as "unsolved" before this rule existed.

**Separate tuning from evaluation.** Parameters were chosen on two development targets only and frozen
before three untouched targets were run.

**Measure, do not assume.** The retirement threshold is worker-local, so the realised number of
descendants per parent was measured (up to 10 at L = 8) rather than assumed to be L.

**Compare with observed races, paired.** The baseline is an observed 64-process portfolio race on a
fresh seed block per repeat, launched in alternating order with the decomposition, not a race inferred
from a trajectory census.

## Status and what remains

Established, for three untouched 22-round targets on one 64-core workstation (AMD Ryzen Threadripper
3990X): the paired advantage over
an observed MiniSat portfolio, the refinement lock and its removal, and the correctness of every returned
answer. In order of priority: a repeated, resource-matched portfolio of a stronger solver
(CryptoMiniSat, Kissat, CaDiCaL); more targets; behaviour at 23 rounds; scaling beyond 64 cores; a
strict master-side retirement bound; complete launch-time provenance. Running a stronger solver inside
Dagster is outside this project's scope. The proceedings' open items R1–R14 give the full list.

## Related publications

Dagster's design and case studies are described in the first two papers; the instance generator in
the third. BibTeX entries are in `proceedings/references.bib`.

* M. A. Burgess, C. Gretton, J. Milthorpe, L. Croak, T. Willingham and A. Tiu. Dagster: Parallel
  Structured Search with Case Studies. *PRICAI 2022*, LNCS 13629, pp. 75–89, Springer, 2022.
  https://doi.org/10.1007/978-3-031-20862-1_6
* M. A. Burgess, C. Gretton, J. Milthorpe, L. Croak, T. Willingham and A. Tiu. Dagster: Parallel
  Structured Search. *AAAI 2023, Student Papers and Demonstrations*, vol. 37, pp. 16404–16406,
  AAAI Press, 2023. https://doi.org/10.1609/aaai.v37i13.27060
* V. Nossum. *SAT-based preimage attacks on SHA-1*. Master's thesis, University of Oslo, 2012.
  Software: https://github.com/vegard/sha1-sat

## Licensing

Source code: Apache-2.0. Proceedings, documentation and project-generated data: CC BY 4.0. The
instance generator in `third_party/sha1-sat/` is GPL-3.0-or-later and the seeded TiniSAT engine is
GPL-2.0-or-later, both derived from third-party software. See `LICENSING.md` and `REUSE.toml`.
Reports of errors, omissions and overlooked prior work are welcome: please write to the corresponding
author, Charles Gretton, at charles.gretton@anu.edu.au.
