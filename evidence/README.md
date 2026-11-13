# Evidence

The derived record behind every number in the results of the proceedings.

| path | what it is |
|---|---|
| `runs/` | one JSON record per run of Phases B and C: `*.watch.json` (Dagster arms C and C′), `*.race.json` (observed portfolio races, arm Br), `*.profile.json` (per-run MiniSat profile); per-target DAG `*.components.json` and `*.manifest.json`; yield summaries; `census/` (the arm-B trajectory census); `exit_signals.csv` (which Dagster runs ended by SIGKILL) |
| `derived/runs.csv` | one row per run (220), censored runs included: phase, arm, target, repeat, L, budget, status, time to verified preimage, end time, winning component or seed, seed block, invalid solutions, SIGKILL |
| `derived/summary.csv` | per phase, arm and target, and pooled: solved, min / median / max time, physical core-hours, verified solutions per physical core-hour |
| `derived/census.csv` | per evaluation target: MiniSat trajectories, solved within the cap, solved times |
| `derived/budget.csv` | per conflict-budget arm: budget-exhaustion events by role |
| `derived/mechanism.csv` | per L = ∞ and L = 8 run: terminal dispatches (and after the first second), refinements retired and emitted, maximum descendants per parent, SIGKILL |
| `instances/` | the five targets: `instances.csv` (seed, digest, sizes, SHA-256 of raw and repaired CNF, DAG and combined DAG CNF) and the planted blocks `g*.planted` |
| `validation/phaseA_report.txt` | Phase A, semantic equivalence regression: PASS |
| `validation/dagster_bounded_v3.diff`, `.manifest.json` | the patch as applied in the campaign build, with stock and patched file hashes (the diff quotes Dagster source, GPL-2.0-or-later) |
| `checksums.txt` | instances, generator, engines, patch, and the raw archives and event logs that are not distributed |
| `provenance.txt` | the campaign environment, including what was not recorded |

## Privacy

Before release, every absolute path in the run records was rewritten relative to the repository root
and every host name replaced by `workstation`. The raw campaign archives and the Dagster event logs
contain absolute paths and host names, so they are not distributed; `checksums.txt` identifies them by
SHA-256.

## Regenerating the tables

From the repository root:

    python3 evidence/scripts/derive.py evidence/runs rederived
    diff evidence/derived/runs.csv rederived/runs.csv          # likewise summary, census, budget

`mechanism.csv` needs the raw event logs: `derive.py evidence/runs rederived --events RAW_RESULTS_DIR`.
For a new campaign, point `derive.py` at its own records; the numbers will be those of the new machine.
`make selftest` performs the regeneration and comparison automatically.
