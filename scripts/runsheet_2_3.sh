#!/usr/bin/env bash
# runsheet_2_3.sh -- executes the development-phase cube-census steps 2 and 3 in order, with checks.
#   ./scripts/runsheet_2_3.sh            steps 2a, 2b, 3a (+ analyses)
#   RUN_3B=1 ./scripts/runsheet_2_3.sh    also step 3b (see decision rule)
#   ONLY=3a  ./scripts/runsheet_2_3.sh    run a single step
# Runs are strictly sequential: a census must own the 63 cores alone.
# Fatal (non-zero exit, nothing further runs): harness failure, wrong record count,
# any status other than SAT_VERIFIED / TIMEOUT / TIMEOUT_KILLED, a solved or TIMEOUT
# record without conflicts, or a regenerated target differing from the campaign's.
set -u
export LC_ALL=C
cd "$(dirname "$0")/.."
WORKERS=${WORKERS:-63}      # concurrent cube runs: only affects how long a census takes
ANALYSIS_K=${ANALYSIS_K:-63} # K in the campaign analysis: 63 matches the portfolio races
                             # and Dagster at 64 ranks (1 master + 63 workers). Keep 63
                             # unless comparing against a 64-worker Dagster run.
PIN=${PIN:-1}; TIMEOUT_SEC=${TIMEOUT_SEC:-300}
TARGETS=${TARGETS:-"10001 10002 10003 10004 10005"}
SEEDS=${SEEDS:-128}          # 128 = every cube of the 7-bit split; override only for testing
EXPECTED=$(( SEEDS * $(wc -w <<<"$TARGETS") ))
common=(env CUBE_BITS=7 CUBE_VARS=1-7 CUBE_PERMUTE=0
        SOLVER=portfolio/engines/tinisat_cube.sh "SOLVER_ARGS_TEMPLATE={seed} {cnf} {model}"
        "TARGET_SEEDS=$TARGETS" SOLVER_SEEDS=$SEEDS SOLVER_SEED_START=1
        WORKERS=$WORKERS PIN=$PIN TIMEOUT_SEC=$TIMEOUT_SEC)
say() { printf '\n==== %s  [%s]\n' "$*" "$(date -u +%H:%M:%SZ)"; }
census() {   # census <step> <round> <dagster_config 0|1> <experiment_id>
  local step=$1 r=$2 cfg=$3 id=$4
  [[ -n "${ONLY:-}" && "$ONLY" != "$step" ]] && return 0
  if [[ -e results/$id/completion.json ]]; then say "$step: $id already complete, skipping"; return 0; fi
  [[ -e results/$id ]] && { echo "ERROR: results/$id exists but is incomplete; move it aside and rerun" >&2; exit 2; }
  say "$step: census r=$r TINISAT_DAGSTER_CONFIG=$cfg -> results/$id (<= ~55 min)"
  "${common[@]}" TINISAT_DAGSTER_CONFIG=$cfg ROUNDS=$r EXPERIMENT_ID=$id \
      portfolio/portfolio_harness.sh > "logs_$id.txt" 2>&1
  local rc=$?
  tail -8 "logs_$id.txt"
  (( rc == 0 )) || { echo "ERROR: $step harness exit $rc -- see logs_$id.txt" >&2; exit 3; }
  local got; got=$(jq -r .records "results/$id/completion.json")
  [[ "$got" == "$EXPECTED" ]] || { echo "ERROR: $step expected $EXPECTED records, got $got" >&2; exit 4; }
  # Accepted outcomes are exactly the harness's success and censoring classes.
  local bad; bad=$(jq -r 'select(.status!="SAT_VERIFIED" and .status!="TIMEOUT" and .status!="TIMEOUT_KILLED")|"\(.run_id) \(.status)"' "results/$id/manifest.jsonl")
  if [[ -n "$bad" ]]; then
    echo "ERROR: $step has records that are neither solved nor censored:" >&2; echo "$bad" | head -20 >&2; exit 6
  fi
  # Every solved or TERM-censored run must carry a conflict count. Only a run
  # KILLed after the grace period (TIMEOUT_KILLED) legitimately has none.
  local nulls; nulls=$(jq -r 'select(.conflicts==null and .status!="TIMEOUT_KILLED")|.run_id' "results/$id/manifest.jsonl" | wc -l)
  if (( nulls > 0 )); then
    echo "ERROR: $step has $nulls solved/TIMEOUT records without conflicts: engine not rebuilt? (make engines)" >&2; exit 7
  fi
  local killed; killed=$(jq -r 'select(.status=="TIMEOUT_KILLED")|.run_id' "results/$id/manifest.jsonl" | wc -l)
  (( killed == 0 )) || echo "   note: $killed TIMEOUT_KILLED runs (censored, no conflict threshold; conflict-metric estimates will refuse this round)"
  jq -c .engine_env "results/$id/campaign.json"
  # the regenerated targets must be byte-identical to the original portfolio campaign
  if [[ ! -d results/tinisat_r20-22/cnfs/solver && "${SKIP_IDENTITY:-0}" != 1 ]]; then
    echo "ERROR: reference results/tinisat_r20-22 missing: symlink it or set SKIP_IDENTITY=1" >&2; exit 8
  fi
  if [[ -d results/tinisat_r20-22/cnfs/solver ]]; then
    for f in results/$id/cnfs/solver/r${r}_g*.cnf; do
      cmp -s "$f" "results/tinisat_r20-22/cnfs/solver/$(basename "$f")" \
        || { echo "ERROR: target $(basename "$f") differs from the portfolio campaign" >&2; exit 5; }
    done; echo "   targets identical to results/tinisat_r20-22"
  fi
  python3 portfolio/analyse_portfolio.py "results/$id" > "out_$id.txt"
  python3 portfolio/census_campaign.py "results/$id/manifest.jsonl" --workers "$ANALYSIS_K" --bits 7 > "census_$id.txt"
  echo "   wrote out_$id.txt census_$id.txt"; cat "census_$id.txt"
}
census 2a 21 0 cube_r21_std
census 2b 21 1 cube_r21_dagcfg
census 3a 22 1 cube_r22_dagcfg
# Step 3b: pre-registered rule, evaluated by portfolio/config_differs.py.
# RUN_3B=1 forces it, RUN_3B=0 forces skip, unset applies the rule. ONLY=3b forces it.
if [[ "${ONLY:-}" == 3b || "${RUN_3B:-}" == 1 ]]; then
  census 3b 22 0 cube_r22_std
elif [[ -z "${RUN_3B:-}" && -z "${ONLY:-}" ]]; then
  say "3b decision (rule: 2a vs 2b differ >2x in F(300) or campaign median on >=3 of 5 targets)"
  python3 portfolio/config_differs.py results/cube_r21_std/manifest.jsonl results/cube_r21_dagcfg/manifest.jsonl \
      --workers "$ANALYSIS_K" --bits 7 | tee decision_3b.txt
  case ${PIPESTATUS[0]} in
    0) census 3b 22 0 cube_r22_std ;;
    1) echo "   rule not met: 3b skipped" ;;
    *) echo "   !! rule could not be evaluated: 3b NOT run; inspect decision_3b.txt" ;;
  esac
fi
say "multiplier r21->r22 under cubing"
if [[ -e results/cube_r21_dagcfg/completion.json && -e results/cube_r22_dagcfg/completion.json ]]; then
  { python3 tools/estimate_multiplier.py results/cube_r21_dagcfg/manifest.jsonl results/cube_r22_dagcfg/manifest.jsonl
    echo; python3 tools/estimate_multiplier.py results/cube_r21_dagcfg/manifest.jsonl results/cube_r22_dagcfg/manifest.jsonl --metric conflicts \
      || echo "   (conflict-metric estimate refused: see message above; wall-time estimate stands)"
  } | tee multiplier_cube_r21_r22.txt
fi
say "bundle for review"
tar czf send_back_2_3.tgz decision_3b.txt out_cube_*.txt census_cube_*.txt multiplier_cube_*.txt logs_cube_*.txt \
    $(for d in results/cube_r2*; do echo "$d/manifest.jsonl $d/campaign.json $d/completion.json $d/targets"; done) 2>/dev/null
ls -la send_back_2_3.tgz
