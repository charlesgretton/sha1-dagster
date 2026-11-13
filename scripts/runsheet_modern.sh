#!/usr/bin/env bash
# runsheet_modern.sh -- unrestricted Kissat and CaDiCaL portfolios at r=21 and r=22,
# under the SAME execution context as the TiniSAT campaign (63 pinned workers,
# 300 s), on the same five targets. Strictly sequential.
#   ./scripts/runsheet_modern.sh                 both engines
#   ENGINES=kissat ./scripts/runsheet_modern.sh  one engine
# Fatal (non-zero exit): harness failure, wrong record count, status other than
# SAT_VERIFIED/TIMEOUT/TIMEOUT_KILLED, a solved/TIMEOUT record without conflicts,
# a target differing from results/tinisat_r20-22, or that reference missing.
set -u
export LC_ALL=C
cd "$(dirname "$0")/.."
ENGINES=${ENGINES:-"kissat cadical"}
WORKERS=${WORKERS:-63}; PIN=${PIN:-1}; TIMEOUT_SEC=${TIMEOUT_SEC:-300}
ROUNDS=${ROUNDS:-"21 22"}
TARGETS=${TARGETS:-"10001 10002 10003 10004 10005"}
SEEDS=${SEEDS:-100}
REF=results/tinisat_r20-22
EXPECTED=$(( SEEDS * $(wc -w <<<"$TARGETS") * $(wc -w <<<"$ROUNDS") ))
say() { printf '\n==== %s  [%s]\n' "$*" "$(date -u +%H:%M:%SZ)"; }
[[ -d "$REF/cnfs/solver" || "${SKIP_IDENTITY:-0}" == 1 ]] || {
  echo "ERROR: reference campaign $REF missing: symlink it (see the development-phase notes in README.md) or set SKIP_IDENTITY=1" >&2; exit 8; }
for eng in $ENGINES; do
  [[ -x portfolio/engines/$eng ]] || { echo "ERROR: portfolio/engines/$eng missing: make modern-engines" >&2; exit 9; }
done
for eng in $ENGINES; do
  id=modern_${eng}_r$(tr ' ' '-' <<<"$ROUNDS")
  if [[ -e results/$id/completion.json ]]; then say "$eng: $id already complete, skipping"; continue; fi
  [[ -e results/$id ]] && { echo "ERROR: results/$id exists but is incomplete; move it aside" >&2; exit 2; }
  say "$eng: unrestricted portfolio, rounds $ROUNDS, $EXPECTED runs -> results/$id"
  env SOLVER=portfolio/engines/modern.sh "SOLVER_ARGS_TEMPLATE=$eng {seed} {cnf} {model}" \
      "ROUNDS=$ROUNDS" "TARGET_SEEDS=$TARGETS" SOLVER_SEEDS=$SEEDS SOLVER_SEED_START=1 \
      WORKERS=$WORKERS PIN=$PIN TIMEOUT_SEC=$TIMEOUT_SEC EXPERIMENT_ID=$id \
      portfolio/portfolio_harness.sh > "logs_$id.txt" 2>&1
  rc=$?; tail -6 "logs_$id.txt"
  (( rc == 0 )) || { echo "ERROR: $eng harness exit $rc -- see logs_$id.txt" >&2; exit 3; }
  got=$(jq -r .records "results/$id/completion.json")
  [[ "$got" == "$EXPECTED" ]] || { echo "ERROR: $eng expected $EXPECTED records, got $got" >&2; exit 4; }
  bad=$(jq -r 'select(.status!="SAT_VERIFIED" and .status!="TIMEOUT" and .status!="TIMEOUT_KILLED")|"\(.run_id) \(.status)"' "results/$id/manifest.jsonl")
  [[ -z "$bad" ]] || { echo "ERROR: $eng records neither solved nor censored:" >&2; echo "$bad" | head -20 >&2; exit 6; }
  nulls=$(jq -r 'select(.conflicts==null and .status!="TIMEOUT_KILLED")|.run_id' "results/$id/manifest.jsonl" | wc -l)
  (( nulls == 0 )) || { echo "ERROR: $eng has $nulls solved/TIMEOUT records without conflicts" >&2; exit 7; }
  if [[ "${SKIP_IDENTITY:-0}" != 1 ]]; then
    for f in results/$id/cnfs/solver/r*_g*.cnf; do
      cmp -s "$f" "$REF/cnfs/solver/$(basename "$f")" || { echo "ERROR: target $(basename "$f") differs from $REF" >&2; exit 5; }
    done; echo "   targets identical to $REF"
  fi
  { python3 tools/estimate_multiplier.py "results/$id/manifest.jsonl"
    echo; python3 tools/estimate_multiplier.py "results/$id/manifest.jsonl" --metric conflicts || true
  } > "multiplier_$id.txt" 2>&1
  python3 portfolio/analyse_portfolio.py "results/$id" > "out_$id.txt"
done
say "side-by-side against the TiniSAT campaign"
args=("tinisat=$REF/manifest.jsonl")
for eng in $ENGINES; do id=modern_${eng}_r$(tr ' ' '-' <<<"$ROUNDS"); [[ -e results/$id/manifest.jsonl ]] && args+=("$eng=results/$id/manifest.jsonl"); done
[[ -e $REF/manifest.jsonl ]] && python3 portfolio/compare_campaigns.py "${args[@]}" --workers "$WORKERS" | tee compare_modern.txt
tar czf send_back_modern.tgz compare_modern.txt out_modern_*.txt multiplier_modern_*.txt logs_modern_*.txt \
    $(for d in results/modern_*; do echo "$d/manifest.jsonl $d/campaign.json $d/completion.json $d/targets"; done) 2>/dev/null
ls -la send_back_modern.tgz
