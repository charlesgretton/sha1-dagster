#!/usr/bin/env bash
# portfolio_harness.sh -- seed-only portfolio baseline for reduced-round SHA-1.
#
# Same instance, same engine, only the solver seed varies. Measures F_r(t),
# the per-target runtime CDF, which is the denominator for every later claim.
#
#   SOLVER=/path/to/minisat ./portfolio/portfolio_harness.sh
#
# Config (env):
#   SOLVER                solver binary (required)
#   SOLVER_ARGS_TEMPLATE  default "-rnd-init -rnd-seed={seed} {cnf} {model}"
#   ROUNDS                default "20 21 22"
#   TARGET_SEEDS          generator seeds, paired across rounds
#                         default "10001 10002 10003 10004 10005"
#   SOLVER_SEEDS          trajectories per target        default 100
#   SOLVER_SEED_START     first solver seed (shard a campaign across jobs,
#                         e.g. 1, 51, 101 ...)             default 1
#   WORKERS               concurrent trajectories        default 1
#   TIMEOUT_SEC           per-trajectory limit           default 300
#   KILL_AFTER            TERM->KILL grace (s)           default 5
#   PIN                   1 = pin worker i to core i mod WORKERS
#   EXPERIMENT_ID         default exp_<utc timestamp>
#   RESULTS_ROOT          default ./results
#   SKIP_PREFLIGHT        1 = skip the seed-diversification check (tests only)
set -euo pipefail
export LC_ALL=C

HERE=$(cd "$(dirname "$0")" && pwd)
BUNDLE=$(cd "$HERE/.." && pwd)

: "${SOLVER:?set SOLVER to the solver binary}"
SOLVER=$(readlink -f "$SOLVER")
SOLVER_ARGS_TEMPLATE=${SOLVER_ARGS_TEMPLATE:-"-rnd-init -rnd-seed={seed} {cnf} {model}"}
ROUNDS=${ROUNDS:-"20 21 22"}
TARGET_SEEDS=${TARGET_SEEDS:-"10001 10002 10003 10004 10005"}
SOLVER_SEEDS=${SOLVER_SEEDS:-100}
SOLVER_SEED_START=${SOLVER_SEED_START:-1}
WORKERS=${WORKERS:-1}
TIMEOUT_SEC=${TIMEOUT_SEC:-300}
KILL_AFTER=${KILL_AFTER:-5}
PIN=${PIN:-0}
EXPERIMENT_ID=${EXPERIMENT_ID:-exp_$(date -u +%Y%m%dT%H%M%SZ)}
RESULTS_ROOT=${RESULTS_ROOT:-results}
GEN="$BUNDLE/bin/sha1gen"
VERIFIER="$BUNDLE/tools/verify_preimage.py"
DIR="$RESULTS_ROOT/$EXPERIMENT_ID"

die() { echo "ERROR: $*" >&2; exit 2; }

# ---- refuse to touch an existing campaign ---------------------------------
[[ -e "$DIR" ]] && die "campaign directory exists, refusing to append: $DIR"
for t in jq flock setsid pkill /usr/bin/time timeout python3; do
    command -v "$t" >/dev/null || die "missing required tool: $t"
done
[[ -x "$GEN" ]] || die "generator not built: run 'make' in $BUNDLE"
[[ -x "$SOLVER" ]] || die "solver not executable: $SOLVER"

mkdir -p "$DIR"/{cnfs/raw,cnfs/solver,targets,logs,timing,models,verification,records}
DIR=$(readlink -f "$DIR")
export DIR SOLVER SOLVER_ARGS_TEMPLATE TIMEOUT_SEC KILL_AFTER VERIFIER

# ---- campaign provenance ----------------------------------------------------
smt=$(cat /sys/devices/system/cpu/smt/active 2>/dev/null || echo unknown)
jq -n \
  --arg id "$EXPERIMENT_ID" --arg host "$(hostname)" \
  --arg started "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg solver "$SOLVER" --arg solver_sha "$(sha256sum "$SOLVER" | cut -d' ' -f1)" \
  --arg tmpl "$SOLVER_ARGS_TEMPLATE" \
  --arg gen_sha "$(sha256sum "$GEN" | cut -d' ' -f1)" \
  --arg rounds "$ROUNDS" --arg tseeds "$TARGET_SEEDS" \
  --arg job "${PBS_JOBID:-${SLURM_JOB_ID:-none}}" \
  --arg affinity "$(taskset -pc $$ 2>/dev/null | awk -F': ' '{print $2}')" \
  --arg smt "$smt" --arg kernel "$(uname -r)" \
  --argjson nproc "$(nproc)" --argjson workers "$WORKERS" \
  --argjson seeds "$SOLVER_SEEDS" --argjson seed_start "$SOLVER_SEED_START" --argjson timeout "$TIMEOUT_SEC" \
  --argjson kill_after "$KILL_AFTER" --argjson pin "$PIN" \
  --arg tdc "${TINISAT_DAGSTER_CONFIG:-0}" --arg cb "${CUBE_BITS:-}" \
  --arg cv "${CUBE_VARS:-}" --arg cp "${CUBE_PERMUTE:-}" \
  '{experiment_id:$id, hostname:$host, started:$started, scheduler_job:$job,
    logical_cpus:$nproc, cpu_affinity:$affinity, smt_active:$smt, kernel:$kernel,
    solver:$solver, solver_sha256:$solver_sha, solver_args_template:$tmpl,
    generator_sha256:$gen_sha, rounds:$rounds, target_seeds:$tseeds,
    solver_seeds_per_target:$seeds, solver_seed_start:$seed_start, workers:$workers, timeout_sec:$timeout,
    kill_after_sec:$kill_after, pinned:$pin,
    engine_env:{TINISAT_DAGSTER_CONFIG:$tdc, CUBE_BITS:$cb, CUBE_VARS:$cv, CUBE_PERMUTE:$cp}}' >"$DIR/campaign.json"

# ---- target generation: paired seeds across rounds, full provenance ---------
echo "== generating targets"
for gs in $TARGET_SEEDS; do
  for r in $ROUNDS; do
    raw="$DIR/cnfs/raw/r${r}_g${gs}.cnf"
    sol="$DIR/cnfs/solver/r${r}_g${gs}.cnf"
    gcmd="$GEN --attack=preimage --rounds=$r --hash-bits=160 --seed=$gs --cnf --tseitin-adders"
    SHA1GEN_PLANTED_OUT="$DIR/targets/r${r}_g${gs}.planted" $gcmd >"$raw"
    # repaired header, comments stripped (solver input); raw keeps the symbol
    # table and is what the verifier checks against
    python3 - "$raw" "$sol" "$BUNDLE/tools" <<'PY'
import sys; sys.path.insert(0, sys.argv[3])
from sha1_cnf import Sha1Cnf
c = Sha1Cnf(sys.argv[1]); c.write_repaired(sys.argv[2])
units = {abs(l[0]): l[0] > 0 for l in c.clauses if len(l) == 1}
hexw = []
for j in range(5):
    w = 0
    for b, v in enumerate(c.h_out(j)):
        if units.get(v): w |= 1 << b
    hexw.append("%08x" % w)
print(" ".join(hexw))
PY
  done
done >"$DIR/targets/.hashes"
i=0
for gs in $TARGET_SEEDS; do
  for r in $ROUNDS; do
    i=$((i+1))
    raw="$DIR/cnfs/raw/r${r}_g${gs}.cnf"; sol="$DIR/cnfs/solver/r${r}_g${gs}.cnf"
    jq -n --argjson r "$r" --argjson gs "$gs" \
      --arg cmd "$GEN --attack=preimage --rounds=$r --hash-bits=160 --seed=$gs --cnf --tseitin-adders" \
      --arg target "$(sed -n "${i}p" "$DIR/targets/.hashes")" \
      --arg raw_sha "$(sha256sum "$raw" | cut -d' ' -f1)" \
      --arg sol_sha "$(sha256sum "$sol" | cut -d' ' -f1)" \
      --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      --arg planted "$(tr '\n' ' ' <"$DIR/targets/r${r}_g${gs}.planted" 2>/dev/null)" \
      '{rounds:$r, generator_seed:$gs, generator_command:$cmd, target_hash:$target,
        raw_cnf_sha256:$raw_sha, repaired_cnf_sha256:$sol_sha, generated:$ts,
        planted_message_words:$planted}' \
      >"$DIR/targets/r${r}_g${gs}.json"
  done
done
rm -f "$DIR/targets/.hashes"

# ---- validate inputs: fail, never skip --------------------------------------
missing=0
for gs in $TARGET_SEEDS; do for r in $ROUNDS; do
  for f in "$DIR/cnfs/raw/r${r}_g${gs}.cnf" "$DIR/cnfs/solver/r${r}_g${gs}.cnf"; do
    [[ -s "$f" ]] || { echo "missing/empty: $f" >&2; missing=1; }
  done
done; done
(( missing == 0 )) || die "input validation failed"

# ---- preflight: does the seed actually change the trajectory? ---------------
# MiniSat's -rnd-seed is a no-op without -rnd-init; a portfolio of identical
# trajectories measures nothing. Run two seeds on a dedicated EASY instance
# (r=20; the seed's effect on the engine does not depend on the target) and
# require two verified, DIFFERENT models. Model comparison is solver-agnostic:
# with ~2^352 preimages, distinct trajectories essentially never agree.
if [[ "${SKIP_PREFLIGHT:-0}" != 1 ]]; then
  pf_raw="$DIR/cnfs/preflight_r20_raw.cnf"; pf="$DIR/cnfs/preflight_r20.cnf"
  "$GEN" --attack=preimage --rounds=20 --hash-bits=160 --seed=1 --cnf --tseitin-adders >"$pf_raw"
  python3 -c "import sys; sys.path.insert(0,'$BUNDLE/tools'); from sha1_cnf import Sha1Cnf; Sha1Cnf('$pf_raw').write_repaired('$pf')"
  shas=()
  for s in 1 2; do
    mf="$DIR/cnfs/preflight_seed$s.model"
    read -r -a tmpl <<<"$SOLVER_ARGS_TEMPLATE"; argv=()
    for a in "${tmpl[@]}"; do a=${a//\{seed\}/$s}; a=${a//\{cnf\}/$pf}; a=${a//\{model\}/$mf}; argv+=("$a"); done
    set +e; timeout "${PREFLIGHT_TIMEOUT:-120}" "$SOLVER" "${argv[@]}" >"$DIR/cnfs/preflight_seed$s.log" 2>&1; rc=$?; set -e
    if [[ $rc != 10 ]]; then
      echo "---- preflight solver output (seed $s, exit $rc) ----" >&2
      tail -n 15 "$DIR/cnfs/preflight_seed$s.log" >&2
      echo "------------------------------------------------------" >&2
      case $rc in
        127) hint="command not found or a shared library failed to load (check 'make engines' and LD_LIBRARY_PATH)";;
        126) hint="found but not executable";;
        124|137) hint="timed out on the easy r=20 check instance";;
        *) hint="unexpected solver exit";;
      esac
      die "preflight: seed $s did not return SAT on the r=20 check instance (exit $rc: $hint)"
    fi
    python3 "$VERIFIER" "$pf_raw" "$mf" >/dev/null 2>&1 || die "preflight: seed $s model failed verification"
    shas+=("$(sha256sum "$mf" | cut -d' ' -f1)")
  done
  [[ "${shas[0]}" != "${shas[1]}" ]] || die "preflight: seeds 1 and 2 returned the identical model. The seed has no effect -- check SOLVER_ARGS_TEMPLATE (MiniSat needs -rnd-init)."
  echo "== preflight ok: seeds 1 and 2 give distinct verified models"
fi

# ---- process management -----------------------------------------------------
SID_FILE="$DIR/.active_sessions"
: >"$SID_FILE"
declare -a ACTIVE=()

sweep_session() {   # kill everything still in a worker's session
    pkill -KILL -s "$1" 2>/dev/null || true
}
is_running() {      # zombies count as finished
    local st
    [[ -r /proc/$1/stat ]] || return 1
    st=$(awk '{print $3}' "/proc/$1/stat" 2>/dev/null) || return 1
    [[ "$st" != Z ]]
}
reap() {
    local keep=() p
    for p in "${ACTIVE[@]}"; do
        if is_running "$p"; then keep+=("$p")
        else wait "$p" 2>/dev/null || true; sweep_session "$p"; fi
    done
    ACTIVE=("${keep[@]}")
}
wait_for_capacity() {
    while (( ${#ACTIVE[@]} >= WORKERS )); do
        reap; (( ${#ACTIVE[@]} < WORKERS )) && break; sleep 0.2
    done
}
cleanup() {
    local rc=${1:-$?}
    trap - EXIT INT TERM HUP
    if [[ -s "$SID_FILE" ]]; then
        while read -r sid; do pkill -TERM -s "$sid" 2>/dev/null || true; done <"$SID_FILE"
        sleep 2
        while read -r sid; do pkill -KILL -s "$sid" 2>/dev/null || true; done <"$SID_FILE"
    fi
    wait 2>/dev/null || true
    exit "$rc"
}
trap 'cleanup' EXIT
trap 'echo "!! interrupted: killing all worker sessions" >&2; cleanup 130' INT
trap 'echo "!! terminated: killing all worker sessions" >&2; cleanup 143' TERM
trap 'cleanup 129' HUP

# ---- dispatch ------------------------------------------------------------------
total=0
for r in $ROUNDS; do for gs in $TARGET_SEEDS; do total=$((total + SOLVER_SEEDS)); done; done
echo "== dispatching $total trajectories, $WORKERS concurrent, ${TIMEOUT_SEC}s limit"
n=0
for r in $ROUNDS; do
  for gs in $TARGET_SEEDS; do
    raw="$DIR/cnfs/raw/r${r}_g${gs}.cnf"; sol="$DIR/cnfs/solver/r${r}_g${gs}.cnf"
    for ((s=SOLVER_SEED_START; s<SOLVER_SEED_START+SOLVER_SEEDS; s++)); do
      wait_for_capacity
      run_id="r${r}_g${gs}_s${s}"
      core=""
      [[ "$PIN" == 1 ]] && core=$(( n % WORKERS ))
      PIN_CORE="$core" setsid "$HERE/run_one.sh" "$run_id" "$r" "$gs" "$s" "$raw" "$sol" &
      pid=$!
      ACTIVE+=("$pid"); echo "$pid" >>"$SID_FILE"
      n=$((n+1))
      (( n % 50 == 0 )) && echo "   dispatched $n/$total"
    done
  done
done
while (( ${#ACTIVE[@]} > 0 )); do reap; sleep 0.2; done

# ---- assemble: validate every record, deterministic order ----------------------
bad=0
for f in "$DIR"/records/*.json; do jq -e . "$f" >/dev/null 2>&1 || { echo "corrupt record: $f" >&2; bad=1; }; done
got=$(find "$DIR/records" -name '*.json' | wc -l)
find "$DIR/records" -name '*.json' -print0 | sort -z | xargs -0 cat >"$DIR/manifest.jsonl"
jq -n --arg done "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson exp "$total" --argjson got "$got" \
   '{finished:$done, expected_records:$exp, records:$got}' >"$DIR/completion.json"
echo "== $got/$total records -> $DIR/manifest.jsonl"
(( got == total && bad == 0 )) || die "record count or integrity mismatch"
jq -r '.status' "$DIR/manifest.jsonl" | sort | uniq -c
if jq -e 'select(.status=="UNSAT")' "$DIR/manifest.jsonl" >/dev/null 2>&1; then
  echo "!! INTEGRITY WARNING: UNSAT on a planted (known-satisfiable) target" >&2
fi
if jq -e 'select(.status=="INVALID_MODEL")' "$DIR/manifest.jsonl" >/dev/null 2>&1; then
  echo "!! INTEGRITY WARNING: solver returned models that failed verification" >&2
fi
