#!/usr/bin/env bash
# run_one.sh -- execute ONE solver trajectory and write ONE immutable JSON record.
#
# Launched by portfolio_harness.sh under `setsid`, so this process is a session
# leader: its PID is the session id the parent uses to sweep stragglers.
#
# Process hierarchy (timeout owns the solver directly; time only measures):
#     run_one.sh -> /usr/bin/time -> timeout -> solver
#
# args: run_id round gen_seed solver_seed raw_cnf solver_cnf
# env : DIR SOLVER SOLVER_ARGS_TEMPLATE TIMEOUT_SEC KILL_AFTER VERIFY PIN_CORE
set -uo pipefail
export LC_ALL=C

run_id=$1 round=$2 gen_seed=$3 solver_seed=$4 raw_cnf=$5 solver_cnf=$6

log_file="$DIR/logs/$run_id.log"
time_file="$DIR/timing/$run_id.time"
model_file="$DIR/models/$run_id.model"
ver_file="$DIR/verification/$run_id.log"
record_tmp="$DIR/records/.$run_id.json.tmp"
record="$DIR/records/$run_id.json"

# solver argv: template with {seed} {cnf} {model} substituted
read -r -a tmpl <<<"$SOLVER_ARGS_TEMPLATE"
argv=()
for a in "${tmpl[@]}"; do
    a=${a//\{seed\}/$solver_seed}
    a=${a//\{cnf\}/$solver_cnf}
    a=${a//\{model\}/$model_file}
    argv+=("$a")
done

pin=()
affinity="unpinned"
if [[ -n "${PIN_CORE:-}" ]]; then
    pin=(taskset -c "$PIN_CORE")
    affinity="core:$PIN_CORE"
fi

start_ts=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
start_ns=$(date +%s%N)
"${pin[@]}" /usr/bin/time -o "$time_file" \
    -f 'wall=%e\nuser=%U\nsys=%S\nrss_kb=%M\ntime_exit=%x' \
    timeout --signal=TERM --kill-after="${KILL_AFTER}s" "${TIMEOUT_SEC}s" \
    "$SOLVER" "${argv[@]}" >"$log_file" 2>&1
exit_code=$?
end_ns=$(date +%s%N)
wall=$(awk -v a="$start_ns" -v b="$end_ns" 'BEGIN{printf "%.4f", (b-a)/1e9}')

# ---- classify (exhaustive; GNU timeout and signal codes included) ----------
case "$exit_code" in
    10)          status=SAT_UNVERIFIED ;;
    20)          status=UNSAT ;;
    124)         status=TIMEOUT ;;
    137)         status=TIMEOUT_KILLED ;;   # KILL, incl. after --kill-after
    143)         status=TERMINATED ;;
    125|126|127) status=HARNESS_ERROR ;;
    *)           status=SOLVER_ERROR ;;
esac

# ---- verification upgrades SAT_UNVERIFIED; exit code 10 alone never counts --
verified=false
if [[ $status == SAT_UNVERIFIED ]]; then
    if [[ ! -s "$model_file" ]]; then
        status=SAT_NO_MODEL
    elif [[ "${VERIFY:-1}" == 1 ]]; then
        if python3 "$VERIFIER" "$raw_cnf" "$model_file" >"$ver_file" 2>&1 \
           && grep -q "VALID PREIMAGE" "$ver_file"; then
            status=SAT_VERIFIED; verified=true
        else
            status=INVALID_MODEL
        fi
    fi
fi

# ---- statistics: last anchored line, JSON null when absent (never 0) --------
stat() {   # stat <label> <file>   ->  number or null
    awk -v L="$1" '$1==L && $2==":" { v=$3; gsub(",","",v) } END { print (v=="" ? "null" : v) }' "$2"
}
tval() {   # tval <key>  from the /usr/bin/time -o file
    awk -F= -v k="$1" '$1==k { v=$2 } END { print (v=="" ? "null" : v) }' "$time_file" 2>/dev/null || echo null
}
num() {    # coerce to JSON number or null
    [[ "$1" =~ ^-?[0-9]+(\.[0-9]+)?$ ]] && echo "$1" || echo null
}

model_sha=null
[[ -s "$model_file" ]] && model_sha="\"$(sha256sum "$model_file" | cut -d' ' -f1)\""

jq -n -c \
  --arg run_id "$run_id" --arg status "$status" --arg start "$start_ts" \
  --arg affinity "$affinity" --arg host "$(hostname)" \
  --arg cmd "$SOLVER ${argv[*]}" \
  --argjson round "$round" --argjson gen_seed "$gen_seed" \
  --argjson solver_seed "$solver_seed" --argjson exit_code "$exit_code" \
  --argjson wall "$(num "$wall")" \
  --argjson user "$(num "$(tval user)")" --argjson sys "$(num "$(tval sys)")" \
  --argjson rss "$(num "$(tval rss_kb)")" --argjson verified "$verified" \
  --argjson model_sha "$model_sha" \
  --argjson conflicts "$(num "$(stat conflicts "$log_file")")" \
  --argjson decisions "$(num "$(stat decisions "$log_file")")" \
  --argjson propagations "$(num "$(stat propagations "$log_file")")" \
  --argjson restarts "$(num "$(stat restarts "$log_file")")" \
  '{run_id:$run_id, rounds:$round, hash_bits:160, message_bits:0,
    generator_seed:$gen_seed, solver_seed:$solver_seed,
    status:$status, verified:$verified, exit_code:$exit_code,
    wall_seconds:$wall, user_seconds:$user, system_seconds:$sys,
    peak_rss_kb:$rss, conflicts:$conflicts, decisions:$decisions,
    propagations:$propagations, restarts:$restarts,
    model_sha256:$model_sha, start_timestamp:$start,
    hostname:$host, cpu_affinity:$affinity, solver_command:$cmd}' \
  >"$record_tmp" && mv "$record_tmp" "$record"      # atomic publish
