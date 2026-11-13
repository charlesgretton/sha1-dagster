#!/bin/sh
# Experiment 0 -- k independent solvers on the SAME unrestricted CNF, differing
# only in random seed, racing to first VERIFIED preimage.  This is the null
# hypothesis: that gains attributed to structured decomposition are really
# solver-trajectory diversification.
#
#   scripts/portfolio.sh instances/pre_r21_hb160.cnf 64 300
#
# SOLVER_WRAPPER has a fixed interface so the harness is engine-neutral:
#   wrapper SEED CNF MODEL  ->  exit 10 SAT (model written) / 20 UNSAT / other
# Reference: scripts/solvers/pysat_wrapper.py.  For the strict null control,
# wrap the SAME engine and configuration Dagster uses and vary only the seed.
#
# Concurrency notes (each of these was a real bug in an earlier version):
#  * liveness is tested with per-worker .done markers, never `jobs %%`, which
#    reports only the most recently launched job and would end the race as soon
#    as the LAST-launched worker finished;
#  * each worker records its OWN pid from inside a setsid'd process, so the
#    recorded value really is the process-group leader -- $! from the parent is
#    the enclosing subshell and may not be the group leader;
#  * logs, models and the manifest are copied to a persistent RESULTDIR before
#    the scratch directory is removed, so CSV paths still resolve afterwards.
set -eu

# ---------------------------------------------------------------- worker mode
if [ "${1:-}" = "--worker" ]; then
    w=$2; seed=$3; TIMEOUT=$4; SOLVER_WRAPPER=$5; CNF=$6; RUN=$7
    echo "$$" > "$RUN/$w.pgid"        # we are the session/group leader here
    t0=$(date +%s.%N)
    set +e
    timeout "$TIMEOUT" "$SOLVER_WRAPPER" "$seed" "$RUN/pf.cnf" "$RUN/$w.model" \
        > "$RUN/$w.log" 2>&1
    rc=$?
    set -e
    t1=$(date +%s.%N)
    wall=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.3f", b-a}')

    verified=no
    case $rc in
      10) if python3 tools/verify_preimage.py "$CNF" "$RUN/$w.model" \
               > "$RUN/$w.verify" 2>&1 && grep -q "VALID PREIMAGE" "$RUN/$w.verify"; then
              status=SAT_VERIFIED; verified=yes
              if mkdir "$RUN/winner.lock" 2>/dev/null; then
                  printf '%s\n' "$w" > "$RUN/winner"
              fi
          else
              status=INVALID_MODEL
          fi ;;
      20)      status=UNSAT ;;        # suspicious on an unrestricted instance
      124)     status=TIMEOUT ;;
      143|137) status=CANCELLED ;;
      *)       status=ERROR ;;
    esac
    printf '%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$w" "$seed" "$SOLVER_WRAPPER" "$status" "$rc" "$wall" "$verified" "logs/$w.log" \
        > "$RUN/$w.csv"
    : > "$RUN/$w.done"
    exit 0
fi

# ---------------------------------------------------------------- parent mode
CNF=${1:?usage: portfolio.sh CNF [K] [TIMEOUT]}
K=${2:-64}
TIMEOUT=${3:-300}
SOLVER_WRAPPER=${SOLVER_WRAPPER:-scripts/solvers/pysat_wrapper.py}
GRACE=${GRACE:-5}
RESULTDIR=${RESULTDIR:-portfolio-results/$(date +%Y%m%d_%H%M%S)}
SELF=$(cd "$(dirname "$0")" && pwd)/$(basename "$0")

RUN=$(mktemp -d "${TMPDIR:-/tmp}/sha1-portfolio.XXXXXX")
cancel_all() {
    [ -d "$RUN" ] || return 0
    for f in "$RUN"/*.pgid; do
        [ -f "$f" ] || continue
        pg=$(cat "$f" 2>/dev/null) || continue
        kill -TERM "-$pg" 2>/dev/null || kill -TERM "$pg" 2>/dev/null || true
    done
}
cleanup() {
    cancel_all
    sleep "$GRACE"
    for f in "$RUN"/*.pgid; do
        [ -f "$f" ] || continue
        pg=$(cat "$f" 2>/dev/null) || continue
        kill -KILL "-$pg" 2>/dev/null || kill -KILL "$pg" 2>/dev/null || true
    done
    rm -rf "$RUN"
}
trap cleanup EXIT INT TERM HUP

mkdir -p "$RESULTDIR/logs"
OUT=${OUT:-$RESULTDIR/portfolio.csv}

# Repair the DIMACS header.  Verification uses the ORIGINAL CNF, which still
# carries the sha1-sat symbol table.
python3 tools/sha1_dagify.py "$CNF" --out-cnf "$RUN/pf.cnf" \
        --out-dag "$RUN/pf.dag" --strategy onenode --quiet

{
  echo "date=$(date -Is)"
  echo "host=$(hostname)"
  echo "cnf=$CNF"
  echo "cnf_sha256=$(sha256sum "$CNF" | cut -d' ' -f1)"
  echo "repaired_cnf_sha256=$(sha256sum "$RUN/pf.cnf" | cut -d' ' -f1)"
  echo "wrapper=$SOLVER_WRAPPER"
  echo "wrapper_sha256=$(sha256sum "$SOLVER_WRAPPER" 2>/dev/null | cut -d' ' -f1)"
  echo "k=$K timeout=$TIMEOUT"
} > "$RESULTDIR/manifest.txt"

start=$(date +%s.%N)
i=1
while [ "$i" -le "$K" ]; do
    w=$(printf 'w%05d' "$i")
    seed=$((i * 7919))
    if command -v setsid >/dev/null 2>&1; then
        setsid "$SELF" --worker "$w" "$seed" "$TIMEOUT" "$SOLVER_WRAPPER" "$CNF" "$RUN" &
    else
        "$SELF" --worker "$w" "$seed" "$TIMEOUT" "$SOLVER_WRAPPER" "$CNF" "$RUN" &
    fi
    i=$((i + 1))
done

# wait for a verified winner, or for ALL workers to finish -- counting .done
# markers, so an early finish by any single worker cannot end the race
while :; do
    [ -f "$RUN/winner" ] && break
    done_n=$(set -- "$RUN"/*.done; [ -e "$1" ] && echo $# || echo 0)
    [ "$done_n" -ge "$K" ] && break
    sleep 0.2
done
end=$(date +%s.%N)

cancel_all
sleep 1
wait 2>/dev/null || true

# preserve everything the CSV refers to, before the trap wipes scratch
cp "$RUN"/*.log "$RESULTDIR/logs/" 2>/dev/null || true
cp "$RUN"/*.verify "$RESULTDIR/logs/" 2>/dev/null || true
if [ -f "$RUN/winner" ]; then
    win=$(cat "$RUN/winner")
    cp "$RUN/$win.model" "$RESULTDIR/winner.model" 2>/dev/null || true
    cp "$RUN/winner" "$RESULTDIR/winner.txt" 2>/dev/null || true
fi

printf 'worker,seed,solver,status,exit_code,wall_s,verified,log\n' > "$OUT"
i=1
while [ "$i" -le "$K" ]; do
    w=$(printf 'w%05d' "$i")
    if [ -f "$RUN/$w.csv" ]; then
        cat "$RUN/$w.csv" >> "$OUT"
    else
        printf '%s,%s,%s,CANCELLED,,,no,logs/%s.log\n' \
            "$w" "$((i * 7919))" "$SOLVER_WRAPPER" "$w" >> "$OUT"
    fi
    i=$((i + 1))
done

if [ -f "$RESULTDIR/winner.txt" ]; then
    echo "first VERIFIED preimage: $(cat "$RESULTDIR/winner.txt")  after $(awk -v a="$start" -v b="$end" 'BEGIN{printf "%.3f", b-a}')s  (k=$K)"
else
    echo "no verified preimage (k=$K, timeout ${TIMEOUT}s)"
fi
echo "results in $RESULTDIR"
exit 0
