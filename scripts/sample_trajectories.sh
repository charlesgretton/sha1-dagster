#!/bin/sh
# EXPERIMENT 0 -- measurement harness.
#
# Runs N INDEPENDENT trajectories per instance to completion or timeout, with
# NO first-solution cancellation, and records every one.  This is the opposite
# of scripts/portfolio.sh: that races and cancels, which right-censors every
# loser and destroys the runtime distribution.  Use this to MEASURE, and
# portfolio.sh only to VALIDATE that racing works end to end.
#
#   scripts/sample_trajectories.sh instances/pre_r21_hb160.cnf 100 300 16
#       CNF  N-trajectories  timeout-per-trajectory  parallel-workers
#
# Every trajectory is verified with tools/verify_preimage.py; an unverified
# SAT is recorded as INVALID_MODEL, never silently counted as a success.
set -eu

CNF=${1:?usage: sample_trajectories.sh CNF [N] [TIMEOUT] [PAR]}
N=${2:-100}
TIMEOUT=${3:-300}
PAR=${4:-16}
SOLVER_WRAPPER=${SOLVER_WRAPPER:-scripts/solvers/pysat_wrapper.py}
RESULTDIR=${RESULTDIR:-traj-results/$(date +%Y%m%d_%H%M%S)}
SEED_BASE=${SEED_BASE:-1}

base=$(basename "$CNF" .cnf)
rounds=$(grep -m1 "nr_rounds" "$CNF" | sed 's/.*= *//' | tr -d '\r')
hb=$(grep -m1 "Fix .* hash bits" "$CNF" | awk '{print $3}')
[ -n "$rounds" ] || rounds="?"
[ -n "$hb" ] || hb="?"

mkdir -p "$RESULTDIR/logs"
OUT="$RESULTDIR/trajectories.csv"
RUN=$(mktemp -d "${TMPDIR:-/tmp}/sha1-traj.XXXXXX")
trap 'rm -rf "$RUN"' EXIT INT TERM HUP

python3 tools/sha1_dagify.py "$CNF" --out-cnf "$RUN/t.cnf" \
        --out-dag "$RUN/t.dag" --strategy onenode --quiet

{
  echo "date=$(date -Is)"
  echo "host=$(hostname)"
  echo "cnf=$CNF"
  echo "cnf_sha256=$(sha256sum "$CNF" | cut -d' ' -f1)"
  echo "rounds=$rounds hash_bits=$hb"
  echo "n=$N timeout=$TIMEOUT parallel=$PAR seed_base=$SEED_BASE"
  echo "wrapper=$SOLVER_WRAPPER"
  echo "wrapper_sha256=$(sha256sum "$SOLVER_WRAPPER" 2>/dev/null | cut -d' ' -f1)"
} > "$RESULTDIR/manifest.txt"

one() {                      # $1 = trajectory index
    j=$1
    seed=$((SEED_BASE + j))
    tag=$(printf 't%05d' "$j")
    t0=$(date +%s.%N)
    set +e
    timeout "$TIMEOUT" "$SOLVER_WRAPPER" "$seed" "$RUN/t.cnf" "$RUN/$tag.model" \
        > "$RESULTDIR/logs/$tag.log" 2>&1
    rc=$?
    set -e
    t1=$(date +%s.%N)
    wall=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.3f", b-a}')
    case $rc in
      10) if python3 tools/verify_preimage.py "$CNF" "$RUN/$tag.model" \
               > "$RESULTDIR/logs/$tag.verify" 2>&1 \
             && grep -q "VALID PREIMAGE" "$RESULTDIR/logs/$tag.verify"; then
              st=SAT_VERIFIED
          else
              st=INVALID_MODEL
          fi ;;
      20)  st=UNSAT ;;
      124) st=TIMEOUT ;;
      *)   st=ERROR ;;
    esac
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$tag" "$seed" "$rounds" "$hb" "$SOLVER_WRAPPER" "$TIMEOUT" "$st" "$rc" "$wall" \
        >> "$RUN/rows"
    printf '  %s seed=%-8s %-14s %8.1fs\n' "$tag" "$seed" "$st" "$wall" >&2
}

batches=$(( (N + PAR - 1) / PAR ))
echo "Experiment 0: r=$rounds hb=$hb  n=$N  timeout=${TIMEOUT}s  parallel=$PAR"
echo "  wrapper: $SOLVER_WRAPPER"
echo "  $batches batch(es) of up to $PAR; a batch runs until its SLOWEST member"
echo "  finishes, so worst case is $batches x ${TIMEOUT}s = $((batches * TIMEOUT))s"
echo "  (~$(( batches * TIMEOUT / 60 )) min) if every trajectory times out."
if [ $((N % PAR)) -ne 0 ]; then
    echo "  NOTE: n=$N is not a multiple of par=$PAR, so the final batch of"
    echo "  $((N % PAR)) leaves $((PAR - N % PAR)) workers idle for a whole timeout."
fi
echo "  progress below; results land in $RESULTDIR"
echo

printf 'trajectory,seed,rounds,hash_bits,solver,timeout_s,status,exit_code,wall_s\n' > "$OUT"
: > "$RUN/rows"

j=1
running=0
while [ "$j" -le "$N" ]; do
    one "$j" &
    running=$((running + 1))
    if [ "$running" -ge "$PAR" ]; then
        wait                       # simple batch barrier: portable, no job control
        running=0
    fi
    j=$((j + 1))
done
wait

sort "$RUN/rows" >> "$OUT"
nsolved=$(grep -c ",SAT_VERIFIED," "$OUT" || true)
echo
echo "wrote $OUT  ($(($(wc -l < "$OUT") - 1)) trajectories, $nsolved solved)"
if [ "$nsolved" -eq 0 ]; then
    echo
    echo "WARNING: zero trajectories solved. This bounds F(${TIMEOUT}s) from above"
    echo "and settles nothing -- with n=$N and no events the exact 95% upper bound"
    echo "is roughly $(awk -v n="$N" 'BEGIN{printf "%.3f", 1-0.05^(1/n)}'), far above the ~1e-4 decision threshold."
    echo "Pilot at a round where events occur (try r=20), or raise the timeout,"
    echo "before spending a large campaign here."
fi
echo "now: python3 tools/analyze_trajectories.py $OUT"
