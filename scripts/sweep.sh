#!/bin/sh
# Parameter sweeps for the SHA-1 / Dagster experiments.
#
#   scripts/sweep.sh                      # seed x strategy x bits
#   scripts/sweep.sh --mode message-bits  # Experiment A: PLANTED restriction sweep
#
# Every experimental cell gets its OWN clean instance directory, so runner.py
# can never pick up CNFs left behind by an earlier round, seed or HB setting.
#
# --mode message-bits: sha1gen fixes the selected message bits to the
# generator's hidden reference message, so the instance stays SAT for every N
# from 0 to 512.  This is a planted-witness restriction experiment, NOT
# ApproxMC-style isolation, and N > 352 is NOT unsatisfiable here.  The CSV
# records restriction_semantics=planted so it cannot later be mistaken for
# random restrictions.  See README section 7.
set -eu

MODE=strategy
if [ "${1:-}" = "--mode" ]; then MODE=${2:?--mode needs a value}; fi

DAGSTER=${DAGSTER:-../dagster-main/dagster/dagster}
CORES=${CORES:-64}
TIMEOUT=${TIMEOUT:-300}
ROUNDS=${ROUNDS:-"21"}
SEEDS=${SEEDS:-"1 2 3 4 5"}
HB=${HB:-160}
RUNDIR=${RUNDIR:-sweep_$(date +%Y%m%d_%H%M%S)}
# Extra args forwarded to runner.py, e.g. RUNNER_ARGS="--oversubscribe".
# TRUSTED SHELL FRAGMENT: expanded unquoted, so it word-splits and globs.
# Safe for simple flags; do NOT pass values containing spaces or metacharacters.
RUNNER_ARGS=${RUNNER_ARGS:-}

export OMPI_MCA_btl=self,tcp GLOG_v=0 GLOG_logtostderr=true
mkdir -p "$RUNDIR"

# provenance, so a CSV can always be traced back to the exact inputs
{
  echo "date=$(date -Is)"
  echo "host=$(hostname)"
  echo "mode=$MODE cores=$CORES timeout=$TIMEOUT hb=$HB"
  echo "runner_args=$RUNNER_ARGS"
  echo "rounds=$ROUNDS seeds=$SEEDS"
  echo "dagster=$DAGSTER"
  echo "sha1gen_md5=$(md5sum bin/sha1gen 2>/dev/null | cut -d' ' -f1)"
} > "$RUNDIR/manifest.txt"

if [ "$MODE" = "message-bits" ]; then
  NLIST=${NLIST:-"0 64 128 192 256 288 320 336 344 348 352 360 384 448 512"}
  OUT="$RUNDIR/sweep_mbits.csv"
  printf 'generator_seed,rounds,hash_bits,message_bits,restriction_semantics,cnf_sha256,instance,runner_rounds,strategy,bits,cores,nodes,jobs,validation,status,wall_s,solution\n' > "$OUT"
  for seed in $SEEDS; do
    for r in $ROUNDS; do
      for N in $NLIST; do
        # one clean directory per (seed, round, N) cell
        cell="$RUNDIR/inst_s${seed}_r${r}_N${N}"
        rm -rf "$cell"; mkdir -p "$cell"
        f="$cell/pre_r${r}_hb${HB}_N${N}.cnf"
        ./bin/sha1gen --attack=preimage --rounds="$r" --hash-bits="$HB" \
                      --message-bits="$N" --seed="$seed" --cnf --tseitin-adders > "$f"
        digest=$(sha256sum "$f" | cut -d' ' -f1)
        python3 scripts/runner.py --dagster "$DAGSTER" --instances "$cell" \
            --cores "$CORES" --strategies onenode --timeout "$TIMEOUT" \
            $RUNNER_ARGS --out "$cell/result.csv" >/dev/null
        tail -n +2 "$cell/result.csv" \
          | sed "s|^|$seed,$r,$HB,$N,planted,$digest,|" >> "$OUT"
      done
    done
  done
  echo "wrote $OUT"
  exit 0
fi

# ---- strategy x bits x seed ----
# single:14 is depth- AND job-matched to dual-frontier:7 (both fix 14 bits,
# both produce 16384 jobs), which is the cell that isolates topology.
CONFIGS=${CONFIGS:-"single:7 naive-message:7 dual-frontier:7 single:14 naive-message:14"}
OUT="$RUNDIR/sweep_all.csv"
printf 'generator_seed,rounds,hash_bits,cnf_sha256,instance,runner_rounds,strategy,bits,cores,nodes,jobs,validation,status,wall_s,solution\n' > "$OUT"
for seed in $SEEDS; do
  for r in $ROUNDS; do
    for h in $HB; do
      # one clean cell per (seed, round, hash-bits) so runner.py sees exactly
      # one CNF and the recorded digest belongs to that CNF
      cell="$RUNDIR/inst_s${seed}_r${r}_hb${h}"
      rm -rf "$cell"; mkdir -p "$cell"
      SEED=$seed ./scripts/generate.sh "$cell" "$r" "$h" >/dev/null
      cnf="$cell/pre_r${r}_hb${h}.cnf"
      digest=$(sha256sum "$cnf" | cut -d' ' -f1)
      for cfg in $CONFIGS; do
        strat=${cfg%:*}; bits=${cfg#*:}
        res="$cell/res_${strat}_b${bits}.csv"
        python3 scripts/runner.py --dagster "$DAGSTER" --instances "$cell" \
            --cores "$CORES" --bits "$bits" --strategies "$strat" \
            --timeout "$TIMEOUT" $RUNNER_ARGS --out "$res" >/dev/null
        tail -n +2 "$res" | sed "s|^|$seed,$r,$h,$digest,|" >> "$OUT"
      done
    done
  done
done
echo "wrote $OUT"
