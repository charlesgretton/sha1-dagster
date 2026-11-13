#!/usr/bin/env bash
# modern.sh ENGINE SEED CNF MODEL      ENGINE = kissat | cadical
# Harness contract for modern solvers: exit code passed through (10 SAT,
# 20 UNSAT), model written in competition format ("s"/"v" lines, which
# verify_preimage.py accepts), final statistics re-emitted in MiniSat
# "name : value" form so run_one.sh records conflicts and decisions.
# Both solvers print statistics on SIGTERM (measured), so a run stopped by
# the timeout still reports its conflicts: a per-run censoring threshold.
# Diversification: --seed=N. Measured: seeds 1 and 2 give different models,
# and the same seed reproduces the identical model, for both solvers.
set -u
eng=$1 seed=$2 cnf=$3 model=$4
here=$(cd "$(dirname "$0")" && pwd)
bin="$here/$eng"
[[ -x "$bin" ]] || { echo "modern.sh: $bin missing -- run: make modern-engines" >&2; exit 127; }
out=$(mktemp "${TMPDIR:-/tmp}/${eng}_${seed}_XXXXXX.out")
emit() {
  awk '$1=="c" && ($2=="conflicts:" || $2=="decisions:") { n=$2; sub(":","",n); v[n]=$3 }
       END { if ("conflicts" in v) printf "conflicts             : %s\n", v["conflicts"];
             if ("decisions" in v) printf "decisions             : %s\n", v["decisions"] }' "$out"
}
echo "c engine $eng sha256 $(sha256sum "$bin" | cut -c1-16) version $("$bin" --version 2>/dev/null | head -1) seed $seed"
"$bin" --seed="$seed" "$cnf" > "$out" 2>&1 &
pid=$!
trap 'wait "$pid" 2>/dev/null; emit; rm -f "$out"; exit 143' TERM
wait "$pid"; rc=$?
if [[ $rc == 10 ]]; then grep -E '^(s|v) ' "$out" > "$model"; fi
emit; rm -f "$out"; exit "$rc"
