#!/bin/sh
# Repair DIMACS headers and emit DAG files for every strategy.
#
#   scripts/prepare.sh INSTANCE.cnf OUTDIR BITS
set -e
CNF=$1
OUT=${2:-prepared}
BITS=${3:-7}
base=$(basename "$CNF" .cnf)
mkdir -p "$OUT"
for s in onenode single naive-message dual-frontier; do
  python3 tools/sha1_dagify.py "$CNF" \
      --out-cnf "$OUT/${base}.cnf" \
      --out-dag "$OUT/${base}.${s}.dag" \
      --strategy "$s" --bits "$BITS"
  echo
done
