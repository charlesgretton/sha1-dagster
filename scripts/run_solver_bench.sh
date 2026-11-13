#!/usr/bin/env bash
# run_solver_bench.sh -- reproduce the report's single-solver table (one core, one seed per cell).
#   scripts/run_solver_bench.sh [OUTDIR]        (default results/solver_bench)
# Generates r21 g10003 and r22 g10005 (plain and native-XOR), then runs MiniSat 2.2, CaDiCaL 1.9.5
# and CryptoMiniSat on them with the report's caps. Needs: pip install python-sat pycryptosat
set -uo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.."
OUT=${1:-results/solver_bench}; mkdir -p "$OUT"; J=$OUT/bench.jsonl; : > "$J"
make -s >/dev/null 2>&1 || { echo "ERROR: make failed" >&2; exit 2; }
for spec in 21:10003 22:10005; do r=${spec%%:*}; t=${spec##*:}
  bin/sha1gen --attack=preimage --rounds=$r --hash-bits=160 --seed=$t --cnf --tseitin-adders       > "$OUT/r${r}_g$t.cnf"
  bin/sha1gen --attack=preimage --rounds=$r --hash-bits=160 --seed=$t --cnf --tseitin-adders --xor > "$OUT/r${r}_g$t.xor.cnf"
done
python3 tools/solver_bench.py minisat22  "$OUT/r21_g10003.cnf"     600 "$J"
python3 tools/solver_bench.py cadical195 "$OUT/r21_g10003.cnf"     400 "$J"
python3 tools/solver_bench.py cms        "$OUT/r21_g10003.cnf"     400 "$J"
python3 tools/solver_bench.py cms        "$OUT/r21_g10003.xor.cnf" 400 "$J"
python3 tools/solver_bench.py cms        "$OUT/r22_g10005.xor.cnf" 900 "$J"
python3 tools/solver_bench.py cms        "$OUT/r22_g10005.cnf"     900 "$J"
echo "results: $J"
