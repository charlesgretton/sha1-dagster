#!/bin/sh
# Regenerate preimage benchmark CNFs. The copies in Benchmarks/sha1 are
# git-LFS pointers, so unless you have pulled LFS you must generate them.
# Output reproduces the shipped instances byte for byte.
#
#   scripts/generate.sh OUTDIR "20 21 22" "160"
set -e
# resolve paths relative to the bundle root, not the caller's cwd
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
if [ ! -x bin/sha1gen ]; then
    echo "bin/sha1gen not built yet -- running make" >&2
    make || {
        echo "make failed: you need a C++11 compiler (g++ or clang++)." >&2
        exit 1
    }
fi

OUT=${1:-instances}
ROUNDS=${2:-"20 21 22 24 30 40"}
HB=${3:-"160"}
SEED=${SEED:-123123}
mkdir -p "$OUT"
for r in $ROUNDS; do
  for h in $HB; do
    f="$OUT/pre_r${r}_hb${h}.cnf"
    ./bin/sha1gen --attack=preimage --rounds=$r --hash-bits=$h \
                  --seed=$SEED --cnf --tseitin-adders > "$f"
    echo "generated $f  ($(grep -c ' 0$' "$f") clauses)"
  done
done
