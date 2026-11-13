#!/usr/bin/env bash
# tinisat_cube.sh SEED CNF MODEL
# Cube-without-Dagster control. Each trajectory = full formula + one cube,
# solved by the same TiniSAT the portfolio uses. Reproduces Dagster's cube jobs
# outside Dagster, separating "fixing k bits helps" from "Dagster helps".
#
#   CUBE_BITS     cube width                                   default 7
#   CUBE_VARS     cube variables ("1-7" = low bits of w[0],    default 1-$CUBE_BITS
#                 i.e. Dagster's naive-message strategy)
#   CUBE_PERMUTE  0 = original clause order, as a Dagster worker sees it (default)
#                 1 = also permute clauses with the seed
#
# Cube index = (SEED-1) mod 2^CUBE_BITS, so seeds 1..2^bits are distinct cubes.
set -u
seed=$1 cnf=$2 model=$3
here=$(cd "$(dirname "$0")" && pwd)
bits=${CUBE_BITS:-7}; vars=${CUBE_VARS:-1-$bits}; perm=${CUBE_PERMUTE:-0}
[[ -x "$here/tinisat_seeded" ]] || { echo "tinisat_cube.sh: $here/tinisat_seeded missing -- run make engines" >&2; exit 127; }
idx=$(( (seed - 1) % (1 << bits) ))
pseed=0; [[ "$perm" == 1 ]] && pseed=$seed
tmp=$(mktemp "${TMPDIR:-/tmp}/cube_${seed}_XXXXXX.cnf")
trap 'rm -f "$tmp"' EXIT
trap 'rm -f "$tmp"; exit 143' TERM
python3 "$here/cube_cnf.py" "$cnf" "$tmp" "$idx" "$bits" "$vars" "$pseed" || exit 125
echo "c cube index $idx bits $bits vars $vars permute $perm"
"$here/tinisat_seeded" "$tmp" "$model" false 1
