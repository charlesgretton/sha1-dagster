#!/usr/bin/env bash
# tinisat_perm.sh SEED CNF MODEL
# Dagster's TiniSAT is deterministic in its Dagster configuration (seeds do not
# change the trajectory -- measured), so diversification comes from a seeded
# clause-order permutation instead. The solver's internal seed is held at 1.
# Permutation time is inside the timed region, as it would be for a real worker.
set -u
seed=$1 cnf=$2 model=$3
here=$(cd "$(dirname "$0")" && pwd)
if [[ ! -x "$here/tinisat_seeded" ]]; then
    echo "tinisat_perm.sh: $here/tinisat_seeded is missing -- run: make engines DAGSTER_SRC=/path/to/dagster-main/dagster" >&2
    exit 127
fi
if ldd "$here/tinisat_seeded" 2>/dev/null | grep -q "not found"; then
    echo "tinisat_perm.sh: tinisat_seeded cannot load:" >&2
    ldd "$here/tinisat_seeded" | grep "not found" >&2
    echo "  -> add the library directory (e.g. your glog prefix/lib) to LD_LIBRARY_PATH" >&2
    exit 127
fi
tmp=$(mktemp "${TMPDIR:-/tmp}/perm_${seed}_XXXXXX.cnf")
trap 'rm -f "$tmp"' EXIT
trap 'rm -f "$tmp"; exit 143' TERM
python3 "$here/permute_cnf.py" "$cnf" "$tmp" "$seed" || exit 125
echo "c diversifier: clause-permutation seed $seed; tinisat internal seed 1"
"$here/tinisat_seeded" "$tmp" "$model" false 1
