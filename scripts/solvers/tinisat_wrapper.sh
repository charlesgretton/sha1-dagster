#!/usr/bin/env bash
# Compatibility shim: keeps the old  wrapper SEED CNF MODEL  interface but fixes
# the two defects in the previous version.
#  1. Diversification. TiniSAT's seed does not change its trajectory in this
#     configuration (measured: seeds 1, 2, 3 returned the byte-identical model),
#     so a seed-only portfolio is one trajectory run many times. This delegates
#     to the clause-permutation engine, which does diversify, reproducibly.
#  2. Exit codes. The old wrapper returned 20 (UNSAT) for anything not SAT, so
#     timeouts, crashes and a missing binary were recorded as UNSAT. Exit codes
#     now pass through unchanged: 10 SAT, 20 UNSAT, 124/137 from timeout,
#     126/127 harness errors, anything else a solver error.
set -u
here=$(cd "$(dirname "$0")" && pwd)
eng=${TINISAT_PERM:-$here/../../portfolio/engines/tinisat_perm.sh}
[[ -x "$eng" ]] || { echo "tinisat_wrapper.sh: engine not found: $eng (set TINISAT_PERM)" >&2; exit 127; }
exec "$eng" "$@"
