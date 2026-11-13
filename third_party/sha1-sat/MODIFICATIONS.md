# sha1-sat in this repository: origin, licence and modifications

This directory contains a **modified copy of sha1-sat**, the SAT instance generator for SHA-1 by
Vegard Nossum (https://github.com/vegard/sha1-sat), Copyright (C) 2011-2012, 2021 Vegard Nossum.
sha1-sat is free software under the **GNU General Public License, version 3 or (at your option) any
later version**; the full licence text is in `COPYING` (also `LICENSES/GPL-3.0-or-later.txt`). It is
the only third-party program source distributed in this repository, and it is **not** covered by the
repository's Apache-2.0 licence.

| file | origin |
|---|---|
| `main_noboost.cc` | upstream `main.cc`, modified by the sha1-dagster project in 2026 (below) |
| `format.hh`, `verify-preimage.cc`, `README.sha1-sat.md` | upstream, unmodified |
| `COPYING` | the GNU GPL version 3 text, as distributed upstream |

## Modifications (GPL-3.0 section 5(a) notice)

`main_noboost.cc` was modified by the sha1-dagster project in 2026:

1. **No Boost.** Boost.Program_options was replaced by a small hand-written argument parser, so the
   generator builds with any C++11 compiler (`make all`). The options used here (`--attack`,
   `--rounds`, `--hash-bits`, `--seed`, `--cnf`, `--tseitin-adders`, `--xor`) behave as upstream.
   The upstream README's Boost build instructions therefore do not apply to this copy.
2. **Planted block output.** When the environment variable `SHA1GEN_PLANTED_OUT` names a file, the
   generator also writes the randomly drawn (planted) message block there, as 16 hexadecimal words.
   It is used only to validate DAG constructions; it is never given to a solver.
3. **Configurable Ch rounds.** The environment variable `SHA1GEN_CH_ROUNDS` (20 to 40; default 20,
   the SHA-1 standard) sets how many initial rounds use the Ch function, moving the Ch/Parity
   boundary within rounds 20-39 while leaving the round constants unchanged. It was used only in a
   development experiment on the 21-round difficulty cliff; every reported instance uses the default.

Note on the planted block (upstream behaviour, not a modification): upstream draws each planted
word with `lrand48()`, which is below 2^31, so bit 31 of every planted word is 0. This affects only
which digest is targeted; no solver is told the planted block, and the solvers search all 512 bits.

With neither variable set, the generated CNF for the options above is the same as upstream's.

## Why the modifications are described here and not in the file

The SHA-256 of `main_noboost.cc` (in `evidence/checksums.txt`) binds it to the instances reported in
the proceedings, and `make selftest` checks that it regenerates them byte for byte. Any edit, even
to a comment, would break that binding, so the notice required by GPL-3.0 section 5(a) is given in
this file, beside the modified source, rather than inside it.
