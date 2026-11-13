# Licensing

| Material | Licence | Files |
|---|---|---|
| Source code, build and job scripts | Apache License 2.0 (`LICENSE`) | `*.py`, `*.sh`, `Makefile`, and other program text, except as below |
| Proceedings, documentation | CC BY 4.0 (`LICENSE-CC-BY-4.0.md`) | `*.tex`, `*.pdf`, `*.md`, `*.bib`, and the proceedings generally |
| Data produced by the project | CC BY 4.0 | run records, derived tables, instances' metadata, planted blocks, checksums (`*.json`, `*.jsonl`, `*.csv`, `*.txt`, `*.planted`, `*.cnf`, `*.dag`, `*.diff` produced here, `*.sha256`) |
| Instance generator `third_party/sha1-sat/` | GPL-3.0-or-later (`third_party/sha1-sat/COPYING`, `LICENSES/GPL-3.0-or-later.txt`) | a modified copy of sha1-sat, Copyright (C) 2011-2012, 2021 Vegard Nossum; modifications listed in `third_party/sha1-sat/MODIFICATIONS.md` |
| Seeded TiniSAT engine `portfolio/engines/tinisat_seeded.cc`, and `patches/tinisat-seed.patch` | GPL-2.0-or-later (`LICENSES/GPL-2.0-or-later.txt`) | derived from Dagster's standalone TiniSAT, Copyright 2020 Mark Burgess |

Machine-readable form: `REUSE.toml` (REUSE specification 3.2).

**Third-party software.** Dagster (GPL-2.0-or-later), MiniSat (embedded in Dagster), glog, CUDD and
MPICH are not bundled; they are obtained from their own repositories and keep their own licences.
The scripts in `patches/` are this project's, but they edit a local Dagster checkout, and the patched
source remains under Dagster's licence. The proceedings' patch-listing appendix and
`evidence/validation/dagster_bounded_v3.diff` quote Dagster source code, which likewise remains under
Dagster's licence (GPL-2.0-or-later). Material by others that is cited here keeps its original terms.

**Provenance note.** Licence notices are declared at repository level rather than in file headers,
because the instance generator (`third_party/sha1-sat/main_noboost.cc`) and the Dagster patch script
(`patches/dagster_bounded_v3.py`) are bound by SHA-256 to the instances and builds reported in the
proceedings, and any edit, even to a comment, would break that binding. For the same reason the
GPL-3.0 modification notice for the generator is given in `third_party/sha1-sat/MODIFICATIONS.md`.

**Errata.** Reports of errors, omissions and overlooked prior work are welcome: please write to the
corresponding author, Charles Gretton, at charles.gretton@anu.edu.au. Every report that leads to a
correction will be acknowledged in the errata and in any revised edition, cited as a private
communication; where the point was already made in a publication that appeared before the report's
publication date, that publication will be cited as well.
