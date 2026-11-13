# Virtual proceedings

`sha1_dagster_proceedings.tex` is the complete, self-contained source of the volume: every diagram is
TikZ and every plot is pgfplots with its data embedded, so it needs no external files.

Rebuild from the repository root with `make proceedings`, or here with `pdflatex` run three times.
Latin Modern is used where installed, otherwise Computer Modern.

Every number in the results is taken from `../evidence/derived/` or recomputed from `../evidence/runs/`
by `../tools/report_stats.py`; the appendices "Evidence in the repository" and "Instance artefacts"
identify the instances and raw results by SHA-256. The bibliography is embedded in the source;
`references.bib` gives the same entries in BibTeX.
