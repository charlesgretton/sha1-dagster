#!/usr/bin/env bash
# capture_provenance.sh OUTFILE -- record the platform and software provenance the report lists as
# open item R14 (CPU model, MPI and compiler versions, Dagster commit, patched-binary hash, ...).
# Called automatically by runsheet_demo.sh; safe to run by hand. Never fails the caller.
OUT=${1:?usage: capture_provenance.sh OUTFILE}
{
  echo "# provenance captured $(date -u +%Y-%m-%dT%H:%M:%SZ) on $(hostname)"
  echo "## uname";            uname -a
  echo "## os-release";       cat /etc/os-release 2>/dev/null | grep -E '^(NAME|VERSION)='
  echo "## lscpu";            lscpu 2>/dev/null | grep -E 'Model name|Socket|Core\(s\) per socket|Thread\(s\) per core|^CPU\(s\)|NUMA node\(s\)|On-line CPU'
  echo "## cpu numbering (logical cpu -> core, socket)"; lscpu -p=CPU,CORE,SOCKET 2>/dev/null | grep -v '^#' | head -130 | tr '\n' ' '; echo
  echo "## mpi";              mpirun --version 2>&1 | head -6
  echo "## compilers";        (g++ --version 2>&1 | head -1); (mpic++ --version 2>&1 | head -1)
  echo "## python";           python3 --version 2>&1
  python3 - <<'PY' 2>&1
for m in ("pysat", "pycryptosat"):
    try:
        mod = __import__(m); print("%s %s" % (m, getattr(mod, "__version__", "?")))
    except Exception as e:
        print("%s not installed" % m)
PY
  if [ -n "${DAGSTER:-}" ]; then
    echo "## dagster binary";   sha256sum "$DAGSTER" 2>&1
    echo "## dagster patch line (strings)"; strings "$DAGSTER" 2>/dev/null | grep -m1 'DAGSTER_PATCH v3' || echo "no v3 marker (stock binary)"
    echo "## dagster libraries"; ldd "$DAGSTER" 2>/dev/null | grep -E 'glog|cudd|libz|mpi|stdc' 
  fi
  if [ -n "${DAGSTER_SRC:-}" ]; then
    echo "## dagster source commit"; (git -C "$DAGSTER_SRC" rev-parse HEAD 2>/dev/null || echo "not a git checkout: record the source archive's SHA-256")
    git -C "$DAGSTER_SRC" status --porcelain 2>/dev/null | head -5
  fi
  echo "## bundle";           sha256sum bin/sha1gen third_party/sha1-sat/main_noboost.cc patches/dagster_bounded_v3.py 2>/dev/null
  echo "## affinity of this shell"; (taskset -cp $$ 2>/dev/null || echo n/a)
} > "$OUT" 2>&1 || true
exit 0
