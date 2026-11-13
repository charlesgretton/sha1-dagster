#!/usr/bin/env bash
# phaseA_regression.sh -- semantic regression of the v3 controls BEFORE any tuning or campaign.
#
#   STOCK=/path/to/stock/dagster PATCHED=/path/to/patched/dagster scripts/phaseA_regression.sh
#
# On one r=21 target (g10003) and a 3-clause UNSAT instance, compares:
#   stock (controls off) twice, patched (controls off) twice   -> final model identical? emission
#       order within stock's OWN run-to-run variation? (stock is not reproducible beyond ~31
#       emissions: MPI timing changes the interleaving)
#   UNSAT under stock and patched (L=1, terminal budget 1000)   -> no solution under both
#   staged, patched L=1 and L=8                                 -> REALISED descendants per parent
#   staged, patched L=1 + terminal budget 20000                 -> budget events terminal-only;
#                                                                  each quantum ~20000 conflicts
#   invalid value                                               -> fatal at start-up
# Writes results/phaseA/report.txt. Uses 4 ranks per Dagster run; needs >= 4 cores.
set -uo pipefail; export LC_ALL=C
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.."
: "${STOCK:?set STOCK=/path/to/stock/dagster executable}"; : "${PATCHED:?set PATCHED=/path/to/patched/dagster executable}"
OUT=results/phaseA; rm -rf "$OUT"; mkdir -p "$OUT"
# harmless off-sandbox; needed only where OpenMPI runs as root (e.g. a container)
export OMPI_ALLOW_RUN_AS_ROOT=${OMPI_ALLOW_RUN_AS_ROOT:-1} OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=${OMPI_ALLOW_RUN_AS_ROOT_CONFIRM:-1}
export OMPI_MCA_btl=${OMPI_MCA_btl:-self,tcp}
MPIFWD=(); OVER=()
if mpirun --version 2>&1 | grep -qi "open mpi"; then
  MPIFWD=(-x GLOG_v -x GLOG_logtostderr -x GLOG_logbufsecs -x GLOG_log_dir -x DAGSTER_MAX_INTERMEDIATE_SOLUTIONS
          -x DAGSTER_MINISAT_TERMINAL_CONFLICT_BUDGET -x DAGSTER_MINISAT_CONFLICT_QUANTUM)
  [[ "${ALLOW_OVERSUBSCRIBE:-0}" == 1 ]] && OVER=(--oversubscribe)
fi
make -s >/dev/null 2>&1 || { echo "ERROR: make failed" >&2; exit 2; }
DAGSTER=$PATCHED scripts/capture_provenance.sh "$OUT/provenance.txt"
RAW=$OUT/g10003.cnf
SHA1GEN_PLANTED_OUT=$RAW.planted bin/sha1gen --attack=preimage --rounds=21 --hash-bits=160 --seed=10003 --cnf --tseitin-adders > "$RAW"
python3 tools/compositional_race.py build "$RAW" --out-prefix $OUT/l16 --components low16 > /dev/null || exit 2
python3 tools/compositional_race.py build "$RAW" --out-prefix $OUT/st --components staged > /dev/null || exit 2
python3 -c "
import sys; sys.path.insert(0,'tools'); from sha1_dagify import write_dag
open('$OUT/unsat.cnf','w').write('p cnf 2 3\n1 2 0\n-1 0\n-2 0\n'); write_dag('$OUT/unsat.dag', [[0,1,2]], {}, [1,2])"
run(){ local tag=$1 bin=$2 dag=$3 cnf=$4 n=$5 cap=$6; shift 6
  mkdir -p $OUT/$tag/logs; local sidf=$OUT/$tag/sid; : > $sidf
  env "$@" GLOG_v=4 GLOG_logtostderr=false GLOG_logbufsecs=0 GLOG_log_dir=$OUT/$tag/logs \
    setsid bash -c 'echo $$ > "$0"; exec "$@"' $sidf timeout -k 5 $cap mpirun "${OVER[@]}" -n $n "${MPIFWD[@]}" \
    $bin -m 4 -q 2 -b 0 -e 0 -i 1 -o $OUT/$tag/out.sol $dag $cnf > $OUT/$tag/mpirun.out 2>&1 &
  local jp=$!; while [[ ! -s $sidf ]]; do sleep 0.05; done; wait $jp; echo $? > $OUT/$tag/rc
  pkill -KILL -s "$(cat $sidf)" 2>/dev/null; echo "   $tag exit=$(cat $OUT/$tag/rc)"; }
echo "== runs"
run stock_a   $STOCK   $OUT/l16.dag $OUT/l16.cnf 3 120
run stock_b   $STOCK   $OUT/l16.dag $OUT/l16.cnf 3 120
run patched_a $PATCHED $OUT/l16.dag $OUT/l16.cnf 3 120
run patched_b $PATCHED $OUT/l16.dag $OUT/l16.cnf 3 120
run unsat_stock   $STOCK   $OUT/unsat.dag $OUT/unsat.cnf 2 60
run unsat_patched $PATCHED $OUT/unsat.dag $OUT/unsat.cnf 2 60 DAGSTER_MAX_INTERMEDIATE_SOLUTIONS=1 DAGSTER_MINISAT_TERMINAL_CONFLICT_BUDGET=1000
run staged_L1      $PATCHED $OUT/st.dag $OUT/st.cnf 6 300 DAGSTER_MAX_INTERMEDIATE_SOLUTIONS=1
run staged_L8      $PATCHED $OUT/st.dag $OUT/st.cnf 6 300 DAGSTER_MAX_INTERMEDIATE_SOLUTIONS=8
run staged_L1_T20k $PATCHED $OUT/st.dag $OUT/st.cnf 6 300 DAGSTER_MAX_INTERMEDIATE_SOLUTIONS=1 DAGSTER_MINISAT_TERMINAL_CONFLICT_BUDGET=20000
run invalid_env    $PATCHED $OUT/l16.dag $OUT/l16.cnf 3 60 DAGSTER_MAX_INTERMEDIATE_SOLUTIONS=hello
echo "== analysis"
python3 - "$OUT" "$RAW" <<'PY' | tee $OUT/report.txt
import sys, os, re, glob, json, hashlib, subprocess
out, raw = sys.argv[1], sys.argv[2]
def ver(t):
    p = os.path.join(out, t, "out.sol")
    if not os.path.exists(p) or os.path.getsize(p) == 0: return "no solution"
    r = subprocess.run([sys.executable, "tools/verify_preimage.py", raw, p], capture_output=True, text=True)
    return "VALID" if "VALID PREIMAGE" in r.stdout else "INVALID"
def seq(t):
    L = []
    for p in glob.glob(os.path.join(out, t, "logs", "*")):
        if os.path.islink(p): continue                       # glog's symlink duplicates one rank
        for l in open(p, errors="replace"):
            m = re.search(r"^[IWEF]\d{4} (\S+).*Solution message mapped to: message about node 0 assignments:([-\d ]*)", l)
            if m: L.append((m.group(1), hashlib.sha256(m.group(2).strip().encode()).hexdigest()[:12]))
    return [h for _, h in sorted(L)]
def agree(a, b):
    n = min(len(a), len(b)); return next((i for i in range(n) if a[i] != b[i]), n), n
fails = []
print("1. final models")
for t in ("stock_a", "stock_b", "patched_a", "patched_b"): print("   %-10s %s" % (t, ver(t)))
rd = lambda t: open(os.path.join(out, t, "out.sol"), "rb").read() if os.path.exists(os.path.join(out, t, "out.sol")) else None
outs = [rd(t) for t in ("stock_a", "stock_b", "patched_a", "patched_b")]
same = None not in outs and len(set(outs)) == 1
if not same: fails.append("final models differ or missing")
print("   all four -o outputs byte-identical: %s" % same)
print("2. emission order (merged by timestamp): identical prefix length")
ss = agree(seq("stock_a"), seq("stock_b")); sp = agree(seq("stock_a"), seq("patched_a")); pp = agree(seq("stock_b"), seq("patched_b"))
print("   stock vs stock   : %d of %d" % ss); print("   stock vs patched : %d of %d" % sp); print("   stock vs patched : %d of %d" % pp)
print("   (informational: emission ORDER is timing-dependent in BOTH binaries -- stock has diverged from")
print("    itself after ~31 emissions in other runs -- so a single pair cannot rank them; the binding")
print("    equivalence check is the identical verified model above. Compare distributions over repeats.)")
print("3. UNSAT: stock %s, patched %s" % (ver("unsat_stock"), ver("unsat_patched")))
if ver("unsat_stock") != "no solution" or ver("unsat_patched") != "no solution": fails.append("UNSAT")
print("4. realised refinement descendants per parent, and budget events by role")
for t in ("staged_L1", "staged_L8", "staged_L1_T20k"):
    subprocess.run([sys.executable, "tools/minisat_profile.py", os.path.join(out, t, "logs"),
                    os.path.join(out, "st.components.json"), "--out", os.path.join(out, t, "profile.json")], capture_output=True)
    P = json.load(open(os.path.join(out, t, "profile.json"))); r = P["refinement"]
    print("   %-15s %s | parents %d, max %d, histogram %s | budget exhausted %s" % (t, ver(t), r["parents"],
          r["descendants_per_parent_max"], r["histogram"], P["budget_exhausted_by_role"] or "{}"))
    if ver(t) == "INVALID": fails.append(t)
P = json.load(open(os.path.join(out, "staged_L1_T20k", "profile.json")))
if set(P["budget_exhausted_by_role"]) - {"terminal"}: fails.append("budget leaked to non-terminal roles")
qs = []
for p in glob.glob(os.path.join(out, "staged_L1_T20k", "logs", "*")):
    if os.path.islink(p): continue
    qs += [int(m.group(1)) for m in re.finditer(r"MINISAT_BUDGET_EXHAUSTED .*conflicts_this_call=(\d+)", open(p, errors="replace").read())]
print("5. quantum sizes of exhausted calls (budget 20000): %s" % (sorted(set(qs)) or "none observed"))
if qs and not all(20000 <= q <= 20010 for q in qs): fails.append("quantum size")
inv = any("invalid value" in open(p, errors="replace").read() for p in glob.glob(os.path.join(out, "invalid_env", "*")) + glob.glob(os.path.join(out, "invalid_env", "logs", "*")) if os.path.isfile(p))
print("6. invalid value fatal: %s" % inv)
if not inv: fails.append("invalid env not fatal")
print("PHASE A: %s" % ("PASS" if not fails else "FAIL: " + ", ".join(fails)))
sys.exit(1 if fails else 0)
PY
