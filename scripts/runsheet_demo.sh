#!/usr/bin/env bash
# runsheet_demo.sh -- the compositional-race demonstrator.
#
# Claim under test: a Dagster DAG of a sound trans-round relaxation reaches independently
# verified reduced-round SHA-1 preimages sooner, and in greater number, than a conventional
# portfolio of the SAME solver on the SAME cores; and Dagster's depth-first allocation
# concentrates workers on the productive pipeline, expressed entirely in the DAG.
#
# One solver throughout: MiniSat 2.2 built from Dagster's own tree (portfolio/engines/minisat
# for arm B; Dagster mode 4 for arms C/C'). Fair by construction.
#
# Arms:  B  flat portfolio, K seeded trajectories of the full formula (harness, pinned)
#        C  compositional race: low16 + staged + part16, direct EXCLUDED (it contributes
#           only replicas and dominates shutdown -- see FINDINGS), -e 0, ended by the watcher
#        C' low16 pipeline alone
# Plus:  a fixed-length -e 2 YIELD run per target (distinct verified preimages per component)
#        optional SCALING of C at several core counts, against B's exact race at the same K.
#
# Required:  DAGSTER=/path/to/dagster (executable)   DAGSTER_SRC=/path/to/dagster (source tree,
#            to build the MiniSat engine)
# Optional:  TARGETS ("10001 10002 10003 10004 10005")  ROUNDS_TARGET (21)  HB (160)
#            K (physical cores, capped at nproc)  REPEATS (10)  CAP (300)  YIELD_CAP (120)
#            SCALING ("")  STALL (60)  PREFIX (results/demo)  KEEP_LOGS (0)
#            ARMS ("B C Cp Y")  -- e.g. ARMS="C Cp Y" to rerun only the Dagster arms; arm B is
#                                   skipped automatically when its campaign already completed
# Exit 8: a Dagster launch that never started (its master logged nothing) -- never "unsolved".
# Exit 9: remedies requested via DAGSTER_MAX_INTERMEDIATE_SOLUTIONS / DAGSTER_MINISAT_CONFLICT_BUDGET
#         but the Dagster binary did not log the matching DAGSTER_PATCH line.
#
# Exit: 2 build  3 planted-path  4 environment  5 oversubscription  7 invalid solution
set -uo pipefail; export LC_ALL=C
# bundle root = the directory that has this script's tools/ and Makefile, found from $0
SELF=$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)
ROOT=$SELF; [[ -f "$ROOT/Makefile" ]] || ROOT=$(dirname "$SELF")
[[ -f "$ROOT/Makefile" && -d "$ROOT/tools" ]] || { echo "ERROR: cannot find the bundle root (no Makefile/tools/ near $SELF)" >&2; exit 4; }
LAUNCH=$PWD
cd "$ROOT"
: "${DAGSTER:?set DAGSTER=/path/to/dagster executable}"
: "${DAGSTER_SRC:?set DAGSTER_SRC=/path/to/dagster source tree (for the MiniSat engine)}"
abs(){ case "$1" in /*) printf %s "$1";; *) printf %s "$LAUNCH/$1";; esac; }
DAGSTER=$(abs "$DAGSTER"); DAGSTER_SRC=$(abs "$DAGSTER_SRC")
[[ -x "$DAGSTER" && ! -d "$DAGSTER" ]] || { echo "ERROR: DAGSTER must be the executable" >&2; exit 4; }
TARGETS=${TARGETS:-"10001 10002 10003 10004 10005"}; ROUNDS_TARGET=${ROUNDS_TARGET:-21}; HB=${HB:-160}
CORES=$(nproc); K=${K:-$CORES}; ALLOW_OVERSUBSCRIBE=${ALLOW_OVERSUBSCRIBE:-0}
(( K > CORES && ALLOW_OVERSUBSCRIBE != 1 )) && { echo "ERROR: K=$K > $CORES cores (ALLOW_OVERSUBSCRIBE=1 for smoke tests only)" >&2; exit 5; }
REPEATS=${REPEATS:-10}; CAP=${CAP:-300}; YIELD_CAP=${YIELD_CAP:-120}; SCALING=${SCALING:-}
STALL=${STALL:-60}; KEEP_LOGS=${KEEP_LOGS:-0}; ARMS=${ARMS:-"B Br C Cp Y"}
has(){ [[ " $ARMS " == *" $1 "* ]]; }
PREFIX=${PREFIX:-results/demo}; PREFIX=$(abs "$PREFIX"); mkdir -p "$(dirname "$PREFIX")"
export OMPI_MCA_btl=${OMPI_MCA_btl:-self,tcp} OMPI_ALLOW_RUN_AS_ROOT=1 OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1
# v3 controls (patches/dagster_bounded_v3.py). 0 = off. Unset role budgets fall back to the quantum.
export DAGSTER_MAX_INTERMEDIATE_SOLUTIONS=${DAGSTER_MAX_INTERMEDIATE_SOLUTIONS:-0}
export DAGSTER_MINISAT_CONFLICT_QUANTUM=${DAGSTER_MINISAT_CONFLICT_QUANTUM:-0}
export DAGSTER_MINISAT_TERMINAL_CONFLICT_BUDGET=${DAGSTER_MINISAT_TERMINAL_CONFLICT_BUDGET:-$DAGSTER_MINISAT_CONFLICT_QUANTUM}
export DAGSTER_MINISAT_INTERMEDIATE_CONFLICT_BUDGET=${DAGSTER_MINISAT_INTERMEDIATE_CONFLICT_BUDGET:-$DAGSTER_MINISAT_CONFLICT_QUANTUM}
export DAGSTER_MINISAT_SOURCE_CONFLICT_BUDGET=${DAGSTER_MINISAT_SOURCE_CONFLICT_BUDGET:-$DAGSTER_MINISAT_CONFLICT_QUANTUM}
PATCH_LINE="DAGSTER_PATCH v3 L=$DAGSTER_MAX_INTERMEDIATE_SOLUTIONS budget_terminal=$DAGSTER_MINISAT_TERMINAL_CONFLICT_BUDGET budget_intermediate=$DAGSTER_MINISAT_INTERMEDIATE_CONFLICT_BUDGET budget_source=$DAGSTER_MINISAT_SOURCE_CONFLICT_BUDGET"
CONTROLS_ON=$(( DAGSTER_MAX_INTERMEDIATE_SOLUTIONS + DAGSTER_MINISAT_TERMINAL_CONFLICT_BUDGET + DAGSTER_MINISAT_INTERMEDIATE_CONFLICT_BUDGET + DAGSTER_MINISAT_SOURCE_CONFLICT_BUDGET > 0 ))
REQUIRE_PATCH=${REQUIRE_PATCH:-$CONTROLS_ON}    # set REQUIRE_PATCH=1 for patched-but-disabled controls
# MPI flavour. OpenMPI needs -x to forward the environment; MPICH/Hydra forwards it and
# REJECTS OpenMPI options (v16 hard-coded --oversubscribe: every Dagster launch on the workstation died
# in 0.09 s with "unrecognized argument oversubscribe").
if mpirun --version 2>&1 | grep -qi "open mpi"; then
  MPIFWD=(-x GLOG_v -x GLOG_logtostderr -x GLOG_logbufsecs -x GLOG_log_dir -x DAGSTER_MAX_INTERMEDIATE_SOLUTIONS -x DAGSTER_MINISAT_TERMINAL_CONFLICT_BUDGET -x DAGSTER_MINISAT_INTERMEDIATE_CONFLICT_BUDGET -x DAGSTER_MINISAT_SOURCE_CONFLICT_BUDGET -x DAGSTER_MINISAT_CONFLICT_QUANTUM)
  if [[ "$ALLOW_OVERSUBSCRIBE" == 1 ]]; then MPIOPT=(--oversubscribe); else MPIOPT=(--bind-to core); fi
else
  MPIFWD=(); if [[ "$ALLOW_OVERSUBSCRIBE" == 1 ]]; then MPIOPT=(); else MPIOPT=(-bind-to core); fi
fi
say(){ printf '\n==== %s  [%s]\n' "$*" "$(date -u +%H:%M:%SZ)"; }
# Cleanup is SESSION-SCOPED: every Dagster run and portfolio race is launched in its own session
# (setsid) and only that session is signalled. Never `pkill -x dagster/mpirun/minisat`: on a
# shared node that kills other users' and other campaigns' processes.
CURRENT_SID=""
kill_session(){ local s=$1; [[ -n "$s" ]] || return 0; pkill -TERM -s "$s" 2>/dev/null; sleep 2; pkill -KILL -s "$s" 2>/dev/null; return 0; }
cleanup(){ kill_session "$CURRENT_SID"; }
trap cleanup EXIT

say "0. build generator + engines"
scripts/capture_provenance.sh "$PREFIX.provenance.txt"; echo "   provenance: $PREFIX.provenance.txt"
make -s > "$PREFIX.make1.log" 2>&1 || { echo "ERROR: make failed"; cat "$PREFIX.make1.log"; exit 2; }
make -s engines DAGSTER_SRC="$DAGSTER_SRC" > "$PREFIX.make2.log" 2>&1 || { echo "ERROR: make engines failed"; cat "$PREFIX.make2.log"; exit 2; }
[[ -x portfolio/engines/minisat ]] || { echo "ERROR: MiniSat engine missing" >&2; exit 2; }
python3 -c "import sys;sys.path.insert(0,'tools');import pysat_compat;print('   planted-path solver:',pysat_compat.SOLVER_NAME)"

# one Dagster run under the watcher: dag_run TAG DAG CNF COMPONENTS RANKS EMODE CAP WATCHERARGS...
dag_run(){
  local keep=0; [[ "$1" == KEEP ]] && { keep=1; shift; }
  local tag=$1 dag=$2 cnf=$3 comp=$4 n=$5 e=$6 cap=$7; shift 7
  (( n > CORES && ALLOW_OVERSUBSCRIBE != 1 )) && { echo "ERROR: $tag needs $n ranks on $CORES cores" >&2; exit 5; }
  rm -rf "$PREFIX.$tag.logs" "$PREFIX.$tag.sol" "$PREFIX.$tag.ck"; mkdir -p "$PREFIX.$tag.logs" "$PREFIX.$tag.ck"
  local sidf="$PREFIX.$tag.sid" t0; t0=$(date +%s.%N); : > "$sidf"
  GLOG_v=4 GLOG_logtostderr=false GLOG_logbufsecs=0 GLOG_log_dir="$PREFIX.$tag.logs" \
    setsid bash -c 'echo $$ > "$0"; exec "$@"' "$sidf" \
    timeout -k 10 "$((cap+120))" mpirun "${MPIOPT[@]}" -n "$n" "${MPIFWD[@]}" \
    "$DAGSTER" -m 4 -q 2 -b 0 -e "$e" -i 1 -v 10 -z "$PREFIX.$tag.ck/ck" -o "$PREFIX.$tag.sol" "$dag" "$cnf" \
    > "$PREFIX.$tag.mpirun.out" 2>&1 &
  local jp=$!; while [[ ! -s "$sidf" ]]; do sleep 0.05; done; CURRENT_SID=$(cat "$sidf")
  timeout "$((cap+150))" python3 tools/watcher.py --logs "$PREFIX.$tag.logs" --components "$comp" \
    --raw "$RAW" --sid "$(cat "$sidf")" --t0 "$t0" --out "$PREFIX.$tag.watch.json" --stall "$STALL" "$@" &
  local wp=$!; wait "$jp" 2>/dev/null; wait "$wp" 2>/dev/null; kill_session "$CURRENT_SID"; CURRENT_SID=""
  if (( REQUIRE_PATCH )) && ! grep -qs "$PATCH_LINE" "$PREFIX.$tag.logs"/* ; then
    echo "ERROR: $tag: expected '$PATCH_LINE'" >&2
    echo "       but no worker logged the matching DAGSTER_PATCH line: DAGSTER is unpatched or the" >&2
    echo "       variables did not reach the ranks. See patches/dagster_bounded.py." >&2; exit 9
  fi
  if ! grep -qs "MASTER" "$PREFIX.$tag.logs"/* ; then
    echo "ERROR: $tag never started: Dagster's master logged nothing. mpirun said:" >&2
    head -5 "$PREFIX.$tag.mpirun.out" >&2; exit 8
  fi
  if (( REQUIRE_PATCH )); then python3 tools/minisat_profile.py "$PREFIX.$tag.logs" "$comp" --out "$PREFIX.$tag.profile.json" >/dev/null 2>&1; fi
  if [[ "$keep" != 1 ]]; then python3 tools/dagster_events.py "$PREFIX.$tag.logs" "$comp" --out "$PREFIX.$tag" >/dev/null 2>&1; rm -rf "$PREFIX.$tag.logs" "$PREFIX.$tag.ck"; fi
  local status; status=$(python3 -c "import json;print(json.load(open('$PREFIX.$tag.watch.json'))['status'])")
  local inv; inv=$(python3 -c "import json;print(len(json.load(open('$PREFIX.$tag.watch.json'))['invalid_solutions']))")
  (( inv == 0 )) || { echo "ERROR: $tag produced an INVALID solution" >&2; exit 7; }
  printf '   %-26s %s\n' "$tag" "$(python3 -c "import json;w=json.load(open('$PREFIX.$tag.watch.json'));print('%s t_wall=%s comp=%s replicas=%s'%(w['status'],('%.2f'%w['t_wall']) if w.get('t_wall') else '-',w.get('component','-'),w.get('replicas_max_workers_on_one_terminal_message',0)))")"
}

# one OBSERVED portfolio race (arm Br): K pinned seeded MiniSat trajectories, disjoint seed block
race_run(){
  local tag=$1 seed_start=$2 sidf="$PREFIX.$1.sid"; : > "$sidf"
  local pin=(); [[ "$ALLOW_OVERSUBSCRIBE" == 1 ]] || pin=(--pin)
  setsid bash -c 'echo $$ > "$0"; exec "$@"' "$sidf" timeout -k 10 "$((CAP+30))" \
    python3 tools/portfolio_race.py --solver portfolio/engines/minisat --cnf "$REPAIRED" --raw "$RAW" \
    --k "$K" --seed-start "$seed_start" --cap "$CAP" --out "$PREFIX.$tag.race.json" "${pin[@]}" \
    > "$PREFIX.$tag.race.out" 2>&1 &
  local jp=$!; while [[ ! -s "$sidf" ]]; do sleep 0.05; done; CURRENT_SID=$(cat "$sidf")
  wait "$jp" 2>/dev/null; kill_session "$CURRENT_SID"; CURRENT_SID=""
  rm -rf "$PREFIX.$tag.race"                      # per-seed model files; the winner is verified
  printf '   %-26s %s\n' "$tag" "$(python3 -c "import json;r=json.load(open('$PREFIX.$tag.race.json'));print('%s t_wall=%s seed=%s block=%s'%(r['status'],('%.2f'%r['t_wall']) if r.get('t_wall') else '-',r.get('seed','-'),r['seed_block']))")"
}

# provenance of a patched binary: the unified diff and manifest written by the patcher
for f in dagster_bounded_v3.diff dagster_bounded_v3.manifest.json; do
  [[ -f "$(dirname "$DAGSTER")/$f" ]] && cp "$(dirname "$DAGSTER")/$f" "$PREFIX.$f"
done

for t in $TARGETS; do
  RAW="$PREFIX.g$t.cnf"; REPAIRED="$PREFIX.g$t.repaired.cnf"
  say "target g$t: generate + build DAGs"
  SHA1GEN_PLANTED_OUT="$RAW.planted" bin/sha1gen --attack=preimage --rounds="$ROUNDS_TARGET" \
    --hash-bits="$HB" --seed="$t" --cnf --tseitin-adders > "$RAW" 2>/dev/null || { echo "ERROR: sha1gen g$t" >&2; exit 2; }
  python3 -c "import sys;sys.path.insert(0,'tools');from sha1_cnf import Sha1Cnf;Sha1Cnf('$RAW').write_repaired('$REPAIRED')" \
    || { echo "ERROR: repair g$t" >&2; exit 2; }
  python3 tools/compositional_race.py build "$RAW" --out-prefix "$PREFIX.C_g$t" \
    --components low16,staged,part16 > "$PREFIX.C_g$t.build.txt" 2>&1
  python3 -c "import json,sys;e=json.load(open('$PREFIX.C_g$t.manifest.json'))['validation']['errors'];sys.exit(3 if any('planted' in x for x in e) else (2 if e else 0))" \
    || { echo "ERROR: C DAG g$t (see build.txt)" >&2; grep -i error "$PREFIX.C_g$t.build.txt" >&2; exit $?; }
  python3 tools/compositional_race.py build "$RAW" --out-prefix "$PREFIX.Cp_g$t" --components low16 > "$PREFIX.Cp_g$t.build.txt" 2>&1
  python3 tools/compositional_race.py build "$RAW" --out-prefix "$PREFIX.Y_g$t" \
    --components low16,staged,part16,direct > "$PREFIX.Y_g$t.build.txt" 2>&1

  if has B && [[ ! -s "$(dirname "$PREFIX")/$(basename "$PREFIX").B_g$t/completion.json" ]]; then
  say "arm B (flat portfolio, K=$K seeded MiniSat trajectories)"
  SOLVER=portfolio/engines/minisat SOLVER_ARGS_TEMPLATE="-rnd-init -rnd-seed={seed} {cnf} {model}" \
    ROUNDS="$ROUNDS_TARGET" TARGET_SEEDS="$t" SOLVER_SEEDS="$K" WORKERS="$K" PIN=1 TIMEOUT_SEC="$CAP" \
    RESULTS_ROOT="$(dirname "$PREFIX")" EXPERIMENT_ID="$(basename "$PREFIX").B_g$t" \
    portfolio/portfolio_harness.sh > "$PREFIX.B_g$t.harness.log" 2>&1 \
    || { echo "ERROR: arm B harness g$t" >&2; tail -3 "$PREFIX.B_g$t.harness.log" >&2; exit 2; }
  else say "arm B g$t: skipped (not in ARMS, or already complete)"; fi

  say "arms Br, C, C' ($REPEATS repeats each; Br = observed portfolio race, seed block rep*K+1..)"
  for rep in $(seq 1 "$REPEATS"); do
    # odd repeats: Br first; even repeats: Br last (alternating order controls drift)
    (( rep % 2 == 1 )) && has Br && race_run "Br_g${t}_r$rep" "$(( rep * K + 1 ))"
    has C && dag_run "C_g${t}_r$rep"  "$PREFIX.C_g$t.dag"  "$PREFIX.C_g$t.cnf"  "$PREFIX.C_g$t.components.json"  "$((K+1))" 0 "$CAP" --kill-stall 1
    has Cp && dag_run "Cp_g${t}_r$rep" "$PREFIX.Cp_g$t.dag" "$PREFIX.Cp_g$t.cnf" "$PREFIX.Cp_g$t.components.json" "$((K+1))" 0 "$CAP"
    (( rep % 2 == 0 )) && has Br && race_run "Br_g${t}_r$rep" "$(( rep * K + 1 ))"
  done

  if has Y; then
  say "yield run (-e 2, ${YIELD_CAP}s, distinct verified preimages per component)"
  dag_run KEEP "Y_g$t" "$PREFIX.Y_g$t.dag" "$PREFIX.Y_g$t.cnf" "$PREFIX.Y_g$t.components.json" "$((K+1))" 2 "$YIELD_CAP" --no-kill
  export PREFIX RAW
  python3 tools/recover_solutions.py logs "$PREFIX.Y_g$t.logs" "$PREFIX.Y_g$t.components.json" --out-dir "$PREFIX.Y_g$t.recovered" >/dev/null 2>&1
  python3 - "$t" <<'PY'
import sys, os, glob, re, json, subprocess
t=sys.argv[1]; pre=os.environ["PREFIX"]; raw=os.environ["RAW"]
C=json.load(open("%s.Y_g%s.components.json"%(pre,t))); name={}
for c in C["components"]:
    for r,n in c["nodes"].items(): name[n]=c["name"]
seen=set(); per={}
for m in sorted(glob.glob("%s.Y_g%s.recovered/*.model"%(pre,t))):
    node=int(re.search(r"node(\d+)_",m).group(1))
    lits=tuple(sorted((int(x) for x in open(m).read().split()),key=abs))
    if lits in seen: continue
    seen.add(lits)
    ok=b"VALID PREIMAGE" in subprocess.run([sys.executable,"tools/verify_preimage.py",raw,m],capture_output=True).stdout
    d=per.setdefault(name.get(node,"?"),{"distinct":0,"verified":0}); d["distinct"]+=1; d["verified"]+=ok
json.dump(per, open("%s.Y_g%s.yield.json"%(pre,t),"w"), indent=1)
print("   yield g%s: "%t + ", ".join("%s=%d"%(k,v["verified"]) for k,v in sorted(per.items())))
PY
  export PREFIX RAW
  python3 tools/dagster_events.py "$PREFIX.Y_g$t.logs" "$PREFIX.Y_g$t.components.json" --out "$PREFIX.Y_g$t" >/dev/null 2>&1
  [[ "$KEEP_LOGS" == 1 ]] || rm -rf "$PREFIX.Y_g$t.logs" "$PREFIX.Y_g$t.ck"
  fi
done

if [[ -n "$SCALING" ]]; then
  say "scaling: arm C at ranks {$SCALING}, $REPEATS repeats, first target"
  st=$(echo $TARGETS | awk '{print $1}')
  for n in $SCALING; do
    (( n > CORES && ALLOW_OVERSUBSCRIBE != 1 )) && { echo "   skip $n > $CORES cores"; continue; }
    for rep in $(seq 1 "$REPEATS"); do
      RAW="$PREFIX.g$st.cnf" dag_run "S_g${st}_n${n}_r$rep" "$PREFIX.C_g$st.dag" "$PREFIX.C_g$st.cnf" "$PREFIX.C_g$st.components.json" "$n" 0 "$CAP" --kill-stall 1
    done
  done
fi

say "summary + figures"
python3 tools/demo_summary.py --out "$PREFIX" --targets "$TARGETS" --b-dir "$PREFIX" \
  --k "$K" --cap "$CAP" --rounds "$ROUNDS_TARGET" ${SCALING:+--scaling "$SCALING"} | tee "$PREFIX.summary.txt"
# NOTE: --b-dir points demo_summary at each target's own B_g<t> manifest; it globs per target.

say "send-back archive"
( cd "$(dirname "$PREFIX")" && ls -d "$(basename "$PREFIX")".* | grep -vE '\.cnf$|send_back' > "$PREFIX.arch" )
tar czf "$PREFIX.send_back.tgz" -C "$(dirname "$PREFIX")" -T "$PREFIX.arch" $(cd "$(dirname "$PREFIX")" && ls "$(basename "$PREFIX")".*.cnf 2>/dev/null) 2>/dev/null || true
ls -la "$PREFIX.send_back.tgz" | awk '{print "   "$5" bytes  "$NF}'
