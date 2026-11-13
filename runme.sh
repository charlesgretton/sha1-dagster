#!/bin/sh
# One-command driver, in the style of dagster/experiment_scripts/*/runme.sh.
#
# Assumes you already have a working dagster binary. Point DAGSTER at it:
#   DAGSTER=../dagster-main/dagster/dagster ./runme.sh
set -e
DAGSTER=${DAGSTER:-../dagster-main/dagster/dagster}
CORES=${CORES:-16}
ROUNDS=${ROUNDS:-"20 21 22"}
HB=${HB:-160}
BITS=${BITS:-7}
TIMEOUT=${TIMEOUT:-600}

export OMPI_MCA_btl=self,tcp
export GLOG_v=0
export GLOG_logtostderr=true

echo "== building sha1-sat tools =="
make

echo "== generating instances =="
scripts/generate.sh instances "$ROUNDS" "$HB"

echo "== running experiment =="
python3 scripts/runner.py \
    --dagster "$DAGSTER" \
    --instances instances \
    --cores "$CORES" \
    --bits "$BITS" \
    --timeout "$TIMEOUT" \
    --out results.csv

echo
echo "results written to results.csv"
