# env.example.sh -- copy to ~/sha1_env.sh, edit the three paths, then `source ~/sha1_env.sh`
# after every login and before any phase. Everything else follows the report's frozen configuration.
export STOCK=/path/to/dagster-stock/dagster/dagster       # unpatched Dagster executable (the control)
export PATCHED=/path/to/dagster-v3/dagster/dagster        # Dagster executable built after patch v3
export DAGSTER_SRC=/path/to/dagster-stock/dagster         # stock source tree (builds the MiniSat engine)
export DAGSTER=$PATCHED REQUIRE_PATCH=1 K=64 ROUNDS_TARGET=22 CAP=1800
# K must equal the number of PHYSICAL cores (64 on the workstation used for the reported campaign), not hardware threads.
