#!/usr/bin/env bash
# Fault-injection solver. Mode chosen by -rnd-seed=N. Uses REAL_SOLVER for valid models.
seed=1; for a in "$@"; do [[ $a == -rnd-seed=* ]] && seed=${a#-rnd-seed=}; done
cnf=${@: -2:1}; model=${@: -1}
case $seed in
  1) exec "$REAL_SOLVER" -rnd-init -rnd-seed=1 "$cnf" "$model" ;;          # valid model
  2) echo "conflicts             : 5"; exit 10 ;;                            # SAT, no model
  3) printf 'SAT\n%s 0\n' "$(seq -s' ' -512 -1)" >"$model"; exit 10 ;;     # wrong model
  4) echo UNSAT >"$model"; exit 20 ;;                                        # UNSAT on planted
  5) sleep 60 ;;                                                             # plain timeout
  6) trap '' TERM; sleep 60 ;;                                               # ignores TERM
  7) echo "segfault-ish"; exit 1 ;;                                          # crash
  8) nohup sleep 1001 >/dev/null 2>&1 &                                      # orphan child
     exec "$REAL_SOLVER" -rnd-init -rnd-seed=2 "$cnf" "$model" ;;
esac
