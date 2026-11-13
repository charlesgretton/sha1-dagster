#!/usr/bin/env python3
"""planted_model.py RAW_CNF PLANTED_WORDS OUT_MODEL
Turn the generator's planted message (16 hex words, from SHA1GEN_PLANTED_OUT)
into a model over the CNF's message variables (LSB-first, as sha1-sat lays
words out), so it can be checked with verify_preimage.py or used as x* for
planted cube and XOR-cell controls."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sha1_cnf import Sha1Cnf
c = Sha1Cnf(sys.argv[1])
words = [int(l, 16) for l in open(sys.argv[2]) if l.strip()]
assert len(words) == 16
lits = [(v if (words[i] >> b) & 1 else -v) for i in range(16) for b, v in enumerate(c.w(i))]
open(sys.argv[3], "w").write("SAT\n" + " ".join(map(str, lits)) + " 0\n")
