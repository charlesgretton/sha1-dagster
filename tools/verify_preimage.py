#!/usr/bin/env python3
"""
verify_preimage.py ORIGINAL.cnf DAGSTER_OUT.txt

Independently checks a Dagster solution against the benchmark it claims to
solve.  Reads w[0..15] out of the reported assignment, runs reduced-round SHA-1
forward in plain Python, and compares against the target hash recovered from
the unit clauses that sha1-sat emitted for the fixed h_out bits.

Bit convention: sha1-sat's new_vars() lays a 32-bit word out LSB-first, and
constant32() fixes bit i with (value >> i) & 1, so var (start + i) is bit i.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sha1_cnf import Sha1Cnf

M32 = 0xFFFFFFFF


def rotl(x, n):
    return ((x << n) | (x >> (32 - n))) & M32


def sha1_rounds(nr, w16):
    w = list(w16) + [0] * (80 - 16)
    for i in range(16, 80):
        w[i] = rotl(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1)
    h = [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0]
    a, b, c, d, e = h
    for i in range(nr):
        if i < 20:
            f, k = (b & c) | (~b & d) & M32, 0x5A827999
        elif i < 40:
            f, k = b ^ c ^ d, 0x6ED9EBA1
        elif i < 60:
            f, k = (b & c) | (b & d) | (c & d), 0x8F1BBCDC
        else:
            f, k = b ^ c ^ d, 0xCA62C1D6
        t = (rotl(a, 5) + (f & M32) + e + k + w[i]) & M32
        a, b, c, d, e = t, a, rotl(b, 30), c, d
    return [(h[0] + a) & M32, (h[1] + b) & M32, (h[2] + c) & M32,
            (h[3] + d) & M32, (h[4] + e) & M32]


def main():
    cnf_path, out_path = sys.argv[1], sys.argv[2]
    c = Sha1Cnf(cnf_path)
    r = c.rounds

    # Accepts three model formats:
    #   MiniSat:     "SAT" then one line of literals ("UNSAT" -> no model)
    #   competition: "s SATISFIABLE" plus "v ..." lines
    #   Dagster:     one solution per line; the FIRST line is checked
    with open(out_path) as f:
        lines = [l.strip() for l in f if l.strip()]
    if not lines:
        print("RESULT: INVALID (empty model file)"); return 1
    head = lines[0].split()[0]
    if head in ("UNSAT", "UNSATISFIABLE"):
        print("RESULT: INVALID (solver reported UNSAT)"); return 1
    if head in ("SAT", "SATISFIABLE"):
        toks = lines[1].split() if len(lines) > 1 else []
    elif any(l.startswith("v ") for l in lines):
        toks = [t for l in lines if l.startswith("v ") for t in l.split()[1:]]
    else:
        toks = lines[0].split()
    sign = {}
    for tok in toks:
        try:
            v = int(tok)
        except ValueError:
            print("RESULT: INVALID (non-integer token %r)" % tok); return 1
        if v != 0:
            sign[abs(v)] = (v > 0)
    if not sign:
        print("RESULT: INVALID (no literals)"); return 1

    # recover message words
    w16 = []
    for i in range(16):
        vs = c.w(i)
        missing = [v for v in vs if v not in sign]
        if missing:
            print("  ! w[%d]: %d bits not reported" % (i, len(missing)))
            print("RESULT: INVALID (incomplete message assignment)")
            return 1
        word = 0
        for bit, v in enumerate(vs):
            if sign.get(v, False):
                word |= (1 << bit)
        w16.append(word)

    # target hash from the unit clauses sha1-sat emitted for fixed h_out bits
    units = {}
    for cl in c.clauses:
        if len(cl) == 1:
            units[abs(cl[0])] = (cl[0] > 0)
    target, nfixed = [], 0
    for j in range(5):
        vs = c.h_out(j)
        word, known = 0, 0
        for bit, v in enumerate(vs):
            if v in units:
                known += 1
                if units[v]:
                    word |= (1 << bit)
        nfixed += known
        target.append((word, known))

    got = sha1_rounds(r, w16)

    print("instance      : %s  (rounds=%d)" % (os.path.basename(cnf_path), r))
    print("message w[0..15]:")
    for j in range(0, 16, 4):
        print("   " + " ".join("%08x" % x for x in w16[j:j + 4]))
    print("hash bits fixed by the benchmark: %d" % nfixed)
    ok = True
    for j in range(5):
        word, known = target[j]
        if known == 32:
            match = (got[j] == word)
            ok &= match
            print("   h_out%d  computed=%08x  target=%08x  %s"
                  % (j, got[j], word, "MATCH" if match else "*** MISMATCH ***"))
        elif known:
            mask = 0
            for bit, v in enumerate(c.h_out(j)):
                if v in units:
                    mask |= (1 << bit)
            match = ((got[j] & mask) == (word & mask))
            ok &= match
            print("   h_out%d  computed=%08x  target=%08x mask=%08x  %s"
                  % (j, got[j], word, mask, "MATCH" if match else "*** MISMATCH ***"))
        else:
            print("   h_out%d  unconstrained" % j)
    print()
    print("RESULT: %s" % ("VALID PREIMAGE" if ok else "INVALID"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
