#!/usr/bin/env python3
"""
schedule_rank.py -- exact GF(2) dependency analysis of the SHA-1 message schedule.

Every expanded word is a linear function of the 512-bit input message:

    W_t = L_t . M        L_t in GF(2)^{32 x 512}

built from  W_t = ROTL1(W_{t-3} xor W_{t-8} xor W_{t-14} xor W_{t-16}).

This exists to stop dependency claims being made by visual inspection of the
recurrence.  It answers, exactly:

  * which input words W_0..W_15 influence which expanded words;
  * rank and kernel dimension of the stacked schedule map for any round range;
  * whether a candidate "neutral word" really is neutral for a given r.

Bit convention matches sha1-sat: bit b of a word has weight 2^b, and
ROTL1(X)[b] = X[(b-1) mod 32].

    python3 schedule_rank.py [max_rounds]
"""

import sys

NBITS = 512


def build_L(max_t):
    """L[t] is a list of 32 bitmasks over the 512 message bits."""
    L = {}
    for i in range(16):                       # input words: identity
        L[i] = [1 << (32 * i + b) for b in range(32)]
    for t in range(16, max_t):
        src = [L[t - 3], L[t - 8], L[t - 14], L[t - 16]]
        rows = []
        for b in range(32):
            s = (b - 1) % 32                  # ROTL by 1
            acc = 0
            for m in src:
                acc ^= m[s]
            rows.append(acc)
        L[t] = rows
    return L


def rank(rows):
    """GF(2) rank by elimination over Python int bitsets."""
    piv, r = [], 0
    for v in rows:
        cur = v
        for p in piv:
            cur = min(cur, cur ^ p)
        if cur:
            piv.append(cur)
            piv.sort(reverse=True)
            r += 1
    return r


def stacked(L, lo, hi):
    out = []
    for t in range(lo, hi):
        out.extend(L[t])
    return out


def influences(L, t, i):
    """Does input word W_i appear anywhere in the definition of W_t?"""
    block = ((1 << 32) - 1) << (32 * i)
    return any(row & block for row in L[t])


def main():
    max_t = int(sys.argv[1]) if len(sys.argv) > 1 else 32
    L = build_L(max_t)

    print("Which input words W_0..W_15 influence each expanded word")
    print("(the first expanded word each input reaches is what matters for")
    print(" neutrality claims)\n")
    print("   W_t  <- input words")
    for t in range(16, min(max_t, 26)):
        ins = [i for i in range(16) if influences(L, t, i)]
        print("   W_%-3d %s" % (t, ",".join("W%d" % i for i in ins)))

    print("\nFirst expanded word reached by each input word:")
    for i in range(16):
        first = next((t for t in range(16, max_t) if influences(L, t, i)), None)
        print("   W_%-2d -> first affects W_%s" % (i, first))

    print("\nSchedule map rank / kernel for a full r-round instance")
    print("(rows = 32 * number of expanded words W_16..W_{r-1})\n")
    print("   r    expanded  rows  rank  dim ker")
    for r in range(17, min(max_t, 33)):
        rows = stacked(L, 16, r)
        rk = rank(rows)
        print("   %-4d %-9d %-5d %-5d %d" % (r, r - 16, len(rows), rk, NBITS - rk))


if __name__ == "__main__":
    main()
