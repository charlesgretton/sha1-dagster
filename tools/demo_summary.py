#!/usr/bin/env python3
"""
demo_summary.py -- summarise the compositional-race demonstrator.

  demo_summary.py --out PREFIX --targets "10001 ..." --b-dir DIR --k 63 --cap 300
                  [--scaling "16 32 64"]

Arms (one solver throughout: MiniSat 2.2 as built from Dagster's own source tree):
  B   flat portfolio: independent seeded MiniSat trajectories of the full formula
      (portfolio harness, pinned, one per physical core). A K-worker race finishes at
      the minimum of K trajectories; resampling the n observed trajectories gives the
      EXACT law  P(race > t) = (1 - F(t))^K  (F = empirical solve CDF, censored at cap).
      Races actually realised from consecutive seed blocks are reported alongside.
  C   compositional race in Dagster (low16 + staged + part16, direct EXCLUDED), -e 0,
      ended by tools/watcher.py on the first VERIFIED preimage.
  C'  the single low16 pipeline, same conditions.
Primary metric: seconds from launch to the first verified preimage (t_wall), censored
at the cap. Secondary: distinct verified preimages found in a fixed-length -e 2 yield
run, per component.

Writes PREFIX.summary.json and PREFIX.summary.csv; PNG figures if matplotlib exists.
"""
import os, sys, json, glob, argparse, statistics as st
from collections import Counter, defaultdict


def race_law(times, n, K, t):
    """P(K-worker race finishes by t) from n trajectories; times = solved wall times."""
    F = sum(1 for x in times if x <= t) / n if n else 0.0
    return 1.0 - (1.0 - F) ** K


def race_median(times, n, K, cap):
    for x in sorted(times):
        if race_law(times, n, K, x) >= 0.5:
            return x
    return None                                            # > cap


def km_median(obs, cap):
    """obs = list of (time, solved). Median of the empirical first-solution time with
    censoring at a COMMON cap (type-I): the empirical CDF, None if below 50% by cap."""
    n = len(obs)
    if not n:
        return None
    solved = sorted(t for t, s in obs if s)
    for i, t in enumerate(solved, 1):
        if i / n >= 0.5:
            return t
    return None


def load_B(b_dir, target, cap, rounds=21):
    rows = []
    for m in glob.glob(b_dir + ".B_g%s/manifest.jsonl" % target):
        for l in open(m):
            if l.strip():
                r = json.loads(l)
                if r.get("rounds") == rounds and r.get("generator_seed") == int(target):
                    rows.append(r)
    rows.sort(key=lambda r: r["solver_seed"])
    times = [r["wall_seconds"] for r in rows if r["status"] == "SAT_VERIFIED" and r["wall_seconds"] <= cap]
    return rows, times


def load_runs(out, arm, target, ranks=None):
    pat = "%s.%s_g%s%s_r*.watch.json" % (out, arm, target, ("_n%s" % ranks) if ranks else "")
    res = []
    for p in sorted(glob.glob(pat)):
        w = json.load(open(p))
        res.append({"file": os.path.basename(p), "solved": w["status"] == "SOLVED",
                    "t": w.get("t_wall"), "component": w.get("component"),
                    "status": w["status"], "stall": w.get("stall_flagged_at_wall"),
                    "replicas": w.get("replicas_max_workers_on_one_terminal_message", 0)})
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True); ap.add_argument("--targets", required=True)
    ap.add_argument("--b-dir", required=True); ap.add_argument("--k", type=int, required=True)
    ap.add_argument("--cap", type=float, required=True); ap.add_argument("--scaling", default="")
    ap.add_argument("--rounds", type=int, default=21)
    a = ap.parse_args()
    targets = a.targets.split(); K, cap = a.k, a.cap
    S = {"k": K, "cap": cap, "targets": {}, "pooled": {}}
    rowsout = []
    print("target | B: traj solved  exact %d-race P(<=cap) median | realised races | "
          "C: solved median winners | C': solved median" % K)
    pool = defaultdict(list); pool["pairs"] = []
    for t in targets:
        rows, bt = load_B(a.b_dir, t, cap, a.rounds)
        n = len(rows)
        realised = []
        for i in range(0, n - K + 1, K):
            blk = rows[i:i + K]
            sv = [r["wall_seconds"] for r in blk if r["status"] == "SAT_VERIFIED" and r["wall_seconds"] <= cap]
            realised.append(min(sv) if sv else None)
        C = load_runs(a.out, "C", t); Cp = load_runs(a.out, "Cp", t)
        rec = {"B": {"trajectories": n, "solved": len(bt),
                     "race_p_by_cap": race_law(bt, n, K, cap) if n else None,
                     "race_median": race_median(bt, n, K, cap) if n else None,
                     "realised_races": realised},
               "C": {"runs": len(C), "solved": sum(r["solved"] for r in C),
                     "median": km_median([(r["t"], r["solved"]) for r in C], cap),
                     "winners": dict(Counter(r["component"] for r in C if r["solved"])),
                     "stalls": sum(1 for r in C if r["stall"]), "max_replicas": max((r["replicas"] for r in C), default=0)},
               "Cp": {"runs": len(Cp), "solved": sum(r["solved"] for r in Cp),
                      "median": km_median([(r["t"], r["solved"]) for r in Cp], cap)}}
        yf = "%s.Y_g%s.yield.json" % (a.out, t)
        if os.path.exists(yf):
            rec["yield"] = json.load(open(yf))
        S["targets"][t] = rec
        f = lambda v: (">%g" % cap) if v is None else "%.1f" % v
        rb = rec["B"]
        print("g%s | %4d/%-4d  %.2f  %s | %s | %d/%d %s %s | %d/%d %s" % (
            t, rb["solved"], n, rb["race_p_by_cap"] or 0, f(rb["race_median"]),
            ",".join(f(x) for x in realised) or "-",
            rec["C"]["solved"], rec["C"]["runs"], f(rec["C"]["median"]), rec["C"]["winners"] or "",
            rec["Cp"]["solved"], rec["Cp"]["runs"], f(rec["Cp"]["median"])))
        rowsout.append([t, n, rb["solved"], rb["race_p_by_cap"], rb["race_median"],
                        rec["C"]["runs"], rec["C"]["solved"], rec["C"]["median"],
                        rec["Cp"]["runs"], rec["Cp"]["solved"], rec["Cp"]["median"]])
        pool["C"] += [(r["t"], r["solved"]) for r in C]
        pool["Cp"] += [(r["t"], r["solved"]) for r in Cp]
        pool["B"].append((bt, n))
        # observed portfolio races (arm Br), paired with C by repeat
        br = {}
        for pth in glob.glob("%s.Br_g%s_r*.race.json" % (a.out, t)):
            rep = int(pth.rsplit("_r", 1)[1].split(".")[0]); r = json.load(open(pth))
            br[rep] = r.get("t_wall") if r["status"] == "SOLVED" else None
        cby = {}
        for pth in glob.glob("%s.C_g%s_r*.watch.json" % (a.out, t)):
            rep = int(pth.rsplit("_r", 1)[1].split(".")[0]); w = json.load(open(pth))
            cby[rep] = w.get("t_wall") if w["status"] == "SOLVED" else None
        pairs = [(cby[k], br[k]) for k in sorted(set(br) & set(cby))]
        win = lambda x, y: (x is not None) and (y is None or x < y)
        rec["Br"] = {"races": len(br), "solved": sum(1 for v in br.values() if v is not None),
                     "median": km_median([(v, v is not None) for v in br.values()], cap),
                     "paired": len(pairs), "C_faster": sum(1 for c, b in pairs if win(c, b)),
                     "Br_faster": sum(1 for c, b in pairs if win(b, c)),
                     "both_censored": sum(1 for c, b in pairs if c is None and b is None)}
        pool["pairs"] += pairs
    from math import comb
    P = pool["pairs"]; cw = sum(1 for c, b in P if (c is not None) and (b is None or c < b))
    bw = sum(1 for c, b in P if (b is not None) and (c is None or b < c)); n = cw + bw
    p2 = min(1.0, 2 * sum(comb(n, k) for k in range(0, min(cw, bw) + 1)) / 2 ** n) if n else None
    S["paired_observed"] = {"pairs": len(P), "C_faster": cw, "Br_faster": bw, "decisive": n, "sign_test_two_sided_p": p2,
                            "note": "observed C races vs observed portfolio races (arm Br), paired by target and repeat"}
    S["B_census_note"] = "arm B is a trajectory census; its race figures are MODEL-DERIVED (order statistic), not observed races"
    if P:
        print("paired observed races: %d pairs, C faster %d, portfolio faster %d, sign test p=%s" % (len(P), cw, bw, ("%.3g" % p2) if p2 is not None else "-"))
    S["pooled"] = {"C_median": km_median(pool["C"], cap), "C_solved": sum(s for _, s in pool["C"]),
                   "C_runs": len(pool["C"]), "Cp_median": km_median(pool["Cp"], cap),
                   "Cp_solved": sum(s for _, s in pool["Cp"]), "Cp_runs": len(pool["Cp"])}
    if a.scaling:
        S["scaling"] = {}
        for n_ in a.scaling.split():
            runs = [r for t in targets for r in load_runs(a.out, "S", t, n_)]
            bK = int(n_) - 1
            bmed = [race_median(bt, n, bK, cap) for bt, n in pool["B"] if n]
            S["scaling"][n_] = {"C_runs": len(runs), "C_solved": sum(r["solved"] for r in runs),
                                "C_median": km_median([(r["t"], r["solved"]) for r in runs], cap),
                                "B_exact_race_medians_by_target": bmed}
    json.dump(S, open(a.out + ".summary.json", "w"), indent=1)
    with open(a.out + ".summary.csv", "w") as fcsv:
        fcsv.write("target,B_traj,B_solved,B_race_P_by_cap,B_race_median,C_runs,C_solved,C_median,Cp_runs,Cp_solved,Cp_median\n")
        for r in rowsout:
            fcsv.write(",".join("" if v is None else str(v) for v in r) + "\n")
    print("pooled: C %d/%d solved, median %s s | C' %d/%d solved, median %s s" % (
        S["pooled"]["C_solved"], S["pooled"]["C_runs"], S["pooled"]["C_median"],
        S["pooled"]["Cp_solved"], S["pooled"]["Cp_runs"], S["pooled"]["Cp_median"]))
    figures(a, S, pool, targets, K, cap)


def figures(a, S, pool, targets, K, cap):
    try:
        import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
    except Exception:
        print("matplotlib not available: figures skipped (CSV and JSON written)"); return
    import numpy as np
    ts = np.logspace(-1, np.log10(cap), 300)
    # 1. survival of time-to-first-verified-preimage
    fig, ax = plt.subplots(figsize=(6.4, 4.2))
    if pool["B"]:
        sb = [np.mean([1 - race_law(bt, n, K, x) for bt, n in pool["B"] if n]) for x in ts]
        ax.plot(ts, sb, label="B: flat portfolio, exact %d-worker race" % K)
    for arm, lab in (("C", "C: compositional race"), ("Cp", "C': low16 pipeline")):
        obs = pool[arm]
        if obs:
            ax.step(ts, [sum(1 for t, s in obs if not (s and t <= x)) / len(obs) for x in ts],
                    where="post", label="%s (%d runs)" % (lab, len(obs)))
    ax.set_xscale("log"); ax.set_xlabel("seconds from launch"); ax.set_ylabel("P(no verified preimage yet)")
    ax.set_title("Time to first verified preimage, r=21, pooled over %d target(s)" % len(targets))
    ax.legend(fontsize=8); fig.tight_layout(); fig.savefig(a.out + ".fig_survival.png", dpi=150); plt.close(fig)
    # 2. worker allocation over time in the first yield run, with solutions marked
    for t in targets:
        ev = "%s.Y_g%s.events.jsonl" % (a.out, t)
        if not os.path.exists(ev):
            continue
        E = [json.loads(l) for l in open(ev)]
        D = sorted((e for e in E if e["ev"] == "DISPATCH"), key=lambda e: e["t"])
        if not D:
            continue
        comps = sorted({e["component"] for e in D if e["component"]})
        grid = np.linspace(0, max(e["t"] for e in E), 400)
        cur, i, series = {}, 0, {c: [] for c in comps}
        for g in grid:
            while i < len(D) and D[i]["t"] <= g:
                cur[D[i]["worker"]] = D[i]["component"]; i += 1
            cnt = Counter(cur.values())
            for c in comps:
                series[c].append(cnt.get(c, 0))
        fig, ax = plt.subplots(figsize=(7, 4.2))
        ax.stackplot(grid, [series[c] for c in comps], labels=comps, alpha=0.85)
        sol = [e for e in E if e["ev"] == "EMIT" and (e.get("role") or "").endswith("TERMINAL")]
        for e in sol:
            ax.axvline(e["t"], color="k", lw=0.4, alpha=0.5)
        ax.set_xlabel("seconds (Dagster log clock)"); ax.set_ylabel("workers assigned")
        ax.set_title("g%s: worker allocation by component; black lines = terminal solutions (%d)" % (t, len(sol)), fontsize=9)
        ax.legend(fontsize=7, loc="upper right"); fig.tight_layout()
        fig.savefig("%s.fig_allocation_g%s.png" % (a.out, t), dpi=150); plt.close(fig)
        break
    # 3. yield: distinct verified preimages per component per target
    Y = {t: S["targets"][t].get("yield", {}) for t in targets}
    comps = sorted({c for y in Y.values() for c in y})
    if comps:
        fig, ax = plt.subplots(figsize=(7, 4)); w = 0.8 / max(1, len(targets))
        for j, t in enumerate(targets):
            ax.bar([k + j * w for k in range(len(comps))], [Y[t].get(c, {}).get("verified", 0) for c in comps], w, label="g%s" % t)
        ax.set_xticks([k + 0.4 - w / 2 for k in range(len(comps))]); ax.set_xticklabels(comps, rotation=30, fontsize=8)
        ax.set_ylabel("distinct verified preimages"); ax.set_title("Yield per component in a fixed-length -e 2 run", fontsize=9)
        ax.legend(fontsize=7); fig.tight_layout(); fig.savefig(a.out + ".fig_yield.png", dpi=150); plt.close(fig)
    print("figures written: %s.fig_*.png" % a.out)


if __name__ == "__main__":
    main()
