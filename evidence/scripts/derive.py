#!/usr/bin/env python3
"""
derive.py -- regenerate the evidence tables behind every number in the proceedings' results.

  python3 evidence/scripts/derive.py evidence/runs OUTDIR [--events RAW_RESULTS_DIR]

Inputs (shipped in evidence/runs/): one JSON record per run -- *.watch.json (Dagster arms C and C'),
*.race.json (observed portfolio races, arm Br), *.profile.json (per-run MiniSat profile) -- plus
census/ (the arm-B trajectory census), exit_signals.csv, and the per-target DAG manifests.

Outputs, written to OUTDIR:
  runs.csv       one row per run, censored runs included
  summary.csv    per phase, step and arm: solved, min/median/max time, physical core-hours, yield
  census.csv     per target: trajectories, solved, solved times
  budget.csv     per conflict-budget arm: terminal budget-exhaustion events
  mechanism.csv  per L=inf / L=8 run: terminal dispatches, refinements retired and emitted
                 (only with --events, from the raw Dagster event logs, which are not distributed;
                 their SHA-256 is in evidence/checksums.txt)

Definitions follow the proceedings' protocol: a run's time is from launch to the first independently
verified preimage; an unsolved run is censored at its end time; core-hours count 64 allocated
physical cores x wall time for every arm. Standard library only.
"""
import sys, os, re, json, csv, glob, statistics as st

CORES = 64
RUN = re.compile(r'^(?P<prefix>[\w]+)\.(?P<arm>C|Cp|Br)_g(?P<t>\d+)_r(?P<rep>\d+)\.(?P<kind>watch|race)\.json$')
CONFIG = {   # prefix -> (phase, step, retirement threshold L, terminal conflict budget)
    'dev_cal':        ('B', 'B1 calibration', '8', '0'),
    'dev_Br':         ('B', 'B2 portfolio races', '', ''),
    'dev_L0':         ('B', 'B2 unbounded', 'inf', '0'),
    'dev_L8_B0':      ('B', 'B3 no budget', '8', '0'),
    'dev_L8_B2000':   ('B', 'B3 budget', '8', '2000'),
    'dev_L8_B20000':  ('B', 'B3 budget', '8', '20000'),
    'dev_L8_B200000': ('B', 'B3 budget', '8', '200000'),
    'eval_r22':       ('C', 'C locked evaluation', '8', '0'),
}
ORDER = list(CONFIG)


def f3(x):
    return '' if x is None else ('%.3f' % x)


def load_runs(d):
    sig = {}
    p = os.path.join(d, 'exit_signals.csv')
    if os.path.exists(p):
        sig = {r['run_id']: r['sigkill'] for r in csv.DictReader(open(p))}
    rows = []
    for f in sorted(os.listdir(d)):
        m = RUN.match(f)
        if not m:
            continue
        r = json.load(open(os.path.join(d, f)))
        prefix, arm = m['prefix'], m['arm']
        phase, step, L, budget = CONFIG[prefix]
        solved = r.get('status') == 'SOLVED'
        end = r.get('t_end') if arm == 'Br' else r.get('t_end_wall')
        rid = f[:-len('.%s.json' % m['kind'])]
        rows.append({
            'run_id': rid, 'phase': phase, 'step': step, 'prefix': prefix, 'arm': arm,
            'target': 'g' + m['t'], 'repeat': int(m['rep']),
            'L': '' if arm == 'Br' else L, 'terminal_budget': '' if arm == 'Br' else budget,
            'status': r.get('status'), 'solved': 'yes' if solved else 'no',
            'time_to_verified_preimage_s': f3(r.get('t_wall')) if solved else '',
            'end_s': f3(end), 'censored': 'no' if solved else 'yes',
            'winning_component': (r.get('component') or '') if solved and arm != 'Br' else '',
            'winning_seed': r.get('seed', '') if solved and arm == 'Br' else '',
            'seed_block': '-'.join(map(str, r['seed_block'])) if arm == 'Br' else '',
            'invalid_solutions': len(r.get('invalid_solutions', r.get('invalid', [])) or []),
            'sigkill': sig.get(rid, '') if arm != 'Br' else '',
        })
    rows.sort(key=lambda x: (ORDER.index(x['prefix']), x['arm'], x['target'], x['repeat']))
    return rows


def summarise(rows):
    out = []
    groups = {}
    for r in rows:
        groups.setdefault((r['prefix'], r['arm']), []).append(r)
    for (prefix, arm), g in sorted(groups.items(), key=lambda kv: (ORDER.index(kv[0][0]), kv[0][1])):
        for target in sorted({r['target'] for r in g}) + ['pooled']:
            gg = g if target == 'pooled' else [r for r in g if r['target'] == target]
            s = sorted(float(r['time_to_verified_preimage_s']) for r in gg if r['solved'] == 'yes')
            ch = sum(CORES * float(r['end_s']) for r in gg) / 3600.0
            phase, step, L, budget = CONFIG[prefix]
            out.append({
                'phase': phase, 'step': step, 'prefix': prefix, 'arm': arm, 'target': target,
                'L': '' if arm == 'Br' else L, 'terminal_budget': '' if arm == 'Br' else budget,
                'runs': len(gg), 'solved': len(s), 'sigkilled': sum(1 for r in gg if r['sigkill'] == 'yes'),
                'min_s': f3(s[0]) if s else '', 'median_s': f3(st.median(s)) if s else '',
                'max_s': f3(s[-1]) if s else '', 'physical_core_hours': f3(ch),
                'verified_per_physical_core_hour': f3(len(s) / ch) if ch else '',
            })
    return out


def census(d):
    out = []
    for p in sorted(glob.glob(os.path.join(d, 'census', '*.manifest.jsonl'))):
        R = [json.loads(l) for l in open(p) if l.strip()]
        sv = sorted(r['wall_seconds'] for r in R if r['status'] == 'SAT_VERIFIED')
        t = re.search(r'_g(\d+)', p)[1]
        out.append({'target': 'g' + t, 'trajectories': len(R), 'solved_within_cap': len(sv),
                    'solved_times_s': ' '.join('%.1f' % x for x in sv), 'cap_s': 1800})
    return out


def budget(d):
    out = []
    for prefix in ('dev_L8_B0', 'dev_L8_B2000', 'dev_L8_B20000', 'dev_L8_B200000'):
        ev = {}
        for p in glob.glob(os.path.join(d, '%s.C_g*_r*.profile.json' % prefix)):
            for role, n in (json.load(open(p)).get('budget_exhausted_by_role') or {}).items():
                ev[role] = ev.get(role, 0) + n
        out.append({'prefix': prefix, 'terminal_budget': CONFIG[prefix][3],
                    'terminal_exhaustion_events': ev.get('terminal', 0),
                    'nonterminal_exhaustion_events': sum(v for k, v in ev.items() if k != 'terminal')})
    return out


def mechanism(d, raw):
    sig = {r['run_id']: r['sigkill'] for r in csv.DictReader(open(os.path.join(d, 'exit_signals.csv')))}
    out = []
    for prefix in ('dev_L0', 'dev_L8_B0'):
        for p in sorted(glob.glob(os.path.join(raw, '%s.C_g*_r*.events.jsonl' % prefix))):
            rid = os.path.basename(p)[:-len('.events.jsonl')]
            t = re.search(r'_g(\d+)_', rid)[1]
            comp = json.load(open(os.path.join(d, '%s.C_g%s.components.json' % (prefix, t))))
            role = {int(k): v for k, v in comp['node_roles'].items()}
            E = [json.loads(l) for l in open(p)]
            disp_t = [e['t'] for e in E if e['ev'] == 'DISPATCH' and (role.get(e.get('node')) or '').endswith('TERMINAL')]
            prof = json.load(open(os.path.join(d, rid + '.profile.json')))
            out.append({
                'run_id': rid, 'L': CONFIG[prefix][2], 'run_length_s': f3(max(e['t'] for e in E)),
                'terminal_dispatches': len(disp_t), 'terminal_dispatches_after_1s': sum(1 for x in disp_t if x > 1.0),
                'refinements_retired': sum(1 for e in E if e['ev'] == 'REQUEST' and role.get(e.get('node')) == 'REFINEMENT'),
                'refinement_emissions': sum(1 for e in E if e['ev'] == 'EMIT' and role.get(e.get('node')) == 'REFINEMENT'),
                'max_descendants_per_parent': prof['refinement']['descendants_per_parent_max'],
                'sigkill': sig.get(rid, ''),
            })
    return out


def write(path, rows):
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]), lineterminator='\n')
        w.writeheader(); w.writerows(rows)


def main():
    a = sys.argv[1:]
    if len(a) < 2:
        sys.exit(__doc__)
    d, outdir = a[0], a[1]
    os.makedirs(outdir, exist_ok=True)
    rows = load_runs(d)
    write(os.path.join(outdir, 'runs.csv'), rows)
    write(os.path.join(outdir, 'summary.csv'), summarise(rows))
    write(os.path.join(outdir, 'census.csv'), census(d))
    write(os.path.join(outdir, 'budget.csv'), budget(d))
    if '--events' in a:
        write(os.path.join(outdir, 'mechanism.csv'), mechanism(d, a[a.index('--events') + 1]))
    print('wrote %d run records and summaries to %s' % (len(rows), outdir))


if __name__ == '__main__':
    main()
