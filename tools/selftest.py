#!/usr/bin/env python3
"""
selftest.py -- every check in the release; all must pass before any result is trusted.

  make selftest            (builds bin/sha1gen first)

Runs no Dagster experiment. Needs python-sat (the DAG builder's planted-path validation) and, for the
proceedings text check, pdftotext if available. Exit status 0 only if every check passes.
"""
import os, re, sys, csv, json, glob, shutil, fnmatch, hashlib, tempfile, subprocess, statistics as st

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, os.path.join(ROOT, 'tools'))
PY = sys.executable
RESULTS = []


def sha(p):
    h = hashlib.sha256()
    with open(p, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def check(name):
    def deco(fn):
        try:
            detail = fn()
            RESULTS.append((True, name, detail or ''))
        except Exception as e:
            RESULTS.append((False, name, '%s: %s' % (type(e).__name__, e)))
        return fn
    return deco


def run(cmd, **kw):
    r = subprocess.run(cmd, capture_output=True, text=True, **kw)
    if r.returncode != 0:
        raise RuntimeError('%s failed: %s' % (' '.join(cmd[:3]), (r.stderr or r.stdout)[-300:]))
    return r.stdout


INST = list(csv.DictReader(open('evidence/instances/instances.csv')))
CHECKSUMS = {}
for line in open('evidence/checksums.txt'):
    m = re.match(r'^([0-9a-f]{64})  (\S+)', line)
    if m:
        CHECKSUMS.setdefault(m[2], m[1])


@check('1. every Python file compiles and every shell script parses')
def _():
    py = [p for p in glob.glob('**/*.py', recursive=True)]
    for p in py:
        compile(open(p).read(), p, 'exec')
    sh = glob.glob('**/*.sh', recursive=True)
    for p in sh:
        run(['bash', '-n', p])
    return '%d Python files, %d shell scripts' % (len(py), len(sh))


@check('2. licences, proceedings and evidence are present')
def _():
    need = ['LICENSE', 'LICENSE-CC-BY-4.0.md', 'LICENSING.md', 'NOTICE', 'REUSE.toml', 'VERSION',
            'LICENSES/Apache-2.0.txt', 'LICENSES/CC-BY-4.0.txt', 'LICENSES/GPL-2.0-or-later.txt',
            'LICENSES/GPL-3.0-or-later.txt', 'third_party/sha1-sat/COPYING', 'third_party/sha1-sat/MODIFICATIONS.md',
            'proceedings/sha1_dagster_proceedings.tex', 'proceedings/sha1_dagster_proceedings.pdf',
            'proceedings/references.bib', 'evidence/checksums.txt', 'evidence/provenance.txt']
    missing = [p for p in need if not os.path.isfile(p)]
    assert not missing, 'missing: %s' % missing
    return '%d files' % len(need)


@check('3. generator and patch carry the hashes the proceedings bind them to')
def _():
    g = sha('third_party/sha1-sat/main_noboost.cc')
    assert g.startswith('3bcc3634a4105a03'), 'generator source %s' % g[:16]
    man = json.load(open('evidence/validation/dagster_bounded_v3.manifest.json'))
    p = sha('patches/dagster_bounded_v3.py')
    assert p == man['patch_script_sha256'], 'patch script %s != campaign %s' % (p[:16], man['patch_script_sha256'][:16])
    assert sha('evidence/validation/dagster_bounded_v3.diff') == man['diff_sha256'], 'diff hash'
    return 'generator 3bcc3634..., patch %s... = campaign manifest' % p[:8]


TMP = tempfile.mkdtemp(prefix='sha1-dagster-selftest.')


@check('4. the generator regenerates all five campaign instances byte for byte')
def _():
    assert os.access('bin/sha1gen', os.X_OK), 'bin/sha1gen missing: run make all (make selftest does)'
    for r in INST:
        out = os.path.join(TMP, r['target'] + '.cnf')
        with open(out, 'w') as f:
            subprocess.run(['bin/sha1gen', '--attack=preimage', '--rounds=22', '--hash-bits=160', '--seed=' + r['seed'],
                            '--cnf', '--tseitin-adders'], stdout=f, stderr=subprocess.DEVNULL, check=True)
        assert sha(out) == r['raw_cnf_sha256'], '%s differs' % r['target']
    return ', '.join(r['target'] for r in INST)


@check('5. header repair reproduces the campaign repaired instances')
def _():
    from sha1_cnf import Sha1Cnf
    for r in INST:
        src = os.path.join(TMP, r['target'] + '.cnf'); out = os.path.join(TMP, r['target'] + '.repaired.cnf')
        Sha1Cnf(src).write_repaired(out)
        assert sha(out) == r['repaired_cnf_sha256'], '%s repaired differs' % r['target']
    return '5 repaired CNFs; %s variables, %s clauses each' % (INST[0]['variables'], INST[0]['clauses'])


M32 = 0xffffffff
IV = [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0]
rotl = lambda x, n: ((x << n) | (x >> (32 - n))) & M32


def sha1_reduced(w, rounds):
    """Independent reduced-round SHA-1 compression with feed-forward (FIPS 180-4)."""
    W = list(w)
    for t in range(16, rounds):
        W.append(rotl(W[t - 3] ^ W[t - 8] ^ W[t - 14] ^ W[t - 16], 1))
    a, b, c, d, e = IV
    for t in range(rounds):
        if t < 20:   f, k = (b & c) | (~b & d), 0x5A827999
        elif t < 40: f, k = b ^ c ^ d, 0x6ED9EBA1
        elif t < 60: f, k = (b & c) | (b & d) | (c & d), 0x8F1BBCDC
        else:        f, k = b ^ c ^ d, 0xCA62C1D6
        a, b, c, d, e = (rotl(a, 5) + (f & M32) + e + k + W[t]) & M32, a, rotl(b, 30), c, d
    return [(x + y) & M32 for x, y in zip(IV, (a, b, c, d, e))]


@check('6. planted blocks hash to each target digest, which matches the CNF digest units')
def _():
    from sha1_cnf import Sha1Cnf
    for r in INST:
        w = [int(l, 16) for l in open('evidence/instances/%s.planted' % r['target']) if l.strip()]
        assert len(w) == 16 and not any(x >> 31 for x in w), 'planted block shape'
        dig = sha1_reduced(w, 22)
        assert ''.join('%08x' % x for x in dig) == r['digest'], '%s digest' % r['target']
        c = Sha1Cnf(os.path.join(TMP, r['target'] + '.cnf'))
        units = {abs(cl[0]): cl[0] > 0 for cl in c.clauses[:c.true_cc] if len(cl) == 1}
        fix = [sum((1 << b) for b, v in enumerate(c.h_out(i)) if units.get(v)) for i in range(5)]
        assert fix == dig, '%s digest units' % r['target']
    return 'independent SHA-1 agrees for all 5 targets'


@check('7. every planted block verifies as a preimage with the release verifier')
def _():
    for r in INST:
        cnf = os.path.join(TMP, r['target'] + '.cnf'); model = os.path.join(TMP, r['target'] + '.model')
        run([PY, 'tools/planted_model.py', cnf, 'evidence/instances/%s.planted' % r['target'], model])
        out = run([PY, 'tools/verify_preimage.py', cnf, model])
        assert 'VALID PREIMAGE' in out, '%s: %s' % (r['target'], out.strip()[-80:])
    return '5 valid preimages'


@check('8. the arm-C DAG rebuilds to its recorded SHA-256 (with planted-path validation)')
def _():
    r = INST[0]
    prefix = os.path.join(TMP, 'C_' + r['target'])
    out = run([PY, 'tools/compositional_race.py', 'build', os.path.join(TMP, r['target'] + '.cnf'),
               '--out-prefix', prefix, '--components', 'low16,staged,part16'])
    assert 'validation: OK' in out, out[-200:]
    assert sha(prefix + '.dag') == r['dag_sha256'], 'DAG differs'
    assert sha(prefix + '.cnf') == r['dag_cnf_sha256'], 'combined DAG CNF differs'
    return '%s: 13-node DAG and combined CNF identical; validation OK' % r['target']


@check('9. the evidence tables regenerate exactly from the shipped run records')
def _():
    out = os.path.join(TMP, 'rederived')
    run([PY, 'evidence/scripts/derive.py', 'evidence/runs', out])
    for t in ('runs.csv', 'summary.csv', 'census.csv', 'budget.csv'):
        assert open(os.path.join(out, t)).read() == open(os.path.join('evidence/derived', t)).read(), t + ' differs'
    return 'runs, summary, census, budget identical'


@check('10. the proceedings headline numbers follow from the evidence')
def _():
    S = {(r['prefix'], r['arm']): r for r in csv.DictReader(open('evidence/derived/summary.csv')) if r['target'] == 'pooled'}
    expect = {('eval_r22', 'C'): ('30', '30', 26.9, 1.38), ('eval_r22', 'Cp'): ('30', '30', 40.8, 0.60),
              ('eval_r22', 'Br'): ('28', '30', 552.6, 0.07), ('dev_L8_B0', 'C'): ('20', '20', 21.3, 1.61),
              ('dev_L0', 'C'): ('14', '20', 144.7, 0.22), ('dev_Br', 'Br'): ('19', '20', 499.6, 0.07)}
    for k, (s, n, med, y) in expect.items():
        r = S[k]
        assert (r['solved'], r['runs']) == (s, n), '%s solved %s/%s' % (k, r['solved'], r['runs'])
        assert round(float(r['median_s']), 1) == med, '%s median %s' % (k, r['median_s'])
        exact = int(r['solved']) / float(r['physical_core_hours'])     # from counts, not the 3-decimal column
        assert abs(exact - y) < 0.005, '%s yield %.4f' % (k, exact)
    assert int(S[('dev_L0', 'C')]['sigkilled']) == 6, 'L=inf kills'
    import report_stats as R
    C, B = R.load('evidence/runs/eval_r22', 'C'), R.load('evidence/runs/eval_r22', 'Br')
    w, l, u = R.pair(C, B)
    assert (w, l, u) == (30, 0, 0), 'pairs %s' % ((w, l, u),)
    p = R.sign_test(w, l); lo, hi = R.cp_interval(w, w + l)
    assert abs(p - 1.862645e-9) < 1e-14 and abs(lo - 0.884) < 5e-4, 'p %g, interval %.4f' % (p, lo)
    rat = sorted((B[k][0] or 1800.0) / C[k][0] for k in C)
    assert round(st.median(rat), 1) == 23.6, 'paired ratio median %.2f' % st.median(rat)
    cen = list(csv.DictReader(open('evidence/derived/census.csv')))
    assert [c['solved_within_cap'] for c in cen] == ['2', '2', '2'], 'census'
    bud = {r['terminal_budget']: int(r['terminal_exhaustion_events']) for r in csv.DictReader(open('evidence/derived/budget.csv'))}
    assert bud == {'0': 0, '2000': 610779, '20000': 27271, '200000': 3956}, 'budget events'
    return 'Phase C 30/30 vs 28/30, 30-0, p=1.9e-9, [0.884,1], ratio 23.6; Phase B; census; budgets'


@check('11. every distributed file listed in checksums.txt matches')
def _():
    n = 0
    for line in open('evidence/checksums.txt'):
        m = re.match(r'^([0-9a-f]{64})  (\S+) \(as distributed\)', line)
        if m:
            assert sha(m[2]) == m[1], m[2]; n += 1
    assert n >= 2
    return '%d files' % n


def reuse_patterns():
    pats = []
    for block in open('REUSE.toml').read().split('[[annotations]]')[1:]:
        m = re.search(r'path\s*=\s*\[(.*?)\]', block, re.S)
        pats += re.findall(r'"([^"]+)"', m[1])
    return pats


def glob_match(path, pat):
    if pat.startswith('**/'):
        return fnmatch.fnmatch(os.path.basename(path), pat[3:]) or fnmatch.fnmatch(path, pat[3:])
    if pat.endswith('/**'):
        return path.startswith(pat[:-2])
    return fnmatch.fnmatch(path, pat)


def released_files():
    out = []
    for dp, dn, fn in os.walk('.'):
        dn[:] = [d for d in dn if d not in ('.git', '__pycache__', 'bin', 'build-modern')]
        for f in fn:
            p = os.path.relpath(os.path.join(dp, f), '.')
            if p.endswith(('.pyc', '.aux', '.log', '.out', '.toc')):
                continue
            out.append(p)
    return sorted(out)


@check('12. every released file is covered by a licence annotation (REUSE.toml)')
def _():
    pats = reuse_patterns()
    uncovered = [p for p in released_files()
                 if not (p in ('LICENSE', 'MANIFEST.sha256') or p.startswith('LICENSES/') or p == 'LICENSE-CC-BY-4.0.md'
                         or any(glob_match(p, q) for q in pats))]
    assert not uncovered, 'uncovered: %s' % uncovered[:8]
    return '%d files covered' % len(released_files())


# Tokens that must not appear anywhere in the release (site, host and account names), held only as
# SHA-256 digests so that this file does not itself name them.
FORBIDDEN = {
    'eade9e258f9e', '27f96233ad90', '1abba1444b98', '9386f0dfd8f3',
}
ALLOWED_EMAILS = {'charles.gretton@anu.edu.au', 'vegard.nossum@gmail.com'}
PATH_RE = re.compile(r'(?<![\w.])/(home|Users|scratch|g/data|mnt|tmp)/[\w.-]+')


@check('13. privacy: no account, site or host names, absolute home paths, or unexpected emails')
def _():
    hits = []
    texts = []
    for p in released_files():
        if p.endswith('.pdf'):
            if shutil.which('pdftotext'):
                texts.append((p, subprocess.run(['pdftotext', p, '-'], capture_output=True, text=True).stdout))
            continue
        try:
            texts.append((p, open(p, encoding='utf-8').read()))
        except UnicodeDecodeError:
            continue
    for p, t in texts:
        if p == 'tools/selftest.py':
            continue
        for tok in set(re.findall(r'[a-z0-9]+', t.lower())):
            if hashlib.sha256(tok.encode()).hexdigest()[:12] in FORBIDDEN:
                hits.append('%s: forbidden token' % p)
        for m in PATH_RE.finditer(t):
            hits.append('%s: absolute path %s' % (p, m.group(0)[:30]))
        for e in set(re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', t)):
            if e.rstrip('.') not in ALLOWED_EMAILS:
                hits.append('%s: email %s' % (p, e))
    assert not hits, '; '.join(sorted(set(hits))[:6])
    return '%d text files scanned (PDF via pdftotext)' % len(texts)


@check('14. MANIFEST.sha256, when present, matches every listed file')
def _():
    if not os.path.exists('MANIFEST.sha256'):
        return 'no manifest yet (make manifest)'
    n = 0
    for line in open('MANIFEST.sha256'):
        m = re.match(r'^([0-9a-f]{64})  \./(.+)$', line.rstrip('\n'))
        if m:
            assert sha(m[2]) == m[1], m[2]; n += 1
    return '%d files' % n


shutil.rmtree(TMP, ignore_errors=True)
w = max(len(n) for _, n, _ in RESULTS)
for ok, name, detail in RESULTS:
    print('%s  %-*s  %s' % ('PASS' if ok else 'FAIL', w, name, detail))
failed = sum(1 for ok, _, _ in RESULTS if not ok)
print('\n%d checks, %d failed' % (len(RESULTS), failed))
sys.exit(1 if failed else 0)
