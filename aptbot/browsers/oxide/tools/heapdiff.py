"""Diff two V8 heap snapshots by constructor/type name: count and self size."""
import json, sys, collections
def load(p):
    d = json.load(open(p))
    m = d['snapshot']['meta']; nf = m['node_fields']; NF = len(nf)
    types = m['node_types'][0]; iT, iN, iS = nf.index('type'), nf.index('name'), nf.index('self_size')
    nodes, S = d['nodes'], d['strings']
    c, sz = collections.Counter(), collections.Counter()
    for k in range(0, len(nodes), NF):
        t = types[nodes[k + iT]]
        name = S[nodes[k + iN]] if t in ('object', 'closure', 'native', 'hidden', 'array') else t
        key = f"{t}:{name[:60]}"
        c[key] += 1; sz[key] += nodes[k + iS]
    return c, sz
a, asz = load(sys.argv[1]); b, bsz = load(sys.argv[2])
rows = sorted(set(a) | set(b), key=lambda k: bsz[k] - asz[k], reverse=True)
print(f"total self size: {sum(asz.values())/1e6:.1f} MB -> {sum(bsz.values())/1e6:.1f} MB")
print(f"{'delta MB':>9} {'delta n':>9}  key")
for k in rows[:40]:
    print(f"{(bsz[k]-asz[k])/1e6:9.2f} {b[k]-a[k]:9d}  {k}")
