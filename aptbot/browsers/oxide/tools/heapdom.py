import json, sys, collections
d = json.load(open(sys.argv[1]))
m = d['snapshot']['meta']; nf = m['node_fields']; NF = len(nf); ef = m['edge_fields']; EF = len(ef)
types = m['node_types'][0]; etypes = m['edge_types'][0]
iT, iN, iS, iE = nf.index('type'), nf.index('name'), nf.index('self_size'), nf.index('edge_count')
eT, eN, eTo = ef.index('type'), ef.index('name_or_index'), ef.index('to_node')
nodes, edges, S = d['nodes'], d['edges'], d['strings']
N = len(nodes) // NF
rets = collections.defaultdict(list)
e = 0
for k in range(N):
    for _ in range(nodes[k * NF + iE]):
        t = etypes[edges[e + eT]]; to = edges[e + eTo] // NF
        nm = edges[e + eN]; nm = S[nm] if t in ('property', 'context', 'internal', 'shortcut') else f"[{nm}]"
        if S[nodes[to * NF + iN]].startswith('HTML') and types[nodes[to * NF + iT]] == 'object':
            rets[to].append((t, S[nodes[k * NF + iN]][:40], nm[:60]))
        e += EF
els = [k for k in range(N) if types[nodes[k * NF + iT]] == 'object' and S[nodes[k * NF + iN]].startswith('HTML') and S[nodes[k * NF + iN]].endswith('Element')]
print("element wrappers:", len(els))
c = collections.Counter()
for k in els:
    kinds = frozenset((t, holder, nm if t != 'element' else '[i]') for t, holder, nm in rets[k] if t != 'weak')
    c[tuple(sorted((t + ':' + h + '.' + n) for t, h, n in kinds))[:4]] += 1
for k, v in c.most_common(12): print(v, k)
