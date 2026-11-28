import json, sys, collections
d = json.load(open(sys.argv[1])); target = sys.argv[2]
m = d['snapshot']['meta']; nf = m['node_fields']; NF = len(nf); ef = m['edge_fields']; EF = len(ef)
types = m['node_types'][0]; etypes = m['edge_types'][0]
iT, iN, iE = nf.index('type'), nf.index('name'), nf.index('edge_count')
eT, eN, eTo = ef.index('type'), ef.index('name_or_index'), ef.index('to_node')
nodes, edges, S = d['nodes'], d['edges'], d['strings']
N = len(nodes) // NF
ret = collections.defaultdict(list)
e = 0
for k in range(N):
    for _ in range(nodes[k * NF + iE]):
        t = etypes[edges[e + eT]]
        if t not in ('weak', 'shortcut'):
            to = edges[e + eTo] // NF; nm = edges[e + eN]; nm = S[nm] if t in ('property', 'context', 'internal') else f"[{nm}]"
            if len(ret[to]) < 3: ret[to].append((k, nm))
        e += EF
nm = lambda k: f"{types[nodes[k*NF+iT]]}:{S[nodes[k*NF+iN]][:50]}"
tg = [k for k in range(N) if types[nodes[k*NF+iT]] == 'closure' and S[nodes[k*NF+iN]] == target]
print(target, "closures:", len(tg))
c = collections.Counter(); ex = {}
for k in tg:
    chain = []; cur = k
    for _ in range(8):
        r = ret.get(cur)
        if not r: break
        p, e2 = r[0]; chain.append(f"{nm(p)}.{e2}"); cur = p
    key = " <- ".join(chain[:6]); c[key] += 1
for k, v in c.most_common(6): print(v, k[:900]); print()
