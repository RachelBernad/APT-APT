"""Objects new in snapshot B (ids absent from A): group by retainer chain (3 hops of edge names + holder names)."""
import json, sys, collections, random
def ids_of(p):
    d = json.load(open(p)); m = d['snapshot']['meta']; nf = m['node_fields']; NF = len(nf); iI = nf.index('id')
    return set(d['nodes'][k + iI] for k in range(0, len(d['nodes']), NF))
old = ids_of(sys.argv[1])
d = json.load(open(sys.argv[2]))
m = d['snapshot']['meta']; nf = m['node_fields']; NF = len(nf); ef = m['edge_fields']; EF = len(ef)
types = m['node_types'][0]; etypes = m['edge_types'][0]
iT, iN, iS, iE, iI = nf.index('type'), nf.index('name'), nf.index('self_size'), nf.index('edge_count'), nf.index('id')
eT, eN, eTo = ef.index('type'), ef.index('name_or_index'), ef.index('to_node')
nodes, edges, S = d['nodes'], d['edges'], d['strings']
N = len(nodes) // NF
ret = [None] * N
e = 0
for k in range(N):
    for _ in range(nodes[k * NF + iE]):
        t = etypes[edges[e + eT]]
        if t not in ('weak', 'shortcut'):
            to = edges[e + eTo] // NF
            nm = edges[e + eN]; nm = S[nm] if t in ('property', 'context', 'internal') else f"[{nm}]"
            if ret[to] is None: ret[to] = []
            if len(ret[to]) < 4: ret[to].append((k, nm))
        e += EF
def nname(k):
    t = types[nodes[k * NF + iT]]; n = S[nodes[k * NF + iN]]
    return f"{t}:{n[:40]}"
new = [k for k in range(N) if nodes[k * NF + iI] not in old]
print("new nodes:", len(new), "self MB:", round(sum(nodes[k * NF + iS] for k in new) / 1e6, 1))
newset = set(new)
# Climb from each new object to the first OLD ancestor (the thing that keeps the new subgraph alive).
anchor = collections.Counter(); anchor_sz = collections.Counter(); example = {}
random.seed(1)
sample = random.sample(new, min(40000, len(new)))
for k in sample:
    cur, chain = k, []
    for _ in range(60):
        r = ret[cur]
        if not r: break
        p, nm = r[0]
        chain.append(nm)
        cur = p
        if cur not in newset: break
    key = nname(cur) + "  ." + ".".join(reversed(chain[-3:]))
    anchor[key] += 1; anchor_sz[key] += nodes[k * NF + iS]
    example.setdefault(key, cur)
def up(k, depth=10):
    out = []
    for _ in range(depth):
        r = ret[k]
        if not r: break
        p, nm = r[0]; out.append(f"{nname(p)} --{nm}-->"); k = p
    return " <- ".join(out)
for key, c in anchor.most_common(14):
    print(f"{c:6d} {anchor_sz[key]/1e6*len(new)/len(sample):6.1f}MB  {key}")
    print("         path to old anchor root:", up(example[key], 7)[:700])
