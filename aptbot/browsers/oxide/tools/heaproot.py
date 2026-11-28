"""Shortest GC-root path (BFS over strong edges) to sampled objects that are new in B vs A."""
import json, sys, collections, random
from array import array
def ids_of(p):
    d = json.load(open(p)); m = d['snapshot']['meta']; nf = m['node_fields']; NF = len(nf); iI = nf.index('id')
    return set(d['nodes'][k + iI] for k in range(0, len(d['nodes']), NF))
old = ids_of(sys.argv[1])
d = json.load(open(sys.argv[2]))
m = d['snapshot']['meta']; nf = m['node_fields']; NF = len(nf); ef = m['edge_fields']; EF = len(ef)
types = m['node_types'][0]; etypes = m['edge_types'][0]
iT, iN, iE, iI = nf.index('type'), nf.index('name'), nf.index('edge_count'), nf.index('id')
eT, eN, eTo = ef.index('type'), ef.index('name_or_index'), ef.index('to_node')
nodes, edges, S = d['nodes'], d['edges'], d['strings']
N = len(nodes) // NF
first = array('l', [0]) * (N + 1)
for k in range(N): first[k + 1] = first[k] + nodes[k * NF + iE] * EF
par = array('l', [-1]) * N; pedge = array('l', [-1]) * N
par[0] = 0
q = collections.deque([0])
while q:
    k = q.popleft()
    for e in range(first[k], first[k + 1], EF):
        if etypes[edges[e + eT]] in ('weak', 'shortcut'): continue
        to = edges[e + eTo] // NF
        if par[to] == -1: par[to] = k; pedge[to] = e; q.append(to)
def nm(k): return f"{types[nodes[k*NF+iT]]}:{S[nodes[k*NF+iN]][:45]}"
def en(e):
    t = etypes[edges[e + eT]]; v = edges[e + eN]
    return S[v][:60] if t in ('property', 'context', 'internal') else f"[{v}]"
new = [k for k in range(N) if nodes[k*NF+iI] not in old and types[nodes[k*NF+iT]] == 'object' and par[k] != -1]
random.seed(3)
c = collections.Counter(); ex = {}
for k in random.sample(new, min(300, len(new))):
    path = []; cur = k
    while cur != 0 and len(path) < 200:
        path.append((par[cur], pedge[cur])); cur = par[cur]
    path.reverse()
    # signature: first 9 hops from root (root-side) – where retention starts
    sig = " > ".join(f"{nm(p)}.{en(e)}" for p, e in path[:9])
    c[sig] += 1; ex.setdefault(sig, len(path))
for s, v in c.most_common(8): print(v, f"(len {ex[s]})", s[:1500]); print()
