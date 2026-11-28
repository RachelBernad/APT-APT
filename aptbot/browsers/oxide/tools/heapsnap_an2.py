import json, sys, collections
d = json.load(open(sys.argv[1]))
m = d['snapshot']['meta']; nf = m['node_fields']; NF = len(nf); ef = m['edge_fields']; EF = len(ef)
ntypes = m['node_types'][0]; etypes = m['edge_types'][0]
iT, iN, iS, iE = nf.index('type'), nf.index('name'), nf.index('self_size'), nf.index('edge_count')
eT, eN, eTo = ef.index('type'), ef.index('name_or_index'), ef.index('to_node')
nodes, edges, S = d['nodes'], d['edges'], d['strings']
N = len(nodes) // NF
first = [0] * (N + 1)
for k in range(N): first[k + 1] = first[k] + nodes[k * NF + iE] * EF
ret = [[] for _ in range(N)]   # reverse edges (retainers), capped
shape = collections.Counter(); shsz = collections.Counter(); sample = {}
for k in range(N):
    props = []
    for e in range(first[k], first[k + 1], EF):
        et = etypes[edges[e + eT]]; to = edges[e + eTo] // NF
        nm = S[edges[e + eN]] if et in ('property', 'context', 'internal', 'shortcut') else f'[{edges[e+eN]}]'
        if et != 'weak' and len(ret[to]) < 4: ret[to].append((k, et, nm))
        if et == 'property': props.append(nm)
    if ntypes[nodes[k * NF + iT]] == 'object' and S[nodes[k * NF + iN]] == 'Object':
        key = ','.join(sorted(props)[:12])
        shape[key] += 1; shsz[key] += nodes[k * NF + iS]; sample.setdefault(key, k)
def label(k):
    return f"{ntypes[nodes[k*NF+iT]]}:{S[nodes[k*NF+iN]][:50]!r}"
def path(k, depth=14):
    out = []; seen = {k}
    for _ in range(depth):
        r = [x for x in ret[k] if x[0] not in seen]
        if not r: break
        p, et, nm = r[0]; out.append(f"<-{et}:{nm[:40]}- {label(p)}"); seen.add(p); k = p
    return '\n      '.join(out)
MB = 1 << 20
print('TOP Object shapes (count, MB, props):')
for key, c in shape.most_common(12):
    print(f'{c:8} {shsz[key]/MB:6.1f} MB  {{{key[:150]}}}')
    print('      ' + path(sample[key]))
print('\nsetListener closure retainers:')
for k in range(N):
    if ntypes[nodes[k*NF+iT]] == 'closure' and S[nodes[k*NF+iN]] == 'setListener':
        print('      ' + path(k)); break
