import json, sys, collections
def load(p):
    d = json.load(open(p)); m = d['snapshot']['meta']; nf = m['node_fields']; NF = len(nf); ef = m['edge_fields']; EF = len(ef)
    types = m['node_types'][0]; iT, iN, iS, iE = nf.index('type'), nf.index('name'), nf.index('self_size'), nf.index('edge_count')
    eTo = ef.index('to_node'); nodes, edges, S = d['nodes'], d['edges'], d['strings']
    N = len(nodes) // NF
    first = [0] * (N + 1)
    for k in range(N): first[k + 1] = first[k] + nodes[k * NF + iE] * EF
    want = {'CSSStyleDeclaration', 'Element', 'HTMLDivElement', 'HTMLSpanElement', 'Text', 'DOMRect', 'DOMTokenList', 'NamedNodeMap', 'Attr', 'NodeList', 'MutationRecord', 'Comment', 'HTMLAnchorElement', 'SVGElement', 'HTMLImageElement'}
    c = collections.Counter(); sz = collections.Counter()
    for k in range(N):
        if types[nodes[k*NF+iT]] != 'object': continue
        name = S[nodes[k*NF+iN]]
        if name in want or name.startswith('SVG') or name.startswith('HTML'):
            key = name if name in want else ('SVG*' if name.startswith('SVG') else 'HTML*Element')
            c[key] += 1
            # self + direct property arrays
            sz[key] += nodes[k*NF+iS]
            for e in range(first[k], first[k+1], EF):
                to = edges[e + eTo] // NF
                if types[nodes[to*NF+iT]] in ('array', 'hidden') : sz[key] += nodes[to*NF+iS]
    return c, sz
for p in sys.argv[1:]:
    c, sz = load(p)
    print(p.split('/')[-1], " ".join(f"{k}={c[k]}/{sz[k]/1e6:.1f}MB" for k in sorted(c, key=lambda k: -sz[k])))
