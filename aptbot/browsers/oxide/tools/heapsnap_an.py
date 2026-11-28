import json, sys, collections
d = json.load(open(sys.argv[1]))
m = d['snapshot']['meta']; nf = m['node_fields']; NF = len(nf)
types = m['node_types'][0]
iT, iN, iS = nf.index('type'), nf.index('name'), nf.index('self_size')
nodes, strings = d['nodes'], d['strings']
by = collections.Counter(); cnt = collections.Counter(); bytype = collections.Counter()
big_str = collections.Counter()
for i in range(0, len(nodes), NF):
    t = types[nodes[i+iT]]; sz = nodes[i+iS]; nm = strings[nodes[i+iN]]
    bytype[t] += sz
    key = (t, nm if t in ('object', 'closure', 'native', 'hidden', 'object shape', 'code') else '')
    by[key] += sz; cnt[key] += 1
    if t in ('string', 'concatenated string', 'sliced string') and sz > 200_000:
        big_str[nm[:70].replace('\n', ' ')] += sz
MB = 1 << 20
print('TOTAL', sum(bytype.values()) // MB, 'MB')
for t, s in bytype.most_common(): print(f'  {t:22} {s/MB:8.1f} MB')
print('\nTOP (type, constructor):')
for (t, n), s in by.most_common(30): print(f'  {s/MB:8.1f} MB  {cnt[(t,n)]:8}  {t}: {n[:70]}')
print('\nBIG STRINGS (>200KB each), by prefix:')
for p, s in big_str.most_common(15): print(f'  {s/MB:7.1f} MB  {p!r}')
