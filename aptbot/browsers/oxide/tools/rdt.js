(() => {
  const roots = new Set();
  globalThis.__REACT_DEVTOOLS_GLOBAL_HOOK__ = {
    supportsFiber: true, renderers: new Map(), isDisabled: false, checkDCE() {},
    inject(r) { const id = this.renderers.size + 1; this.renderers.set(id, r); return id; },
    onCommitFiberRoot(id, root) { roots.add(root); }, onCommitFiberUnmount() {}, onPostCommitFiberRoot() {}, onScheduleFiberRoot() {},
  };
  globalThis.__rdtRoots = roots;
  globalThis.__findFiber = (el) => {
    for (const r of roots) {
      const st = [r.current];
      while (st.length) { const f = st.pop(); if (f.stateNode === el) return f; if (f.sibling) st.push(f.sibling); if (f.child) st.push(f.child); }
    }
    return null;
  };
  globalThis.__walkHidden = (el) => {
    const f0 = __findFiber(el); if (!f0) return 'no fiber (roots=' + roots.size + ')';
    const out = [];
    for (let f = f0, n = 0; f && n < 600; f = f.return, n++) {
      const p = f.memoizedProps; if (!p || typeof p !== 'object') continue;
      const hk = Object.keys(p).filter(x => /hidden|Hidden|ackgrounded|isVisible|debugName|routeKey|tabKey/.test(x));
      const t = f.type;
      const name = typeof t === 'function' ? (t.displayName || t.name) : (t && typeof t === 'object') ? (t.displayName || (t._context ? 'Provider' : t.render ? 'fwd:' + (t.render.displayName || t.render.name) : t.type ? 'memo:' + (t.type.displayName || t.type.name) : '?')) : String(t);
      let val = '';
      if (p.value && typeof p.value === 'object' && ('hidden' in p.value || 'backgrounded' in p.value)) val = ' value=' + JSON.stringify({ hidden: p.value.hidden, backgrounded: p.value.backgrounded });
      if (hk.length || val) out.push(n + ' ' + name + ' ' + hk.map(x => x + '=' + String(JSON.stringify(p[x])).slice(0, 60)).join(',') + val);
    }
    return out.join('\n');
  };
})();
