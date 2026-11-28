(() => {
  const L = (globalThis.__st = []);
  const H = () => { try { return document.documentElement.scrollHeight; } catch (_) { return -1; } };
  for (const k of ['scrollTo', 'scroll', 'scrollBy']) {
    const o = window[k];
    window[k] = function (...a) { const before = scrollY, h = H(); const r = o.apply(this, a); L.push([k, JSON.stringify(a).slice(0, 80), Math.round(before), Math.round(scrollY), h, '', new Error().stack.split('\n').slice(2, 8).map(x => x.trim().replace(/https:\/\/static[^ ]*\/([^/]{0,30})[^/ ]*\.js/, 'FB:$1')).join(' | ')]); return r; };
  }
  const d = Object.getOwnPropertyDescriptor(Element.prototype, 'scrollTop');
  if (d && d.set) Object.defineProperty(Element.prototype, 'scrollTop', { configurable: true, get: d.get, set(v) { const before = scrollY; d.set.call(this, v); if (this === document.documentElement || this === document.body || this === document.scrollingElement) L.push(['scrollTop=', v, Math.round(before), Math.round(scrollY), H(), this.tagName, new Error().stack.split('\n').slice(2, 7).map(x => x.trim().replace(/https:\/\/static[^ ]*\/([^/]{0,30})[^/ ]*\.js/, 'FB:$1')).join(' | ')]); } });
})();
(() => {
  const L = globalThis.__st;
  for (const k of ['pushState', 'replaceState', 'back', 'go']) {
    const o = history[k];
    history[k] = function (...a) { L.push(['H.' + k, JSON.stringify(a[0] === undefined ? null : a[0]).slice(0, 300), String(a[2] || '').replace(/\d{6,}/g, 'N').slice(0, 80), Math.round(scrollY)]); return o.apply(this, a); };
  }
  addEventListener('popstate', (e) => L.push(['popstate', JSON.stringify(e.state).slice(0, 300), location.pathname.replace(/\d{6,}/g, 'N'), Math.round(scrollY)]));
})();
