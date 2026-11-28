(() => {
  const L = (globalThis.__rs = []);
  const seen = new WeakSet();
  const track = (n) => {
    if (!n || n.nodeType !== 1 || seen.has(n)) return;
    if (n.tagName === 'SCRIPT' || (n.tagName === 'LINK')) {
      seen.add(n);
      const rec = { tag: n.tagName, rel: n.getAttribute('rel'), as: n.getAttribute('as'), src: String(n.src || n.href || '').replace(/^https:\/\/static[^/]*\/rsrc\.php\//, '').slice(0, 50), t: Date.now(), ev: '' , inline: !n.src && n.tagName === 'SCRIPT' };
      L.push(rec);
      n.addEventListener('load', () => rec.ev += 'L'); n.addEventListener('error', () => rec.ev += 'E');
    }
  };
  new MutationObserver((ms) => { for (const m of ms) for (const n of m.addedNodes) track(n); }).observe(document, { childList: true, subtree: true });
})();
