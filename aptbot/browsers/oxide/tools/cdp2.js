(() => {
  const L = (globalThis.__cdp = []);
  const o = Node.prototype.compareDocumentPosition;
  const path = (n) => { const p = []; for (let x = n; x && x.parentElement; x = x.parentElement) { let i = 0; for (let s = x.previousElementSibling; s; s = s.previousElementSibling) i++; p.push(i); } return p.reverse().join('.') + (n && n.isConnected ? '' : '(dc)'); };
  Node.prototype.compareDocumentPosition = function (x) { const r = o.call(this, x); if (globalThis.__cdpOn) L.push([path(this), (this.className || '').split(' ').length, path(x), (x.className || '').split(' ').length, r]); return r; };
  globalThis.__path = path;
})();
