(() => {
  const L = (globalThis.__idb = []);
  const F = indexedDB, o = F.open.bind(F);
  const wrap = (name, kind, fn) => function (ev) {
    L.push(["fire", name, kind]);
    try { return fn.apply(this, arguments); } catch (e) { L.push(["THROW", name, kind, String(e && e.stack || e).slice(0, 600)]); throw e; }
  };
  F.open = function (name, ver) {
    const r = o(name, ver);
    L.push(["open", name, ver]);
    for (const k of ["onupgradeneeded", "onsuccess", "onerror", "onblocked"]) {
      let v = null;
      Object.defineProperty(r, k, { configurable: true, get: () => v, set: (f) => { L.push(["set", name, k]); v = typeof f === "function" ? wrap(name, k, f) : f; } });
    }
    const ael = r.addEventListener.bind(r);
    r.addEventListener = (t, l, ...a) => { L.push(["ael", name, t]); return ael(t, typeof l === "function" ? wrap(name, t, l) : l, ...a); };
    return r;
  };
  const tx = IDBDatabase.prototype.transaction;
  IDBDatabase.prototype.transaction = function (n, m) { L.push(["tx", this.name, String(n), m, [...this.objectStoreNames], new Error().stack.split("\n").slice(2, 5).join(" | ").slice(0, 400)]); return tx.call(this, n, m); };
})();
