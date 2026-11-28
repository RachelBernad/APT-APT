(() => {
  const L = (globalThis.__con = []);
  for (const k of ["error", "warn"]) {
    const orig = console[k];
    console[k] = function (...a) {
      try { L.push([k, a.map((x) => x && x.stack ? String(x.stack) : typeof x === "object" ? (() => { try { return JSON.stringify(x).slice(0, 300); } catch (_) { return String(x); } })() : String(x)).join(" ").slice(0, 1500)]); } catch (_) {}
      return orig && orig.apply(this, a);
    };
  }
})();
