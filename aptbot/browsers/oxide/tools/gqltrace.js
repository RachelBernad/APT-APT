(() => {
  const C = (globalThis.__gql = {});
  const note = (b) => { try { const s = typeof b === 'string' ? b : (b && b.toString && b.constructor && b.constructor.name === 'URLSearchParams') ? b.toString() : (b instanceof FormData ? [...b.entries()].map(([k, v]) => k + '=' + v).join('&') : ''); const m = /fb_api_req_friendly_name=([A-Za-z0-9_]+)/.exec(s); if (m) C[m[1]] = (C[m[1]] || 0) + 1; } catch (_) {} };
  const send = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.send = function (b) { note(b); return send.apply(this, arguments); };
  const f = globalThis.fetch;
  globalThis.fetch = function (u, o) { note(o && o.body); return f.apply(this, arguments); };
})();
