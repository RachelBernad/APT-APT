// Research-only init script: counts feed post ids from graphql responses.
// Page-visible (wraps XMLHttpRequest/fetch); production scrapers should tap the
// network engine-side instead. See kb/caveats.md.
(() => {
  const K = Symbol.for('__research_capture__');
  const st = { ids: new Set(), names: {}, bytes: {} }; const nm = new WeakMap();
  Object.defineProperty(globalThis, K, { value: st, enumerable: false });
  const scan = (txt) => { if (typeof txt !== 'string') return; const re = /"post_id":"(\d+)"/g; let m; while ((m = re.exec(txt))) st.ids.add(m[1]); };
  const name = (b) => { let out = '?'; try { const s = typeof b === 'string' ? b : (b instanceof URLSearchParams ? b.toString() : (b instanceof FormData ? [...b.entries()].map(([k, v]) => k + '=' + v).join('&') : '')); const m = /fb_api_req_friendly_name=([A-Za-z0-9_]+)/.exec(s); if (m) { st.names[m[1]] = (st.names[m[1]] || 0) + 1; out = m[1]; } } catch (_) {} return out; };
  const add = (n, t) => { if (typeof t === 'string') st.bytes[n] = (st.bytes[n] || 0) + t.length; };
  const send = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.send = function (b) { const n = name(b); this.addEventListener('load', () => { try { scan(this.responseText); add(n, this.responseText); } catch (_) {} }); return send.apply(this, arguments); };
  const f = globalThis.fetch;
  globalThis.fetch = function (u, o) { const n = name(o && o.body); return f.apply(this, arguments).then((r) => { try { r.clone().text().then((t) => { scan(t); add(n, t); }, () => {}); } catch (_) {} return r; }); };
  document.addEventListener('DOMContentLoaded', () => scan(document.documentElement.innerHTML));
})();
