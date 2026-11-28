(() => {
  const L = (globalThis.__dr = []);
  const open = XMLHttpRequest.prototype.open, send = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.send = function (b) {
    const s = typeof b === 'string' ? b : (b instanceof URLSearchParams ? b.toString() : (b instanceof FormData ? [...b.entries()].map(([k, v]) => k + '=' + v).join('&') : ''));
    const m = /fb_api_req_friendly_name=([A-Za-z0-9_]+)/.exec(s);
    if (m && /SinglePost|UFI|Comment/.test(m[1])) {
      const rec = { q: m[1], t0: Date.now(), ev: [] };
      L.push(rec);
      for (const e of ['readystatechange', 'progress', 'load', 'error', 'abort', 'timeout', 'loadend']) this.addEventListener(e, () => { let len = -1; try { len = (this.responseText || '').length; } catch (_) {} rec.ev.push([e, this.readyState, this.status, len, Date.now() - rec.t0]); if (e === 'loadend') { try { rec.head = this.responseText.slice(0, 300); rec.ct = this.getResponseHeader('content-type'); rec.n = (this.responseText.match(/\r?\n/g) || []).length; } catch (_) {} } });
    }
    return send.apply(this, arguments);
  };
})();
