(() => {
  const L = (globalThis.__io = []);
  const IO = IntersectionObserver;
  globalThis.IntersectionObserver = function (cb, opt) {
    const o = new IO((entries, ob) => { for (const e of entries) L.push(['E', e.isIntersecting, Math.round(e.boundingClientRect.y), Math.round(e.boundingClientRect.height), e.target.tagName, (e.target.getAttribute && (e.target.getAttribute('data-pagelet') || e.target.getAttribute('role'))) || '']); return cb(entries, ob); }, opt);
    L.push(['new', opt && opt.rootMargin, opt && String(opt.threshold), opt && opt.root ? 'root' : 'vp']);
    return o;
  };
  globalThis.IntersectionObserver.prototype = IO.prototype;
})();
