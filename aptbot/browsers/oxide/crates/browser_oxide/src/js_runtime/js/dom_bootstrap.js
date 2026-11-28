((globalThis) => {
    // Engine diagnostics must not reach the page's console (pages hook it).
    const _boDbg = () => {};
    // The engine's own loads (images, inserted scripts) use the fetch that
    // existed at bootstrap: a page that wraps window.fetch must not see them
    // (a browser never routes <img>/<script> loads through window.fetch).
    const _nativeFetchRef = globalThis.fetch;
    const _engineFetch = (url, init) => {
        let u = url;
        try { u = new URL(String(url), (globalThis.location && globalThis.location.href) || undefined).href; } catch (_e) {}
        return _nativeFetchRef.call(globalThis, u, init);
    };
    const core = Deno.core;
    const ops = core.ops;
    const _nodeIds = new WeakMap();
    const _nodeCache = new Map();
    // Drop a cache slot once its wrapper is collected (the slot is
    // re-created on demand). Without this every node ever wrapped kept a
    // Map entry + WeakRef forever (~100 B/node; FB feeds churn millions).
    const _nodeCacheReg = new FinalizationRegistry((id) => {
        const r = _nodeCache.get(id);
        if (r && !r.deref()) _nodeCache.delete(id);
        // A collected wrapper may have been the last thing keeping a removed subtree.
        if (_reapPending.size) _reapSchedule(8);
    });
    const _scrollState = new Map(); // nodeId -> {top, left}

    // ── Reaper: free removed DOM subtrees on the Rust side ──────────────
    // removeChild / innerHTML / textContent only detach nodes in the Rust arena; the
    // subtree (elements, attributes, text) used to stay allocated for the life of the
    // document. A removed subtree can come back only through a JS wrapper of one of its
    // nodes, so once none of its nodes has a live wrapper it is unreachable and is freed.
    // Runs on a timer (never inside page code that may hold raw ids), batched by removal
    // count. Slot ids are never reused, so stale ids elsewhere cannot alias new nodes.
    const _reapPending = new Set();
    const _reapSetTimeout = globalThis.setTimeout;
    let _reapScore = 0, _reapQueued = false;
    let _reapStats = { runs: 0, freed: 0, kept: 0 };
    function _reapSchedule(n) {
        _reapScore += n;
        if (_reapQueued || _reapScore < 128) return;
        _reapQueued = true;
        _reapSetTimeout(_reapRun, 50);
    }
    // Phase 1: move listener maps of detached nodes out of the strong id-keyed table.
    // The sweep wraps nodes to test them, and a wrapper touched in this job counts as
    // alive until the job ends (WeakRef KeepDuringJob), so the check runs in a later task.
    function _reapRun() {
        _reapQueued = true;
        const hook = globalThis[Symbol.for('__browser_oxide_script_hook__')];
        if (hook && typeof hook.sweepListeners === 'function') hook.sweepListeners();
        _reapSetTimeout(_reapCheck, 0);
    }
    function _reapCheck() {
        _reapQueued = false;
        _reapScore = 0;
        _reapStats.runs++;
        for (const id of ops.op_dom_reap_take()) _reapPending.add(id);
        const hook = globalThis[Symbol.for('__browser_oxide_script_hook__')];
        const done = new Set();
        let kept = 0;
        for (const id of _reapPending) {
            const ids = ops.op_dom_reap_info(id);
            if (!ids.length || done.has(ids[0])) { _reapPending.delete(id); continue; }
            let alive = false;
            for (let i = 0; i < ids.length; i++) {
                const r = _nodeCache.get(ids[i]);
                if (r !== undefined && r.deref() !== undefined) { alive = true; break; }
            }
            if (alive) { kept++; continue; }
            _reapStats.freed += ops.op_dom_reap_free(ids);
            done.add(ids[0]);
            for (let i = 0; i < ids.length; i++) { _nodeCache.delete(ids[i]); _scrollState.delete(ids[i]); }
            if (hook && typeof hook.forgetNodes === 'function') hook.forgetNodes(ids);
            _reapPending.delete(id);
        }
        _reapStats.kept = kept;
    }
    // Viewport (window) scroll. Element rects are viewport-relative, so they
    // shift with it; documentElement.scrollTop mirrors it; changes are
    // clamped to the document size and announced with a trusted `scroll`
    // event at the document (bubbles to window), coalesced like Chrome's
    // once-per-frame scroll events.
    let _vpX = 0, _vpY = 0, _vpEvtQueued = false;
    const _vpMax = () => {
        let w = 0, h = 0;
        try {
            const de = globalThis.document && document.documentElement;
            if (de) { const id = _getNodeId(de); w = ops.op_layout_get_offset_width(id); h = ops.op_layout_get_offset_height(id); }
        } catch (_) {}
        return [Math.max(0, w - (globalThis.innerWidth || 0)), Math.max(0, h - (globalThis.innerHeight || 0))];
    };
    const _vpSet = (x, y) => {
        const [mx, my] = _vpMax();
        x = Number(x); y = Number(y);
        x = Math.min(Math.max(0, Number.isFinite(x) ? x : _vpX), mx);
        y = Math.min(Math.max(0, Number.isFinite(y) ? y : _vpY), my);
        if (x === _vpX && y === _vpY) return;
        _vpX = x; _vpY = y;
        if (_vpEvtQueued) return;
        _vpEvtQueued = true;
        setTimeout(() => {
            _vpEvtQueued = false;
            try { const ev = _trust(new Event('scroll', { bubbles: true })); document.dispatchEvent(ev); } catch (_) {}
        }, 16);
    };

    function _getNodeId(node) {
        if (node === null || node === undefined) return -1;
        if (node === globalThis || node === globalThis.window) return -999;
        // WeakMap.get on a non-object returns undefined per spec — no throw.
        const id = _nodeIds.get(node);
        if (id === undefined) {
            // node is not a registered DOM node. Returning 0 (the DOCUMENT
            // id) here used to be a "resilience" default, but it caused
            // every appendChild(weirdValue) to surface as
            // appendChild(parent, document) → cycle assertion fires.
            // -1 makes the Rust op layer's `dom.get(NodeId(u32::MAX))` miss
            // and silently no-op, which is the right behaviour for a JS
            // mutation against a non-node argument.
            return -1;
        }
        return id;
    }

    function _wrapNode(nodeId) {
        if (nodeId === null || nodeId === undefined || nodeId === -1) return null;
        const cached = _nodeCache.get(nodeId);
        if (cached) {
            const obj = cached.deref();
            if (obj) return obj;
        }
        const nodeType = ops.op_dom_get_node_type(nodeId);
        return _wrapNodeWithType(nodeId, nodeType);
    }

    function _wrapNodeWithType(nodeId, nodeType) {
        if (nodeId === null || nodeId === undefined || nodeId === -1) return null;
        const cached = _nodeCache.get(nodeId);
        if (cached) {
            const obj = cached.deref();
            if (obj) return obj;
        }
        let node;
        switch (nodeType) {
            case 1:
                node = new Element(nodeId);
                _retargetElementProto(node);
                break;
            case 3: node = new Text(nodeId); break;
            case 8: node = new Comment(nodeId); break;
            case 9: node = _document; break;
            case 11: node = new DocumentFragment(nodeId); break;
            default: node = new Node(nodeId); break;
        }
        _nodeCache.set(nodeId, new WeakRef(node));
        if (nodeType !== 9) _nodeCacheReg.register(node, nodeId);
        return node;
    }

    // Tracks base URLs (query-stripped) of scripts currently being sync-fetched.
    // Guards against re-entrant fetch loops: e.g. Yandex Metrika's bootstrap IIFE
    // inserts a new <script src="tag.js?timestamp"> while tag.js is still being
    // evaluated. Without this guard the fetch recurses infinitely.
    const _syncFetchInFlight = new Set();
    const _scriptForceAsync = new WeakSet();
    // Script-inserted scripts with async=false run in insertion order.
    let _orderedScriptChain = Promise.resolve();

    // Tracks nesting depth of sync eval chains. Each _onNodeInserted call that
    // fetches+evals a script increments this. Scripts beyond MAX nesting are
    // degraded to async — prevents C++ stack overflow when deeply-nested
    // third-party SDKs load more scripts during their own synchronous eval
    // (each pending eval adds a large V8 interpreter frame to the C stack;
    // 6-9 levels can overflow an 8 MB Rust thread stack).
    let _syncEvalDepth = 0;
    const _MAX_SYNC_EVAL_DEPTH = 4;

    // Guards against unbounded `document.write` chains. Two failure modes
    // we observed on bot.sannysoft.com:
    //   (a) A script does `document.write('<script>...</script>')` and the
    //       written script does the same — direct cycle. Caught by depth.
    //   (b) `document.write` dispatches every new node through
    //       `_onNodeInserted`, which evals scripts. If a written script
    //       calls `document.write` again during its eval (synchronously),
    //       we re-enter `_onNodeInserted` from inside its own call.
    let _onNodeInsertedDepth = 0;
    const _MAX_NODE_INSERT_DEPTH = 64;

    // ---- image loading ----
    // The engine has no image decoder, so `load`/`error` never fired on <img>
    // and scripts waiting on `new Image().onload` (fingerprint collectors,
    // lazy loaders) hung forever. Fetch the resource and fire the events.
    const _imgState = new WeakMap(); // <img> -> 'loading' | 'complete' | 'error'
    // document.implementation is one object for the page's lifetime (like the real DOM).
    let _domImplementation = null;

    const _imgNatural = new WeakMap(); // <img> -> { w, h } read from the file header

    // The engine has no image decoder, so the intrinsic size is read from the
    // file header instead (PNG, GIF, JPEG, WebP, BMP).
    function _imageSize(b) {
        if (!b || b.byteLength < 12) return null;
        const v = new DataView(b.buffer, b.byteOffset, b.byteLength);
        if (b[0] === 0x89 && b[1] === 0x50 && b[2] === 0x4e && b[3] === 0x47 && b.byteLength >= 24) {
            return { w: v.getUint32(16), h: v.getUint32(20) };
        }
        if (b[0] === 0x47 && b[1] === 0x49 && b[2] === 0x46) {
            return { w: v.getUint16(6, true), h: v.getUint16(8, true) };
        }
        if (b[0] === 0x42 && b[1] === 0x4d && b.byteLength >= 26) {
            return { w: Math.abs(v.getInt32(18, true)), h: Math.abs(v.getInt32(22, true)) };
        }
        if (b[8] === 0x57 && b[9] === 0x45 && b[10] === 0x42 && b[11] === 0x50 && b.byteLength >= 30) {
            const fourcc = String.fromCharCode(b[12], b[13], b[14], b[15]);
            if (fourcc === 'VP8 ') return { w: v.getUint16(26, true) & 0x3fff, h: v.getUint16(28, true) & 0x3fff };
            if (fourcc === 'VP8L') {
                const bits = v.getUint32(21, true);
                return { w: (bits & 0x3fff) + 1, h: ((bits >> 14) & 0x3fff) + 1 };
            }
            if (fourcc === 'VP8X') {
                return { w: 1 + (b[24] | (b[25] << 8) | (b[26] << 16)), h: 1 + (b[27] | (b[28] << 8) | (b[29] << 16)) };
            }
            return null;
        }
        if (b[0] === 0xff && b[1] === 0xd8) {
            let i = 2;
            while (i + 9 < b.byteLength) {
                if (b[i] !== 0xff) { i++; continue; }
                const marker = b[i + 1];
                if (marker === 0xff || marker === 0x01 || marker === 0xd8 || (marker >= 0xd0 && marker <= 0xd7)) { i += 2; continue; }
                if (marker === 0xd9) return null;
                const len = v.getUint16(i + 2);
                if (marker >= 0xc0 && marker <= 0xcf && marker !== 0xc4 && marker !== 0xc8 && marker !== 0xcc) {
                    return { h: v.getUint16(i + 5), w: v.getUint16(i + 7) };
                }
                if (len < 2) return null;
                i += 2 + len;
            }
        }
        return null;
    }

    function _setImageBytes(img, buffer) {
        try {
            const size = _imageSize(new Uint8Array(buffer));
            if (size) _imgNatural.set(img, size);
        } catch (_) { /* ignore */ }
    }

    function _loadImage(img) {
        const raw = img.getAttribute && img.getAttribute('src');
        if (!raw) { _imgState.set(img, 'complete'); return; }
        _imgState.set(img, 'loading');
        _imgNatural.delete(img);
        let url = raw;
        try { url = new URL(raw, globalThis.location ? globalThis.location.href : 'about:blank').href; } catch (_) {}
        if (/^\s*data:/i.test(raw)) {
            try {
                const comma = raw.indexOf(',');
                const payload = comma >= 0 ? raw.slice(comma + 1) : '';
                const bin = /;base64/i.test(raw.slice(0, comma)) ? atob(payload) : decodeURIComponent(payload);
                const bytes = new Uint8Array(bin.length);
                for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
                _setImageBytes(img, bytes.buffer);
            } catch (_) { /* ignore */ }
            _imgState.set(img, 'complete');
            setTimeout(() => { try { img.dispatchEvent(new Event('load')); } catch (_) {} }, 0);
            return;
        }
        // Chrome's <img> request: mode no-cors, dest image, accept image/*,
        // priority `i`.
        _engineFetch(url, { headers: { 'x-browser-oxide-subresource': 'image' } })
            .then(async (resp) => {
                const type = resp && resp.headers ? String(resp.headers.get('content-type') || '') : '';
                // A page served instead of an image is a broken image in a real browser.
                const ok = !!resp && (resp.ok || resp.status === 0) && (!type || /^image\//i.test(type));
                if (ok) { try { _setImageBytes(img, await resp.arrayBuffer()); } catch (_) { /* ignore */ } }
                _imgState.set(img, ok ? 'complete' : 'error');
                try { img.dispatchEvent(new Event(ok ? 'load' : 'error')); } catch (_) {}
            }, () => {
                _imgState.set(img, 'error');
                try { img.dispatchEvent(new Event('error')); } catch (_) {}
            });
    }

    // Start loading images the parser put in the document (they do not pass
    // through the insertion hook that dynamic nodes use).
    function _scanImages(root) {
        let imgs;
        try { imgs = root && root.querySelectorAll ? root.querySelectorAll('img') : []; } catch (_) { return; }
        for (const img of Array.from(imgs)) {
            if (img.getAttribute && img.getAttribute('src') && !_imgState.has(img)) _loadImage(img);
        }
    }

    function _onNodeInserted(child, sync = false) {
        if (!child) return;
        if (_onNodeInsertedDepth >= _MAX_NODE_INSERT_DEPTH) {
            // Bail — log once and skip. This breaks document.write recursion
            // chains that would otherwise blow the C-stack via deep nested
            // eval -> op_dom_document_write -> _onNodeInserted.
            _boDbg(`[DOM] _onNodeInserted depth limit (${_MAX_NODE_INSERT_DEPTH}) — skipping`);
            return;
        }
        _onNodeInsertedDepth++;
        try {
            return _onNodeInsertedInner(child, sync);
        } finally {
            _onNodeInsertedDepth--;
        }
    }

    class DOMPointReadOnly {
        constructor(x = 0, y = 0, z = 0, w = 1) {
            this.x = x; this.y = y; this.z = z; this.w = w;
        }
        static fromPoint(p = {}) { return new (typeof this === "function" ? this : DOMPointReadOnly)(p.x || 0, p.y || 0, p.z || 0, p.w === undefined ? 1 : p.w); }
        toJSON() { return { x: this.x, y: this.y, z: this.z, w: this.w }; }
    }
    globalThis.DOMPointReadOnly = DOMPointReadOnly;

    class DOMPoint extends DOMPointReadOnly {
        constructor(x = 0, y = 0, z = 0, w = 1) { super(x, y, z, w); }
    }
    globalThis.DOMPoint = DOMPoint;

    class DOMRectReadOnly {
        constructor(x = 0, y = 0, width = 0, height = 0) {
            this.x = x; this.y = y; this.width = width; this.height = height;
        }
        get top() { return this.y; }
        get left() { return this.x; }
        get right() { return this.x + this.width; }
        get bottom() { return this.y + this.height; }
        toJSON() { return { x: this.x, y: this.y, width: this.width, height: this.height, top: this.top, left: this.left, right: this.right, bottom: this.bottom }; }
        // Spec: fromRect(other = {}) — called with no argument by real code (Facebook's useResizeObserver).
        static fromRect(r = {}) { return new (typeof this === "function" ? this : DOMRectReadOnly)(r.x || 0, r.y || 0, r.width || 0, r.height || 0); }
    }
    globalThis.DOMRectReadOnly = DOMRectReadOnly;

    class DOMRect extends DOMRectReadOnly {
        constructor(x = 0, y = 0, width = 0, height = 0) { super(x, y, width, height); }
    }
    globalThis.DOMRect = DOMRect;

    if (typeof _maskFunction === 'function') {
        _maskFunction(DOMPointReadOnly, 'DOMPointReadOnly');
        _maskFunction(DOMPoint, 'DOMPoint');
        _maskFunction(DOMRectReadOnly, 'DOMRectReadOnly');
        _maskFunction(DOMRect, 'DOMRect');
    }

    // Run a dynamically inserted script with document.currentScript set to it
    // (bundlers such as Next.js/Turbopack and webpack read it during evaluation).
    function _runScriptCode(el, code) {
        const prev = _currentScript;
        _setCurrentScript(el);
        try { (0, eval)(code); } finally { _setCurrentScript(prev); }
    }

    // Fire a script element's load/error exactly once, on a task, like Chrome.
    // dispatchEvent already runs the onload/onerror property handler, so it
    // must not be called directly as well (that ran every handler twice).
    function _scriptFire(el, type) {
        setTimeout(() => { try { el.dispatchEvent(new Event(type)); } catch (_) {} }, 0);
    }

    // Script-inserted <link>: a stylesheet is fetched, applied and fires
    // load/error; preload/modulepreload/prefetch fire load (the resource is
    // fetched when it is actually used, so no duplicate request).
    const _linkSeen = new WeakSet();
    function _loadLink(el) {
        if (_linkSeen.has(el)) return;
        const rel = String(el.getAttribute('rel') || '').toLowerCase().split(/\s+/);
        const href = el.getAttribute('href');
        if (!href) return;
        let url = href;
        try { url = new URL(href, globalThis.location ? globalThis.location.href : undefined).href; } catch (_) {}
        if (rel.includes('stylesheet')) {
            _linkSeen.add(el);
            if (url.startsWith('data:')) {
                const css = _decodeDataUrl(url);
                if (css) { try { ops.op_dom_add_stylesheet(css); } catch (_) {} }
                _scriptFire(el, css != null ? 'load' : 'error');
                return;
            }
            // Chrome's <link rel=stylesheet> request: accept text/css, dest
            // style, mode no-cors (cors with crossorigin), priority u=0.
            const h = { 'x-browser-oxide-subresource': 'style' };
            (async () => {
                try {
                    const r = await _engineFetch(url, { headers: h });
                    if (!r.ok) { _scriptFire(el, 'error'); return; }
                    const css = await r.text();
                    try { ops.op_dom_add_stylesheet(css); } catch (_) {}
                    _scriptFire(el, 'load');
                } catch (_) { _scriptFire(el, 'error'); }
            })();
        } else if (rel.includes('preload') || rel.includes('modulepreload') || rel.includes('prefetch')) {
            _linkSeen.add(el);
            _scriptFire(el, 'load');
        }
    }

    function _decodeDataUrl(u) {
        const comma = u.indexOf(',');
        if (comma < 0) return null;
        const meta = u.slice(5, comma), payload = u.slice(comma + 1);
        try {
            if (/;\s*base64/i.test(meta)) {
                const bin = atob(decodeURIComponent(payload).replace(/\s+/g, ''));
                const bytes = new Uint8Array(bin.length);
                for (let k = 0; k < bin.length; k++) bytes[k] = bin.charCodeAt(k);
                return (typeof TextDecoder === 'function') ? new TextDecoder().decode(bytes) : bin;
            }
            return decodeURIComponent(payload);
        } catch (_) { return null; }
    }

    function _onNodeInsertedInner(child, sync = false) {
        // 1. Dynamic script loading
        const childTag = (child.tagName || child.nodeName || "").toLowerCase();
        const type = (child.getAttribute?.('type') || '').toLowerCase();
        const isJs = !type || type === 'text/javascript' || type === 'application/javascript' || type === 'module';

        if (childTag === 'img') _loadImage(child);
        if (childTag === 'link') _loadLink(child);

        if (childTag === 'script' && !isJs) {
            return; // Skip non-JS scripts like application/ld+json
        }

        const childSrc = (childTag === 'script') ? (child.src || child.getAttribute?.('src')) : null;

        if (childTag === 'script' && !childSrc) {
            const code = child.textContent || child.innerText || '';
            if (code && code.trim()) {
                _boDbg(`[DOM] executing inline script (${code.length} bytes)`);
                try { _runScriptCode(child, code); } catch (e) {
                    _boDbg(`[DOM] inline eval error: ${e.message}`);
                }
            }
        }

        if (childTag === 'script' && childSrc) {
            const src = childSrc;
            const scriptEl = child;
            let fullUrl = src;
            if (!src.startsWith('http') && !src.startsWith('data:')) {
                try {
                    const base = globalThis.location ? globalThis.location.href : 'about:blank';
                    fullUrl = new URL(src, base).href;
                } catch(e) {}
            }

            // Third-party trackers known to trigger uncontrolled C-stack recursion
            // inside their own VM (not in our shims). Skip them — they add no
            // signal to fingerprint scoring, and crashing the engine on them
            // costs us all subsequent tests on the page.
            // Known offenders identified via stack-overflow crashes on real
            // sites: bot.sannysoft.com loads Yandex Metrika; leboncoin.fr
            // loads it too.
            const _RECURSIVE_TRACKERS = [
                "mc.yandex.ru/metrika/tag.js",
                "mc.yandex.ru/metrika/watch.js",
                "mc.yandex.ru/webvisor/",
            ];
            for (const pat of _RECURSIVE_TRACKERS) {
                if (fullUrl.includes(pat)) {
                    _scriptFire(scriptEl, 'load');
                    return;
                }
            }

            // CSP: a script the page inserts is checked against script-src-elem
            // (non-parser-inserted, so 'strict-dynamic' trust applies). Blocked:
            // securitypolicyviolation on the document + error on the element.
            if (typeof ops.op_csp_script_check === 'function') {
                let violated = '';
                try { violated = ops.op_csp_script_check(String(fullUrl), String(scriptEl.nonce || scriptEl.getAttribute('nonce') || '')); } catch (_e) {}
                if (violated) {
                    console.error(`Refused to load the script '${fullUrl}' because it violates the following Content Security Policy directive: "${violated}".`);
                    setTimeout(() => {
                        try {
                            const C = globalThis.SecurityPolicyViolationEvent || Event;
                            document.dispatchEvent(new C('securitypolicyviolation', { bubbles: true, composed: true, blockedURI: String(fullUrl),
                                violatedDirective: violated, effectiveDirective: violated, disposition: 'enforce', documentURI: String(location.href) }));
                        } catch (_e) {}
                        try { scriptEl.dispatchEvent(new Event('error')); } catch (_e) {}
                    }, 0);
                    return;
                }
            }

            // data: URL scripts (Facebook ships whole bundles this way): decode
            // locally, no fetch. Load/error fire on a task, like a real fetch.
            if (fullUrl.startsWith('data:')) {
                const code = _decodeDataUrl(fullUrl);
                let ok = code != null;
                if (ok) { try { _runScriptCode(scriptEl, code); } catch (e) { console.error(e); } }
                _scriptFire(scriptEl, ok ? 'load' : 'error');
                return;
            }

            if (sync) {
                // Strip query params for in-flight dedup: scripts that reload themselves
                // with a cache-busting timestamp (e.g. Yandex Metrika tag.js?<timestamp>)
                // share the same base URL and would recurse infinitely without this guard.
                const baseUrl = fullUrl.split('?')[0];
                if (_syncFetchInFlight.has(baseUrl)) {
                    // Re-entrant same-URL fetch — fire load event and bail to break the cycle.
                    _scriptFire(scriptEl, 'load');
                    return;
                }
                // Depth guard: if sync evals are already nested beyond the safe limit,
                // degrade to async. This prevents C++ stack overflow from chains like
                // tag.js → pixel.js → tracker.js → … where each level blocks the V8
                // thread inside op_net_fetch_sync while its eval frame stays on stack.
                if (_syncEvalDepth >= _MAX_SYNC_EVAL_DEPTH) {
                    _boDbg(`[DOM] sync eval depth limit (${_MAX_SYNC_EVAL_DEPTH}) — falling back to async: ${fullUrl}`);
                    (async () => {
                        try {
                            const resp = await _engineFetch(fullUrl, { headers: { 'x-browser-oxide-subresource': 'script-async' } });
                            if (resp.ok) {
                                const code = await resp.text();
                                try { _runScriptCode(scriptEl, code); } catch(_) {}
                                _scriptFire(scriptEl, 'load');
                            }
                        } catch(_) {
                            _scriptFire(scriptEl, 'error');
                        }
                    })();
                    return;
                }
                _syncFetchInFlight.add(baseUrl);
                _syncEvalDepth++;
                _boDbg(`[DOM] sync fetching script (depth ${_syncEvalDepth}): ${fullUrl}`);
                try {
                    // "script-write": a document.write'd script — Chrome sends
                    // `priority: u=1` for it (u=2 for a parser-blocking one).
                    const code = ops.op_net_fetch_sync(fullUrl, globalThis.location?.href || "", 'script-write');
                    if (code) {
                        _boDbg(`[DOM] sync executing script (${code.length} bytes): ${fullUrl}`);
                        try {
                            _runScriptCode(scriptEl, code);
                            _boDbg(`[DOM] sync execution SUCCESS: ${fullUrl}`);
                        } catch(e) {
                            _boDbg(`[DOM] sync eval ERROR for ${fullUrl}: ${e.message}\n${e.stack}`);
                        }
                        // A script that throws still "loaded" (Chrome fires load; the
                        // exception goes to window.onerror).
                        _scriptFire(scriptEl, 'load');
                    } else {
                        _boDbg(`[DOM] sync fetch FAILED (empty) for ${fullUrl}`);
                        _scriptFire(scriptEl, 'error');
                    }
                } catch(e) {
                    _boDbg(`[DOM] sync fetch OP error for ${fullUrl}: ${e.message}`);
                    _scriptFire(scriptEl, 'error');
                } finally {
                    _syncFetchInFlight.delete(baseUrl);
                    _syncEvalDepth--;
                }
            } else {
                _boDbg(`[DOM] async fetching script: ${fullUrl}`);
                // Fetch now; execute when it arrives, or — for async=false
                // script-inserted scripts — in insertion order (HTML spec).
                const _ordered = !scriptEl.async;
                // A script the page inserts dynamically: Chrome sends no
                // `priority` header for it (async / dynamic scripts), mode
                // no-cors unless crossorigin, dest script. The net layer
                // builds the whole header set from this marker.
                const _sh = { 'x-browser-oxide-subresource': 'script-async' };
                const _fetchP = (async () => {
                    try {
                        const resp = await _engineFetch(fullUrl, { headers: _sh });
                        if (!resp.ok) return { ok: false, why: `status ${resp.status}` };
                        return { ok: true, code: await resp.text() };
                    } catch (e) { return { ok: false, why: String(e && e.message || e) }; }
                })();
                const _run = async () => {
                    const r = await _fetchP;
                    if (!r.ok) {
                        _boDbg(`[DOM] async fetch FAILED (${r.why}) for ${fullUrl}`);
                        _scriptFire(scriptEl, 'error');
                        return;
                    }
                    _boDbg(`[DOM] async executing script (${r.code.length} bytes): ${fullUrl}`);
                    try { _runScriptCode(scriptEl, r.code); }
                    catch (e) { _boDbg(`[DOM] async eval ERROR for ${fullUrl}: ${e.message}`); try { globalThis.reportError && globalThis.reportError(e); } catch (_) {} }
                    _scriptFire(scriptEl, 'load');
                };
                if (_ordered) _orderedScriptChain = _orderedScriptChain.then(_run, _run);
                else _run();
            }
        }

        // 2. Recursive check for children (handles <div><script>...</script></div>)
        if (child.childNodes && child.childNodes.length > 0) {
            for (let i = 0; i < child.childNodes.length; i++) {
                _onNodeInserted(child.childNodes[i], sync);
            }
        }
    }

    globalThis.__onNodeInserted = _onNodeInserted;

    class NodeList {
        constructor(data, isTyped = false) {
            if (isTyped) {
                this._ids = [];
                for (let i = 0; i < data.length; i += 2) {
                    const id = data[i];
                    const type = data[i+1];
                    this._ids.push(id);
                    this[i/2] = _wrapNodeWithType(id, type);
                }
            } else {
                this._ids = data;
                for (let i = 0; i < data.length; i++) {
                    this[i] = _wrapNode(data[i]);
                }
            }
        }
        get length() { return this._ids.length; }
        item(index) { return index < this._ids.length ? _wrapNode(this._ids[index]) : null; }
        forEach(cb, thisArg) {
            for (let i = 0; i < this._ids.length; i++) {
                cb.call(thisArg, this[i], i, this);
            }
        }
        entries() {
            const self = this;
            let i = 0;
            return { next() { return i < self.length ? { value: [i, self[i++]], done: false } : { done: true }; }, [Symbol.iterator]() { return this; } };
        }
        keys() {
            const self = this;
            let i = 0;
            return { next() { return i < self.length ? { value: i++, done: false } : { done: true }; }, [Symbol.iterator]() { return this; } };
        }
        values() { return this[Symbol.iterator](); }
        [Symbol.iterator]() {
            let i = 0;
            const self = this;
            return {
                next() {
                    if (i < self.length) return { value: self[i++], done: false };
                    return { value: undefined, done: true };
                },
                [Symbol.iterator]() { return this; }
            };
        }
    }

    class DOMTokenList {
        #nodeId;
        constructor(nodeId) { this.#nodeId = nodeId; }
        add(cls) { ops.op_dom_class_list_add(this.#nodeId, cls); }
        remove(cls) { ops.op_dom_class_list_remove(this.#nodeId, cls); }
        toggle(cls) {
            if (this.contains(cls)) { this.remove(cls); return false; }
            this.add(cls); return true;
        }
        contains(cls) {
            const attr = ops.op_dom_get_attribute(this.#nodeId, "class");
            return attr ? attr.split(/\s+/).includes(cls) : false;
        }
        get value() { return ops.op_dom_get_attribute(this.#nodeId, "class") || ""; }
        get length() { return this.value.split(/\s+/).filter(Boolean).length; }
        toString() { return this.value; }
        item(index) {
            const tokens = this.value.split(/\s+/).filter(Boolean);
            return tokens[index] != null ? tokens[index] : null;
        }
        // Real Chrome DOMTokenList is iterable; iterating yields each token
        // string. Some scripts spread element.classList — without
        // Symbol.iterator we throw "non-iterable" while Chrome returns the
        // token array.
        [Symbol.iterator]() {
            const tokens = this.value.split(/\s+/).filter(Boolean);
            let i = 0;
            return {
                next() {
                    if (i < tokens.length) return { value: tokens[i++], done: false };
                    return { value: undefined, done: true };
                },
                [Symbol.iterator]() { return this; }
            };
        }
        entries() {
            const tokens = this.value.split(/\s+/).filter(Boolean);
            let i = 0;
            return {
                next() {
                    if (i < tokens.length) { const idx = i; return { value: [idx, tokens[i++]], done: false }; }
                    return { value: undefined, done: true };
                },
                [Symbol.iterator]() { return this; }
            };
        }
        keys() {
            const n = this.length;
            let i = 0;
            return {
                next() {
                    if (i < n) return { value: i++, done: false };
                    return { value: undefined, done: true };
                },
                [Symbol.iterator]() { return this; }
            };
        }
        values() { return this[Symbol.iterator](); }
        forEach(cb, thisArg) {
            const tokens = this.value.split(/\s+/).filter(Boolean);
            for (let i = 0; i < tokens.length; i++) {
                cb.call(thisArg, tokens[i], i, this);
            }
        }
    }

    // EventTarget is the base of the DOM prototype chain in real Chrome:
    //   EventTarget ← Node ← Element ← HTMLElement ← HTMLDivElement etc.
    // Some scripts check `document instanceof EventTarget === true`
    // and walk Object.getPrototypeOf chains expecting this layout.
    const EventTarget = globalThis.EventTarget || class EventTarget {
        constructor() {}
        addEventListener(type, listener, options) {}
        removeEventListener(type, listener, options) {}
        dispatchEvent(event) { return true; }
    };
    globalThis.EventTarget = EventTarget;

    class Node extends EventTarget {
        constructor(nodeId) {
            super();
            _nodeIds.set(this, nodeId);
        }
        // nodeType constants
        static ELEMENT_NODE = 1;
        static TEXT_NODE = 3;
        static COMMENT_NODE = 8;
        static DOCUMENT_NODE = 9;
        static DOCUMENT_FRAGMENT_NODE = 11;
        static DOCUMENT_TYPE_NODE = 10;
        static PROCESSING_INSTRUCTION_NODE = 7;
        static ATTRIBUTE_NODE = 2;
        static CDATA_SECTION_NODE = 4;

        get nodeType() { return ops.op_dom_get_node_type(_getNodeId(this)); }
        get nodeName() {
            const type = this.nodeType;
            if (type === 1) return ops.op_dom_get_tag_name(_getNodeId(this)).toUpperCase();
            if (type === 3) return "#text";
            if (type === 8) return "#comment";
            if (type === 9) return "#document";
            if (type === 11) return "#document-fragment";
            return "";
        }
        get nodeValue() {
            const type = this.nodeType;
            if (type === 3 || type === 8) return ops.op_dom_get_text_content(_getNodeId(this));
            return null;
        }
        set nodeValue(val) {
            const type = this.nodeType;
            if (type === 3 || type === 8) ops.op_dom_set_text_content(_getNodeId(this), String(val));
        }
        get ownerDocument() {
            return this.nodeType === 9 ? null : _document;
        }
        get isConnected() {
            let n = this;
            while (n) {
                if (n.nodeType === 9) return true;
                n = n.parentNode;
            }
            return false;
        }
        get baseURI() {
            return globalThis.location?.href || "about:blank";
        }
        get parentNode() { return _wrapNode(ops.op_dom_get_parent(_getNodeId(this))); }
        get parentElement() {
            const p = this.parentNode;
            return p && p.nodeType === 1 ? p : null;
        }
        get childNodes() { return new NodeList(ops.op_dom_get_children_with_types(_getNodeId(this)), true); }
        get firstChild() { return _wrapNode(ops.op_dom_get_first_child(_getNodeId(this))); }
        get lastChild() { return _wrapNode(ops.op_dom_get_last_child(_getNodeId(this))); }
        get nextSibling() { return _wrapNode(ops.op_dom_get_next_sibling(_getNodeId(this))); }
        get previousSibling() { return _wrapNode(ops.op_dom_get_prev_sibling(_getNodeId(this))); }
        get textContent() { return ops.op_dom_get_text_content(_getNodeId(this)); }
        set textContent(val) { ops.op_dom_set_text_content(_getNodeId(this), String(val)); _reapSchedule(1); }
        appendChild(child) {
            ops.op_dom_append_child(_getNodeId(this), _getNodeId(child));
            _onNodeInserted(child);
            return child;
        }
        removeChild(child) {
            _ceDisconnected(child);
            ops.op_dom_remove_child(_getNodeId(this), _getNodeId(child));
            _reapSchedule(1);
            return child;
        }
        replaceChild(newChild, oldChild) {
            const parent = _getNodeId(this);
            const oldId = _getNodeId(oldChild);
            const newId = _getNodeId(newChild);
            _ceDisconnected(oldChild);
            ops.op_dom_insert_before(parent, newId, oldId);
            ops.op_dom_remove_child(parent, oldId);
            _reapSchedule(1);
            _onNodeInserted(newChild);
            return oldChild;
        }
        insertBefore(newChild, refChild) {
            if (refChild === null || refChild === undefined) return this.appendChild(newChild);
            ops.op_dom_insert_before(_getNodeId(this), _getNodeId(newChild), _getNodeId(refChild));
            _onNodeInserted(newChild);
            return newChild;
        }
        cloneNode(deep = false) {
            const newId = ops.op_dom_clone_node(_getNodeId(this), !!deep);
            return _wrapNode(newId);
        }
        contains(other) {
            if (!other) return false;
            if (other === this) return true;
            let p = other.parentNode;
            while (p) {
                if (p === this) return true;
                p = p.parentNode;
            }
            return false;
        }
        hasChildNodes() { return ops.op_dom_get_children(_getNodeId(this)).length > 0; }
        getRootNode() {
            let n = this;
            while (n.parentNode) n = n.parentNode;
            return n;
        }
        normalize() {
            // Merge adjacent text nodes
            const children = ops.op_dom_get_children(_getNodeId(this));
            let prevTextId = null;
            for (const cid of children) {
                if (ops.op_dom_get_node_type(cid) === 3) {
                    if (prevTextId !== null) {
                        const prevText = ops.op_dom_get_text_content(prevTextId);
                        const curText = ops.op_dom_get_text_content(cid);
                        ops.op_dom_set_text_content(prevTextId, prevText + curText);
                        ops.op_dom_remove_child(_getNodeId(this), cid);
                    } else {
                        prevTextId = cid;
                    }
                } else {
                    prevTextId = null;
                }
            }
        }
        isEqualNode(other) {
            if (!other) return false;
            if (this === other) return true;
            if (this.nodeType !== other.nodeType) return false;
            if (this.nodeType === 1) return this.outerHTML === other.outerHTML;
            return this.textContent === other.textContent;
        }
        isSameNode(other) { return this === other; }
        // DOM spec tree order. (Was: anything not an ancestor/descendant
        // returned FOLLOWING, so a.cDP(b) and b.cDP(a) both said 4. FB's
        // BaseDocumentScrollView uses it to pick the active scroll layer;
        // the feed then reset to scrollY 0 every time a post dialog closed.)
        compareDocumentPosition(other) {
            if (other === null || other === undefined || typeof other.nodeType !== 'number') throw new TypeError("Failed to execute 'compareDocumentPosition' on 'Node': parameter 1 is not of type 'Node'.");
            if (this === other) return 0;
            const chain = (n) => { const c = []; for (let x = n; x; x = x.parentNode || (x.nodeType === 2 ? x.ownerElement : null)) c.push(x); return c.reverse(); };
            const ca = chain(this), cb = chain(other);
            if (ca[0] !== cb[0]) {
                // DISCONNECTED | IMPLEMENTATION_SPECIFIC + a stable direction.
                return 1 | 32 | (_getNodeId(ca[0]) < _getNodeId(cb[0]) ? 4 : 2);
            }
            let i = 0;
            while (i < ca.length && i < cb.length && ca[i] === cb[i]) i++;
            if (i === ca.length) return 16 | 4;   // other is a descendant: CONTAINED_BY | FOLLOWING
            if (i === cb.length) return 8 | 2;    // other is an ancestor: CONTAINS | PRECEDING
            const x = ca[i], y = cb[i];
            if (x.nodeType === 2 && y.nodeType === 2) return 32 | 4;
            if (x.nodeType === 2) return 4;       // attributes precede their element's children
            if (y.nodeType === 2) return 2;
            for (let n = x.nextSibling; n; n = n.nextSibling) if (n === y) return 4;
            return 2;
        }
    }

    // --- Internal Bridge ---
    if (!globalThis.__browser_oxide) {
        Object.defineProperty(globalThis, '__browser_oxide', { value: {}, enumerable: false, configurable: true });
    }
    globalThis.__browser_oxide._getNodeId = _getNodeId;
    globalThis.__browser_oxide._wrapNode = _wrapNode;
    globalThis.__browser_oxide._setCurrentScript = _setCurrentScript;
    // The Rust script loops (page.rs) set document.currentScript around every
    // parser script. cleanup_bootstrap.js deletes the string-keyed
    // __browser_oxide before page scripts run, so those calls failed silently
    // and document.write() never found its insertion point. Keep the hook in a
    // Symbol-keyed slot (same pattern as stealth_bootstrap.js): the internals
    // purge is string-keyed and __resetPageGlobals baselines symbols.
    Object.defineProperty(globalThis, Symbol.for('__browser_oxide_script_hook__'), {
        value: { set: _setCurrentScript, wrap: _wrapNode, computedStyle: (el) => _createComputedStyle(el), focusable: (el) => _isFocusable(el), viewportScroll: (x, y) => _vpSet(x, y), viewportPos: () => [_vpX, _vpY], reap: () => (_reapCheck(), _reapStats), detachedRoot: (id) => ops.op_dom_detached_root(id), subtreeIds: (id) => ops.op_dom_reap_info(id), liveWrapper: (id) => { const r = _nodeCache.get(id); return r ? (r.deref() || null) : null; }, reapStats: () => ({ ..._reapStats, pending: _reapPending.size }), _setTrust(fn) { _trust = fn; delete this._setTrust; } },
        enumerable: false,
        configurable: true,
        writable: false,
    });

    // ── CSSStyleDeclaration (Chrome shape) ───────────────────────────────
    // Chrome exposes every CSS property as an OWN key of the declaration
    // (741 camelCase names on element.style; computed style adds the 480
    // longhand indices). Methods live on CSSStyleDeclaration.prototype.
    const _cssData = globalThis.__bo_css_data || { names: [], keys: [], div: {}, span: {}, body: {}, html: {} };
    try { delete globalThis.__bo_css_data; } catch (_) {}
    const _cssCamel = _cssData.keys;
    const _toKebab = (p) => {
        if (p === "cssFloat") return "float";
        let k = p.replace(/[A-Z]/g, m => "-" + m.toLowerCase());
        if (/^webkit-/.test(k)) k = "-" + k;
        return k;
    };
    const _camelSet = new Set(_cssCamel);
    const _kebabSet = new Set(_cssCamel.map(_toKebab));
    for (const n of _cssData.names) _kebabSet.add(n);
    const _propKey = (prop) => {
        if (typeof prop !== "string") return null;
        if (_camelSet.has(prop)) return _toKebab(prop);
        if (_kebabSet.has(prop)) return prop;
        if (prop.startsWith("--")) return prop;
        return null;
    };
    const _isIdx = (p) => typeof p === "string" && /^(0|[1-9]\d*)$/.test(p);
    const _cssBackend = new WeakMap();
    const _be = (self) => {
        const b = _cssBackend.get(self);
        if (!b) throw new TypeError("Illegal invocation");
        return b;
    };
    class CSSStyleDeclaration {
        constructor() { throw new TypeError("Illegal constructor"); }
        get cssText() { return _be(this).cssText(); }
        set cssText(v) { _be(this).setCssText(String(v)); }
        get length() { return _be(this).names().length; }
        get parentRule() { return null; }
        get cssFloat() { return _be(this).get("float"); }
        set cssFloat(v) { _be(this).set("float", String(v), "", "setProperty"); }
        getPropertyPriority(name) { return _be(this).prio(String(name).toLowerCase()); }
        getPropertyValue(name) { const n = String(name); return _be(this).get(n.startsWith("--") ? n : n.toLowerCase()); }
        item(i) { return _be(this).names()[i >>> 0] || ""; }
        removeProperty(name) { return _be(this).remove(String(name).toLowerCase()); }
        setProperty(name, value, priority) {
            const n = String(name);
            _be(this).set(n.startsWith("--") ? n : n.toLowerCase(), value == null ? "" : String(value), priority ? String(priority) : "", "setProperty");
        }
    }
    for (const k of ["cssText", "length", "parentRule", "cssFloat", "getPropertyPriority", "getPropertyValue", "item", "removeProperty", "setProperty"]) {
        const d = Object.getOwnPropertyDescriptor(CSSStyleDeclaration.prototype, k);
        d.enumerable = true;
        Object.defineProperty(CSSStyleDeclaration.prototype, k, d);
    }
    Object.defineProperty(CSSStyleDeclaration.prototype, Symbol.toStringTag, { value: "CSSStyleDeclaration", configurable: true });
    Object.defineProperty(CSSStyleDeclaration.prototype, Symbol.iterator, {
        value: Array.prototype[Symbol.iterator] && function* () { const n = _be(this).names(); for (let i = 0; i < n.length; i++) yield n[i]; },
        writable: true, configurable: true,
    });
    globalThis.CSSStyleDeclaration = CSSStyleDeclaration;

    function _makeDecl(backend) {
        const target = Object.create(CSSStyleDeclaration.prototype);
        const proxy = new Proxy(target, {
            get(t, prop, recv) {
                if (_isIdx(prop)) return backend.names()[+prop];
                const k = _propKey(prop);
                if (k !== null && !(prop in CSSStyleDeclaration.prototype)) return backend.get(k);
                return Reflect.get(t, prop, recv);
            },
            set(t, prop, value, recv) {
                if (_isIdx(prop)) return true;
                const k = _propKey(prop);
                if (k !== null && !(prop in CSSStyleDeclaration.prototype)) {
                    backend.set(k, value == null ? "" : String(value), "", null);
                    return true;
                }
                return Reflect.set(t, prop, value, recv);
            },
            has(t, prop) {
                if (_isIdx(prop)) return +prop < backend.names().length;
                if (typeof prop === "string" && (_camelSet.has(prop) || _kebabSet.has(prop))) return true;
                return Reflect.has(t, prop);
            },
            ownKeys(t) {
                const n = backend.names().length;
                const out = [];
                for (let i = 0; i < n; i++) out.push(String(i));
                for (const c of _cssCamel) out.push(c);
                for (const k of Reflect.ownKeys(t)) out.push(k);
                return out;
            },
            getOwnPropertyDescriptor(t, prop) {
                if (_isIdx(prop)) {
                    const v = backend.names()[+prop];
                    return v === undefined ? undefined : { value: v, writable: false, enumerable: true, configurable: true };
                }
                if (typeof prop === "string" && _camelSet.has(prop)) {
                    return { value: backend.get(_toKebab(prop)), writable: true, enumerable: true, configurable: true };
                }
                return Reflect.getOwnPropertyDescriptor(t, prop);
            },
            defineProperty(t, prop, d) { return Reflect.defineProperty(t, prop, d); },
            deleteProperty(t, prop) { return Reflect.deleteProperty(t, prop); },
        });
        _cssBackend.set(proxy, backend);
        _cssBackend.set(target, backend);
        return proxy;
    }

    const _parseDecls = (raw) => {
        const m = new Map();
        for (const part of String(raw || "").split(";")) {
            const idx = part.indexOf(":");
            if (idx <= 0) continue;
            const name = part.slice(0, idx).trim();
            let val = part.slice(idx + 1).trim();
            let prio = "";
            const im = /\s*!\s*important\s*$/i.exec(val);
            if (im) { prio = "important"; val = val.slice(0, im.index).trim(); }
            if (name) m.set(name.startsWith("--") ? name : name.toLowerCase(), { val, prio });
        }
        return m;
    };

    // calc() simplification for specified-value serialization: Chrome folds
    // px-only (and unitless) math, incl. trig, to `calc(<n>px)`; mixed units
    // are kept as written.
    function _cssSimplifyCalc(v) {
        if (typeof v !== 'string' || v.indexOf('calc(') === -1) return v;
        const t = v.trim();
        if (!/^calc\(/i.test(t)) return v;
        // CSS auto-closes unbalanced blocks at end of input.
        let body = t.slice(5);
        const open = (body.match(/\(/g) || []).length + 1, close = (body.match(/\)/g) || []).length;
        if (close > open) return v;
        body += ')'.repeat(open - close);
        const src = body.slice(0, -1);
        let i = 0;
        const ws = () => { while (i < src.length && /\s/.test(src[i])) i++; };
        const fail = () => { throw 0; };
        const num = () => {
            ws();
            const r = /^[+-]?(\d+\.?\d*|\.\d+)(e[+-]?\d+)?/i.exec(src.slice(i));
            if (!r) return null;
            i += r[0].length;
            const n = parseFloat(r[0]);
            const u = /^(px|deg|rad|turn)?/i.exec(src.slice(i))[0].toLowerCase();
            i += u.length;
            if (u === 'deg') return { n: n * Math.PI / 180, len: false };
            if (u === 'turn') return { n: n * 2 * Math.PI, len: false };
            return { n, len: u === 'px' };
        };
        const prim = () => {
            ws();
            const fm = /^(sin|cos|tan|calc|abs|sign|sqrt)\(/i.exec(src.slice(i));
            if (fm) { i += fm[0].length; const a = add(); ws(); if (src[i] !== ')') fail(); i++;
                const f = fm[1].toLowerCase();
                if (f === 'calc' || f === 'abs' || f === 'sign') return f === 'abs' ? { n: Math.abs(a.n), len: a.len } : f === 'sign' ? { n: Math.sign(a.n), len: false } : a;
                if (a.len) fail();
                if (f === 'sqrt') return { n: Math.sqrt(a.n), len: false };
                // Blink evaluates trig on the angle reduced in degrees.
                const deg = (a.n * (180 / Math.PI)) % 360, r = deg * (Math.PI / 180);
                return { n: f === 'sin' ? Math.sin(r) : f === 'cos' ? Math.cos(r) : Math.tan(r), len: false }; }
            if (src[i] === '(') { i++; const a = add(); ws(); if (src[i] !== ')') fail(); i++; return a; }
            const n = num(); if (!n) fail(); return n;
        };
        const mul = () => {
            let a = prim();
            for (;;) { ws(); const c = src[i];
                if (c !== '*' && c !== '/') return a; i++;
                const b = prim();
                if (c === '*') { if (a.len && b.len) fail(); a = { n: a.n * b.n, len: a.len || b.len }; }
                else { if (b.len) fail(); a = { n: a.n / b.n, len: a.len }; } }
        };
        const add = () => {
            let a = mul();
            for (;;) { ws(); const c = src[i];
                if ((c !== '+' && c !== '-') || !/\s/.test(src[i - 1] || ' ')) return a; i++;
                const b = mul(); if (a.len !== b.len) fail();
                a = { n: c === '+' ? a.n + b.n : a.n - b.n, len: a.len }; }
        };
        try {
            const r = add(); ws();
            if (i !== src.length || !r.len || !isFinite(r.n)) return v;
            return 'calc(' + String(Number(r.n.toPrecision(6))) + 'px)';
        } catch (_) { return v; }
    }
    const _sysColors = globalThis[Symbol.for('__bo_system_colors')] || null;

    function _createStyleProxy(nodeId) {
        // Live view of the style attribute (re-parsed when it changes).
        let lastRaw = null, decls = new Map();
        const sync = () => {
            const raw = ops.op_dom_get_attribute(nodeId, "style") || "";
            if (raw !== lastRaw) { lastRaw = raw; decls = _parseDecls(raw); }
            return decls;
        };
        const flush = () => {
            const parts = [];
            for (const [k, d] of decls) if (d.val !== "") parts.push(k + ": " + d.val + (d.prio ? " !important" : ""));
            const txt = parts.join("; ") + (parts.length ? ";" : "");
            lastRaw = txt;
            ops.op_dom_set_attribute(nodeId, "style", txt);
        };
        return _makeDecl({
            names() { return [...sync().keys()]; },
            get(k) { const d = sync().get(k); return d ? d.val : ""; },
            prio(k) { const d = sync().get(k); return d ? d.prio : ""; },
            set(k, v, prio) {
                sync();
                if (v === "") decls.delete(k);
                else { decls.delete(k); decls.set(k, { val: _cssSimplifyCalc(v), prio: prio === "important" ? "important" : "" }); }
                flush();
            },
            remove(k) { sync(); const d = decls.get(k); decls.delete(k); flush(); return d ? d.val : ""; },
            cssText() { const parts = []; for (const [k, d] of sync()) parts.push(k + ": " + d.val + (d.prio ? " !important" : "") + ";"); return parts.join(" "); },
            setCssText(t) { decls = _parseDecls(t); flush(); },
        });
    }

    const _inlineTags = new Set(["span","a","b","i","em","strong","small","big","code","kbd","samp","var","abbr","cite","q","sub","sup","label","font","u","s","strike","mark","time","data","bdi","bdo","tt","ins","del","dfn","nobr","img","input","button","select","textarea","canvas","svg","iframe","video","audio","br","wbr"]);
    const _noneTags = new Set(["head","script","style","meta","link","title","template","noscript","base","param","datalist","source","track"]);
    function _createComputedStyle(el) {
        const nodeId = _getNodeId(el);
        const tag = String((el && el.localName) || "").toLowerCase();
        const roErr = (k) => { throw new DOMException("Failed to execute 'setProperty' on 'CSSStyleDeclaration': These styles are computed, and therefore the '" + k + "' property is read-only.", "NoModificationAllowedError"); };
        const value = (k) => {
            let v;
            try {
                const inl = _parseDecls(ops.op_dom_get_attribute(nodeId, "style") || "").get(k);
                if (inl && inl.val !== "") v = inl.val;
            } catch (_) {}
            if (v === undefined) {
                try {
                    const all = ops.op_dom_get_all_computed_styles(nodeId);
                    if (all && all[k]) v = all[k];
                } catch (_) {}
            }
            if (v === undefined) {
                const diff = tag === "html" ? _cssData.html : tag === "body" ? _cssData.body : _inlineTags.has(tag) ? _cssData.span : null;
                if (diff && k in diff) v = diff[k];
                else if (k === "display" && _noneTags.has(tag)) v = "none";
                else v = _cssData.div[k];
            }
            if ((k === "width" || k === "height" || k === "inline-size" || k === "block-size") && v === "auto" && !_inlineTags.has(tag)) {
                try {
                    if (el.isConnected) {
                        const r = el.getBoundingClientRect();
                        const px = (k === "width" || k === "inline-size") ? r.width : r.height;
                        v = (Math.round(px * 1000) / 1000) + "px";
                    }
                } catch (_) {}
            }
            if (typeof v === "string" && /color$|^fill$|^stroke$/.test(k)) {
                const sc = _sysColors || globalThis[Symbol.for('__bo_system_colors')];
                const hit = sc && sc[v.trim().toLowerCase()];
                if (hit) v = hit;
            }
            return v === undefined ? "" : String(v);
        };
        return _makeDecl({
            names() { return _cssData.names; },
            get(k) { return k.startsWith("--") ? "" : value(k); },
            prio() { return ""; },
            set(k, v, p, via) { roErr(k); },
            remove(k) { throw new DOMException("Failed to execute 'removeProperty' on 'CSSStyleDeclaration': These styles are computed, and therefore the '" + k + "' property is read-only.", "NoModificationAllowedError"); },
            cssText() { return ""; },
            setCssText() { throw new DOMException("Failed to set the 'cssText' property on 'CSSStyleDeclaration': These styles are computed, and therefore read-only.", "NoModificationAllowedError"); },
        });
    }

    const _attrNsName = (el, ns, local) => {
        local = String(local);
        if (el.hasAttribute(local)) return local;
        const pfx = { "http://www.w3.org/1999/xlink": "xlink", "http://www.w3.org/XML/1998/namespace": "xml", "http://www.w3.org/2000/xmlns/": "xmlns" }[ns];
        if (pfx && el.hasAttribute(pfx + ":" + local)) return pfx + ":" + local;
        return null;
    };
    class Element extends Node {
        get tagName() { return ops.op_dom_get_tag_name(_getNodeId(this)).toUpperCase(); }
        get localName() { return ops.op_dom_get_tag_name(_getNodeId(this)); }
        get id() { return ops.op_dom_get_attribute(_getNodeId(this), "id") || ""; }
        set id(val) { ops.op_dom_set_attribute(_getNodeId(this), "id", String(val)); }
        get className() { return ops.op_dom_get_attribute(_getNodeId(this), "class") || ""; }
        set className(val) { ops.op_dom_set_attribute(_getNodeId(this), "class", String(val)); }
        // HTML attribute-backed properties (script.src, link.href, img.src, etc.)
        // `.src` reflects the IDL attribute, which real Chrome returns as an
        // ABSOLUTE URL (resolved against the document base) — not the raw
        // relative attribute. Returning the raw relative value is a parity gap
        // that breaks any script deriving paths from its own `.src`. Resolve
        // against the document base; fall back to the raw value if URL parsing
        // fails, and keep "" for an absent/empty attribute (Chrome parity).
        get src() {
            const _raw = this.getAttribute("src");
            if (!_raw) return "";
            try {
                const _base = (globalThis.location && globalThis.location.href)
                    || (globalThis.__browser_oxide && globalThis.__browser_oxide._baseUrl)
                    || undefined;
                return new URL(_raw, _base).href;
            } catch (_) {
                return _raw;
            }
        }
        set src(val) { this.setAttribute("src", String(val)); }
        get href() { return this.getAttribute("href") || ""; }
        set href(val) { this.setAttribute("href", String(val)); }
        get type() { return this.getAttribute("type") || ""; }
        set type(val) { this.setAttribute("type", String(val)); }
        get rel() { return this.getAttribute("rel") || ""; }
        set rel(val) { this.setAttribute("rel", String(val)); }
        get as() { const v = String(this.getAttribute("as") || "").toLowerCase(); return ["fetch","audio","audioworklet","document","embed","font","image","manifest","object","paintworklet","report","script","serviceworker","sharedworker","style","track","video","worker","xslt","json"].includes(v) ? v : ""; }
        set as(val) { this.setAttribute("as", String(val)); }
        get async() { return this.hasAttribute("async") || _scriptForceAsync.has(this); }
        set async(val) { _scriptForceAsync.delete(this); if (val) this.setAttribute("async", ""); else this.removeAttribute("async"); }
        get defer() { return this.hasAttribute("defer"); }
        set defer(val) { if (val) this.setAttribute("defer", ""); else this.removeAttribute("defer"); }
        get crossOrigin() { return this.getAttribute("crossorigin"); }
        set crossOrigin(val) { if (val != null) this.setAttribute("crossorigin", String(val)); else this.removeAttribute("crossorigin"); }
        get integrity() { return this.getAttribute("integrity") || ""; }
        set integrity(val) { this.setAttribute("integrity", String(val)); }
        get referrerPolicy() { return this.getAttribute("referrerpolicy") || ""; }
        set referrerPolicy(val) { this.setAttribute("referrerpolicy", String(val)); }
        get classList() { return new DOMTokenList(_getNodeId(this)); }
        get innerHTML() { return ops.op_dom_get_inner_html(_getNodeId(this)); }
        set innerHTML(val) { ops.op_dom_set_inner_html(_getNodeId(this), String(val)); _reapSchedule(16); }
        get outerHTML() { return ops.op_dom_get_outer_html(_getNodeId(this)); }
        get children() {
            return new NodeList(ops.op_dom_get_child_elements_with_types(_getNodeId(this)), true);
        }
        get firstElementChild() {
            const els = ops.op_dom_get_child_elements(_getNodeId(this));
            return els.length > 0 ? _wrapNode(els[0]) : null;
        }
        get lastElementChild() {
            const els = ops.op_dom_get_child_elements(_getNodeId(this));
            return els.length > 0 ? _wrapNode(els[els.length - 1]) : null;
        }
        getAttribute(name) { return ops.op_dom_get_attribute(_getNodeId(this), name); }
        setAttribute(name, value) { ops.op_dom_set_attribute(_getNodeId(this), name, String(value)); }
        removeAttribute(name) { ops.op_dom_remove_attribute(_getNodeId(this), name); }
        hasAttribute(name) { return ops.op_dom_has_attribute(_getNodeId(this), name); }
        // Namespaced attributes. The DOM store keys attributes by qualified
        // name (as the HTML parser does for xlink:href etc.), so set by the
        // qualified name and look up by local name or a known-prefix form.
        // React uses setAttributeNS for SVG xlink:/xml: attributes.
        setAttributeNS(_ns, qname, value) { this.setAttribute(String(qname), value); }
        getAttributeNS(ns, local) { const n = _attrNsName(this, ns, local); return n === null ? null : this.getAttribute(n); }
        hasAttributeNS(ns, local) { return _attrNsName(this, ns, local) !== null; }
        removeAttributeNS(ns, local) { const n = _attrNsName(this, ns, local); if (n !== null) this.removeAttribute(n); }
        querySelector(sel) {
            const id = ops.op_dom_query_selector(_getNodeId(this), sel);
            return id !== null ? _wrapNode(id) : null;
        }
        querySelectorAll(sel) {
            return new NodeList(ops.op_dom_query_selector_all(_getNodeId(this), sel));
        }
        matches(sel) {
            const all = ops.op_dom_query_selector_all(
                ops.op_dom_get_parent(_getNodeId(this)) || ops.op_dom_document_node(),
                sel
            );
            return all.includes(_getNodeId(this));
        }
        closest(sel) {
            let el = this;
            while (el) {
                if (el.matches && el.matches(sel)) return el;
                el = el.parentElement;
            }
            return null;
        }
        getElementsByTagName(tag) {
            return new NodeList(ops.op_dom_get_elements_by_tag_name(_getNodeId(this), tag));
        }
        getElementsByClassName(cls) {
            return new NodeList(ops.op_dom_get_elements_by_class_name(_getNodeId(this), cls));
        }
        // Layout APIs (wired to taffy via layout_ext ops)
        getBoundingClientRect() {
            const r = ops.op_layout_get_bounding_rect(_getNodeId(this));
            return new DOMRect(r.x - _vpX, r.y - _vpY, r.width, r.height);
        }
        getClientRects() { return [this.getBoundingClientRect()]; }
        get offsetWidth() { return ops.op_layout_get_offset_width(_getNodeId(this)); }
        get offsetHeight() { return ops.op_layout_get_offset_height(_getNodeId(this)); }
        get offsetTop() { return ops.op_layout_get_offset_top(_getNodeId(this)); }
        get offsetLeft() { return ops.op_layout_get_offset_left(_getNodeId(this)); }
        get clientWidth() { return this.offsetWidth; }
        get clientHeight() { return this.offsetHeight; }
        get scrollWidth() { return this.offsetWidth; }
        get scrollHeight() { return this.offsetHeight; }
        get scrollTop() {
            if (globalThis.document && this === document.documentElement) return _vpY;
            const s = _scrollState.get(_getNodeId(this));
            return s ? s.top : 0;
        }
        set scrollTop(v) {
            if (globalThis.document && this === document.documentElement) { _vpSet(_vpX, v); return; }
            const id = _getNodeId(this);
            const n = Number(v);
            const top = Number.isFinite(n) ? n : 0;
            const cur = _scrollState.get(id);
            if (cur) cur.top = top; else _scrollState.set(id, { top, left: 0 });
        }
        get scrollLeft() {
            if (globalThis.document && this === document.documentElement) return _vpX;
            const s = _scrollState.get(_getNodeId(this));
            return s ? s.left : 0;
        }
        set scrollLeft(v) {
            if (globalThis.document && this === document.documentElement) { _vpSet(v, _vpY); return; }
            const id = _getNodeId(this);
            const n = Number(v);
            const left = Number.isFinite(n) ? n : 0;
            const cur = _scrollState.get(id);
            if (cur) cur.left = left; else _scrollState.set(id, { top: 0, left });
        }
        // Viewport-only scrollIntoView (nested scroll containers are not
        // modelled). block/inline: start | center | end | nearest; boolean
        // arg true = start, false = end. Instant (smooth is not animated).
        scrollIntoView(arg) {
            let block = 'start', inline = 'nearest';
            if (arg === false) block = 'end';
            else if (arg && typeof arg === 'object') { block = arg.block || 'start'; inline = arg.inline || 'nearest'; }
            let r;
            try { r = this.getBoundingClientRect(); } catch (_) { return; }
            const vw = globalThis.innerWidth || 0, vh = globalThis.innerHeight || 0;
            const pick = (mode, pos, size, view) => {
                if (mode === 'start') return pos;
                if (mode === 'end') return pos + size - view;
                if (mode === 'center') return pos + size / 2 - view / 2;
                if (pos >= 0 && pos + size <= view) return 0;          // nearest: already visible
                return (pos < 0 || size > view) ? pos : pos + size - view;
            };
            _vpSet(_vpX + pick(inline, r.x, r.width, vw), _vpY + pick(block, r.y, r.height, vh));
        }
        scrollTo(xOrOpts, y) {
            if (typeof xOrOpts === "object" && xOrOpts !== null) {
                if (xOrOpts.left !== undefined) this.scrollLeft = xOrOpts.left;
                if (xOrOpts.top !== undefined) this.scrollTop = xOrOpts.top;
            } else {
                this.scrollLeft = xOrOpts;
                this.scrollTop = y;
            }
        }
        scrollBy(xOrOpts, y) {
            if (typeof xOrOpts === "object" && xOrOpts !== null) {
                if (xOrOpts.left !== undefined) this.scrollLeft = this.scrollLeft + xOrOpts.left;
                if (xOrOpts.top !== undefined) this.scrollTop = this.scrollTop + xOrOpts.top;
            } else {
                this.scrollLeft = this.scrollLeft + xOrOpts;
                this.scrollTop = this.scrollTop + y;
            }
        }
        get offsetParent() { return this.parentElement; }
        // --- Modern DOM manipulation ---
        remove() {
            const parent = ops.op_dom_get_parent(_getNodeId(this));
            if (parent !== -1 && parent !== null) {
                ops.op_dom_remove_child(parent, _getNodeId(this));
                _reapSchedule(1);
            }
        }
        append(...nodes) {
            for (const node of nodes) {
                if (typeof node === "string") {
                    this.appendChild(_document.createTextNode(node));
                } else {
                    this.appendChild(node);
                }
            }
        }
        prepend(...nodes) {
            const first = this.firstChild;
            for (const node of nodes) {
                const n = typeof node === "string" ? _document.createTextNode(node) : node;
                if (first) {
                    this.insertBefore(n, first);
                } else {
                    this.appendChild(n);
                }
            }
        }
        after(...nodes) {
            const parent = this.parentNode;
            if (!parent) return;
            const next = this.nextSibling;
            for (const node of nodes) {
                const n = typeof node === "string" ? _document.createTextNode(node) : node;
                if (next) {
                    parent.insertBefore(n, next);
                } else {
                    parent.appendChild(n);
                }
            }
        }
        before(...nodes) {
            const parent = this.parentNode;
            if (!parent) return;
            for (const node of nodes) {
                const n = typeof node === "string" ? _document.createTextNode(node) : node;
                parent.insertBefore(n, this);
            }
        }
        replaceWith(...nodes) {
            const parent = this.parentNode;
            if (!parent) return;
            const next = this.nextSibling;
            this.remove();
            for (const node of nodes) {
                const n = typeof node === "string" ? _document.createTextNode(node) : node;
                if (next) {
                    parent.insertBefore(n, next);
                } else {
                    parent.appendChild(n);
                }
            }
        }
        replaceChildren(...nodes) {
            // Remove all existing children
            while (this.firstChild) this.removeChild(this.firstChild);
            this.append(...nodes);
        }
        // --- insertAdjacent family ---
        insertAdjacentHTML(position, html) {
            // The fragment is parsed in this element's context (see the op).
            ops.op_dom_insert_adjacent_html(_getNodeId(this), position, html, this.localName || '');
        }
        insertAdjacentElement(position, element) {
            const parent = this.parentNode;
            switch (position) {
                case "beforebegin":
                    if (parent) parent.insertBefore(element, this);
                    break;
                case "afterbegin":
                    this.insertBefore(element, this.firstChild);
                    break;
                case "beforeend":
                    this.appendChild(element);
                    break;
                case "afterend":
                    if (parent) {
                        const next = this.nextSibling;
                        if (next) parent.insertBefore(element, next);
                        else parent.appendChild(element);
                    }
                    break;
            }
            return element;
        }
        insertAdjacentText(position, text) {
            const textNode = _document.createTextNode(text);
            this.insertAdjacentElement(position, textNode);
        }
        toggleAttribute(name, force) {
            if (force !== undefined) {
                if (force) { this.setAttribute(name, ""); return true; }
                else { this.removeAttribute(name); return false; }
            }
            if (this.hasAttribute(name)) { this.removeAttribute(name); return false; }
            this.setAttribute(name, ""); return true;
        }
        // --- Attribute helpers ---
        // NamedNodeMap of real Attr nodes (fbgb fork). The old shim returned
        // null for every attribute with an empty value (data-sjs="") and had
        // no Attr / *AttributeNode API, which React 19 uses to clear
        // <html>/<body> after a hydration mismatch.
        get attributes() { return _namedNodeMap(this); }
        getAttributeNode(name) { return this.hasAttribute(name) ? _attrNode(this, String(name).toLowerCase()) : null; }
        getAttributeNodeNS(_ns, name) { return this.getAttributeNode(name); }
        setAttributeNode(attr) {
            if (!attr || attr.nodeType !== 2) throw new TypeError("Failed to execute 'setAttributeNode' on 'Element': parameter 1 is not of type 'Attr'.");
            const old = this.hasAttribute(attr.name) ? _attrDetach(this, attr.name) : null;
            const value = attr.value;
            _attrBind(attr, this);
            this.setAttribute(attr.name, value);
            return old;
        }
        setAttributeNodeNS(attr) { return this.setAttributeNode(attr); }
        removeAttributeNode(attr) {
            if (!attr || attr.ownerElement !== this) throw new DOMException("Failed to execute 'removeAttributeNode' on 'Element': The node provided is owned by another element.", 'NotFoundError');
            const detached = _attrDetach(this, attr.name);
            this.removeAttribute(attr.name);
            return detached || attr;
        }
        hasAttributes() { return ops.op_dom_get_attribute_names(_getNodeId(this)).length > 0; }
        get dataset() {
            const el = this;
            const _dsCache = (globalThis[Symbol.for('__bo_dataset_cache')] ||= new WeakMap());
            const _cached = _dsCache.get(this);
            if (_cached) return _cached;
            const id = _getNodeId(this);
            const toKebab = (p) => "data-" + p.replace(/[A-Z]/g, m => "-" + m.toLowerCase());
            const fromKebab = (a) => a.slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase());
            const dataNames = () => ops.op_dom_get_attribute_names(id).filter(n => n.startsWith("data-"));
            const _DSM = globalThis.DOMStringMap;
            const _target = Object.create(typeof _DSM === "function" && _DSM.prototype ? _DSM.prototype : Object.prototype);
            const _px = new Proxy(_target, {
                get(target, prop) {
                    if (typeof prop === "string" && ops.op_dom_has_attribute(id, toKebab(prop))) {
                        const v = ops.op_dom_get_attribute(id, toKebab(prop));
                        return v == null ? "" : v;
                    }
                    return Reflect.get(target, prop);
                },
                set(target, prop, value) {
                    el.setAttribute(toKebab(prop), String(value));
                    return true;
                },
                has(target, prop) {
                    if (typeof prop !== "string") return false;
                    return ops.op_dom_has_attribute(id, toKebab(prop));
                },
                deleteProperty(target, prop) {
                    if (typeof prop === "string") el.removeAttribute(toKebab(prop));
                    return true;
                },
                ownKeys() {
                    return dataNames().map(fromKebab);
                },
                getOwnPropertyDescriptor(target, prop) {
                    if (typeof prop !== "string") return undefined;
                    const attr = toKebab(prop);
                    if (ops.op_dom_has_attribute(id, attr)) {
                        return {
                            value: ops.op_dom_get_attribute(id, attr) || "",
                            enumerable: true, configurable: true, writable: true,
                        };
                    }
                    return undefined;
                }
            });
            _dsCache.set(this, _px);
            return _px;
        }
        get nextElementSibling() {
            let n = this.nextSibling;
            while (n) {
                if (n.nodeType === 1) return n;
                n = n.nextSibling;
            }
            return null;
        }
        get previousElementSibling() {
            let n = this.previousSibling;
            while (n) {
                if (n.nodeType === 1) return n;
                n = n.previousSibling;
            }
            return null;
        }
        get childElementCount() {
            return ops.op_dom_get_child_elements(_getNodeId(this)).length;
        }
        // element.style — CSSStyleDeclaration proxy
        get style() {
            if (!this._style) this._style = _createStyleProxy(_getNodeId(this));
            return this._style;
        }
        // Interaction stubs
        //
        // `click()` must run the element's ACTIVATION BEHAVIOUR, not just
        // dispatch an event: gate flows read back form state immediately
        // (`if (box.checked) …`) and re-drive the flow from it. Without the
        // toggle, `el.click()` looked successful (the event fired) but
        // `.checked` never changed, so checkbox chains stalled. Chrome order
        // is: legacy-pre-activation (toggle) → `click` → `input` → `change`.
        click() {
            const tag = (this.localName || "").toLowerCase();
            let toggled = false;
            if (tag === "input") {
                const type = (this.getAttribute("type") || "text").toLowerCase();
                if (type === "checkbox") {
                    toggled = !this.hasAttribute("checked");
                    if (toggled) this.setAttribute("checked", "");
                    else this.removeAttribute("checked");
                } else if (type === "radio") {
                    // Radios are mutually exclusive per (form, name) group.
                    const name = this.getAttribute("name");
                    if (name) {
                        let scope = null;
                        try { scope = this.closest ? this.closest("form") : null; } catch (_) {}
                        scope = scope || globalThis.document || null;
                        if (scope && scope.querySelectorAll) {
                            // Query the tag and filter, rather than relying on
                            // an attribute-selector implementation: the peer set
                            // must be found on every engine build.
                            const peers = scope.querySelectorAll("input");
                            for (let i = 0; i < peers.length; i++) {
                                const peer = peers[i];
                                if (peer === this) continue;
                                if (!peer.getAttribute) continue;
                                if ((peer.getAttribute("type") || "").toLowerCase() !== "radio") continue;
                                if (name != null && peer.getAttribute("name") === name) peer.removeAttribute("checked");
                            }
                        }
                    }
                    toggled = !this.hasAttribute("checked");
                    this.setAttribute("checked", "");
                }
            }
            this.dispatchEvent(new Event("click", { bubbles: true }));
            if (toggled) {
                this.dispatchEvent(new Event("input", { bubbles: true }));
                this.dispatchEvent(new Event("change", { bubbles: true }));
            }
        }
        focus(options) { _focusElement(this, options); }
        blur() { _blurElement(this); }
        checkVisibility() { return true; }
        animate() { return { finished: Promise.resolve(), cancel() {}, play() {}, pause() {} }; }
        getAnimations() { return []; }
        attachShadow(init = {}) {
            const mode = init.mode || "open";
            const shadowId = ops.op_dom_attach_shadow(_getNodeId(this), mode);
            // Use _wrapNode — _wrap is not a defined helper. Was a stale
            // reference that threw `ReferenceError: _wrap is not defined`
            // whenever attachShadow was actually called — observable to
            // scripts that exercise Shadow DOM.
            const shadowRoot = _wrapNode(shadowId);
            // ShadowRoot inherits Node methods (appendChild, querySelector, etc.)
            Object.defineProperties(shadowRoot, {
                mode: { value: mode, enumerable: true },
                host: { value: this, enumerable: true },
                innerHTML: {
                    get() { return ops.op_dom_get_inner_html(shadowId); },
                    set(html) { ops.op_dom_set_inner_html(shadowId, html); },
                },
            });
            if (mode === "open") this._shadowRoot = shadowRoot;
            return shadowRoot;
        }
        get shadowRoot() { return this._shadowRoot || null; }
    }

    // Full DOM prototype chain:
    //   EventTarget ← Node ← Element ← HTMLElement ← HTML*Element
    // Subclasses are mostly empty markers for instanceof checks. When an
    // element is created via _wrapNode, we do setPrototypeOf based on the
    // tag name to select the right specific class (HTMLDivElement etc.)
    // without having to create a dedicated Rust-side dispatch.
    class HTMLElement extends Element {}
    class HTMLDivElement extends HTMLElement {}
    class HTMLSpanElement extends HTMLElement {}
    class HTMLParagraphElement extends HTMLElement {}
    class HTMLHeadingElement extends HTMLElement {}
    class HTMLAnchorElement extends HTMLElement {}
    class HTMLImageElement extends HTMLElement {}
    // Desktop Chrome shows a broken image as a 16x16 icon (headless Chrome as 0x0).
    function _imgSize(img, axis, natural) {
        const nat = _imgNatural.get(img);
        if (!natural) {
            const attr = img.getAttribute(axis);
            if (attr != null) {
                const n = parseInt(attr, 10);
                if (!Number.isNaN(n)) return n;
            }
        }
        if (nat) return axis === 'width' ? nat.w : nat.h;
        return _imgState.get(img) === 'error' ? 16 : 0;
    }
    Object.defineProperty(HTMLImageElement.prototype, "width", {
        get() { return _imgSize(this, 'width', false); },
        enumerable: true, configurable: true
    });
    Object.defineProperty(HTMLImageElement.prototype, "height", {
        get() { return _imgSize(this, 'height', false); },
        enumerable: true, configurable: true
    });
    Object.defineProperty(HTMLImageElement.prototype, "naturalWidth", {
        get() { return _imgSize(this, 'width', true); },
        enumerable: true, configurable: true
    });
    Object.defineProperty(HTMLImageElement.prototype, "naturalHeight", {
        get() { return _imgSize(this, 'height', true); },
        enumerable: true, configurable: true
    });
    Object.defineProperty(HTMLImageElement.prototype, "complete", {
        get() { return true; }, 
        enumerable: true, configurable: true
    });
    HTMLImageElement.prototype.decode = function() { return Promise.resolve(); };
    class HTMLInputElement extends HTMLElement {}
    class HTMLFormElement extends HTMLElement {
        submit(_submitter) {
            const action = this.action || (globalThis.location ? globalThis.location.href : '');
            const method = (this.method || 'GET').toUpperCase();

            // Serialize form data
            const params = new URLSearchParams();
            const inputs = this.querySelectorAll('input, textarea, select');
            for (let i = 0; i < inputs.length; i++) {
                const el = inputs[i];
                const name = el.name;
                if (!name || el.disabled) continue;

                const type = (el.type || '').toLowerCase();
                if (type === 'submit' || type === 'button' || type === 'image') continue;
                if ((type === 'checkbox' || type === 'radio') && !el.checked) continue;

                params.append(name, el.value || '');
            }
            if (_submitter && _submitter.name) params.append(_submitter.name, _submitter.value || '');

            let finalUrl = action;
            let finalBody = null;

            if (method === 'GET') {
                const url = new URL(action, globalThis.location ? globalThis.location.href : 'about:blank');
                params.forEach((v, k) => url.searchParams.append(k, v));
                finalUrl = url.href;
            } else {
                finalBody = params.toString();
            }

            globalThis.__pendingNavigation = {
                url: finalUrl,
                method: method,
                body: finalBody,
                kind: 'assign'
            };
            // Signal the Rust event loop to short-circuit run_until_idle —
            // see crates/js_runtime/src/extensions/nav_ext.rs.
            try { ops.op_set_pending_nav(); } catch (_) {}
        }
        // Spec: fire a trusted, cancelable `submit` (SubmitEvent with the
        // submitter) and navigate only if no listener cancelled it. React's
        // onSubmit and most login forms rely on this event.
        requestSubmit(submitter) {
            let ev;
            try { ev = new globalThis.SubmitEvent('submit', { bubbles: true, cancelable: true, submitter: submitter || null }); }
            catch (_) {
                ev = new Event('submit', { bubbles: true, cancelable: true });
            }
            try { if (ev.submitter !== (submitter || null)) Object.defineProperty(ev, 'submitter', { value: submitter || null, configurable: true }); } catch (_) {}
            _trust(ev);
            if (this.dispatchEvent(ev)) this.submit(submitter);
        }
    }

    // IDL property ↔ HTML attribute reflection. Scripts that configure form
    // fields via properties (el.name = 'x', form.action = url, form.method =
    // 'POST') expect the read-back to see what they set — which only works if
    // the property setter writes the underlying attribute. Without this,
    // programmatically-built forms look empty to our submit() serializer.
    // Universal primitive — matches HTML spec "reflect" behavior.
    const _reflectStr = (proto, prop, attr = prop, dflt = '') => {
        Object.defineProperty(proto, prop, {
            get() { const v = this.getAttribute(attr); return v == null ? dflt : v; },
            set(v) { this.setAttribute(attr, String(v)); },
            enumerable: true, configurable: true,
        });
    };
    const _reflectBool = (proto, prop, attr = prop) => {
        Object.defineProperty(proto, prop, {
            get() { return this.hasAttribute(attr); },
            set(v) {
                if (v) this.setAttribute(attr, '');
                else this.removeAttribute(attr);
            },
            enumerable: true, configurable: true,
        });
    };
    _reflectStr(HTMLInputElement.prototype, 'name');
    _reflectStr(HTMLInputElement.prototype, 'value');
    _reflectStr(HTMLInputElement.prototype, 'type', 'type', 'text');
    _reflectStr(HTMLInputElement.prototype, 'placeholder');
    _reflectBool(HTMLInputElement.prototype, 'checked');
    _reflectBool(HTMLInputElement.prototype, 'disabled');
    _reflectBool(HTMLInputElement.prototype, 'readOnly', 'readonly');
    _reflectBool(HTMLInputElement.prototype, 'required');
    _reflectStr(HTMLFormElement.prototype, 'action');
    _reflectStr(HTMLFormElement.prototype, 'method', 'method', 'get');
    _reflectStr(HTMLFormElement.prototype, 'enctype', 'enctype', 'application/x-www-form-urlencoded');
    _reflectStr(HTMLFormElement.prototype, 'target');
    _reflectStr(HTMLFormElement.prototype, 'name');
    _reflectBool(HTMLFormElement.prototype, 'noValidate', 'novalidate');

    // HTMLFormElement.prototype.elements — live HTMLFormControlsCollection
    // of the form's listed elements (HTML spec §6.4.3: button, fieldset,
    // input, object, output, select, textarea). Reddit's verify-page solver
    // calls `form.elements.namedItem('solution').value = token`; without
    // this getter that throws TypeError, the SPA's pendingNavigation is
    // never set, and the page returns iter=0 with the challenge stub.
    Object.defineProperty(HTMLFormElement.prototype, 'elements', {
        get() {
            const form = this;
            const controls = form.querySelectorAll(
                'button, fieldset, input, object, output, select, textarea',
            );
            const len = controls.length;
            const ctor = globalThis.HTMLFormControlsCollection;
            const wrap = ctor && ctor.prototype
                ? Object.create(ctor.prototype)
                : Object.create(null);
            for (let i = 0; i < len; i++) {
                Object.defineProperty(wrap, i, {
                    value: controls[i],
                    writable: false, configurable: true, enumerable: true,
                });
            }
            Object.defineProperty(wrap, 'length', {
                value: len,
                writable: false, configurable: true, enumerable: false,
            });
            Object.defineProperty(wrap, 'item', {
                value: function item(idx) {
                    idx = Math.trunc(+idx);
                    return idx >= 0 && idx < len ? wrap[idx] : null;
                },
                writable: true, configurable: true, enumerable: false,
            });
            Object.defineProperty(wrap, 'namedItem', {
                value: function namedItem(name) {
                    if (typeof name !== 'string' || name === '') return null;
                    const matches = [];
                    for (let i = 0; i < len; i++) {
                        const el = controls[i];
                        if (el.name === name || el.id === name) matches.push(el);
                    }
                    if (matches.length === 0) return null;
                    if (matches.length === 1) return matches[0];
                    // Spec: multiple → RadioNodeList. Returning an array
                    // covers reddit's single-name case + iteration.
                    return matches;
                },
                writable: true, configurable: true, enumerable: false,
            });
            Object.defineProperty(wrap, Symbol.iterator, {
                value: function* () {
                    for (let i = 0; i < len; i++) yield wrap[i];
                },
                writable: true, configurable: true, enumerable: false,
            });
            return wrap;
        },
        configurable: true,
        enumerable: true,
    });

    class HTMLButtonElement extends HTMLElement {}
    class HTMLSelectElement extends HTMLElement {}
    class HTMLTextAreaElement extends HTMLElement {}

    // `<textarea>` has no `value` attribute — its value is the element's text
    // content, and its default is that same text. Reflecting it to an
    // attribute would produce `<textarea value="...">`, which real Chrome
    // never emits and the form serializer never reads.
    Object.defineProperty(HTMLTextAreaElement.prototype, 'value', {
        get() { return this.textContent || ''; },
        set(v) { this.textContent = String(v); },
        enumerable: true, configurable: true,
    });
    _reflectStr(HTMLTextAreaElement.prototype, 'name');
    _reflectStr(HTMLTextAreaElement.prototype, 'placeholder');
    _reflectBool(HTMLTextAreaElement.prototype, 'disabled');
    _reflectBool(HTMLTextAreaElement.prototype, 'readOnly', 'readonly');
    _reflectBool(HTMLTextAreaElement.prototype, 'required');
    Object.defineProperty(HTMLTextAreaElement.prototype, 'type', {
        get() { return 'textarea'; },
        configurable: true, enumerable: true,
    });
    Object.defineProperty(HTMLTextAreaElement.prototype, 'defaultValue', {
        get() { return this.textContent || ''; },
        set(v) { this.textContent = String(v); },
        enumerable: true, configurable: true,
    });

    // ── Form state ────────────────────────────────────────────────────────
    // Form state is what gate flows drive and read back: a solver sets
    // `input.value = token`, ticks a checkbox, picks a `<select>` option,
    // then reads the state to decide the next step. Every one of those
    // accessors must round-trip through the DOM (attributes / text content),
    // because that is the only store the Rust-side serializer
    // (`HTMLFormElement.submit`) and `outerHTML` see. Without these:
    //   * `select.value` / `select.selectedIndex` are `undefined`,
    //   * `textarea.value` never reflects typed text,
    //   * `option.selected` / `option.value` do not exist,
    // so any flow keyed on form state silently stalls.
    //
    // `<option>` accessors are installed next to the HTMLOptionElement
    // declaration further down, alongside the other late-declared classes.

    // <select> — options collection plus the selected-option state model.
    // `selectedIndex` is -1 when nothing is selected; `.value` falls back to
    // '' like Chrome rather than throwing, so gate scripts can branch on it.
    Object.defineProperty(HTMLSelectElement.prototype, 'options', {
        get() {
            const opts = this.querySelectorAll('option');
            const len = opts.length;
            const ctor = globalThis.HTMLOptionsCollection;
            const wrap = ctor && ctor.prototype ? Object.create(ctor.prototype) : Object.create(null);
            for (let i = 0; i < len; i++) {
                Object.defineProperty(wrap, i, {
                    value: opts[i], writable: false, configurable: true, enumerable: true,
                });
            }
            Object.defineProperty(wrap, 'length', {
                value: len, writable: false, configurable: true, enumerable: false,
            });
            Object.defineProperty(wrap, 'item', {
                value: function item(i) { i = Math.trunc(+i); return i >= 0 && i < len ? wrap[i] : null; },
                writable: true, configurable: true, enumerable: false,
            });
            Object.defineProperty(wrap, 'namedItem', {
                value: function namedItem(n) {
                    for (let i = 0; i < len; i++) { const o = opts[i]; if (o.id === n || o.name === n) return o; }
                    return null;
                },
                writable: true, configurable: true, enumerable: false,
            });
            Object.defineProperty(wrap, Symbol.iterator, {
                value: function* () { for (let i = 0; i < len; i++) yield wrap[i]; },
                writable: true, configurable: true, enumerable: false,
            });
            return wrap;
        },
        configurable: true, enumerable: true,
    });
    Object.defineProperty(HTMLSelectElement.prototype, 'selectedOptions', {
        get() {
            const o = this.options; const r = [];
            for (let i = 0; i < o.length; i++) if (o[i].selected) r.push(o[i]);
            return r;
        },
        configurable: true, enumerable: true,
    });
    Object.defineProperty(HTMLSelectElement.prototype, 'selectedIndex', {
        get() {
            const o = this.options;
            for (let i = 0; i < o.length; i++) if (o[i].selected) return i;
            return -1;
        },
        set(v) {
            const o = this.options; v = Math.trunc(+v);
            for (let i = 0; i < o.length; i++) o[i].selected = (i === v);
        },
        enumerable: true, configurable: true,
    });
    Object.defineProperty(HTMLSelectElement.prototype, 'value', {
        get() {
            const o = this.options;
            for (let i = 0; i < o.length; i++) if (o[i].selected) return o[i].value;
            return '';
        },
        set(v) {
            const o = this.options; v = String(v);
            for (let i = 0; i < o.length; i++) o[i].selected = (o[i].value === v);
        },
        enumerable: true, configurable: true,
    });
    Object.defineProperty(HTMLSelectElement.prototype, 'length', {
        get() { return this.options.length; },
        configurable: true, enumerable: true,
    });
    Object.defineProperty(HTMLSelectElement.prototype, 'type', {
        get() { return this.multiple ? 'select-multiple' : 'select-one'; },
        configurable: true, enumerable: true,
    });
    _reflectBool(HTMLSelectElement.prototype, 'multiple');
    _reflectBool(HTMLSelectElement.prototype, 'disabled');
    _reflectBool(HTMLSelectElement.prototype, 'required');
    _reflectStr(HTMLSelectElement.prototype, 'name');

    class HTMLCanvasElement extends HTMLElement {}
    Object.defineProperty(HTMLCanvasElement.prototype, "width", {
        get() {
            const attr = this.getAttribute("width");
            return attr ? parseInt(attr, 10) : 300;
        },
        set(v) { this.setAttribute("width", v); },
        enumerable: true, configurable: true
    });
    Object.defineProperty(HTMLCanvasElement.prototype, "height", {
        get() {
            const attr = this.getAttribute("height");
            return attr ? parseInt(attr, 10) : 150;
        },
        set(v) { this.setAttribute("height", v); },
        enumerable: true, configurable: true
    });
    HTMLCanvasElement.prototype.toDataURL = function(type, quality) {
        if (!this._canvasId) {
            let osName = "Linux", canvasSeed = 0n;
            try {
                if (ops.op_has_stealth_profile && ops.op_has_stealth_profile()) {
                    osName = ops.op_get_profile_value("os_name") || "Linux";
                    canvasSeed = BigInt(ops.op_get_profile_value("canvas_seed") || "0");
                }
            } catch (_e) { /* fall back to defaults */ }
            this._canvasId = ops.op_canvas_create(this.width, this.height, osName, canvasSeed);
        }
        return ops.op_canvas_to_data_url(this._canvasId);
    };
    class HTMLScriptElement extends HTMLElement {
        // HTML §4.12.1: `text` reflects the child text content.
        get text() { return this.textContent; }
        set text(v) { this.textContent = v == null ? "" : String(v); }
    }
    class HTMLStyleElement extends HTMLElement {}
    class HTMLLinkElement extends HTMLElement {}
    class HTMLMetaElement extends HTMLElement {}
    class HTMLTableElement extends HTMLElement {}
    class HTMLIFrameElement extends HTMLElement {}
    class HTMLVideoElement extends HTMLElement {}
    class HTMLAudioElement extends HTMLElement {}
    class HTMLBodyElement extends HTMLElement {}
    class HTMLHeadElement extends HTMLElement {}
    class HTMLHtmlElement extends HTMLElement {}
    class HTMLUListElement extends HTMLElement {}
    class HTMLOListElement extends HTMLElement {}
    class HTMLLIElement extends HTMLElement {}
    class HTMLTableRowElement extends HTMLElement {}
    class HTMLTableCellElement extends HTMLElement {}
    class HTMLTableSectionElement extends HTMLElement {}
    class HTMLLabelElement extends HTMLElement {}
    class HTMLOptionElement extends HTMLElement {}

    // <option> state — value defaults to the trimmed text per the HTML spec.
    Object.defineProperty(HTMLOptionElement.prototype, 'value', {
        get() {
            return this.hasAttribute('value')
                ? this.getAttribute('value')
                : (this.textContent || '').trim();
        },
        set(v) { this.setAttribute('value', String(v)); },
        enumerable: true, configurable: true,
    });
    Object.defineProperty(HTMLOptionElement.prototype, 'selected', {
        get() { return this.hasAttribute('selected'); },
        set(v) { if (v) this.setAttribute('selected', ''); else this.removeAttribute('selected'); },
        enumerable: true, configurable: true,
    });
    Object.defineProperty(HTMLOptionElement.prototype, 'text', {
        get() { return (this.textContent || '').trim(); },
        set(v) { this.textContent = String(v); },
        enumerable: true, configurable: true,
    });
    _reflectBool(HTMLOptionElement.prototype, 'defaultSelected', 'selected');
    _reflectBool(HTMLOptionElement.prototype, 'disabled');
    _reflectStr(HTMLOptionElement.prototype, 'label');

    // ---- <table> API ----
    // Bundled apps build their results with `tbody.insertRow()` /
    // `tr.insertCell()`; without them the first render throws and the whole
    // app stops. The parser produces real <tr>/<td> nodes, so the
    // collections are filtered views over the children.
    const _rowsOf = (section) => Array.prototype.filter.call(section.children, (c) => c.tagName === 'TR');
    const _cellsOf = (row) => Array.prototype.filter.call(row.children, (c) => c.tagName === 'TD' || c.tagName === 'TH');
    const _collection = (arr) => {
        arr.item = function (i) { return (i >= 0 && i < this.length) ? this[i] : null; };
        arr.namedItem = function (n) { return this.find((el) => el.id === n || el.getAttribute('name') === n) || null; };
        return arr;
    };
    const _indexError = (method, iface, index, max) => {
        const msg = `Failed to execute '${method}' on '${iface}': The index provided (${index}) is greater than or equal to the maximum allowed index (${max}).`;
        return typeof DOMException === 'function' ? new DOMException(msg, 'IndexSizeError') : new RangeError(msg);
    };
    const _nearest = (el, tag) => {
        for (let cur = el.parentElement; cur; cur = cur.parentElement) {
            if (cur.tagName === tag) return cur;
        }
        return null;
    };
    const _tableSections = (table) => {
        // The order `table.rows` uses: header, bodies, loose rows, footer.
        const out = [];
        for (const pass of ['THEAD', 'TBODY', 'TR', 'TFOOT']) {
            for (const c of table.children) if (c.tagName === pass) out.push(c);
        }
        return out;
    };
    const _def = (proto, name, get) => Object.defineProperty(proto, name, { get, enumerable: true, configurable: true });

    _def(HTMLTableElement.prototype, 'tBodies', function () {
        return _collection(Array.prototype.filter.call(this.children, (c) => c.tagName === 'TBODY'));
    });
    _def(HTMLTableElement.prototype, 'tHead', function () {
        return Array.prototype.find.call(this.children, (c) => c.tagName === 'THEAD') || null;
    });
    _def(HTMLTableElement.prototype, 'tFoot', function () {
        return Array.prototype.find.call(this.children, (c) => c.tagName === 'TFOOT') || null;
    });
    _def(HTMLTableElement.prototype, 'rows', function () {
        const rows = [];
        for (const s of _tableSections(this)) {
            if (s.tagName === 'TR') rows.push(s);
            else rows.push(..._rowsOf(s));
        }
        return _collection(rows);
    });
    HTMLTableElement.prototype.insertRow = function (index) {
        const bodies = this.tBodies;
        let body = bodies.length ? bodies[bodies.length - 1] : null;
        if (!body) {
            body = this.ownerDocument.createElement('tbody');
            const foot = Array.prototype.find.call(this.children, (c) => c.tagName === 'TFOOT');
            if (foot) this.insertBefore(body, foot);
            else this.appendChild(body);
        }
        return body.insertRow(index);
    };
    HTMLTableElement.prototype.deleteRow = function (index) {
        const rows = this.rows;
        if (index === -1) index = rows.length - 1;
        const row = rows[index];
        if (row && row.parentNode) row.parentNode.removeChild(row);
    };
    _def(HTMLTableSectionElement.prototype, 'rows', function () {
        return _collection(_rowsOf(this));
    });
    HTMLTableSectionElement.prototype.insertRow = function (index) {
        const rows = _rowsOf(this);
        const n = rows.length;
        if (index === undefined) index = -1;
        if (index < -1 || index > n) throw _indexError('insertRow', 'HTMLTableSectionElement', index, n);
        const tr = this.ownerDocument.createElement('tr');
        if (index === -1 || index === n) this.appendChild(tr);
        else this.insertBefore(tr, rows[index]);
        return tr;
    };
    HTMLTableSectionElement.prototype.deleteRow = function (index) {
        const rows = this.rows;
        if (index === -1) index = rows.length - 1;
        const row = rows[index];
        if (row && row.parentNode) row.parentNode.removeChild(row);
    };
    _def(HTMLTableRowElement.prototype, 'cells', function () {
        return _collection(_cellsOf(this));
    });
    _def(HTMLTableRowElement.prototype, 'rowIndex', function () {
        const table = _nearest(this, 'TABLE');
        return table ? table.rows.indexOf(this) : -1;
    });
    _def(HTMLTableRowElement.prototype, 'sectionRowIndex', function () {
        const p = this.parentElement;
        return p ? _rowsOf(p).indexOf(this) : -1;
    });
    HTMLTableRowElement.prototype.insertCell = function (index) {
        const cells = _cellsOf(this);
        const n = cells.length;
        if (index === undefined) index = -1;
        if (index < -1 || index > n) throw _indexError('insertCell', 'HTMLTableRowElement', index, n);
        const td = this.ownerDocument.createElement('td');
        if (index === -1 || index === n) this.appendChild(td);
        else this.insertBefore(td, cells[index]);
        return td;
    };
    HTMLTableRowElement.prototype.deleteCell = function (index) {
        const cells = this.cells;
        if (index === -1) index = cells.length - 1;
        const cell = cells[index];
        if (cell && cell.parentNode) cell.parentNode.removeChild(cell);
    };
    _def(HTMLTableCellElement.prototype, 'cellIndex', function () {
        const p = this.parentElement;
        return p ? _cellsOf(p).indexOf(this) : -1;
    });

    // ---- innerText / outerText (rendered text) ----
    // There is no layout to read text from; approximate it: skip content that
    // is not rendered, put line breaks around block boxes, collapse spaces.
    const _NOT_RENDERED = new Set(['SCRIPT', 'STYLE', 'NOSCRIPT', 'TEMPLATE', 'HEAD', 'TITLE', 'META', 'LINK', 'BASE', 'DATALIST']);
    const _BLOCK = new Set(['ADDRESS', 'ARTICLE', 'ASIDE', 'BLOCKQUOTE', 'BODY', 'CAPTION', 'DD', 'DETAILS', 'DIALOG', 'DIV', 'DL', 'DT', 'FIELDSET', 'FIGCAPTION', 'FIGURE', 'FOOTER', 'FORM', 'H1', 'H2', 'H3', 'H4', 'H5', 'H6', 'HEADER', 'HR', 'HTML', 'LI', 'MAIN', 'NAV', 'OL', 'OPTGROUP', 'OPTION', 'P', 'PRE', 'SECTION', 'TABLE', 'TBODY', 'THEAD', 'TFOOT', 'TR', 'UL']);
    const _preserveSpace = (el) => {
        for (let cur = el; cur && cur.tagName; cur = cur.parentElement) {
            if (cur.tagName === 'PRE' || cur.tagName === 'TEXTAREA') return true;
        }
        return false;
    };
    const _renderedText = (node, out) => {
        const type = node.nodeType;
        if (type === 3) { out.push(node.nodeValue || ''); return; }
        if (type !== 1) {
            if (type === 9 || type === 11) for (const c of node.childNodes) _renderedText(c, out);
            return;
        }
        const tag = node.tagName;
        if (_NOT_RENDERED.has(tag) || node.hidden) return;
        try {
            const cs = globalThis.getComputedStyle(node);
            if (cs && (cs.display === 'none' || cs.visibility === 'hidden')) return;
        } catch (_) {}
        if (tag === 'BR') { out.push('\n'); return; }
        const block = _BLOCK.has(tag);
        const cell = tag === 'TD' || tag === 'TH';
        if (block) out.push('\n');
        if (cell) out.push('\t');
        for (const c of node.childNodes) _renderedText(c, out);
        if (cell) out.push('\t');
        if (block) out.push('\n');
    };
    const _innerTextOf = (el) => {
        const out = [];
        _renderedText(el, out);
        const s = out.join('');
        if (_preserveSpace(el)) return s;
        return s.replace(/[ \t\f\r\n]*\n[ \t\f\r\n]*/g, '\n').replace(/[ \t\f\r\n]+/g, ' ').trim();
    };
    Object.defineProperty(HTMLElement.prototype, 'innerText', {
        get() { return _innerTextOf(this); },
        set(v) { this.textContent = String(v); },
        enumerable: true, configurable: true,
    });
    _def(HTMLElement.prototype, 'outerText', function () { return _innerTextOf(this); });

    // ---- <img>: start loading when `src` is set, report completion ----
    Object.defineProperty(HTMLImageElement.prototype, 'src', {
        get() {
            return Object.getOwnPropertyDescriptor(Element.prototype, 'src').get.call(this);
        },
        set(v) { this.setAttribute('src', String(v)); _loadImage(this); },
        enumerable: true, configurable: true,
    });
    _def(HTMLImageElement.prototype, 'complete', function () {
        return _imgState.get(this) !== 'loading';
    });

    class HTMLTemplateElement extends HTMLElement {}
    // Real browsers keep a template's children on `content`, not on the
    // <template> itself. Mirror that on first access. The move uses the raw
    // DOM op so nothing in the content runs, as in a real browser.
    const _templateContent = new WeakMap();
    _def(HTMLTemplateElement.prototype, 'content', function () {
        let frag = _templateContent.get(this);
        if (!frag) {
            frag = this.ownerDocument.createDocumentFragment();
            const fragId = _getNodeId(frag);
            let first = this.firstChild;
            while (first) {
                const next = first.nextSibling;
                try { ops.op_dom_append_child(fragId, _getNodeId(first)); } catch (_) {}
                first = next;
            }
            _templateContent.set(this, frag);
        }
        return frag;
    });
    class HTMLPreElement extends HTMLElement {}
    class HTMLQuoteElement extends HTMLElement {}

    // Tag → specific HTML*Element prototype map. Anything not listed falls
    // back to HTMLElement.prototype.
    const _tagToProto = {
        div: HTMLDivElement.prototype,
        span: HTMLSpanElement.prototype,
        p: HTMLParagraphElement.prototype,
        h1: HTMLHeadingElement.prototype,
        h2: HTMLHeadingElement.prototype,
        h3: HTMLHeadingElement.prototype,
        h4: HTMLHeadingElement.prototype,
        h5: HTMLHeadingElement.prototype,
        h6: HTMLHeadingElement.prototype,
        a: HTMLAnchorElement.prototype,
        img: HTMLImageElement.prototype,
        input: HTMLInputElement.prototype,
        form: HTMLFormElement.prototype,
        button: HTMLButtonElement.prototype,
        select: HTMLSelectElement.prototype,
        textarea: HTMLTextAreaElement.prototype,
        canvas: HTMLCanvasElement.prototype,
        script: HTMLScriptElement.prototype,
        style: HTMLStyleElement.prototype,
        link: HTMLLinkElement.prototype,
        meta: HTMLMetaElement.prototype,
        table: HTMLTableElement.prototype,
        iframe: HTMLIFrameElement.prototype,
        video: HTMLVideoElement.prototype,
        audio: HTMLAudioElement.prototype,
        body: HTMLBodyElement.prototype,
        head: HTMLHeadElement.prototype,
        html: HTMLHtmlElement.prototype,
        ul: HTMLUListElement.prototype,
        ol: HTMLOListElement.prototype,
        li: HTMLLIElement.prototype,
        tr: HTMLTableRowElement.prototype,
        td: HTMLTableCellElement.prototype,
        th: HTMLTableCellElement.prototype,
        thead: HTMLTableSectionElement.prototype,
        tbody: HTMLTableSectionElement.prototype,
        tfoot: HTMLTableSectionElement.prototype,
        label: HTMLLabelElement.prototype,
        option: HTMLOptionElement.prototype,
        template: HTMLTemplateElement.prototype,
        pre: HTMLPreElement.prototype,
        blockquote: HTMLQuoteElement.prototype,
        q: HTMLQuoteElement.prototype,
    };

    // Adjust an Element instance's prototype to the tag-specific subclass
    // so `el instanceof HTMLDivElement` works as in real Chrome.
    function _retargetElementProto(el) {
        try {
            const tag = ops.op_dom_get_tag_name(_getNodeId(el)).toLowerCase();
            const proto = _tagToProto[tag] || HTMLElement.prototype;
            Object.setPrototypeOf(el, proto);
        } catch {}
    }

    // ---- Attr / NamedNodeMap (fbgb fork) ----
    // One Attr object per (element, attribute name) while the attribute
    // exists (identity is stable, as in browsers). Detached Attr nodes
    // (createAttribute, removeAttributeNode) keep their own value.
    const _attrState = new WeakMap();      // Attr -> { owner, name, value }
    const _attrCache = new WeakMap();      // Element -> Map(name -> Attr)
    const _nnmCache = new WeakMap();       // Element -> NamedNodeMap proxy
    class Attr extends Node {
        constructor() { throw new TypeError('Illegal constructor'); }
        get nodeType() { return 2; }
        get name() { return _attrState.get(this).name; }
        get nodeName() { return this.name; }
        get localName() { return this.name; }
        get namespaceURI() { return null; }
        get prefix() { return null; }
        get specified() { return true; }
        get ownerElement() { return _attrState.get(this).owner; }
        get value() {
            const st = _attrState.get(this);
            if (!st.owner) return st.value;
            const v = st.owner.getAttribute(st.name);
            return v == null ? st.value : v;
        }
        set value(v) {
            const st = _attrState.get(this);
            st.value = String(v);
            if (st.owner) st.owner.setAttribute(st.name, st.value);
        }
        get nodeValue() { return this.value; }
        set nodeValue(v) { this.value = v; }
        get textContent() { return this.value; }
        set textContent(v) { this.value = v; }
        get parentNode() { return null; }
        get childNodes() { return new NodeList([]); }
        get firstChild() { return null; }
        get ownerDocument() { return _document; }
        get isConnected() { return false; }
        cloneNode() { return _newAttr(this.name, this.value, null); }
    }
    const _newAttr = (name, value, owner) => {
        const a = Object.create(Attr.prototype);
        _attrState.set(a, { owner, name, value: String(value == null ? '' : value) });
        return a;
    };
    const _attrNode = (el, name) => {
        let m = _attrCache.get(el);
        if (!m) { m = new Map(); _attrCache.set(el, m); }
        let a = m.get(name);
        if (!a) { a = _newAttr(name, el.getAttribute(name), el); m.set(name, a); }
        return a;
    };
    const _attrDetach = (el, name) => {
        const m = _attrCache.get(el);
        const a = m && m.get(name);
        if (!a) return _newAttr(name, el.getAttribute(name), null);
        const st = _attrState.get(a);
        st.value = el.getAttribute(name) || st.value;
        st.owner = null;
        m.delete(name);
        return a;
    };
    const _attrBind = (attr, el) => {
        const st = _attrState.get(attr);
        if (!st) return;
        st.owner = el;
        let m = _attrCache.get(el);
        if (!m) { m = new Map(); _attrCache.set(el, m); }
        m.set(st.name, attr);
    };
    class NamedNodeMap {
        constructor() { throw new TypeError('Illegal constructor'); }
    }
    const _nnmOwner = new WeakMap();
    const _nnmNames = (map) => ops.op_dom_get_attribute_names(_getNodeId(_nnmOwner.get(map)));
    Object.assign(NamedNodeMap.prototype, {
        item(i) { const n = _nnmNames(this)[i >>> 0]; return n === undefined ? null : _attrNode(_nnmOwner.get(this), n); },
        getNamedItem(name) { const el = _nnmOwner.get(this); const n = String(name).toLowerCase(); return el.hasAttribute(n) ? _attrNode(el, n) : null; },
        getNamedItemNS(_ns, name) { return this.getNamedItem(name); },
        setNamedItem(attr) { return _nnmOwner.get(this).setAttributeNode(attr); },
        setNamedItemNS(attr) { return this.setNamedItem(attr); },
        removeNamedItem(name) {
            const el = _nnmOwner.get(this); const n = String(name).toLowerCase();
            if (!el.hasAttribute(n)) throw new DOMException(`Failed to execute 'removeNamedItem' on 'NamedNodeMap': No item with name '${name}' was found.`, 'NotFoundError');
            const a = _attrDetach(el, n); el.removeAttribute(n); return a;
        },
        removeNamedItemNS(_ns, name) { return this.removeNamedItem(name); },
    });
    Object.defineProperty(NamedNodeMap.prototype, 'length', { get() { return _nnmNames(this).length; }, enumerable: true, configurable: true });
    Object.defineProperty(NamedNodeMap.prototype, Symbol.iterator, {
        value: function* values() { const el = _nnmOwner.get(this); for (const n of _nnmNames(this)) yield _attrNode(el, n); },
        writable: true, configurable: true,
    });
    Object.defineProperty(NamedNodeMap.prototype, Symbol.toStringTag, { value: 'NamedNodeMap', configurable: true });
    const _namedNodeMap = (el) => {
        let p = _nnmCache.get(el);
        if (p) return p;
        const target = Object.create(NamedNodeMap.prototype);
        _nnmOwner.set(target, el);
        const idx = (prop) => typeof prop === 'string' && /^\d+$/.test(prop);
        p = new Proxy(target, {
            get(t, prop, recv) {
                if (idx(prop)) { const n = _nnmNames(t)[+prop]; return n === undefined ? undefined : _attrNode(el, n); }
                if (typeof prop === 'string' && !(prop in t) && el.hasAttribute(prop)) return _attrNode(el, prop);
                const v = Reflect.get(t, prop, t);
                return v;
            },
            has(t, prop) {
                if (idx(prop)) return +prop < _nnmNames(t).length;
                return (prop in t) || (typeof prop === 'string' && el.hasAttribute(prop));
            },
            ownKeys(t) { const ns = _nnmNames(t); return ns.map((_, i) => String(i)).concat(ns.filter((n) => !(n in t))); },
            getOwnPropertyDescriptor(t, prop) {
                if (idx(prop)) { const n = _nnmNames(t)[+prop]; if (n !== undefined) return { value: _attrNode(el, n), writable: false, enumerable: true, configurable: true }; return undefined; }
                if (typeof prop === 'string' && !(prop in t) && el.hasAttribute(prop)) return { value: _attrNode(el, prop), writable: false, enumerable: false, configurable: true };
                return Reflect.getOwnPropertyDescriptor(t, prop);
            },
        });
        _nnmOwner.set(p, el);
        _nnmCache.set(el, p);
        return p;
    };
    globalThis.Attr = Attr;
    globalThis.NamedNodeMap = NamedNodeMap;

    class Text extends Node {
        get data() { return ops.op_dom_get_text_content(_getNodeId(this)); }
        set data(val) { ops.op_dom_set_text_content(_getNodeId(this), String(val)); }
        get length() { return this.data.length; }
        get wholeText() { return this.data; }
    }

    class Comment extends Node {
        get data() { return ops.op_dom_get_text_content(_getNodeId(this)); }
        set data(val) { ops.op_dom_set_text_content(_getNodeId(this), String(val)); }
    }

    class DocumentFragment extends Node {
        getElementById(id) {
            const want = String(id);
            const walk = (n) => {
                for (const c of n.childNodes) {
                    if (c.nodeType === 1) {
                        if (c.getAttribute('id') === want) return c;
                        const r = walk(c);
                        if (r) return r;
                    }
                }
                return null;
            };
            return walk(this);
        }
    }

    let _currentScript = null;
    function _setCurrentScript(el) { _currentScript = el; }

    // ---- Focus management ----
    // document.activeElement follows focus()/blur(); the focus events carry
    // relatedTarget and fire in Chrome's order: blur, focusout, focus, focusin.
    // UA-fired focus events are trusted in Chrome even when script calls
    // focus(), so they are minted through `_trust` (installed once by
    // event_bootstrap.js via the hook's one-shot `_setTrust`).
    // Held weakly, with Chrome's focus fixup: a focused element that leaves
    // the document stops being active (activeElement falls back to body).
    // A strong ref here kept every closed FB post dialog alive (its focused
    // node -> handlers -> the whole React tree: ~14 MB per dialog).
    let _activeRef = null;
    const _act = () => {
        const el = _activeRef && _activeRef.deref();
        if (!el) { _activeRef = null; return null; }
        if (el.isConnected === false) { _activeRef = null; return null; }
        return el;
    };
    const _setAct = (el) => { _activeRef = el ? new WeakRef(el) : null; };
    let _trust = (ev) => ev;
    const _FOCUSABLE_TAGS = new Set(['INPUT', 'TEXTAREA', 'SELECT', 'BUTTON', 'IFRAME', 'SUMMARY']);
    const _isFocusable = (el) => {
        if (!el || el.nodeType !== 1) return false;
        if (el.hasAttribute && el.hasAttribute('disabled') && _FOCUSABLE_TAGS.has(el.tagName)) return false;
        if (_FOCUSABLE_TAGS.has(el.tagName)) return !(el.tagName === 'INPUT' && (el.getAttribute('type') || '').toLowerCase() === 'hidden');
        if (el.tagName === 'A' && el.hasAttribute('href')) return true;
        if (el.hasAttribute && el.hasAttribute('tabindex')) return true;
        const ce = el.getAttribute && el.getAttribute('contenteditable');
        return ce === '' || ce === 'true' || ce === 'plaintext-only';
    };
    const _focusEvent = (type, bubbles, related) => {
        const C = globalThis.FocusEvent || Event;
        return _trust(new C(type, { bubbles, cancelable: false, composed: true, relatedTarget: related || null, view: globalThis }));
    };
    function _blurElement(el) {
        if (_act() !== el || !el) return;
        _setAct(null);
        try { el.dispatchEvent(_focusEvent('blur', false, null)); } catch (_) {}
        try { el.dispatchEvent(_focusEvent('focusout', true, null)); } catch (_) {}
    }
    function _focusElement(el, options) {
        // WebIDL FocusOptions conversion happens first, always (members read
        // in alphabetical order, like Chrome). Pages feature-detect
        // preventScroll with a getter on the options object (FB FocusManager).
        let preventScroll = false;
        if (options !== undefined && options !== null) {
            if (typeof options !== 'object' && typeof options !== 'function') throw new TypeError("Failed to execute 'focus' on 'HTMLElement': The provided value is not of type 'FocusOptions'.");
            void options.focusVisible;
            preventScroll = !!options.preventScroll;
        }
        if (!_isFocusable(el) || el.isConnected === false) return;
        if (!preventScroll) { try { el.scrollIntoView({ block: 'nearest', inline: 'nearest' }); } catch (_) {} }
        if (_act() === el) return;
        const prev = _act();
        _setAct(null);
        if (prev) {
            try { prev.dispatchEvent(_focusEvent('blur', false, el)); } catch (_) {}
            try { prev.dispatchEvent(_focusEvent('focusout', true, el)); } catch (_) {}
        }
        _setAct(el);
        try { el.dispatchEvent(_focusEvent('focus', false, prev)); } catch (_) {}
        try { el.dispatchEvent(_focusEvent('focusin', true, prev)); } catch (_) {}
    }

    class HTMLAllCollection {
        constructor(doc) {
            this._doc = doc;
        }
        get length() { return this._doc.querySelectorAll("*").length; }
        item(i) { return this._doc.querySelectorAll("*")[i] || null; }
        namedItem(n) {
            return this._doc.getElementById(n) || 
                   this._doc.querySelector(`[name="${CSS.escape(n)}"]`) || 
                   null;
        }
        [Symbol.iterator]() {
            const nodes = this._doc.querySelectorAll("*");
            let i = 0;
            return {
                next() {
                    return i < nodes.length ? { value: nodes[i++], done: false } : { value: undefined, done: true };
                },
                [Symbol.iterator]() { return this; }
            };
        }
    }

    class Document extends Node {
        constructor(nodeId) {
            // Forward the document node id to Node so _getNodeId returns
            // the real Rust-side Document. Without this, document.nodeType
            // resolved to 0 (the "no such node" sentinel), which broke
            // anything walking parentNode→isConnected. Phase 7 follow-up.
            super(nodeId);
            if (!globalThis.__browser_oxide) {
                Object.defineProperty(globalThis, '__browser_oxide', { value: {}, enumerable: false, configurable: true });
            }
            // Capture initial base URL from ops or a global hint
            globalThis.__browser_oxide._baseUrl = ops.op_dom_get_base_url && ops.op_dom_get_base_url();

            const all = new HTMLAllCollection(this);
            // Hide 'all' from enumeration but keep it truthy
            Object.defineProperty(this, 'all', {
                get() { return all; },
                enumerable: false,
                configurable: true
            });
        }
        get scripts() { return this.getElementsByTagName("script"); }
        get currentScript() { return _currentScript; }
        get visibilityState() { return "visible"; }
        get hidden() { return false; }
        get webkitVisibilityState() { return "visible"; }
        get webkitHidden() { return false; }
        get fullscreenEnabled() { return true; }
        get webkitFullscreenEnabled() { return true; }
        get webkitIsFullScreen() { return false; }

        get documentElement() {
            const els = ops.op_dom_get_child_elements(ops.op_dom_document_node());
            return els.length > 0 ? _wrapNode(els[0]) : null;
        }
        get head() { return this.querySelector("head"); }
        get body() { return this.querySelector("body"); }
        get title() {
            const el = this.querySelector("title");
            return el ? el.textContent : "";
        }
        set title(val) {
            let el = this.querySelector("title");
            if (el) { el.textContent = val; }
        }
        getElementById(id) {
            const nodeId = ops.op_dom_get_element_by_id(id);
            return nodeId !== null ? _wrapNode(nodeId) : null;
        }
        getElementsByTagName(tag) {
            return new NodeList(ops.op_dom_get_elements_by_tag_name(ops.op_dom_document_node(), tag));
        }
        getElementsByClassName(cls) {
            return new NodeList(ops.op_dom_get_elements_by_class_name(ops.op_dom_document_node(), cls));
        }
        querySelector(sel) {
            const id = ops.op_dom_query_selector(ops.op_dom_document_node(), sel);
            return id !== null ? _wrapNode(id) : null;
        }
        querySelectorAll(sel) {
            return new NodeList(ops.op_dom_query_selector_all(ops.op_dom_document_node(), sel));
        }
        createElement(tag) {
            const el = _wrapNode(ops.op_dom_create_element(tag));
            // Script elements created by script are "force-async" (HTML spec):
            // .async reads true until set, and they never block.
            if (String(tag).toLowerCase() === "script") _scriptForceAsync.add(el);
            return el;
        }
        createElementNS(ns, tag) {
            // For now, treat namespaced elements same as regular ones.
            return this.createElement(tag);
        }
        createTextNode(text) {
            return _wrapNode(ops.op_dom_create_text_node(text));
        }
        createDocumentFragment() {
            return _wrapNode(ops.op_dom_create_document_fragment());
        }
        createComment(text) {
            // Comment nodes have nodeType 8 in the DOM; use text node with special handling
            const id = ops.op_dom_create_text_node(""); // TODO: proper comment op
            return _wrapNode(id);
        }
        createEvent(type) {
            // Legacy factory per DOM spec: case-insensitive interface table,
            // NotSupportedError otherwise. Chrome only accepts TouchEvent when
            // touch is enabled (maxTouchPoints > 0) — desktop throws.
            const t = String(type).toLowerCase();
            const map = {
                event: 'Event', events: 'Event', htmlevents: 'Event', svgevents: 'Event',
                customevent: 'CustomEvent', uievent: 'UIEvent', uievents: 'UIEvent',
                mouseevent: 'MouseEvent', mouseevents: 'MouseEvent',
                keyboardevent: 'KeyboardEvent', focusevent: 'FocusEvent',
                compositionevent: 'CompositionEvent', textevent: 'TextEvent',
                messageevent: 'MessageEvent', storageevent: 'StorageEvent',
                hashchangeevent: 'HashChangeEvent', beforeunloadevent: 'BeforeUnloadEvent',
                devicemotionevent: 'DeviceMotionEvent', deviceorientationevent: 'DeviceOrientationEvent',
                dragevent: 'DragEvent', touchevent: 'TouchEvent',
            };
            const name = map[t];
            const touchOk = name !== 'TouchEvent' || (globalThis.navigator && navigator.maxTouchPoints > 0);
            const C = name && touchOk && globalThis[name];
            if (typeof C !== 'function') {
                throw new DOMException("Failed to execute 'createEvent' on 'Document': The provided event type (\"" + type + "\") is invalid.", 'NotSupportedError');
            }
            let e;
            try { e = new C(''); } catch (_) { e = new Event(''); }
            return e;
        }
        createRange() {
            return new Range();
        }
        createTreeWalker(root, whatToShow, filter) {
            return { currentNode: root, nextNode() { return null; }, previousNode() { return null; } };
        }
        createNodeIterator(root, whatToShow, filter) {
            return { nextNode() { return null; }, previousNode() { return null; } };
        }
        importNode(node, deep) { return node.cloneNode(deep); }
        adoptNode(node) {
            // Detach from current parent, adopt into this document
            if (node.parentNode) node.parentNode.removeChild(node);
            return node;
        }
        createAttribute(name) {
            return _newAttr(String(name).toLowerCase(), "", null);
        }
        getElementsByName(name) {
            const n = String(name);
            const all = this.getElementsByTagName('*');
            const out = [];
            for (let i = 0; i < all.length; i++) { if (all[i].getAttribute && all[i].getAttribute('name') === n) out.push(all[i]); }
            return new NodeList(out);
        }
        // document.open/close — reset and finalize document stream
        open() { return this; }
        close() {}
        write(html) {
            // Document.write in Chrome synchronously executes any <script> tags
            // it inserts. Since op_dom_document_write returns the IDs of the
            // newly created nodes, we wrap them and trigger our insertion logic.
            // A script run by the parser writes right after itself, into its
            // parent's context (so `<td>` written inside a `<tr>` is a cell).
            let after = -1;
            let context = '';
            const current = _currentScript;
            if (current && current.parentNode) {
                after = _getNodeId(current);
                context = (current.parentNode.localName || '').toLowerCase();
            }
            const newIds = ops.op_dom_document_write(String(html), after, context);
            if (Array.isArray(newIds)) {
                for (const id of newIds) {
                    const node = _wrapNode(id);
                    if (node) _onNodeInserted(node, true); // Always sync for document.write
                }
            }
        }
        writeln(html) {
            this.write(html + "\n");
        }
        // Selection and editing
        execCommand(command, showUI, value) { return false; }
        queryCommandSupported(command) { return false; }
        queryCommandEnabled(command) { return false; }
        getSelection() { return globalThis.getSelection ? globalThis.getSelection() : null; }
        // Point-based queries. Per spec, a point OUTSIDE the viewport
        // (negative, or >= innerWidth/innerHeight) returns null / []. Real
        // Chrome returns null for elementFromPoint(-1,-1) and (99999,99999);
        // the previous unconditional `return this.body` differed from
        // real Chrome's layout behaviour for out-of-bounds points.
        // We lack full layout, so an in-viewport
        // point still approximates the topmost element with body (falling back
        // to documentElement) — but the viewport-bounds null result, which is
        // the detectable behaviour, is now spec-correct.
        _pointInViewport(x, y) {
            x = +x; y = +y;
            const w = globalThis.innerWidth || 0;
            const h = globalThis.innerHeight || 0;
            return x >= 0 && y >= 0 && x < w && y < h;
        }
        elementFromPoint(x, y) {
            if (!this._pointInViewport(x, y)) return null;
            return this.body || this.documentElement || null;
        }
        elementsFromPoint(x, y) {
            if (!this._pointInViewport(x, y)) return [];
            return this.body ? [this.body] : [];
        }
        caretPositionFromPoint(x, y) { return null; }
        hasFocus() { return true; }  // Anti-bot: must return true
        get readyState() { 
            return (globalThis._browser_oxide && globalThis._browser_oxide.__documentReadyState) || "complete"; 
        }
        get URL() { return globalThis.location?.href || "about:blank"; }
        get documentURI() { return this.URL; }
        get domain() { return globalThis.location?.hostname || ""; }
        get location() { return globalThis.location; }
        set location(val) { if (globalThis.location) globalThis.location.href = val; }
        get referrer() { return ""; }
        get hidden() { return false; }
        get visibilityState() { return "visible"; }
        get cookie() {
            // Unified cookie jar: returns the mirror of net::cookies for this origin.
            // The mirror is refreshed synchronously on every page navigation and after
            // each fetch() response via _syncCookiesFromNet().
            if (!globalThis.__jsCookies) globalThis.__jsCookies = {};
            return Object.entries(globalThis.__jsCookies)
                .map(([k, v]) => `${k}=${v}`)
                .join("; ");
        }
        set cookie(val) {
            // Parse "name=value; path=/; ..." — update local mirror AND push to net::cookies.
            if (!globalThis.__jsCookies) globalThis.__jsCookies = {};
            const parts = String(val).split(";");
            const [name, ...rest] = (parts[0] || "").split("=");
            const key = name.trim();
            const value = rest.join("=").trim();
            if (!key) return;
            // Check for max-age=0 or expires in the past (delete cookie)
            const lower = String(val).toLowerCase();
            if (lower.includes("max-age=0") || lower.includes("max-age=-")) {
                delete globalThis.__jsCookies[key];
            } else {
                globalThis.__jsCookies[key] = value;
            }
            // Fire-and-forget propagation to the net layer.
            try {
                let url = globalThis.location?.href;
                if (!url || url === "about:blank" || url === "javascript:;" || url === "") {
                    url = globalThis.__browser_oxide && globalThis.__browser_oxide._baseUrl;
                }
                if (url) {
                    // Persist into the Rust
                    // jar SYNCHRONOUSLY. The async op_cookie_set was
                    // fire-and-forget, so a cookie set in the last microtasks
                    // before location.reload() (e.g. a challenge token) was
                    // lost — the reload re-fetched the stub. op_cookie_set_sync
                    // writes immediately (try_lock) with an async fallback.
                    if (ops.op_cookie_set_sync) {
                        ops.op_cookie_set_sync(url, String(val));
                    } else if (ops.op_cookie_set) {
                        ops.op_cookie_set(url, String(val));
                    }
                }
            } catch (e) { /* ignore */ }
        }
        // HTML legacy default per HTML Standard §2.4 — Chrome reports
        // "windows-1252" for HTML documents without an explicit
        // `<meta charset>` declaration. Verified against a real browser
        // (which reports "windows-1252").
        get characterSet() { return "windows-1252"; }
        get charset() { return "windows-1252"; }
        get contentType() { return "text/html"; }
        get compatMode() { return "CSS1Compat"; }
        // document.implementation — the DOMImplementation API (see getter below).
        get implementation() {
            if (_domImplementation) return _domImplementation;
            const owner = this;
            // Build a detached document from real nodes of this document's
            // arena. jQuery (so every jQuery site) feature-tests
            // `createHTMLDocument("").body.childNodes.length` while loading and
            // throws on a stub, which takes the whole page's scripts with it.
            const buildDocument = (rootTag, title) => {
                const rootEl = owner.createElement(rootTag);
                const headEl = owner.createElement("head");
                const bodyEl = owner.createElement("body");
                const titleEl = owner.createElement("title");
                titleEl.textContent = title == null ? "" : String(title);
                headEl.appendChild(titleEl);
                rootEl.appendChild(headEl);
                rootEl.appendChild(bodyEl);
                return {
                    nodeType: 9,
                    ownerDocument: null,
                    defaultView: null,
                    documentElement: rootEl,
                    head: headEl,
                    body: bodyEl,
                    get title() { return titleEl.textContent; },
                    set title(v) { titleEl.textContent = v == null ? "" : String(v); },
                    get childNodes() { return [rootEl]; },
                    get children() { return [rootEl]; },
                    get firstChild() { return rootEl; },
                    get lastChild() { return rootEl; },
                    get activeElement() { return bodyEl; },
                    get readyState() { return "complete"; },
                    get compatMode() { return "CSS1Compat"; },
                    get contentType() { return "text/html"; },
                    get characterSet() { return "windows-1252"; },
                    get charset() { return "windows-1252"; },
                    get URL() { return "about:blank"; },
                    get documentURI() { return "about:blank"; },
                    get referrer() { return ""; },
                    get domain() { return ""; },
                    get cookie() { return ""; },
                    set cookie(v) { /* ignore */ },
                    get images() { return bodyEl.getElementsByTagName("img"); },
                    get scripts() { return bodyEl.getElementsByTagName("script"); },
                    get forms() { return bodyEl.getElementsByTagName("form"); },
                    get links() { return bodyEl.getElementsByTagName("a"); },
                    get implementation() { return owner.implementation; },
                    createElement(tag) { return owner.createElement(tag); },
                    createElementNS(ns, tag) { return owner.createElementNS(ns, tag); },
                    createTextNode(data) { return owner.createTextNode(data); },
                    createComment(data) { return owner.createComment(data); },
                    createDocumentFragment() { return owner.createDocumentFragment(); },
                    createAttribute(name) { return owner.createAttribute(name); },
                    createEvent(type) { return owner.createEvent(type); },
                    createRange() { return owner.createRange(); },
                    createTreeWalker(root, whatToShow, filter) { return owner.createTreeWalker(root, whatToShow, filter); },
                    createNodeIterator(root, whatToShow, filter) { return owner.createNodeIterator(root, whatToShow, filter); },
                    getElementsByTagName(name) { return rootEl.getElementsByTagName(name); },
                    getElementsByTagNameNS(ns, name) { return rootEl.getElementsByTagNameNS(ns, name); },
                    getElementsByClassName(name) { return rootEl.getElementsByClassName(name); },
                    getElementById(id) {
                        try { return rootEl.querySelector("#" + id); } catch (e) { return null; }
                    },
                    querySelector(sel) { return rootEl.querySelector(sel); },
                    querySelectorAll(sel) { return rootEl.querySelectorAll(sel); },
                    hasFocus() { return false; },
                    addEventListener() {},
                    removeEventListener() {},
                    dispatchEvent() { return true; },
                    write() {},
                    writeln() {},
                    open() { return this; },
                    close() {},
                    importNode(node, deep) { return node.cloneNode(deep); },
                    adoptNode(node) { return node; },
                };
            };
            _domImplementation = {
                createHTMLDocument(title) { return buildDocument("html", title); },
                createDocument(ns, qualifiedName, doctype) { return buildDocument(qualifiedName || "html", ""); },
                createDocumentType(qualifiedName, publicId, systemId) {
                    return {
                        nodeType: 10, nodeName: qualifiedName, name: qualifiedName,
                        publicId, systemId, ownerDocument: null, parentNode: null,
                    };
                },
                hasFeature() { return true; },
            };
            return _domImplementation;
        }
        get doctype() { return null; }
        get defaultView() { return globalThis; }
        get activeElement() { return _act() || this.body; }
        get scripts() { return this.getElementsByTagName("script"); }
        get forms() { return this.getElementsByTagName("form"); }
        get images() { const list = this.getElementsByTagName("img"); _scanImages(this); return list; }
        get links() { return this.getElementsByTagName("a"); }
        get embeds() { return this.getElementsByTagName("embed"); }
        get anchors() { return this.querySelectorAll("a[name]"); }
        get styleSheets() {
            const count = ops.op_dom_get_stylesheet_count();
            const sheets = [];
            for (let i = 0; i < count; i++) {
                sheets.push(new CSSStyleSheet(i));
            }
            return sheets;
        }
        get fullscreenElement() { return null; }
        get pointerLockElement() { return null; }
        exitFullscreen() { return Promise.resolve(); }
        exitPointerLock() {}
    }

    // --- CSSOM ---
    class CSSStyleSheet {
        constructor(index) { this._index = index; }
        get type() { return "text/css"; }
        get disabled() { return false; }
        get ownerNode() { return null; }
        get parentStyleSheet() { return null; }
        get title() { return null; }
        get media() { return { length: 0, mediaText: "" }; }
        get cssRules() {
            const raw = ops.op_dom_get_stylesheet_rules(this._index);
            return raw.map(r => new CSSStyleRule(r));
        }
        get rules() { return this.cssRules; }
        insertRule(_rule, _index) { return 0; }
        deleteRule(_index) {}
    }

    class CSSStyleRule {
        constructor({ selector_text, css_text, rule_type }) {
            this.selectorText = selector_text;
            this.cssText = css_text;
            this.type = rule_type;
            // Parse declarations into style-like object
            const styleObj = {};
            const declMatch = css_text.match(/\{([^}]*)\}/);
            if (declMatch) {
                for (const part of declMatch[1].split(";")) {
                    const [prop, ...vals] = part.split(":");
                    if (prop && vals.length) {
                        const p = prop.trim();
                        const v = vals.join(":").trim();
                        styleObj[p] = v;
                        // Also set camelCase version
                        const camel = p.replace(/-([a-z])/g, (_, c) => c.toUpperCase());
                        if (camel !== p) styleObj[camel] = v;
                    }
                }
            }
            this.style = styleObj;
        }
    }

    // --- Range (minimal) ---
    class Range {
        constructor() {
            this.startContainer = null; this.startOffset = 0;
            this.endContainer = null; this.endOffset = 0;
            this.collapsed = true; this.commonAncestorContainer = null;
        }
        setStart(node, offset) { this.startContainer = node; this.startOffset = offset; this.collapsed = false; }
        setEnd(node, offset) { this.endContainer = node; this.endOffset = offset; }
        collapse(toStart) { this.collapsed = true; }
        cloneRange() { return new Range(); }
        getBoundingClientRect() { return new DOMRect(); }
        getClientRects() { return []; }
        createContextualFragment(html) {
            const div = _document.createElement("div");
            div.innerHTML = html;
            const frag = _document.createDocumentFragment();
            while (div.firstChild) frag.appendChild(div.firstChild);
            return frag;
        }
        toString() { return ""; }
    }

    // --- Selection (minimal) ---
    class Selection {
        get anchorNode() { return null; }
        get anchorOffset() { return 0; }
        get focusNode() { return null; }
        get focusOffset() { return 0; }
        get isCollapsed() { return true; }
        get rangeCount() { return 0; }
        getRangeAt(i) { return new Range(); }
        addRange(range) {}
        removeRange(range) {}
        removeAllRanges() {}
        collapse(node, offset) {}
        toString() { return ""; }
    }
    const _selection = new Selection();

    // Create the global document
    const _document = new Document(ops.op_dom_document_node());
    _nodeCache.set(ops.op_dom_document_node(), new WeakRef(_document));

    // Set globals
    // Symbol.toStringTag on every DOM class — some scripts
    // check Object.prototype.toString.call(node) and expect the Chrome
    // WebIDL brand name like "[object HTMLDivElement]". Without these
    // tags every node shows as "[object Object]", which differs from
    // real Chrome.
    const _tag = (cls, name) => {
        try {
            Object.defineProperty(cls.prototype, Symbol.toStringTag, {
                value: name, configurable: true,
            });
        } catch {}
    };
    _tag(EventTarget, "EventTarget");
    _tag(Node, "Node");
    _tag(Element, "Element");
    _tag(HTMLElement, "HTMLElement");
    _tag(HTMLDivElement, "HTMLDivElement");
    _tag(HTMLSpanElement, "HTMLSpanElement");
    _tag(HTMLParagraphElement, "HTMLParagraphElement");
    _tag(HTMLHeadingElement, "HTMLHeadingElement");
    _tag(HTMLAnchorElement, "HTMLAnchorElement");
    _tag(HTMLImageElement, "HTMLImageElement");
    _tag(HTMLInputElement, "HTMLInputElement");
    _tag(HTMLFormElement, "HTMLFormElement");
    _tag(HTMLButtonElement, "HTMLButtonElement");
    _tag(HTMLSelectElement, "HTMLSelectElement");
    _tag(HTMLTextAreaElement, "HTMLTextAreaElement");
    _tag(HTMLCanvasElement, "HTMLCanvasElement");
    _tag(HTMLScriptElement, "HTMLScriptElement");
    // ParentNode mixin (DOM §4.2.6) on Document and DocumentFragment.
    for (const _C of [DocumentFragment, Document]) {
        for (const _k of ["append", "prepend", "replaceChildren", "children", "firstElementChild",
                          "lastElementChild", "childElementCount", "querySelector", "querySelectorAll"]) {
            if (Object.prototype.hasOwnProperty.call(_C.prototype, _k)) continue;
            const _d = Object.getOwnPropertyDescriptor(Element.prototype, _k);
            if (_d) Object.defineProperty(_C.prototype, _k, _d);
        }
    }
    _tag(HTMLStyleElement, "HTMLStyleElement");
    _tag(HTMLLinkElement, "HTMLLinkElement");
    _tag(HTMLMetaElement, "HTMLMetaElement");
    _tag(HTMLTableElement, "HTMLTableElement");
    _tag(HTMLIFrameElement, "HTMLIFrameElement");
    _tag(HTMLVideoElement, "HTMLVideoElement");
    _tag(HTMLAudioElement, "HTMLAudioElement");
    _tag(HTMLBodyElement, "HTMLBodyElement");
    _tag(HTMLHeadElement, "HTMLHeadElement");
    _tag(HTMLHtmlElement, "HTMLHtmlElement");
    _tag(HTMLUListElement, "HTMLUListElement");
    _tag(HTMLOListElement, "HTMLOListElement");
    _tag(HTMLLIElement, "HTMLLIElement");
    _tag(HTMLTableRowElement, "HTMLTableRowElement");
    _tag(HTMLTableCellElement, "HTMLTableCellElement");
    _tag(HTMLTableSectionElement, "HTMLTableSectionElement");
    _tag(HTMLLabelElement, "HTMLLabelElement");
    _tag(HTMLOptionElement, "HTMLOptionElement");
    _tag(HTMLTemplateElement, "HTMLTemplateElement");
    _tag(HTMLPreElement, "HTMLPreElement");
    _tag(HTMLQuoteElement, "HTMLQuoteElement");
    _tag(Text, "Text");
    _tag(Comment, "Comment");
    _tag(DocumentFragment, "DocumentFragment");
    // Chrome exposes document as HTMLDocument (which extends Document).
    _tag(Document, "HTMLDocument");
    _tag(NodeList, "NodeList");
    _tag(DOMTokenList, "DOMTokenList");

    // documentElement (HTMLHtmlElement) and body (HTMLBodyElement) layout
    // dimensions in standards mode are viewport-clipped, NOT full document.
    // Default Element getters return offsetWidth/Height = full document
    // (e.g. 1914 × 28638 on some sites) which differs from real Chrome.
    // Real Chrome returns innerWidth × innerHeight (1440 × 789 on a typical
    // macOS 1440x900 viewport).
    {
        const _viewportW = () => (globalThis.innerWidth | 0) || 1440;
        const _viewportH = () => (globalThis.innerHeight | 0) || 789;
        Object.defineProperty(HTMLHtmlElement.prototype, 'clientWidth',  { get() { return _viewportW(); }, configurable: true });
        Object.defineProperty(HTMLHtmlElement.prototype, 'clientHeight', { get() { return _viewportH(); }, configurable: true });
        // documentElement.scrollWidth/Height are still full content size,
        // so leave the inherited offset-based getters in place for those.
    }

    globalThis.document = _document;
    globalThis.Document = Document;
    globalThis.HTMLDocument = Document;
    globalThis.Node = Node;
    globalThis.Element = Element;
    // Expose the real HTMLElement subclasses — the prototype chain is
    // EventTarget ← Node ← Element ← HTMLElement ← HTML*Element so that
    // `el instanceof HTMLDivElement` etc. works as in real Chrome.
    globalThis.HTMLElement = HTMLElement;
    globalThis.HTMLDivElement = HTMLDivElement;
    globalThis.HTMLSpanElement = HTMLSpanElement;
    globalThis.HTMLParagraphElement = HTMLParagraphElement;
    globalThis.HTMLHeadingElement = HTMLHeadingElement;
    globalThis.HTMLAnchorElement = HTMLAnchorElement;
    globalThis.HTMLImageElement = HTMLImageElement;
    globalThis.HTMLInputElement = HTMLInputElement;
    globalThis.HTMLFormElement = HTMLFormElement;
    globalThis.HTMLButtonElement = HTMLButtonElement;
    globalThis.HTMLSelectElement = HTMLSelectElement;
    globalThis.HTMLTextAreaElement = HTMLTextAreaElement;
    globalThis.HTMLCanvasElement = HTMLCanvasElement;
    globalThis.HTMLScriptElement = HTMLScriptElement;
    globalThis.HTMLStyleElement = HTMLStyleElement;
    globalThis.HTMLLinkElement = HTMLLinkElement;
    globalThis.HTMLMetaElement = HTMLMetaElement;
    globalThis.HTMLTableElement = HTMLTableElement;
    globalThis.HTMLIFrameElement = HTMLIFrameElement;
    globalThis.HTMLVideoElement = HTMLVideoElement;
    globalThis.HTMLAudioElement = HTMLAudioElement;
    globalThis.HTMLBodyElement = HTMLBodyElement;
    globalThis.HTMLHeadElement = HTMLHeadElement;
    globalThis.HTMLHtmlElement = HTMLHtmlElement;
    globalThis.HTMLUListElement = HTMLUListElement;
    globalThis.HTMLOListElement = HTMLOListElement;
    globalThis.HTMLLIElement = HTMLLIElement;
    globalThis.HTMLTableRowElement = HTMLTableRowElement;
    globalThis.HTMLTableCellElement = HTMLTableCellElement;
    globalThis.HTMLTableSectionElement = HTMLTableSectionElement;
    globalThis.HTMLLabelElement = HTMLLabelElement;
    globalThis.HTMLOptionElement = HTMLOptionElement;
    globalThis.HTMLTemplateElement = HTMLTemplateElement;
    globalThis.HTMLPreElement = HTMLPreElement;
    globalThis.HTMLQuoteElement = HTMLQuoteElement;
    globalThis.SVGElement = Element;
    globalThis.Text = Text;
    globalThis.Comment = Comment;
    globalThis.DocumentFragment = DocumentFragment;
    globalThis.Document = Document;
    globalThis.NodeList = NodeList;
    globalThis.DOMTokenList = DOMTokenList;
    globalThis.DOMRect = DOMRect;
    globalThis.DOMRectReadOnly = DOMRect;
    globalThis.Range = Range;
    globalThis.Selection = Selection;
    globalThis.getSelection = function() { return _selection; };

    // Image constructor — new Image(width, height). Returns an
    // HTMLImageElement whose naturalWidth/naturalHeight/complete are
    // accessors defined on the prototype (getters; not writable).
    // Constructor return of an object is the caller's `new Image(...)`.
    globalThis.Image = function Image(width, height) {
        const el = _document.createElement("img");
        if (width !== undefined) el.setAttribute("width", String(width));
        if (height !== undefined) el.setAttribute("height", String(height));
        return el;
    };

    // DOMParser
    globalThis.DOMParser = class DOMParser {
        parseFromString(str, type) {
            // Returns a minimal document-like object
            const frag = _document.createElement("div");
            frag.innerHTML = str;
            return {
                documentElement: frag,
                body: frag,
                querySelector(sel) { return frag.querySelector(sel); },
                querySelectorAll(sel) { return frag.querySelectorAll(sel); },
                getElementById(id) { return frag.querySelector("#" + id); },
            };
        }
    };

    // --- MutationObserver (real implementation) ---
    const _moObservers = []; // { observer, target, options }

    class MutationRecord {
        constructor(type, target) {
            this.type = type;
            this.target = target;
            this.addedNodes = [];
            this.removedNodes = [];
            this.attributeName = null;
            this.oldValue = null;
            this.previousSibling = null;
            this.nextSibling = null;
        }
    }

    class MutationObserver {
        constructor(callback) {
            this._callback = callback;
            this._records = [];
            this._active = false;
            this._targets = new Map(); // nodeId → options
        }
        observe(target, options = {}) {
            const nodeId = _getNodeId(target);
            this._targets.set(nodeId, { target, options });
            this._active = true;
            _moObservers.push(this);
        }
        disconnect() {
            this._active = false;
            this._targets.clear();
            const idx = _moObservers.indexOf(this);
            if (idx !== -1) _moObservers.splice(idx, 1);
        }
        takeRecords() {
            const r = this._records.slice();
            this._records = [];
            return r;
        }
        _notify(record) {
            if (!this._active) return;
            this._records.push(record);
            // Schedule microtask to deliver
            if (this._records.length === 1) {
                Promise.resolve().then(() => {
                    if (!this._active) return;
                    const batch = this._records.slice();
                    this._records = [];
                    if (batch.length > 0) this._callback(batch, this);
                });
            }
        }
    }

    // Notify matching observers of a mutation
    function _notifyMO(type, targetNodeId, init) {
        for (const obs of _moObservers) {
            if (!obs._active) continue;
            // Check if this observer watches this target (or subtree ancestor)
            let matched = obs._targets.has(targetNodeId);
            if (!matched) {
                // Check subtree: walk ancestors
                for (const [watchedId, { options }] of obs._targets) {
                    if (options.subtree) {
                        // Walk up from targetNodeId to see if watchedId is ancestor
                        let nid = targetNodeId;
                        while (nid !== -1 && nid !== null) {
                            if (nid === watchedId) { matched = true; break; }
                            nid = ops.op_dom_get_parent(nid);
                        }
                    }
                    if (matched) break;
                }
            }
            if (!matched) continue;

            // Check options match
            const opts = obs._targets.get(targetNodeId)?.options ||
                         [...obs._targets.values()].find(v => v.options.subtree)?.options || {};
            if (type === "childList" && !opts.childList) continue;
            if (type === "attributes" && !opts.attributes) continue;
            if (type === "characterData" && !opts.characterData) continue;

            const record = new MutationRecord(type, init.target || null);
            if (init.addedNodes) record.addedNodes = init.addedNodes;
            if (init.removedNodes) record.removedNodes = init.removedNodes;
            if (init.attributeName) record.attributeName = init.attributeName;
            obs._notify(record);
        }
    }

    // Custom element lifecycle helper
    function _ceConnected(el) {
        if (el && el._ceUpgraded && typeof el.connectedCallback === "function") {
            try { el.connectedCallback(); } catch (e) { console.error(e); }
        }
    }
    function _ceDisconnected(el) {
        if (el && el._ceUpgraded && typeof el.disconnectedCallback === "function") {
            try { el.disconnectedCallback(); } catch (e) { console.error(e); }
        }
    }

    // Window frame registry: tracks appended iframes so window[0], window[1], etc.
    // work correctly. Some scripts access window.frames[0].navigator.webdriver
    // (which is window[0] since frames===window in our engine). Without this,
    // window[0] is undefined → TypeError "Cannot read properties of undefined
    // (reading 'webdriver')".
    const _appendedIframes = [];

    // Wrap DOM mutation methods to fire MO notifications
    const _origAppendChild = Node.prototype.appendChild;
    Node.prototype.appendChild = function(child) {
        const result = _origAppendChild.call(this, child);
        if (_moObservers.length > 0) {
            _notifyMO("childList", _getNodeId(this), { target: this, addedNodes: [child] });
        }
        return result;
    };

    const _origRemoveChild = Node.prototype.removeChild;
    Node.prototype.removeChild = function(child) {
        const result = _origRemoveChild.call(this, child);
        if (_moObservers.length > 0) {
            _notifyMO("childList", _getNodeId(this), { target: this, removedNodes: [child] });
        }
        return result;
    };

    const _origInsertBefore = Node.prototype.insertBefore;
    Node.prototype.insertBefore = function(newChild, refChild) {
        const result = _origInsertBefore.call(this, newChild, refChild);
        if (_moObservers.length > 0) {
            _notifyMO("childList", _getNodeId(this), { target: this, addedNodes: [newChild] });
        }
        return result;
    };

    const _origSetAttribute = Element.prototype.setAttribute;
    Element.prototype.setAttribute = function(name, value) {
        const oldVal = this.getAttribute(name);
        _origSetAttribute.call(this, name, value);
        if (this.tagName === 'IFRAME' && String(name).toLowerCase() === 'src') { try { _maybeLoadFrame(this); } catch (_e) {} }
        if (_moObservers.length > 0) {
            _notifyMO("attributes", _getNodeId(this), { target: this, attributeName: name });
        }
        // Custom element attributeChangedCallback
        if (this._ceUpgraded && typeof this.attributeChangedCallback === "function") {
            const observed = this.constructor.observedAttributes;
            if (Array.isArray(observed) && observed.includes(name)) {
                try { this.attributeChangedCallback(name, oldVal, value); } catch (e) { console.error(e); }
            }
        }
    };

    const _origRemoveAttribute = Element.prototype.removeAttribute;
    Element.prototype.removeAttribute = function(name) {
        const oldVal = this.getAttribute(name);
        _origRemoveAttribute.call(this, name);
        if (_moObservers.length > 0) {
            _notifyMO("attributes", _getNodeId(this), { target: this, attributeName: name });
        }
        // Custom element attributeChangedCallback
        if (this._ceUpgraded && typeof this.attributeChangedCallback === "function") {
            const observed = this.constructor.observedAttributes;
            if (Array.isArray(observed) && observed.includes(name)) {
                try { this.attributeChangedCallback(name, oldVal, null); } catch (e) { console.error(e); }
            }
        }
    };

    // Element.remove() also triggers childList on parent
    const _origRemove = Element.prototype.remove;
    Element.prototype.remove = function() {
        const parent = this.parentNode;
        _ceDisconnected(this);
        _origRemove.call(this);
        if (_moObservers.length > 0 && parent) {
            _notifyMO("childList", _getNodeId(parent), { target: parent, removedNodes: [this] });
        }
    };


    // ---- Real iframe documents (fbgb fork; Rust side: frames.rs) ----------
    // An http(s) <iframe> gets its own document in a separate runtime. From
    // here it is reachable only the way a cross-origin frame is in Chrome: a
    // window stub whose postMessage/closed/frames/length/location/parent/top
    // work and every other property throws SecurityError. Messages and the
    // element's `load` event arrive through the hook (deliverMessage /
    // frameLoaded) when the top page's event loop services the frames.
    const _hookF = globalThis[Symbol.for('__browser_oxide_script_hook__')];
    const _dynFrameOf = new WeakMap();   // <iframe> -> { id, src }
    const _dynFrameEls = new Map();      // id -> <iframe> (frames this document created)
    const _remoteWins = new Map();       // id -> window stub
    let _selfFrame = { id: 0, parent: -1 };
    const _frameOps = () => typeof ops.op_frame_request === 'function';
    const _connected = (el) => { for (let n = el; n; n = n.parentNode) { if (n.nodeType === 9) return true; } return false; };
    const _xoError = () => new DOMException('Blocked a frame with origin "' + ((globalThis.location && globalThis.location.origin) || 'null')
        + '" from accessing a cross-origin frame.', 'SecurityError');

    function _remoteWindow(id) {
        if (id === _selfFrame.id) return globalThis;
        let w = _remoteWins.get(id);
        if (w) return w;
        const postMessage = ({ postMessage(message, targetOrigin, transfer) {
            let to = targetOrigin;
            if (to && typeof to === 'object') to = to.targetOrigin;
            if (to === undefined) to = '/';
            let json;
            try { json = JSON.stringify(message === undefined ? null : message); }
            catch (_e) { throw new DOMException("Failed to execute 'postMessage' on 'Window': could not be cloned.", 'DataCloneError'); }
            ops.op_frame_post(id, json === undefined ? 'null' : json, String(to));
        } }).postMessage;
        const noop = (n) => ({ [n]() {} })[n];
        const loc = Object.create(null);
        Object.defineProperty(loc, 'href', { set(_v) {}, configurable: true });
        loc.replace = noop('replace');
        const isChild = () => _dynFrameEls.has(id);
        const allowed = {
            postMessage, close: noop('close'), focus: noop('focus'), blur: noop('blur'),
            get closed() { return !_remoteWins.has(id); },
            get frames() { return w; }, get self() { return w; }, get window() { return w; },
            get length() { return 0; }, get opener() { return null; },
            get parent() { return isChild() ? globalThis : globalThis.top; },
            get top() { return globalThis.top; },
            get location() { return loc; },
        };
        const has = (p) => Object.prototype.hasOwnProperty.call(allowed, p);
        if (typeof _maskFunction === 'function') { _maskFunction(postMessage, 'postMessage'); }
        w = new Proxy(Object.create(null), {
            get(_t, p) {
                if (typeof p === 'symbol' || p === 'then') return undefined;
                if (has(p)) return allowed[p];
                if (/^\d+$/.test(p)) return undefined;
                throw _xoError();
            },
            set(_t, p) { if (p === 'location' || p === 'href') return true; throw _xoError(); },
            has(_t, p) { return has(p); },
            ownKeys() { return []; },
            getOwnPropertyDescriptor(_t, p) {
                if (has(p)) return { value: allowed[p], writable: false, enumerable: false, configurable: true };
                if (typeof p === 'symbol') return undefined;
                throw _xoError();
            },
            defineProperty() { throw _xoError(); },
            deleteProperty() { throw _xoError(); },
            getPrototypeOf() { return null; },
            setPrototypeOf() { throw _xoError(); },
            isExtensible() { return true; },
            preventExtensions() { throw _xoError(); },
        });
        _remoteWins.set(id, w);
        return w;
    }

    function _detachFrame(el) {
        const cur = _dynFrameOf.get(el);
        if (!cur) return;
        _dynFrameOf.delete(el);
        _dynFrameEls.delete(cur.id);
        _remoteWins.delete(cur.id);
        try { ops.op_frame_detach(cur.id); } catch (_e) {}
    }

    function _maybeLoadFrame(el) {
        if (!_frameOps() || !el || el.tagName !== 'IFRAME') return;
        const raw = el.getAttribute('src');
        if (!raw) return;
        let abs;
        try { abs = new URL(raw, (document.baseURI || globalThis.location.href)).href; } catch (_e) { return; }
        if (!/^https?:/i.test(abs) || !_connected(el)) return;
        const cur = _dynFrameOf.get(el);
        if (cur && cur.src === abs) return;
        if (cur) _detachFrame(el);
        let id = 0;
        try { id = ops.op_frame_request(abs, String(globalThis.location.href)); } catch (_e) {}
        if (!id) return;
        _dynFrameOf.set(el, { id, src: abs });
        _dynFrameEls.set(id, el);
        try { _iframeState.delete(el); } catch (_e) {}
    }

    // window[N] / window.length follow the connected <iframe>s in document order.
    function _syncFrames() {
        let list = [];
        try { list = Array.prototype.slice.call(document.getElementsByTagName('iframe')); } catch (_e) {}
        for (const k of Object.getOwnPropertyNames(globalThis)) {
            if (/^\d+$/.test(k) && Number(k) >= list.length) { try { delete globalThis[k]; } catch (_e) {} }
        }
        list.forEach((el, i) => {
            const get = Object.getOwnPropertyDescriptor({ get [i]() { return el.contentWindow; } }, String(i)).get;
            try { Object.defineProperty(globalThis, String(i), { get, enumerable: false, configurable: true }); } catch (_e) {}
        });
        try { Object.defineProperty(globalThis, 'length', { value: list.length, configurable: true, writable: true, enumerable: true }); } catch (_e) {}
    }

    const _framesIn = (node) => {
        if (!node || (node.nodeType !== 1 && node.nodeType !== 11)) return [];
        if (node.tagName === 'IFRAME') return [node];
        try { return Array.prototype.slice.call(node.getElementsByTagName ? node.getElementsByTagName('iframe') : node.querySelectorAll('iframe')); } catch (_e) { return []; }
    };
    // about:blank / srcdoc frames live in this realm (child realm). Chrome
    // fires `load` synchronously when an about:blank iframe is inserted, and
    // after parsing for srcdoc.
    const _inRealmLoad = (f) => {
        if (_dynFrameOf.has(f)) return;
        const src = f.getAttribute('src');
        const hasSrcdoc = f.hasAttribute('srcdoc') || (typeof f.srcdoc === 'string' && f.srcdoc !== '');
        if (src && !/^\s*about:blank\s*$/i.test(src) && !/^javascript:/i.test(src)) return;
        const fire = () => { try { if (_connected(f)) f.dispatchEvent(_trust(new Event('load'))); } catch (_e) {} };
        if (hasSrcdoc) setTimeout(fire, 0); else fire();
    };
    const _afterInsert = (node) => {
        const fs = _framesIn(node); if (!fs.length) return;
        for (const f of fs) _maybeLoadFrame(f);
        _syncFrames();
        for (const f of fs) _inRealmLoad(f);
    };
    const _afterRemove = (node) => { const fs = _framesIn(node); if (!fs.length) return; for (const f of fs) _detachFrame(f); _syncFrames(); };

    {
        const wrap = (proto, name, before, after) => {
            const orig = proto[name];
            const w = ({ [name](...args) {
                const ctx = before ? before(this, args) : null;
                const r = orig.apply(this, args);
                try { after(this, args, ctx, r); } catch (_e) {}
                return r;
            } })[name];
            Object.defineProperty(w, 'length', { value: orig.length, configurable: true });
            Object.defineProperty(proto, name, { value: w, writable: true, enumerable: true, configurable: true });
        };
        wrap(Node.prototype, 'appendChild', null, (_t, a) => _afterInsert(a[0]));
        wrap(Node.prototype, 'insertBefore', null, (_t, a) => _afterInsert(a[0]));
        wrap(Node.prototype, 'removeChild', null, (_t, a) => _afterRemove(a[0]));
        wrap(Node.prototype, 'replaceChild', null, (_t, a) => { _afterRemove(a[1]); _afterInsert(a[0]); });
        wrap(Element.prototype, 'remove', null, (t) => _afterRemove(t));
        for (const n of ['append', 'prepend', 'after', 'before', 'replaceWith']) {
            if (typeof Element.prototype[n] === 'function') wrap(Element.prototype, n, null, (_t, a) => { for (const x of a) _afterInsert(x); });
        }
    }

    if (_hookF) {
        const def = (k, v) => Object.defineProperty(_hookF, k, { value: v, configurable: true });
        def('initFrame', (id, parentId, _parentOrigin, _topOrigin) => {
            _selfFrame = { id, parent: parentId };
            const p = _remoteWindow(parentId);
            const t = parentId === 0 ? p : _remoteWindow(0);
            if (typeof _hookF._setFrameAncestors === 'function') _hookF._setFrameAncestors(p, t);
        });
        def('deliverMessage', (dataJson, origin, sourceId) => {
            let data = null;
            try { data = JSON.parse(dataJson); } catch (_e) {}
            const source = sourceId === _selfFrame.parent ? globalThis.parent : _remoteWindow(sourceId);
            const ev = new MessageEvent('message', { data, origin, source, lastEventId: '', ports: [] });
            _trust(ev);
            globalThis.dispatchEvent(ev);
        });
        def('frameLoaded', (id) => {
            const el = _dynFrameEls.get(id);
            if (!el) return;
            try { el.dispatchEvent(_trust(new Event('load', { bubbles: false, cancelable: false }))); } catch (_e) {}
        });
        def('frameIdOf', (el) => { const d = _dynFrameOf.get(el); return d ? d.id : 0; });
    }
    // iframes that came with the HTML: event_bootstrap.js calls this on
    // DOMContentLoaded (addEventListener does not exist yet at this point).
    if (_hookF) Object.defineProperty(_hookF, 'scanParsedFrames', { value: () => {
        try { for (const f of Array.prototype.slice.call(document.getElementsByTagName('iframe'))) _maybeLoadFrame(f); } catch (_e) {}
        _syncFrames();
    }, configurable: true });
    globalThis.MutationObserver = MutationObserver;
    globalThis.MutationRecord = MutationRecord;

    // --- iframe support (contentWindow / contentDocument) ---
    //
    // Many scripts perform iframe-realm checks:
    // they create or find an <iframe>, access `.contentWindow`, then pull
    // native constructors (TextEncoder, Function, Array, ...) from the iframe
    // window to compare against the main window's versions. A mismatch
    // reveals monkey-patching; an `undefined` contentWindow reveals a headless
    // browser that doesn't support iframes.
    //
    // We install `contentWindow` and `contentDocument` as GETTERS on
    // HTMLIFrameElement.prototype so EVERY iframe — whether parsed from HTML
    // or created via document.createElement — returns a valid window-shaped
    // Proxy that falls through to globalThis for any unknown property. The
    // per-iframe state is cached in a WeakMap keyed by the element.

    const _iframeState = new WeakMap();

    // Build a mirror realm: fresh constructors that mimic the parent's shape
    // but are reference-distinct, so cross-realm probes like
    //   iframe.contentWindow.Navigator !== Navigator
    //   iframe.contentWindow.Navigator.prototype !== Navigator.prototype
    // hold true while own-property-names lists remain identical. Each
    // mirrored function carries _nativeTag so Function.prototype.toString
    // produces "function NAME() { [native code] }" cross-realm.
    const _MIRRORED_CONSTRUCTORS = [
        "Navigator", "Window", "Document", "HTMLDocument",
        "EventTarget", "Node", "Element", "HTMLElement",
        "HTMLDivElement", "HTMLSpanElement", "HTMLBodyElement",
        "HTMLAnchorElement", "HTMLImageElement", "HTMLInputElement",
        "HTMLFormElement", "HTMLButtonElement", "HTMLSelectElement",
        "HTMLTextAreaElement", "HTMLCanvasElement", "HTMLScriptElement",
        "HTMLIFrameElement", "Event", "CustomEvent", "MouseEvent",
        "KeyboardEvent", "MessageEvent", "Array", "Object", "Function",
        "String", "Number", "Boolean", "Promise", "Error", "TypeError",
        "RangeError", "Map", "Set", "WeakMap", "WeakSet", "Date",
        "RegExp", "Symbol",
    ];

    // Capture the native-tag Symbol from the parent realm. stealth_bootstrap.js
    // exposes it as globalThis._nativeTag. We capture explicitly so the
    // freshToString and _mkNativeFn don't accidentally see undefined when
    // bare-identifier scope chain is shadowed by the IIFE parameter.
    const _NATIVE_TAG_SYMBOL = globalThis._nativeTag || Symbol.for('__browser_oxide_native__');

    function _mkNativeFn(name) {
        const fn = function() {};
        try {
            Object.defineProperty(fn, "name", { value: name, configurable: true });
            Object.defineProperty(fn, _NATIVE_TAG_SYMBOL, { value: name, configurable: true });
            // Per-instance toString returning native shape — used when the
            // patched Function.prototype.toString is bypassed by direct
            // .toString() calls. Mirrors stealth_bootstrap's _maskFunction.
            const ts = function toString() { return "function " + name + "() { [native code] }"; };
            Object.defineProperty(ts, _NATIVE_TAG_SYMBOL, { value: "toString", configurable: true });
            Object.defineProperty(ts, "name", { value: "toString", configurable: true });
            Object.defineProperty(fn, "toString", { value: ts, configurable: true });
        } catch (_) {}
        return fn;
    }

    // Constructors where `new w.X(...)` is genuinely "Illegal constructor"
    // in real Chrome (DOM interfaces with no exposed constructor). Calls
    // to `new` on these throw `TypeError: Illegal constructor`.
    // Constructors NOT in this set are real callable types — for those we
    // delegate `new` to the parent realm's constructor via `Reflect.construct`
    // so e.g. `new iframe.contentWindow.Function("return 1")` returns a
    // function in the iframe realm, matching real Chrome. Some scripts use
    // `new w.Function(...)` to materialize a fresh-realm function; if we
    // throw where real Chrome succeeds, that differs from real Chrome.
    const _ILLEGAL_CONSTRUCTORS = new Set([
        "Navigator", "Window", "Document", "HTMLDocument",
        "Node", "Element", "HTMLElement",
        "HTMLDivElement", "HTMLSpanElement", "HTMLBodyElement",
        "HTMLAnchorElement", "HTMLImageElement", "HTMLInputElement",
        "HTMLFormElement", "HTMLButtonElement", "HTMLSelectElement",
        "HTMLTextAreaElement", "HTMLCanvasElement", "HTMLScriptElement",
        "HTMLIFrameElement",
    ]);

    function _mkMirroredConstructor(parentCtor, name, freshGrandparentProto) {
        // Fresh constructor function — different identity than parent's.
        // For DOM-interface types real Chrome throws on `new`; for genuine
        // callable types (Function/Array/Map/Date/Event/...) we delegate to
        // the parent constructor via Reflect.construct so the result lives
        // in our fresh realm (via fresh.prototype = freshProto below).
        const isIllegal = _ILLEGAL_CONSTRUCTORS.has(name);
        const fresh = isIllegal
            ? function() {
                throw new TypeError("Failed to construct '" + name + "': Illegal constructor");
            }
            : function(...args) {
                try {
                    return Reflect.construct(parentCtor, args, fresh);
                } catch (e) {
                    // Symbol() throws on `new`; re-throw with the parent's
                    // exact shape (don't reword) so feature-detection that
                    // catches "Symbol is not a constructor" still matches.
                    throw e;
                }
            };
        try {
            Object.defineProperty(fresh, "name", { value: name, configurable: true });
            Object.defineProperty(fresh, _NATIVE_TAG_SYMBOL, { value: name, configurable: true });
            const ts = function toString() { return "function " + name + "() { [native code] }"; };
            Object.defineProperty(ts, _NATIVE_TAG_SYMBOL, { value: "toString", configurable: true });
            Object.defineProperty(ts, "name", { value: "toString", configurable: true });
            Object.defineProperty(fresh, "toString", { value: ts, configurable: true });
        } catch (_) {}

        // Build a fresh prototype mirroring own-property-names of parent's prototype.
        // Each method/getter/setter is a fresh function with native toString shape.
        let parentProto = null;
        try { parentProto = parentCtor && parentCtor.prototype; } catch (_) {}
        // The fresh prototype's own __proto__ must point at the FRESH grandparent
        // prototype (built earlier in _buildRemoteRealm's topological pass),
        // NOT at the parent realm's grandparent. Crossing realms here makes
        // a prototype-chain walk traverse the parent realm's full chain on
        // top of the fresh chain, multiplying its work O(N) → O(N²+).
        const freshProto = Object.create(freshGrandparentProto || Object.prototype);

        if (parentProto) {
            const propNames = Object.getOwnPropertyNames(parentProto);
            for (const propName of propNames) {
                if (propName === "constructor") continue;
                let desc;
                try { desc = Object.getOwnPropertyDescriptor(parentProto, propName); } catch (_) { continue; }
                if (!desc) continue;
                const newDesc = {
                    configurable: desc.configurable !== false,
                    enumerable: !!desc.enumerable,
                };
                if (desc.get || desc.set) {
                    if (desc.get) newDesc.get = _mkNativeFn("get " + propName);
                    if (desc.set) newDesc.set = _mkNativeFn("set " + propName);
                } else {
                    newDesc.writable = desc.writable !== false;
                    if (typeof desc.value === "function") {
                        // Function-valued props: replace with our fresh native-shape stub
                        // (so cross-realm Function.prototype.toString.call(this) returns
                        // "function NAME() { [native code] }").
                        newDesc.value = _mkNativeFn(propName);
                    } else {
                        newDesc.value = desc.value;
                    }
                }
                try { Object.defineProperty(freshProto, propName, newDesc); } catch (_) {}
            }
        }

        try {
            Object.defineProperty(freshProto, "constructor", {
                value: fresh, writable: true, enumerable: false, configurable: true,
            });
            Object.defineProperty(fresh, "prototype", {
                value: freshProto, writable: false, enumerable: false, configurable: false,
            });
        } catch (_) {}
        return fresh;
    }

    // For each mirrored constructor name, find the nearest ancestor in
    // _MIRRORED_CONSTRUCTORS by walking the real prototype chain. Returns
    // an array of names in topological order (ancestors before descendants)
    // and a name -> direct-parent-name map.
    function _topoSortMirrored(names) {
        const realCtors = {};
        for (const n of names) {
            try {
                const c = globalThis[n];
                if (typeof c === "function") realCtors[n] = c;
            } catch (_) {}
        }
        const directParent = {};
        for (const n of names) {
            const ctor = realCtors[n];
            if (!ctor) { directParent[n] = null; continue; }
            let proto = null;
            try { proto = Object.getPrototypeOf(ctor.prototype); } catch (_) {}
            let parentName = null;
            let guard = 0;
            while (proto && guard++ < 32) {
                for (const m of names) {
                    const mc = realCtors[m];
                    if (mc && mc.prototype === proto) { parentName = m; break; }
                }
                if (parentName) break;
                try { proto = Object.getPrototypeOf(proto); } catch (_) { break; }
            }
            directParent[n] = parentName;
        }
        const ordered = [];
        const remaining = new Set(names);
        while (remaining.size > 0) {
            let progress = false;
            for (const n of Array.from(remaining)) {
                const p = directParent[n];
                if (p == null || !remaining.has(p)) {
                    ordered.push(n);
                    remaining.delete(n);
                    progress = true;
                }
            }
            if (!progress) {
                // Defensive: cyclic dependency in the real prototype graph
                // shouldn't happen, but if it does, append remaining without
                // ordering rather than infinite-looping.
                for (const n of remaining) ordered.push(n);
                break;
            }
        }
        return { ordered: ordered, directParent: directParent };
    }

    // Module-level cache: every iframe in this realm shares the same set of
    // mirrored constructors. Some scripts tag function/descriptor objects on
    // a first scope-chain walk and re-read them on a later walk; without this
    // cache every _getIframeWindow() call rebuilt the realm and any such
    // sentinel property set by the script was lost on the second read.
    let _cachedRemoteRealm = null;

    function _buildRemoteRealm() {
        if (_cachedRemoteRealm) return _cachedRemoteRealm;
        const realm = {};
        const sorted = _topoSortMirrored(_MIRRORED_CONSTRUCTORS);
        for (const name of sorted.ordered) {
            try {
                const parentCtor = globalThis[name];
                if (typeof parentCtor !== "function") continue;
                const parentName = sorted.directParent[name];
                const freshGrandparentProto = parentName && realm[parentName]
                    ? realm[parentName].prototype
                    : Object.prototype;
                realm[name] = _mkMirroredConstructor(parentCtor, name, freshGrandparentProto);
            } catch (_) {}
        }
        _cachedRemoteRealm = realm;
        return realm;
    }

    // Monotonically-increasing ID for child realms; used as the Rust-side
    // cache key in IframeRealmStore (HashMap<u32, ...>).
    let _nextRealmId = 0;

    // Frame registry: window[0], window[1], ... and window.length.
    // Some scripts access child iframes via window[N]
    // (frames[N]), NOT via iframe.contentWindow. Real Chrome updates
    // window[N] and window.length when iframes are appended to the DOM.
    const _frameRegistry = [];

    // Register contentWindow cw at frame index _fi in the main window.
    // Pass the iframe element el so we can find its DOM position and also
    // handle cases where the iframe was inserted via a non-tracked method
    // (insertBefore, innerHTML, insertAdjacentHTML, etc.).
    function _registerFrame(cw, el) {
        // window[N] is kept in sync with the DOM by _syncFrames (fbgb fork).
        if (typeof _syncFrames === 'function') { _syncFrames(); return; }
        // Try to find the iframe's true DOM position
        var _fi = -1;
        // First: check if el is already tracked in _appendedIframes
        if (el) {
            for (var _ai = 0; _ai < _appendedIframes.length; _ai++) {
                if (_appendedIframes[_ai] === el) { _fi = _ai; break; }
            }
        }
        // Second: if not tracked, query the DOM for its position
        if (_fi < 0) {
            try {
                var _all = document.getElementsByTagName && document.getElementsByTagName('iframe');
                if (_all) {
                    for (var _di = 0; _di < _all.length; _di++) {
                        if (_all[_di] === el) { _fi = _di; break; }
                    }
                }
            } catch (_) {}
        }
        // Fallback: use sequential registry length
        if (_fi < 0) {
            _fi = _frameRegistry.length;
        }
        // Track in registry
        while (_frameRegistry.length <= _fi) _frameRegistry.push(null);
        _frameRegistry[_fi] = cw;
        // Register in _appendedIframes if not already there (for lazy getter)
        if (el && _fi >= _appendedIframes.length) {
            while (_appendedIframes.length < _fi) _appendedIframes.push(null);
            _appendedIframes.push(el);
        }
        // Install as window[N] — replace lazy getter (if any) with actual value
        try {
            Object.defineProperty(globalThis, String(_fi), {
                value: cw, writable: true, enumerable: true, configurable: true,
            });
        } catch (_) {}
        // Update window.length
        var _newLen = _fi + 1;
        try {
            const _ld = Object.getOwnPropertyDescriptor(globalThis, 'length');
            if (_ld && _ld.writable) {
                if (globalThis.length < _newLen) globalThis.length = _newLen;
            } else {
                Object.defineProperty(globalThis, 'length', {
                    value: _newLen, writable: true, configurable: true, enumerable: true,
                });
            }
        } catch (_) {}
    }

    // Extract scheme+host+port from a URL without using new URL().
    // Returns "null" for non-http(s) URLs (data:, about:, etc.) or empty input.
    const _xOrigin = function(u) {
        var m = u && u.match(/^(https?:\/\/[^/?#:]+(?::\d+)?)/i);
        return m ? m[1].toLowerCase() : "null";
    };

    function _getIframeWindow(el) {
        { const df = _dynFrameOf.get(el); if (df) return _remoteWindow(df.id); }
        // Chrome: an iframe that is not in a document has no browsing context.
        if (el && el.parentNode !== undefined && !_connected(el)) return null;
        let state = _iframeState.get(el);
        if (state) {
            // Cross-origin transition: a script creates an iframe with no src, accesses
            // contentWindow (creates child realm), then sets src to a cross-origin URL and re-accesses.
            // When src changes to cross-origin, invalidate the cached realm and return a
            // SecurityError proxy — exactly what real Chrome does.
            try {
                const _cSrc = (el && typeof el.getAttribute === "function")
                    ? (el.getAttribute("src") || el.src || "")
                    : (el && el.src || "");
                if (_cSrc && _cSrc !== "about:blank" && !/^javascript:/i.test(_cSrc) && _cSrc !== "") {
                    const _pOrig = _xOrigin((globalThis.location && globalThis.location.href) || "");
                    const _sOrig = _xOrigin(_cSrc);
                    if (_sOrig !== _pOrig) {
                        const _xM = 'Blocked a frame with origin "' + _pOrig + '" from accessing a cross-origin frame.';
                        const _xo2 = new Proxy({}, {
                            get(t, p) { if (typeof p === 'symbol') return undefined; throw new DOMException(_xM, 'SecurityError'); },
                            set() { throw new DOMException(_xM, 'SecurityError'); },
                            has() { return false; },
                        });
                        const _xoS2 = { contentWindow: _xo2, contentDocument: null, _realmId: undefined, _processedSrcdoc: '' };
                        _iframeState.set(el, _xoS2);
                        return _xo2;
                    }
                }
            } catch (_) {}
            // Re-run srcdoc scripts if srcdoc was set after initial contentWindow access.
            // A script may set iframe.srcdoc = "..." before or after first contentWindow
            // access; in either case we must execute the scripts in the child realm.
            if (state._realmId !== undefined) {
                let _cur = "";
                try { _cur = el.getAttribute("srcdoc") || el.srcdoc || ""; } catch (_) {}
                if (_cur && _cur !== state._processedSrcdoc) {
                    state._processedSrcdoc = _cur;
                    try {
                        const _re = /<script[^>]*>([\s\S]*?)<\/script>/gi;
                        let _m2;
                        while ((_m2 = _re.exec(_cur)) !== null) {
                            const _s2 = _m2[1];
                            if (_s2 && _s2.trim()) {
                                try { ops.op_eval_in_child_realm(state._realmId, _s2); } catch (_) {}
                            }
                        }
                    } catch (_) {}
                }
            }
            return state.contentWindow;
        }

        // ── Cross-origin iframe detection ────────────────────
        // Some scripts create an iframe with a different origin (e.g. a
        // data: URI or cross-origin https URL) and expect a SecurityError
        // when accessing contentWindow.document. Return a Proxy that throws
        // SecurityError on any property read — matches real Chrome behaviour.
        try {
            const _iSrc = (el && typeof el.getAttribute === "function")
                ? (el.getAttribute("src") || el.src || "")
                : (el && el.src || "");
            if (_iSrc && _iSrc !== "about:blank" && !/^javascript:/i.test(_iSrc) && _iSrc !== "") {
                const _pOrigin = _xOrigin((globalThis.location && globalThis.location.href) || "");
                const _srcOrigin = _xOrigin(_iSrc);
                if (_srcOrigin !== _pOrigin) {
                    const _xMsg = 'Blocked a frame with origin "' + _pOrigin + '" from accessing a cross-origin frame.';
                    const _xo = new Proxy({}, {
                        get(t, p) {
                            if (typeof p === 'symbol') return undefined;
                            throw new DOMException(_xMsg, 'SecurityError');
                        },
                        set() { throw new DOMException(_xMsg, 'SecurityError'); },
                        has() { return false; },
                    });
                    const _xoState = { contentWindow: _xo, contentDocument: null, _realmId: undefined, _processedSrcdoc: '' };
                    _iframeState.set(el, _xoState);
                    _registerFrame(_xo, el);
                    return _xo;
                }
            }
        } catch (_) {}

        // ── Build the iframe document shell ──────────────────────────────
        // srcdoc iframes: expose the source text for
        // reads (`iframe.contentDocument.body.innerHTML`).
        let _srcdoc = "";
        try {
            if (el && typeof el.getAttribute === "function") {
                _srcdoc = el.getAttribute("srcdoc") || "";
            }
            // Also check direct JS property (set via el.srcdoc = "...") since
            // property assignment may not update the HTML attribute in our DOM.
            if (!_srcdoc && el && typeof el.srcdoc === "string") {
                _srcdoc = el.srcdoc;
            }
        } catch (_) {}
        // Real-node frame document: a detached <html><head><body> subtree
        // in this arena, so elements have style/append/layout like Chrome's
        // about:blank / srcdoc documents (fingerprinters measure text in
        // them).
        const _root = _document.createElement("html");
        const _head = _document.createElement("head");
        const _body = _document.createElement("body");
        _root.appendChild(_head);
        _root.appendChild(_body);
        if (_srcdoc) {
            try {
                const _hm = /<head[^>]*>([\s\S]*?)(?:<\/head>|<body|$)/i.exec(_srcdoc);
                const _bm = /<body[^>]*>([\s\S]*?)(?:<\/body>|$)/i.exec(_srcdoc);
                let _headHtml = _hm ? _hm[1] : "";
                let _bodyHtml = _bm ? _bm[1] : "";
                if (!_hm && !_bm) {
                    // No explicit sections: metadata goes to head, rest to body.
                    const _rest = _srcdoc.replace(/<!doctype[^>]*>/i, "").replace(/<\/?html[^>]*>/ig, "");
                    _headHtml = (_rest.match(/<(?:meta|title|link|base)\b[^>]*>(?:[^<]*<\/title>)?/ig) || []).join("");
                    _bodyHtml = _rest.replace(/<(?:meta|link|base)\b[^>]*>/ig, "").replace(/<title\b[^>]*>[\s\S]*?<\/title>/ig, "");
                }
                // Scripts are executed separately in the child realm.
                const _noScript = (h) => h.replace(/<script\b[\s\S]*?<\/script>/ig, "");
                if (_headHtml) _head.innerHTML = _noScript(_headHtml);
                if (_bodyHtml) _body.innerHTML = _noScript(_bodyHtml);
            } catch (_) {}
        }
        try {
            const _fw = (el && el.offsetWidth) || 300, _fh = (el && el.offsetHeight) || 150;
            ops.op_layout_set_root_viewport(_getNodeId(_root), _fw, _fh);
        } catch (_) {}
        const _docEl = _root;
        const iframeDoc = {
            nodeType: 9,
            nodeName: "#document",
            documentElement: _root,
            head: _head,
            body: _body,
            get title() { const t = _head.querySelector("title"); return t ? t.textContent : ""; },
            set title(v) { let t = _head.querySelector("title"); if (!t) { t = _document.createElement("title"); _head.appendChild(t); } t.textContent = String(v); },
            readyState: "complete",
            visibilityState: "visible",
            hidden: false,
            compatMode: "CSS1Compat",
            characterSet: "UTF-8",
            charset: "UTF-8",
            contentType: "text/html",
            get childNodes() { return new NodeList([_root]); },
            get children() { return new NodeList([_root]); },
            get firstChild() { return _root; },
            get lastChild() { return _root; },
            get firstElementChild() { return _root; },
            get activeElement() { return _body; },
            get fonts() { return _document.fonts; },
            get implementation() { return _document.implementation; },
            hasFocus() { return false; },
            contains(n) { return n === this || _root.contains(n); },
            querySelector(sel) { return _root.querySelector(sel); },
            querySelectorAll(sel) { return _root.querySelectorAll(sel); },
            getElementById(id) { const _w = (n) => { if (n.nodeType === 1 && n.getAttribute("id") === String(id)) return n; for (const c of n.children || []) { const r = _w(c); if (r) return r; } return null; }; return _w(_root); },
            getElementsByTagName(tag) {
                const t = String(tag).toLowerCase();
                if (t === "html") return new NodeList([_root]);
                return _root.getElementsByTagName(tag);
            },
            getElementsByClassName(c) { return _root.getElementsByClassName(c); },
            getElementsByName(n) { return _root.querySelectorAll('[name="' + String(n).replace(/"/g, '\\"') + '"]'); },
            createElement(tag) { return _document.createElement(tag); },
            createElementNS(ns, tag) { return _document.createElementNS(ns, tag); },
            createEvent(type) { return _document.createEvent(type); },
            createRange() { return _document.createRange(); },
            createTextNode(text) { return _document.createTextNode(text); },
            createComment(text) { return _document.createComment(text); },
            createDocumentFragment() { return _document.createDocumentFragment(); },
            createTreeWalker(r, w, f) { return _document.createTreeWalker(r, w, f); },
            importNode(n, deep) { return n.cloneNode(deep); },
            adoptNode(n) { return n; },
            append(...n) { return _body.append(...n); },
            write(html) { try { _body.insertAdjacentHTML("beforeend", String(html)); } catch (_) {} },
            writeln(html) { this.write(String(html) + "\n"); },
            open() { _head.innerHTML = ""; _body.innerHTML = ""; return this; },
            close() {},
            addEventListener() {}, removeEventListener() {}, dispatchEvent() { return true; },
        };
        // ── Screen mirror ─────────────────────────────────────────────────
        const _parentScreen = globalThis.screen || {};
        const _iframeScreen = {
            availWidth:  _parentScreen.availWidth  || 1920,
            availHeight: _parentScreen.availHeight || 1080,
            width:       _parentScreen.width       || 1920,
            height:      _parentScreen.height      || 1080,
            availLeft:   _parentScreen.availLeft   || 0,
            availTop:    _parentScreen.availTop    || 0,
            colorDepth:  _parentScreen.colorDepth  || 24,
            pixelDepth:  _parentScreen.pixelDepth  || 24,
            orientation: _parentScreen.orientation,
        };
        if (!/Firefox\/|Gecko\/20100101/.test(
            (typeof navigator !== "undefined" && navigator.userAgent) || ""
        )) {
            _iframeScreen.isExtended = false;
        }

        // ── Obtain the child window object ───────────────────────────────
        // PRIMARY PATH: genuine v8::Context child realm.
        // op_create_child_realm returns the child global:
        //   - Real, realm-distinct native intrinsics (Object/Function/… ≠ parent)
        //   - constructor.name === "Window" (set up in Rust)
        //   - Genuine-native Function.prototype.toString in child realm
        //   - self/window/globalThis/frames self-refs (set in Rust)
        // Matches real Chrome, where contentWindow is a genuine realm rather
        // than a Proxy or a parent alias.
        const _realmId = _nextRealmId++;
        let cw = null;
        try {
            const _got = ops.op_create_child_realm(_realmId);
            if (_got && typeof _got === "object") cw = _got;
        } catch (_) {}

        if (cw) {
            // ── Populate child realm with DOM/FP properties ───────────────
            // CRITICAL: use op_set_child_realm_prop for properties that must be
            // visible to code running INSIDE the child realm (e.g. srcdoc
            // script eval). Direct `cw.x = v` from parent JS goes to the global PROXY's
            // own dict; code inside the realm reads from the INNER global.
            // op_set_child_realm_prop enters the child ContextScope and calls
            // child_global.set() which forwards via [[Set]] to the inner global.
            const _sp = (k, v) => {
                try { ops.op_set_child_realm_prop(_realmId, k, v); } catch (_) {}
            };

            // iframeDoc back-reference to default view (set before _sp calls)
            try { iframeDoc.defaultView = cw; } catch (_) {}

            // Document
            _sp("document", iframeDoc);

            // Location stub — about:blank inherits the parent origin per HTML spec.
            // Some scripts read document.domain (= hostname) and
            // location.origin; empty values differ from real Chrome.
            const _pLoc = globalThis.location || {};
            _sp("location", {
                href: "about:blank",
                origin: _pLoc.origin || "null",
                pathname: "/",
                hash: "", search: "",
                host: _pLoc.host || "",
                hostname: _pLoc.hostname || "",
                port: _pLoc.port || "",
                protocol: _pLoc.protocol || "https:",
                assign() {}, replace() {}, reload() {},
                toString() { return "about:blank"; },
            });

            // Parent / top / name
            _sp("parent", globalThis);
            _sp("top", globalThis);
            _sp("name", "");

            // Screen mirror (some scripts read these from inside child realm)
            _sp("screen", _iframeScreen);
            _sp("availWidth",  _iframeScreen.availWidth);
            _sp("availHeight", _iframeScreen.availHeight);

            // Viewport dimensions
            _sp("innerWidth",   globalThis.innerWidth  || 1920);
            _sp("innerHeight",  globalThis.innerHeight || 1080);
            _sp("outerWidth",   globalThis.outerWidth  || 1920);
            _sp("outerHeight",  globalThis.outerHeight || 1080);
            _sp("scrollX", 0); _sp("scrollY", 0);
            _sp("pageXOffset", 0); _sp("pageYOffset", 0);
            // Window state properties some scripts expect to be present.
            _sp("closed", false);
            _sp("name", "");
            _sp("status", "");
            _sp("defaultStatus", "");
            _sp("screenTop", globalThis.screenTop || 0);
            _sp("screenLeft", globalThis.screenLeft || 0);
            _sp("screenX", globalThis.screenX || 0);
            _sp("screenY", globalThis.screenY || 0);
            // history stub — basic object so `.toString()` doesn't throw.
            // history for an about:blank/srcdoc frame: keeps state, and the
            // same 200-per-10-s flooding limit Chrome applies per frame.
            {
                let _st = null, _n = 0, _t0 = 0;
                const _ok = () => { const now = Date.now(); if (now - _t0 > 10000) { _t0 = now; _n = 0; } return ++_n <= 200; };
                const _set = (s) => { if (_ok()) { try { _st = (s === undefined) ? null : structuredClone(s); } catch (_e) { _st = s; } } };
                _sp("history", { length: 1, get state() { return _st; }, scrollRestoration: "auto",
                    back() {}, forward() {}, go() {}, pushState(s) { _set(s); }, replaceState(s) { _set(s); } });
            }
            // Storage stubs — some scripts may call `.toString()` on these.
            const _storageStub = Object.create(null);
            Object.defineProperty(_storageStub, Symbol.toStringTag, { value: "Storage", configurable: true });
            _storageStub.length = 0;
            _storageStub.getItem = function getItem() { return null; };
            _storageStub.setItem = function setItem() {};
            _storageStub.removeItem = function removeItem() {};
            _storageStub.clear = function clear() {};
            _storageStub.key = function key() { return null; };
            try { _sp("localStorage", _storageStub); } catch (_) {}
            try { _sp("sessionStorage", _storageStub); } catch (_) {}
            // indexedDB — basic stub so typeof is "object".
            _sp("indexedDB", { open() {}, deleteDatabase() {}, databases() { return Promise.resolve([]); }, cmp() { return 0; } });
            // visualViewport — propagate from parent (some scripts may call .toString()).
            try { if (globalThis.visualViewport !== undefined) _sp("visualViewport", globalThis.visualViewport); } catch (_) {}

            // Event handler stubs — Chrome defines all on* handlers as null (data property,
            // enumerable:true) on the Window global. The child realm gets genuine V8 natives
            // but NOT these Window interface additions. Some scripts iterate the parent
            // window's enumerable properties and for each key check it in the child realm;
            // calling .toString() on the undefined value throws, while null.toString()
            // would throw too but with the correct Chrome-matching TypeError shape.
            // Setting them null here makes child[key] !== undefined for all on* keys.
            const _onHandlers = [
                'onabort','onafterprint','onanimationcancel','onanimationend',
                'onanimationiteration','onanimationstart','onappinstalled','onauxclick',
                'onbeforeinput','onbeforeinstallprompt','onbeforematch','onbeforeprint',
                'onbeforetoggle','onbeforeunload','onbeforexrselect','onblur',
                'oncancel','oncanplay','oncanplaythrough','onchange',
                'onclick','onclose','oncommand','oncontentvisibilityautostatechange',
                'oncontextlost','oncontextmenu','oncontextrestored','oncuechange',
                'ondblclick','ondrag','ondragend','ondragenter',
                'ondragleave','ondragover','ondragstart','ondrop',
                'ondurationchange','onemptied','onended','onfocus',
                'onformdata','ongamepadconnected','ongamepaddisconnected','ongotpointercapture',
                'onhashchange','oninput','oninvalid','onkeydown',
                'onkeypress','onkeyup','onlanguagechange','onload',
                'onloadeddata','onloadedmetadata','onloadstart','onlostpointercapture',
                'onmessage','onmessageerror','onmousedown','onmouseenter',
                'onmouseleave','onmousemove','onmouseout','onmouseover',
                'onmouseup','onmousewheel','onoffline','ononline',
                'onpagehide','onpagereveal','onpageshow','onpageswap',
                'onpause','onplay','onplaying','onpointercancel',
                'onpointerdown','onpointerenter','onpointerleave','onpointermove',
                'onpointerout','onpointerover','onpointerrawupdate','onpointerup','onpopstate',
                'onprogress','onratechange','onrejectionhandled','onreset',
                'onresize','onscroll','onscrollend','onscrollsnapchange',
                'onscrollsnapchanging','onsearch','onsecuritypolicyviolation','onseeked',
                'onseeking','onselect','onselectionchange','onselectstart',
                'onslotchange','onstalled','onstorage','onsubmit',
                'onsuspend','ontimeupdate','ontoggle','ontransitioncancel',
                'ontransitionend','ontransitionrun','ontransitionstart','onunhandledrejection',
                'onunload','onvolumechange','onwaiting','onwebkitanimationend',
                'onwebkitanimationiteration','onwebkitanimationstart','onwebkittransitionend','onwheel',
            ];
            for (const _oh of _onHandlers) {
                try { _sp(_oh, null); } catch (_) {}
            }

            // Blanket-copy ALL remaining enumerable parent-window properties to child
            // realm. Some scripts iterate parent window's enumerable props and
            // check them in child; any that are undefined in child cause errors.
            // Real Chrome child frames have the same complete set as parent.
            // We skip child-specific properties (document, location, self-refs) that
            // are already set above or will be overridden below with correct values.
            const _basSkip = new Set([
                'window','self','globalThis','frames','top','parent',
                'document','location','opener',
                'length',
                // Carefully configured below (accessor or child-specific value):
                'devicePixelRatio','navigator','fetch','postMessage',
                // Already set above:
                'screen','availWidth','availHeight','innerWidth','innerHeight',
                'outerWidth','outerHeight','scrollX','scrollY','pageXOffset','pageYOffset',
                'screenTop','screenLeft','screenX','screenY',
                'closed','name','status','defaultStatus',
                'history','localStorage','sessionStorage','indexedDB','visualViewport',
            ]);
            try {
                for (const _bk of Object.keys(globalThis)) {
                    if (_basSkip.has(_bk)) continue;
                    // Skip numeric frame indices (not enumerable in real Chrome iframes)
                    if (_bk.length <= 4 && /^\d+$/.test(_bk)) continue;
                    try {
                        const _bv = globalThis[_bk];
                        _sp(_bk, _bv !== undefined ? _bv : null);
                    } catch (_) {}
                }
            } catch (_) {}

            // devicePixelRatio: define as a native-tagged accessor so that
            // A script inspecting these sees both a proper descriptor (getter:fn,
            // not data) AND [native code] from Function.prototype.toString.
            // The eval runs inside the child realm so Symbol.for resolves via
            // the isolate-level global symbol registry (same symbol as parent).
            const _dprVal = globalThis.devicePixelRatio || 1;
            try {
                ops.op_eval_in_child_realm(_realmId,
                    `(function(){var _nt=Symbol.for('__browser_oxide_native__');var _g=function(){return ${_dprVal};};Object.defineProperty(_g,_nt,{value:'get devicePixelRatio',configurable:true});Object.defineProperty(_g,'name',{value:'get devicePixelRatio',configurable:true});var _s=function(v){Object.defineProperty(this,'devicePixelRatio',{value:v,writable:true,enumerable:true,configurable:true});};Object.defineProperty(_s,_nt,{value:'set devicePixelRatio',configurable:true});Object.defineProperty(_s,'name',{value:'set devicePixelRatio',configurable:true});Object.defineProperty(globalThis,'devicePixelRatio',{get:_g,set:_s,enumerable:true,configurable:true});})();`
                );
            } catch (_) {
                _sp("devicePixelRatio", _dprVal);
            }

            // ── iframe EventTarget + bidirectional postMessage (FP-E1) ───────
            // The child v8::Context has a genuine MessageEvent but NO
            // addEventListener/dispatchEvent: those live on the parent's
            // EventTarget/Window prototype chain, which the own-enumerable
            // blanket-copy above never reaches. So a framed document's
            // `window.addEventListener('message', …)` threw (swallowed),
            // leaving the iframe unable to receive OR answer messages. That
            // both (a) gates real iframe-based challenge flows (which load
            // the challenge in an <iframe> and postMessage with it) and (b)
            // differs from real Chrome (real iframes expose these). Install a
            // native-shaped EventTarget backed
            // by a realm-local listener registry + a `__deliverMessage` hook the
            // parent uses to post INTO the realm. `parent`/`top` identity is
            // left untouched (set to globalThis above) — replies route via the
            // delivered event's `source` (the standard postMessage pattern), so
            // no `iframe.contentWindow.parent === window` FP invariant changes.
            try {
                ops.op_eval_in_child_realm(_realmId,
                    "(function(){var _nt=Symbol.for('__browser_oxide_native__');var _L=Object.create(null);"
                    + "function _n(fn,nm){try{Object.defineProperty(fn,'name',{value:nm,configurable:true});"
                    + "Object.defineProperty(fn,_nt,{value:nm,configurable:true});var ts=function toString(){return 'function '+nm+'() { [native code] }'};"
                    + "Object.defineProperty(ts,_nt,{value:'toString',configurable:true});Object.defineProperty(ts,'name',{value:'toString',configurable:true});"
                    + "Object.defineProperty(fn,'toString',{value:ts,configurable:true});}catch(_){}return fn;}"
                    + "function ael(type,fn){if(!(typeof fn==='function'||(fn&&typeof fn.handleEvent==='function')))return;var t=String(type);(_L[t]||(_L[t]=[])).push(fn);}"
                    + "function rel(type,fn){var a=_L[String(type)];if(a){var i=a.indexOf(fn);if(i>=0)a.splice(i,1);}}"
                    + "function de(ev){try{var t=ev&&ev.type;var a=_L[t];if(a)a.slice().forEach(function(h){try{(typeof h==='function'?h:h.handleEvent).call(globalThis,ev);}catch(_){}});"
                    + "var on=globalThis['on'+t];if(typeof on==='function'){try{on.call(globalThis,ev);}catch(_){}}}catch(_){}return true;}"
                    + "Object.defineProperty(globalThis,'addEventListener',{value:_n(ael,'addEventListener'),writable:true,configurable:true});"
                    + "Object.defineProperty(globalThis,'removeEventListener',{value:_n(rel,'removeEventListener'),writable:true,configurable:true});"
                    + "Object.defineProperty(globalThis,'dispatchEvent',{value:_n(de,'dispatchEvent'),writable:true,configurable:true});"
                    + "Object.defineProperty(globalThis,'__deliverMessage',{value:function(data,origin,source){Promise.resolve().then(function(){try{de(new MessageEvent('message',{data:data,origin:origin||'',source:source||null}));}catch(_){}});},configurable:true});})();"
                );
            } catch (_) {}

            // child→parent reply target: a Proxy over the real parent window
            // whose ONLY override is postMessage — lands a 'message' on the MAIN
            // window with source === this iframe's contentWindow (cw), what
            // solvers assert (`event.source === iframe.contentWindow`). Exposed
            // to the framed doc as the delivered event's `source`, NOT as
            // `parent`, so the parent-identity invariant is preserved.
            const _parentOrigin = (globalThis.location && globalThis.location.origin) || "";
            const _postToParent = function postMessage(msg, origin) {
                Promise.resolve().then(() => {
                    try {
                        globalThis.dispatchEvent(new MessageEvent("message", {
                            data: msg,
                            origin: (origin && origin !== "*") ? String(origin) : _parentOrigin,
                            source: cw,
                        }));
                    } catch (_) {}
                });
            };
            let _msgSource = null;
            try {
                _msgSource = new Proxy(globalThis, {
                    get(t, p) { return (p === "postMessage") ? _postToParent : Reflect.get(t, p); },
                });
            } catch (_) { _msgSource = { postMessage: _postToParent }; }
            _sp("__msgSource", _msgSource);

            // parent→child: cw.postMessage(...) (and the framed doc's own
            // window.postMessage) deliver a 'message' INTO the child realm. Data
            // crosses the realm boundary as a JSON literal; the event's source
            // is the reply-routing proxy above.
            const _pm = function postMessage(msg, origin) {
                Promise.resolve().then(() => {
                    try {
                        const _dj = JSON.stringify(msg === undefined ? null : msg);
                        const _oj = JSON.stringify((origin && origin !== "*") ? String(origin) : _parentOrigin);
                        ops.op_eval_in_child_realm(_realmId,
                            "try{globalThis.__deliverMessage((" + _dj + ")," + _oj + ",(globalThis.__msgSource||null));}catch(_){}"
                        );
                    } catch (_) {}
                });
            };
            _sp("postMessage", _pm);

            // Navigator: fresh instance proxying parent values.
            try {
                const _parentNav = globalThis.navigator;
                const _nav = Object.create(Object.prototype);
                for (const _k of [
                    "userAgent", "platform", "language", "languages",
                    "hardwareConcurrency", "deviceMemory", "maxTouchPoints",
                    "vendor", "vendorSub", "product", "productSub",
                    "appName", "appVersion", "appCodeName", "cookieEnabled",
                    "onLine", "doNotTrack", "pdfViewerEnabled",
                    "plugins", "mimeTypes",
                ]) {
                    try {
                        const _v = _parentNav[_k];
                        if (_v !== undefined) Object.defineProperty(_nav, _k, { value: _v, writable: true, configurable: true, enumerable: true });
                    } catch (_) {}
                }
                // webdriver: `false` in modern Chrome (property present,
                // value false; `undefined` would differ from real Chrome).
                // Some scripts check cw.navigator.webdriver; false is the
                // Chrome-faithful value.
                Object.defineProperty(_nav, 'webdriver', { value: false, writable: true, configurable: true, enumerable: true });
                _sp("navigator", _nav);
            } catch (_) {}

            // Own realm `fetch` — distinct reference (cw.fetch !== parent.fetch)
            try {
                const _ifetch = function fetch(...a) { return globalThis.fetch.apply(this, a); };
                Object.defineProperty(_ifetch, "name", { value: "fetch", configurable: true });
                Object.defineProperty(_ifetch, "length", { value: 1, configurable: true });
                Object.defineProperty(_ifetch, _NATIVE_TAG_SYMBOL, { value: "fetch", configurable: true });
                _sp("fetch", _ifetch);
            } catch (_) {}

            // Copy key browser APIs that some scripts read from the child realm.
            // e.g. reading MediaSource.isTypeSupported from inside the child realm.
            const _apisToCopy = [
                'MediaSource', 'MediaSourceHandle', 'MediaCapabilities',
                'MediaRecorder', 'MediaStream', 'MediaStreamTrack',
                'HTMLVideoElement', 'HTMLAudioElement', 'HTMLMediaElement',
                'AudioContext', 'OfflineAudioContext',
                'RTCPeerConnection', 'RTCDataChannel',
                'Blob', 'File', 'FileReader',
                'URL', 'URLSearchParams',
                'WebSocket', 'Worker',
                'CSS', 'crypto', 'performance',
                'structuredClone', 'queueMicrotask', 'reportError',
                'crossOriginIsolated', 'isSecureContext', 'origin',
                'CustomEvent', 'Event', 'EventTarget',
                'PromiseRejectionEvent', 'ErrorEvent',
                'MessageChannel', 'MessagePort', 'MessageEvent',
                'MutationObserver', 'IntersectionObserver', 'ResizeObserver',
                'PerformanceObserver',
                'TextEncoder', 'TextDecoder',
                'AbortController', 'AbortSignal',
                'ReadableStream', 'WritableStream', 'TransformStream',
                'Request', 'Response', 'Headers', 'FormData',
                'XMLHttpRequest', 'DOMParser',
                'Node', 'Element', 'Document',
                'HTMLElement', 'DocumentFragment',
                'Notification',
                // Singleton constructors the npc/crs probes expect in child realm.
                'Navigator', 'Location', 'History', 'Screen',
                'Performance', 'Permissions', 'ScreenOrientation',
                // The canvas/graphics constructor surface. Without these,
                // an iframe child realm has `CanvasRenderingContext2D ===
                // undefined` (all ctx2d proto methods missing on the child
                // realm). A script that fetches such a constructor/method
                // from the child realm gets `undefined` and then accessing a
                // property on it throws `TypeError: Cannot read properties of
                // undefined`, which differs from real Chrome. Real Chrome
                // iframe realms expose the full set. Only names that are
                // genuine main-realm globals are copied (the loop skips
                // `undefined`), so this is Chrome-faithful, not a stub.
                'CanvasRenderingContext2D', 'HTMLCanvasElement',
                'OffscreenCanvas', 'ImageData', 'Path2D', 'ImageBitmap',
                'WebGLRenderingContext', 'WebGL2RenderingContext',
                'DOMMatrix', 'DOMMatrixReadOnly', 'DOMPoint',
                'DOMRect', 'DOMRectReadOnly',
            ];
            for (const _ak of _apisToCopy) {
                try {
                    const _v = globalThis[_ak];
                    if (_v !== undefined) _sp(_ak, _v);
                } catch (_) {}
            }

            // A separate realm has its own interface objects:
            // iframe.contentWindow.Navigator !== window.Navigator (stealth
            // plugins that hand back the parent window fail exactly this).
            try {
                const _rr = _buildRemoteRealm();
                const _jsBuiltins = new Set(['Array', 'Object', 'Function', 'String', 'Number', 'Boolean', 'Promise', 'Error',
                    'TypeError', 'RangeError', 'Map', 'Set', 'WeakMap', 'WeakSet', 'Date', 'RegExp', 'Symbol']);
                for (const _n of Object.keys(_rr)) {
                    if (!_jsBuiltins.has(_n)) _sp(_n, _rr[_n]);
                }
            } catch (_) {}


            // Some scripts read MediaSource.isTypeSupported from inside the
            // child realm. Wrap in IIFE to prevent __kms leaking into child realm globals
            // (some scripts detect unexpected global variables).
            // globalThis.X = Y inside an IIFE IS visible to subsequent op_eval_in_child_realm
            // calls because they all run in the same child v8::Context.
            try {
                ops.op_eval_in_child_realm(_realmId,
                    '(function(){\n' +
                    'var __kms=new Set(["video/mp4","video/webm","audio/mp4","audio/webm",' +
                    '"audio/mpeg","audio/aac","audio/x-m4a","audio/mp3","audio/x-wav",' +
                    '"audio/ogg","audio/acc","audio/mp4;codecs=\\"mp4a.40.2\\"",' +
                    '"video/mp4;codecs=\\"avc1.42E01E,mp4a.40.2\\"",' +
                    '"video/webm;codecs=\\"vp9\\""]);\n' +
                    'var _its=function isTypeSupported(t){if(typeof t!=="string")return false;var b=t.split(";")[0].trim();return __kms.has(t)||__kms.has(b);};\n' +
                    'if(typeof MediaSource==="undefined"||MediaSource===undefined){\n' +
                    'globalThis.MediaSource=function MediaSource(){throw new TypeError("Failed to construct \'MediaSource\': Illegal constructor");};\n' +
                    '}\n' +
                    'if(typeof MediaSource.isTypeSupported!=="function") MediaSource.isTypeSupported=_its;\n' +
                    'if(typeof MediaRecorder==="undefined"||MediaRecorder===undefined){\n' +
                    'globalThis.MediaRecorder=function MediaRecorder(){throw new TypeError("Failed to construct \'MediaRecorder\': Illegal constructor");};\n' +
                    '}\n' +
                    'if(typeof MediaRecorder.isTypeSupported!=="function") MediaRecorder.isTypeSupported=_its;\n' +
                    '})();\n'
                );
            } catch (_) {}

            // Align child realm globals with main window so the realms don't diverge.
            // Chrome without COOP/COEP: SharedArrayBuffer is disabled in all frames.
            // Our V8 child context natively has SAB; delete it to match.
            try {
                ops.op_eval_in_child_realm(_realmId,
                    'if(typeof SharedArrayBuffer!=="undefined"&&typeof globalThis.SharedArrayBuffer!=="undefined")' +
                    '{try{delete globalThis.SharedArrayBuffer;}catch(_){globalThis.SharedArrayBuffer=undefined;}}'
                );
            } catch (_) {}

            // Execute srcdoc scripts in the child realm.
            // Some scripts inject content via srcdoc to
            // run code inside the iframe. A real browser executes those
            // scripts; we extract and eval them in the child realm context.
            if (_srcdoc) {
                try {
                    const _scriptRe = /<script[^>]*>([\s\S]*?)<\/script>/gi;
                    let _m;
                    while ((_m = _scriptRe.exec(_srcdoc)) !== null) {
                        const _src = _m[1];
                        if (_src && _src.trim()) {
                            try { ops.op_eval_in_child_realm(_realmId, _src); } catch (_) {}
                        }
                    }
                } catch (_) {}
            }

            // ── Same-origin src document: fetch + execute ────────
            // Real iframe-based challenge flows
            // point the iframe at a same-origin URL whose document
            // runs the challenge and postMessages the result to the parent.
            // Cross-origin src already returned a SecurityError proxy above, so
            // any src reaching here is same-origin. Fetch the doc, reflect its
            // URL into the child realm's location (challenge scripts read
            // location.search for ?parentOrigin=…), and execute its scripts in
            // document order. Bounded + best-effort: a failed/slow fetch is
            // swallowed and the (empty) realm is returned — never hangs the nav.
            let _iSrcUrl2 = "";
            try {
                const _rawSrc2 = (el && typeof el.getAttribute === "function")
                    ? (el.getAttribute("src") || el.src || "") : (el && el.src || "");
                if (_rawSrc2 && _rawSrc2 !== "about:blank"
                    && !/^javascript:/i.test(_rawSrc2) && !/^data:/i.test(_rawSrc2)) {
                    try { _iSrcUrl2 = new URL(_rawSrc2, (globalThis.location && globalThis.location.href) || undefined).href; }
                    catch (_) { _iSrcUrl2 = _rawSrc2; }
                }
            } catch (_) {}
            if (_iSrcUrl2) {
                try {
                    let _u2 = null;
                    try { _u2 = new URL(_iSrcUrl2); } catch (_) {}
                    if (_u2) {
                        _sp("location", {
                            href: _u2.href, origin: _u2.origin, pathname: _u2.pathname,
                            search: _u2.search, hash: _u2.hash, host: _u2.host,
                            hostname: _u2.hostname, port: _u2.port, protocol: _u2.protocol,
                            assign() {}, replace() {}, reload() {},
                            toString() { return _u2.href; },
                        });
                    }
                    const _docHtml = ops.op_net_fetch_sync(_iSrcUrl2, (globalThis.location && globalThis.location.href) || "", 'frame');
                    if (_docHtml && typeof _docHtml === "string" && _docHtml.length < 5000000) {
                        const _tagRe = /<script\b([^>]*)>([\s\S]*?)<\/script>/gi;
                        let _sm;
                        let _guard = 0;
                        while ((_sm = _tagRe.exec(_docHtml)) !== null && _guard++ < 64) {
                            const _attrs = _sm[1] || "";
                            const _inline = _sm[2] || "";
                            const _typeM = /\btype\s*=\s*["']?([^"'\s>]+)/i.exec(_attrs);
                            const _ty = _typeM ? _typeM[1].toLowerCase() : "";
                            if (_ty && _ty !== "text/javascript" && _ty !== "application/javascript" && _ty !== "module") continue;
                            const _srcM = /\bsrc\s*=\s*["']([^"']+)["']/i.exec(_attrs);
                            if (_srcM) {
                                let _eu = _srcM[1];
                                try { _eu = new URL(_eu, _iSrcUrl2).href; } catch (_) {}
                                try {
                                    const _code = ops.op_net_fetch_sync(_eu, _iSrcUrl2, 'script-parser');
                                    if (_code && typeof _code === "string") {
                                        try { ops.op_eval_in_child_realm(_realmId, _code); } catch (_) {}
                                    }
                                } catch (_) {}
                            } else if (_inline && _inline.trim()) {
                                try { ops.op_eval_in_child_realm(_realmId, _inline); } catch (_) {}
                            }
                        }
                    }
                } catch (_) {}
            }

            state = { contentWindow: cw, contentDocument: iframeDoc, _realmId: _realmId, _processedSrcdoc: _srcdoc };
            _iframeState.set(el, state);
            _registerFrame(cw, el);
            return cw;
        }

        // ── FALLBACK: Proxy-based approach (if op unavailable) ───────────
        // Keeps existing behaviour when op_create_child_realm is not accessible
        // (e.g. worker runtime that doesn't load dom_extension).
        const remoteRealm = _buildRemoteRealm();
        const iframeLocals = {
            document: iframeDoc,
            location: { href: "about:blank" },
            parent: globalThis,
            top: globalThis,
            self: null,
            frames: [],
            screen: _iframeScreen,
            innerWidth:  globalThis.innerWidth  || 1920,
            innerHeight: globalThis.innerHeight || 1080,
            outerWidth:  globalThis.outerWidth  || 1920,
            outerHeight: globalThis.outerHeight || 1080,
            scrollX: 0, scrollY: 0, pageXOffset: 0, pageYOffset: 0,
            postMessage(msg, origin) {
                Promise.resolve().then(() => {
                    globalThis.dispatchEvent(new MessageEvent("message", { data: msg, origin: origin || "" }));
                });
            },
        };
        try {
            if (remoteRealm.Window && remoteRealm.Window.prototype) {
                Object.setPrototypeOf(iframeLocals, remoteRealm.Window.prototype);
            }
        } catch (_) {}
        try {
            const _ifetch = function fetch(...a) { return globalThis.fetch.apply(this, a); };
            Object.defineProperty(_ifetch, "name", { value: "fetch", configurable: true });
            Object.defineProperty(_ifetch, "length", { value: 1, configurable: true });
            Object.defineProperty(_ifetch, _NATIVE_TAG_SYMBOL, { value: "fetch", configurable: true });
            iframeLocals.fetch = _ifetch;
        } catch (_) {}
        try {
            const _dg = function () { return globalThis.devicePixelRatio || 1; };
            const _ds = function(v) {
                Object.defineProperty(iframeLocals, "devicePixelRatio", {
                    value: v, writable: true, enumerable: true, configurable: true,
                });
            };
            Object.defineProperty(_dg, _NATIVE_TAG_SYMBOL, { value: "get devicePixelRatio", configurable: true });
            Object.defineProperty(_dg, "name", { value: "get devicePixelRatio", configurable: true });
            Object.defineProperty(_ds, _NATIVE_TAG_SYMBOL, { value: "set devicePixelRatio", configurable: true });
            Object.defineProperty(_ds, "name", { value: "set devicePixelRatio", configurable: true });
            Object.defineProperty(iframeLocals, "devicePixelRatio", {
                get: _dg, set: _ds, enumerable: true, configurable: true,
            });
        } catch (_) {}
        const iframeWindow = new Proxy(iframeLocals, {
            get(target, prop) {
                if (prop in target) return target[prop];
                if (typeof prop === "string" && prop in remoteRealm) return remoteRealm[prop];
                try { return globalThis[prop]; } catch { return undefined; }
            },
            has(target, prop) {
                return prop in target || prop in remoteRealm || prop in globalThis;
            },
            getOwnPropertyDescriptor(target, prop) {
                if (prop in target) {
                    return Object.getOwnPropertyDescriptor(target, prop);
                }
                if (typeof prop === "string" && prop in remoteRealm) {
                    return { value: remoteRealm[prop], writable: true, enumerable: true, configurable: true };
                }
                return undefined;
            },
        });
        iframeLocals.self = iframeWindow;
        iframeLocals.window = iframeWindow;
        iframeLocals.globalThis = iframeWindow;
        iframeLocals.frames = iframeWindow;
        iframeLocals.length = 0;
        state = { contentWindow: iframeWindow, contentDocument: iframeDoc };
        _iframeState.set(el, state);
        _registerFrame(iframeWindow, el);
        return iframeWindow;
    }
    function _getIframeDocument(el) {
        _getIframeWindow(el); // ensure state is built
        return _iframeState.get(el).contentDocument;
    }

    // Install on HTMLIFrameElement.prototype — covers parsed AND created iframes.
    if (typeof HTMLIFrameElement !== 'undefined') {
        Object.defineProperty(HTMLIFrameElement.prototype, 'contentWindow', {
            get: function() {
                return _getIframeWindow(this);
            },
            configurable: true,
            enumerable: true,
        });
        Object.defineProperty(HTMLIFrameElement.prototype, 'contentDocument', {
            get: function() { return _dynFrameOf.has(this) ? null : _getIframeDocument(this); },
            configurable: true,
            enumerable: true,
        });
        // srcdoc setter: when a script sets iframe.srcdoc = "..." BEFORE the first
        // contentWindow access, the value lands on the element's own property dict
        // (no setter exists, so JS creates an own data property). Our fallback in
        // _getIframeWindow reads el.srcdoc if getAttribute("srcdoc") is empty.
        //
        // When srcdoc is set AFTER the first contentWindow access (child realm
        // already cached), this setter fires immediately and re-executes the scripts.
        const _srcdocValues = new WeakMap();
        Object.defineProperty(HTMLIFrameElement.prototype, 'srcdoc', {
            get: function() { return _srcdocValues.get(this) || this.getAttribute('srcdoc') || ''; },
            set: function(v) {
                _srcdocValues.set(this, String(v));
                const _st = _iframeState.get(this);
                if (_st && _st._realmId !== undefined && v && String(v) !== _st._processedSrcdoc) {
                    _st._processedSrcdoc = String(v);
                    try {
                        const _re = /<script[^>]*>([\s\S]*?)<\/script>/gi;
                        let _m3;
                        while ((_m3 = _re.exec(String(v))) !== null) {
                            const _s3 = _m3[1];
                            if (_s3 && _s3.trim()) {
                                try { ops.op_eval_in_child_realm(_st._realmId, _s3); } catch (_) {}
                            }
                        }
                    } catch (_) {}
                }
            },
            configurable: true,
            enumerable: true,
        });
    }

    // Keep the createElement customElements-upgrade hook — still needed for
    // user-defined custom elements.
    const _origCreateElement = Document.prototype.createElement;
    Document.prototype.createElement = function(tag) {
        const el = _origCreateElement.call(this, tag);
        const ceEntry = globalThis._customElementsRegistry && globalThis._customElementsRegistry.get(tag.toLowerCase());
        if (ceEntry) {
            Object.setPrototypeOf(el, ceEntry.constructor.prototype);
            try { ceEntry.constructor.call(el); } catch (e) { console.error(e); }
            el._ceUpgraded = true;
        }
        return el;
    };

    // ================================================================
    // Native-code mask sweep for every JS-defined Web API method.
    //
    // Without this, Function.prototype.toString called on attachShadow,
    // queueMicrotask, Document.createElement, etc. returns the literal
    // JS source — including our deno_core op names like
    // `op_dom_attach_shadow`. Real Chrome returns
    // `function NAME() { [native code] }`; without masking, scripts that
    // inspect these would see our op names and detect the difference.
    //
    // Strategy: walk every named own property of every Web API
    // prototype we define, find any function-typed values + getters +
    // setters, and apply _maskFunction. Idempotent — re-masking a
    // tagged function is a no-op.
    if (typeof globalThis._maskFunction === 'function') {
        const _mask = globalThis._maskFunction;
        const _walkProto = (ctor, ctorName) => {
            if (!ctor) return;
            try { _mask(ctor, ctorName); } catch (_) {}
            const proto = ctor.prototype;
            if (!proto) return;
            for (const key of Object.getOwnPropertyNames(proto)) {
                if (key === 'constructor') continue;
                const desc = Object.getOwnPropertyDescriptor(proto, key);
                if (!desc) continue;
                try {
                    if (typeof desc.value === 'function') _mask(desc.value, key);
                    if (typeof desc.get === 'function') _mask(desc.get, `get ${key}`);
                    if (typeof desc.set === 'function') _mask(desc.set, `set ${key}`);
                } catch (_) {}
            }
        };
        // Every JS-defined Web API class in this bootstrap, plus
        // siblings from window_bootstrap, fetch_bootstrap,
        // canvas_bootstrap, etc. Listed by name so the sweep is
        // conservative — only masks what we've verified exists.
        const _toMask = [
            'EventTarget', 'Node', 'Element', 'HTMLElement',
            'Document', 'HTMLDocument', 'DocumentFragment',
            'ShadowRoot', 'Text', 'Comment', 'Attr',
            'NodeList', 'HTMLCollection', 'NamedNodeMap',
            'DOMTokenList', 'CSSStyleDeclaration',
            // Window-bootstrap-defined classes that previously leaked
            // their JS source via Function.prototype.toString.
            'Bluetooth', 'StorageManager', 'SharedWorker',
            'WorkerGlobalScope', 'NetworkInformation', 'MediaDevices',
            'ServiceWorkerContainer', 'Permissions', 'PermissionStatus',
            'Notification', 'Clipboard', 'CredentialsContainer',
            'PresentationConnection', 'XRSystem', 'GPUAdapter',
            // Canvas/Audio
            'AudioContext', 'BaseAudioContext', 'OfflineAudioContext',
            'AudioWorkletNode', 'OscillatorNode', 'GainNode',
            'AnalyserNode', 'BiquadFilterNode', 'DynamicsCompressorNode',
            // Workers
            'Worker', 'BroadcastChannel', 'MessageChannel', 'MessagePort',
            // Media
            'MediaRecorder', 'MediaSource', 'MediaSession',
            // HTML element subclasses (mostly empty markers, but their
            // class source still leaks via toString without masking).
            'HTMLDivElement', 'HTMLSpanElement', 'HTMLParagraphElement',
            'HTMLAnchorElement', 'HTMLImageElement', 'HTMLCanvasElement',
            'HTMLScriptElement', 'HTMLStyleElement', 'HTMLLinkElement',
            'HTMLMetaElement', 'HTMLTableElement', 'HTMLIFrameElement',
            'HTMLBodyElement', 'HTMLHtmlElement', 'HTMLHeadElement',
            'HTMLInputElement', 'HTMLButtonElement', 'HTMLSelectElement',
            'HTMLTextAreaElement', 'HTMLFormElement', 'HTMLLabelElement',
            'HTMLOptionElement', 'HTMLUListElement', 'HTMLOListElement',
            'HTMLLIElement', 'HTMLHeadingElement', 'HTMLHRElement',
            'HTMLBRElement', 'HTMLPreElement', 'HTMLBlockquoteElement',
            'HTMLVideoElement', 'HTMLAudioElement', 'HTMLMediaElement',
            'HTMLSourceElement', 'HTMLTrackElement', 'HTMLPictureElement',
            'HTMLTemplateElement', 'HTMLSlotElement', 'HTMLDialogElement',
            'HTMLDetailsElement', 'HTMLProgressElement', 'HTMLMeterElement',
        ];
        for (const name of _toMask) {
            const ctor = globalThis[name];
            if (typeof ctor === 'function') _walkProto(ctor, name);
        }

        // Top-level globalThis function-typed members that should be
        // native. queueMicrotask + fetch were the worst offenders —
        // both leaked their literal JS source via
        // Function.prototype.toString.
        const _topLevelFns = [
            'queueMicrotask', 'fetch', 'setTimeout', 'clearTimeout',
            'setInterval', 'clearInterval', 'requestAnimationFrame',
            'cancelAnimationFrame', 'requestIdleCallback', 'cancelIdleCallback',
            'structuredClone', 'reportError',
            'getComputedStyle', 'matchMedia', 'scroll', 'scrollTo', 'scrollBy',
            'alert', 'confirm', 'prompt', 'open', 'close', 'focus', 'blur',
            'postMessage', 'addEventListener', 'removeEventListener',
            'dispatchEvent',
        ];
        for (const name of _topLevelFns) {
            const fn = globalThis[name];
            if (typeof fn === 'function') {
                try { _mask(fn, name); } catch (_) {}
            }
        }
    }

    // Minimal window stub
    globalThis.window = globalThis;
    globalThis.self = globalThis;

    // Expose node-id resolution to sibling bootstrap files that need it
    // (event_bootstrap.js wires listeners by nodeId, not by Node identity).
    // Installed non-enumerable; cleanup_bootstrap.js deletes __browser_oxide
    // before page scripts run. Callers must CAPTURE the helper during
    // their own bootstrap execution, not look it up per-call.
    Object.defineProperty(globalThis, '__browser_oxide', {
        value: { _getNodeId },
        enumerable: false,
        configurable: true,
        writable: false,
    });

    // Warm-reuse DOM-registry reaper. Every registry below is module-private
    // and keyed by (or holding) state that belongs to ONE document, yet it
    // lives as long as the `JsRuntime`. On the cold path that is exactly the
    // life of the page, so nothing was ever pruned; on the warm path
    // (`PagePool` / `Page::navigate_warm`) `replace_dom` swaps the document
    // underneath them and they accumulate forever. See
    // `Page::reset_for_reuse`, which calls this.
    //
    // `_nodeCache` is doubly wrong across a swap: it is keyed by `nodeId`, and
    // node IDs restart at zero for the new document, so a surviving entry
    // hands the NEW page's node the OLD page's wrapper (with the old page's
    // expandos on it). The `WeakRef` values do not save us — an old wrapper
    // stays alive as long as any listener closure references it.
    Object.defineProperty(globalThis, '__resetDomRegistries', {
        value: function __resetDomRegistries() {
            _nodeCache.clear();
            _scrollState.clear();
            _syncFetchInFlight.clear();
            // Observers registered by the previous page's scripts. Pages
            // routinely never call `disconnect()`, so this only shrinks on
            // reuse — each retained observer pins its callback closure and
            // every observed target wrapper.
            _moObservers.length = 0;
            _appendedIframes.length = 0;
            _frameRegistry.length = 0;
            // Re-seed the document wrapper: `_wrapNode` must keep returning
            // the singleton `_document` for the document node id, which
            // `replace_dom` preserves.
            try { _nodeCache.set(ops.op_dom_document_node(), new WeakRef(_document)); } catch (_) {}
        },
        writable: true,
        configurable: true,
        enumerable: false,
    });
})(globalThis);
