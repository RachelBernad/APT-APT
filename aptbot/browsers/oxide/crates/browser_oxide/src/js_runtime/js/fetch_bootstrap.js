((globalThis) => {
    const ops = Deno.core.ops;

    class Headers {
        #map;
        constructor(init = {}) {
            this.#map = {};
            if (init instanceof Headers) {
                init.forEach((v, k) => { this.#map[k] = v; });
            } else if (Array.isArray(init)) {
                for (const [k, v] of init) this.#map[k.toLowerCase()] = v;
            } else if (typeof init === "object") {
                for (const [k, v] of Object.entries(init)) this.#map[k.toLowerCase()] = String(v);
            }
        }
        get(name) { return this.#map[name.toLowerCase()] ?? null; }
        set(name, value) { this.#map[name.toLowerCase()] = String(value); }
        has(name) { return name.toLowerCase() in this.#map; }
        delete(name) { delete this.#map[name.toLowerCase()]; }
        forEach(cb) { for (const [k, v] of Object.entries(this.#map)) cb(v, k, this); }
        entries() { return Object.entries(this.#map)[Symbol.iterator](); }
        keys() { return Object.keys(this.#map)[Symbol.iterator](); }
        values() { return Object.values(this.#map)[Symbol.iterator](); }
        [Symbol.iterator]() { return this.entries(); }
    }

    class Response {
        #body; #rawBytes; #status; #statusText; #headers; #url; #ok; #bodyStream; #src;
        constructor(body, init = {}) {
            this.#src = null;
            // Normalise BodyInit like Chrome: streams are drained lazily,
            // binary views become raw bytes, everything else is stringified.
            if (body != null && typeof globalThis.ReadableStream === "function"
                && body instanceof globalThis.ReadableStream) {
                this.#src = body; body = "";
            } else if (body instanceof ArrayBuffer) {
                init = { ...init, _rawBytes: new Uint8Array(body.slice(0)) }; body = "";
            } else if (ArrayBuffer.isView(body)) {
                init = { ...init, _rawBytes: new Uint8Array(body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength)) }; body = "";
            } else if (body != null && typeof body === "object" && globalThis[Symbol.for('__bo_blob_bytes')](body)) {
                const _bb = globalThis[Symbol.for('__bo_blob_bytes')](body);
                if (body.type && !(init.headers && new Headers(init.headers).has('content-type'))) init = { ...init, headers: { ...(init.headers || {}), 'content-type': body.type } };
                init = { ...init, _rawBytes: _bb }; body = "";
            } else if (body != null && typeof body !== "string") {
                body = String(body);
            }
            this.#body = body ?? "";
            // Optional authoritative binary payload. When set,
            // arrayBuffer()/blob() hand back the exact bytes without
            // a TextEncoder round-trip — critical for blob: fetches
            // with non-UTF-8 content (e.g. PNG, WASM).
            this.#rawBytes = init._rawBytes ?? null;
            this.#status = init.status ?? 200;
            this.#statusText = init.statusText ?? "OK";
            this.#headers = new Headers(init.headers ?? {});
            this.#url = init.url ?? "";
            this.#ok = this.#status >= 200 && this.#status < 300;
            this.#bodyStream = null;
        }
        get status() { return this.#status; }
        get statusText() { return this.#statusText; }
        get ok() { return this.#ok; }
        get headers() { return this.#headers; }
        get url() { return this.#url; }
        // `body` is a ReadableStream per the Fetch spec. We build it
        // lazily so responses that never read `.body` don't pay for
        // stream construction. The stream yields the cached body as
        // one chunk then closes — enough to satisfy `reader.read()`
        // probes and async-iterator consumption.
        async #drain() {
            if (!this.#src) return;
            const r = this.#src.getReader(); this.#src = null;
            const parts = []; let n = 0;
            for (;;) {
                const { done, value } = await r.read();
                if (done) break;
                const u = value instanceof Uint8Array ? value
                    : ArrayBuffer.isView(value) ? new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
                    : value instanceof ArrayBuffer ? new Uint8Array(value)
                    : new TextEncoder().encode(String(value));
                parts.push(u); n += u.byteLength;
            }
            const all = new Uint8Array(n); let o = 0;
            for (const u of parts) { all.set(u, o); o += u.byteLength; }
            this.#rawBytes = all;
        }
        get body() {
            if (this.#src) return this.#src;
            if (this.#bodyStream) return this.#bodyStream;
            if (typeof globalThis.ReadableStream !== "function") return null;
            const raw = this.#rawBytes;
            const textBody = this.#body;
            this.#bodyStream = new globalThis.ReadableStream({
                start(controller) {
                    let bytes = null;
                    if (raw instanceof Uint8Array) {
                        bytes = raw;
                    } else if (textBody != null && textBody !== "") {
                        bytes = new TextEncoder().encode(String(textBody));
                    }
                    if (bytes && bytes.byteLength > 0) controller.enqueue(bytes);
                    controller.close();
                },
            });
            return this.#bodyStream;
        }
        get bodyUsed() {
            return !!this.#bodyStream && this.#bodyStream.locked;
        }
        async text() {
            await this.#drain();
            if (this.#rawBytes && this.#body === "") return new TextDecoder().decode(this.#rawBytes);
            return this.#body;
        }
        async json() { return JSON.parse(await this.text()); }
        async arrayBuffer() {
            await this.#drain();
            if (this.#rawBytes && this.#rawBytes.buffer) {
                return this.#rawBytes.buffer.slice(
                    this.#rawBytes.byteOffset,
                    this.#rawBytes.byteOffset + this.#rawBytes.byteLength
                );
            }
            return new TextEncoder().encode(this.#body).buffer;
        }
        async blob() {
            await this.#drain();
            if (this.#rawBytes) {
                return globalThis[Symbol.for('__bo_blob_from')](this.#rawBytes, this.#headers.get('content-type') || '');
            }
            return new Blob([this.#body]);
        }
        clone() {
            return new Response(this.#body, {
                _rawBytes: this.#rawBytes,
                status: this.#status,
                statusText: this.#statusText,
                headers: this.#headers,
                url: this.#url,
            });
        }
    }

    class Request {
        constructor(input, init = {}) {
            if (typeof input === "string") {
                this.url = input;
            } else if (input instanceof Request) {
                this.url = input.url;
                init = { method: input.method, headers: input.headers, body: input.body, ...init };
            }
            this.method = (init.method ?? "GET").toUpperCase();
            this.headers = new Headers(init.headers ?? {});
            this.body = init.body ?? null;
            this._signal = init.signal ?? null;
        }
        // Task#2: real Chrome's Request has a readonly `signal`
        // accessor ON Request.prototype (per the Fetch spec). Defined
        // as a class getter so `Object.hasOwnProperty.call(
        // Request.prototype,"signal")` is true — duolingo's
        // `supportsAbortController` capability gate requires exactly
        // that; without it the homepage self-redirects to
        // /errors/not-supported.html. Lazily backs an AbortSignal so
        // `request.signal` is a non-null AbortSignal like real Chrome.
        get signal() {
            if (this._signal == null
                && typeof globalThis.AbortController === "function") {
                try {
                    this._signal = new globalThis.AbortController().signal;
                } catch (_e) { /* leave null if AbortController throws */ }
            }
            return this._signal;
        }
    }

    // Pull the net::cookies jar snapshot for the current origin into
    // globalThis.__jsCookies so that document.cookie is always in sync.
    // Called after every fetch response and by page.rs after navigation.
    async function _syncCookiesFromNet(url) {
        try {
            url = url || globalThis.location?.href;
            if (!url || url === "about:blank") return;
            const cookieStr = await ops.op_cookie_get(url);
            if (!globalThis.__jsCookies) globalThis.__jsCookies = {};
            if (!cookieStr) return;
            for (const pair of cookieStr.split(";")) {
                const eq = pair.indexOf("=");
                if (eq < 0) continue;
                const k = pair.slice(0, eq).trim();
                const v = pair.slice(eq + 1).trim();
                if (k) globalThis.__jsCookies[k] = v;
            }
        } catch (e) { /* ignore */ }
    }
    globalThis.__syncCookiesFromNet = _syncCookiesFromNet;

    // Normalize any Headers/object/array init into a plain {lower-name: value} object
    function _flattenHeaders(h) {
        const out = {};
        if (!h) return out;
        if (h instanceof Headers) {
            h.forEach((v, k) => { out[k.toLowerCase()] = String(v); });
        } else if (Array.isArray(h)) {
            for (const [k, v] of h) out[String(k).toLowerCase()] = String(v);
        } else if (typeof h === "object") {
            for (const [k, v] of Object.entries(h)) out[String(k).toLowerCase()] = String(v);
        }
        return out;
    }

    // Real fetch() using Rust op
    globalThis.fetch = async function fetch(input, init = {}) {
        let url, method, body, headers;

        if (typeof input === "string") {
            url = input;
        } else if (input instanceof Request) {
            url = input.url;
            init = { method: input.method, headers: input.headers, body: input.body, ...init };
        } else {
            url = String(input);
        }

        // Resolve relative URLs against current location (like a real browser).
        // Note: URLs like "ftp:" (scheme with no authority) are treated by the
        // URL spec as a relative path component, so `new URL("ftp:", base)`
        // resolves to "<base_origin>/ftp:" — matching real Chrome exactly.
        if (typeof url === "string" && !url.startsWith("http://") && !url.startsWith("https://") && !url.startsWith("data:") && !url.startsWith("blob:")) {
            try {
                const base = globalThis.location?.href || "about:blank";
                if (base && base !== "about:blank") {
                    // Empty url resolves to the document URL itself (real Chrome:
                    // fetch('') hits location.href). The previous `if (url && …)`
                    // guard skipped the empty case, so fetch('') reached op_fetch
                    // unresolved → "relative URL without a base" → a
                    // challenge POST (fetch('') / sendBeacon('')) silently
                    // failed. Our URL polyfill throws on `new URL('',base)`,
                    // so handle empty explicitly.
                    url = (url === "") ? base : new URL(url, base).href;
                }
            } catch (e) { /* keep original URL if resolution fails */ }
        }

        // Reject URLs that still can't be fetched after resolution.
        if (
            url &&
            !url.startsWith("http://") &&
            !url.startsWith("https://") &&
            !url.startsWith("data:") &&
            !url.startsWith("blob:")
        ) {
            throw new TypeError("Failed to fetch");
        }

        // blob: URLs short-circuit the HTTP client: look up the bytes
        // in the Rust BlobRegistry and synthesise a Response. Matches
        // Chrome's `fetch(URL.createObjectURL(blob))` shape — returns
        // 200 with the blob's bytes and content-type.
        if (url && url.startsWith("blob:")) {
            let resp;
            try {
                resp = ops.op_blob_fetch_bytes(url);
            } catch (e) {
                throw new TypeError("Failed to fetch: " + e.message);
            }
            if (!resp || !resp.found) {
                // Unknown blob URL — the spec says a network error.
                throw new TypeError("Failed to fetch: unknown blob URL");
            }
            // `resp.bytes` comes back from serde as an array of numbers;
            // coerce to Uint8Array so Response.arrayBuffer/blob hand
            // back exact bytes.
            const bytes = resp.bytes instanceof Uint8Array
                ? resp.bytes
                : new Uint8Array(resp.bytes);
            const contentType = resp.content_type || "application/octet-stream";
            // text() on the Response returns UTF-8 decoded bytes so
            // `fetch(blob:URL).text()` works for text blobs; binary
            // blobs should prefer `arrayBuffer()` / `blob()` which
            // use the raw byte path.
            const decoded = new TextDecoder("utf-8", { fatal: false }).decode(bytes);
            return new Response(decoded, {
                _rawBytes: bytes,
                status: 200,
                statusText: "OK",
                headers: { "content-type": contentType },
                url,
            });
        }

        method = (init.method ?? "GET").toUpperCase();
        // Body can be: string, ArrayBuffer, TypedArray (Uint8Array), Blob,
        // FormData, URLSearchParams, or null. We must preserve binary
        // fidelity for `application/octet-stream` POSTs.
        //
        // Rust op_fetch accepts the body as a marker-prefixed string:
        //   "s:<text>"   — plain UTF-8 string body
        //   "b:<base64>" — base64-encoded binary body
        const rawBody = init.body;
        // When the body is a FormData, the browser ALWAYS serializes it to
        // multipart/form-data with an auto-generated boundary and sets its
        // own Content-Type (a manually-set Content-Type is ignored for
        // FormData). Track the boundary so we can force the header below.
        let multipartBoundary = null;
        let urlencodedBody = false;
        const _isFormData =
            typeof FormData !== "undefined" && rawBody instanceof FormData;
        const _isUSP =
            typeof URLSearchParams !== "undefined" && rawBody instanceof URLSearchParams;
        let _enc = null;
        if (rawBody == null) {
            body = "";
        } else if (typeof rawBody === "string") {
            body = "s:" + rawBody;
        } else {
            _enc = globalThis[Symbol.for('__bo_encode_body')](rawBody);
            body = globalThis[Symbol.for('__bo_body_wire')](_enc.bytes);
            if (_isFormData) multipartBoundary = _enc.contentType.split("boundary=")[1];
            else if (_isUSP) urlencodedBody = true;
        }
        headers = _flattenHeaders(init.headers);

        // FIX-FORMDATA: a FormData body's Content-Type is browser-controlled —
        // FORCE our generated boundary, overriding any (boundaryless)
        // Content-Type the page set, exactly as Chrome does.
        if (multipartBoundary) {
            headers["content-type"] =
                "multipart/form-data; boundary=" + multipartBoundary;
        } else if (urlencodedBody && !headers["content-type"]) {
            headers["content-type"] = "application/x-www-form-urlencoded;charset=UTF-8";
        }

        if (_enc && !_isFormData && !_isUSP && _enc.contentType && !headers["content-type"]
            && !(rawBody instanceof ArrayBuffer || ArrayBuffer.isView(rawBody))) {
            headers["content-type"] = _enc.contentType;  // Blob.type / text default
        }
        // Auto-set Content-Type for POSTs with a body (mirrors Chrome fetch default)
        if (typeof rawBody === "string" && !headers["content-type"] && (method === "POST" || method === "PUT" || method === "PATCH")) {
            headers["content-type"] = "text/plain;charset=UTF-8";
        }

        // The initiator document URL travels to the net layer as its own
        // argument: it derives Referer (strict-origin-when-cross-origin),
        // sec-fetch-site, Origin and the client hints from it. Chrome's
        // fetch API always carries these.
        let _initiator = '';
        try {
            const loc = globalThis.location;
            if (loc && loc.href && loc.href !== 'about:blank') _initiator = String(loc.href);
        } catch {}

        try {
            const startTime = performance.now();
            const result = await ops.op_fetch(url, method, Object.entries(headers), body, _initiator);
            
            const browser_oxide = globalThis._browser_oxide;
            const fetchLog = browser_oxide && browser_oxide.__fetchLog;
            if (fetchLog) {
                fetchLog.push({ method, url, status: result.status });
            }

            const entries = browser_oxide && browser_oxide.__perfResourceEntries;
            if (entries) {
                entries.push({ url, type: "fetch", startTime, duration: performance.now() - startTime, size: result.body ? result.body.length : 0 });
            }

            // Sync cookies from the net jar into document.cookie so subsequent JS
            // reads (including challenge polling loops) see Set-Cookie
            // values that arrived via this response.
            await _syncCookiesFromNet(url);
            return new Response(result.body, {
                status: result.status,
                statusText: result.status_text,
                headers: result.headers,
                url: result.url,
            });
        } catch (e) {
            // Log error for audit
            const browser_oxide = globalThis._browser_oxide;
            const fetchLog = browser_oxide && browser_oxide.__fetchLog;
            if (fetchLog) {
                fetchLog.push({ method, url, status: 0, error: e.message });
            }
            throw new TypeError("Failed to fetch: " + e.message);
        }
    };

    globalThis.Headers = Headers;
    globalThis.Response = Response;
    globalThis.Request = Request;

    // CSP violation drain — pulls violations the Rust gates queued and
    // dispatches `securitypolicyviolation` events for each on document
    // and window. Real Chrome dispatches the event synchronously at
    // the moment of block; we batch + drain because our gates run
    // off-OpState. Drain on a few timed checkpoints so listeners
    // installed early in page lifecycle catch the events.
    function _drainCspViolations() {
        try {
            const violations = ops.op_drain_csp_violations();
            if (!violations || !violations.length) return;
            for (const v of violations) {
                let ev;
                try {
                    ev = new SecurityPolicyViolationEvent("securitypolicyviolation", {
                        bubbles: true,
                        cancelable: false,
                        blockedURI: v.blockedURI,
                        violatedDirective: v.violatedDirective,
                        effectiveDirective: v.effectiveDirective,
                        disposition: v.disposition,
                    });
                } catch (_) {
                    // Fallback if SecurityPolicyViolationEvent is unavailable.
                    ev = new CustomEvent("securitypolicyviolation", {
                        bubbles: true, cancelable: false, detail: v,
                    });
                }
                try { if (globalThis.document) globalThis.document.dispatchEvent(ev); } catch (_) {}
                try { globalThis.dispatchEvent(ev); } catch (_) {}
                if (globalThis.console && typeof console.error === 'function') {
                    console.error(
                        "Refused to load '" + v.blockedURI +
                        "' because it violates the following Content Security Policy directive: \"" +
                        v.effectiveDirective + "\"."
                    );
                }
            }
        } catch (_) { /* best-effort */ }
    }

    Promise.resolve().then(_drainCspViolations);
    if (typeof setTimeout === "function") {
        setTimeout(_drainCspViolations, 0);
        setTimeout(_drainCspViolations, 50);
        setTimeout(_drainCspViolations, 250);
    }
    Object.defineProperty(globalThis, "__drainCspViolations", {
        value: _drainCspViolations, enumerable: false, configurable: true,
    });

    // Mask Function.prototype.toString so scripts that inspect it
    // see `function fetch() { [native code] }` (as in real Chrome)
    // instead of our literal source.
    if (typeof globalThis._maskFunction === 'function') {
        try { globalThis._maskFunction(globalThis.fetch, 'fetch'); } catch (_) {}
        if (globalThis.Request) try { globalThis._maskFunction(globalThis.Request, 'Request'); } catch (_) {}
        if (globalThis.Response) try { globalThis._maskFunction(globalThis.Response, 'Response'); } catch (_) {}
        if (globalThis.Headers) try { globalThis._maskFunction(globalThis.Headers, 'Headers'); } catch (_) {}
    }
})(globalThis);
