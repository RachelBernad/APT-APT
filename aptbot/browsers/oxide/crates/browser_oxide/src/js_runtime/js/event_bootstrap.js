((globalThis) => {
    // ---- Trusted-event authenticity (v0.1.0 behavioral E1) ----------------
    // `isTrusted` MUST be both unforgeable and shaped like a real browser's:
    //   * a GETTER on Event.prototype — NOT an own data property. Scripts
    //     that read `getOwnPropertyDescriptor(evt,'isTrusted')` can flag an
    //     own-data `isTrusted` as synthetic (real browsers expose it via
    //     the prototype).
    //   * backed by a MODULE-PRIVATE WeakSet that page JS cannot reach. The
    //     old design keyed trust off `Symbol.for('__bo_trusted__')` — the
    //     GLOBAL symbol registry — so any page could re-derive the symbol and
    //     forge a trusted event (`new Event('x', {[Symbol.for(...)]: true})`).
    // Only our privileged init scripts mint trust, via `_markTrusted`, handed
    // off below through a temp global they capture-and-delete before any page
    // script runs. There is no in-band (options/symbol) path from page JS.
    const _trustedEvents = new WeakSet();
    const _markTrusted = (ev) => {
        try { if (ev && typeof ev === 'object') _trustedEvents.add(ev); } catch (_) {}
        return ev;
    };

    class Event {
        constructor(type, options = {}) {
            this.type = type;
            this.bubbles = !!options.bubbles;
            this.cancelable = !!options.cancelable;
            this.composed = !!options.composed;
            this.defaultPrevented = false;
            this.target = null;
            this.currentTarget = null;
            this.eventPhase = 0;
            // NOTE: `isTrusted` is intentionally NOT set here. It is a prototype
            // getter (installed below) reading the private WeakSet — default
            // false for page-constructed events; trusted only when our
            // privileged dispatch path calls `_markTrusted(ev)`.
            this.timeStamp = performance.now();
            this._stopped = false;
            this._stoppedImmediate = false;
        }
        preventDefault() {
            if (this.cancelable && !this.__inPassive) this.defaultPrevented = true;
        }
        // Legacy initializer (DOM §2.2): no-op while the event is being dispatched.
        initEvent(type, bubbles = false, cancelable = false) {
            if (arguments.length < 1) throw new TypeError("Failed to execute 'initEvent' on 'Event': 1 argument required, but only 0 present.");
            if (this.eventPhase !== 0) return;
            this.type = String(type);
            this.bubbles = !!bubbles;
            this.cancelable = !!cancelable;
            this.defaultPrevented = false;
            this._stopped = false;
            this._stoppedImmediate = false;
            this.target = null;
        }
        get returnValue() { return !this.defaultPrevented; }
        set returnValue(v) { if (!v) this.preventDefault(); }
        get cancelBubble() { return !!this._stopped; }
        set cancelBubble(v) { if (v) this._stopped = true; }
        get srcElement() { return this.target; }
        stopPropagation() { this._stopped = true; }
        stopImmediatePropagation() { this._stopped = true; this._stoppedImmediate = true; }
        composedPath() {
            const path = [];
            let node = this.target;
            while (node) { path.push(node); node = node.parentNode; }
            return path;
        }
        // Phase constants
        static NONE = 0;
        static CAPTURING_PHASE = 1;
        static AT_TARGET = 2;
        static BUBBLING_PHASE = 3;
    }

    // isTrusted as an inherited, native-masked prototype accessor backed by the
    // private WeakSet. Subclasses (CustomEvent, MouseEvent, …) inherit it. The
    // descriptor shape matches real Chrome: {get: ƒ, set: undefined,
    // enumerable: true, configurable: true}.
    Object.defineProperty(Event.prototype, 'isTrusted', {
        configurable: true,
        enumerable: true,
        get: (typeof _maskFunction === 'function')
            ? _maskFunction(function () { return _trustedEvents.has(this); }, 'get isTrusted')
            : function () { return _trustedEvents.has(this); },
    });

    class CustomEvent extends Event {
        constructor(type, options = {}) {
            super(type, options);
            this.detail = options.detail !== undefined ? options.detail : null;
        }
        initCustomEvent(type, bubbles = false, cancelable = false, detail = null) {
            if (this.eventPhase !== 0) return;
            this.initEvent(type, bubbles, cancelable);
            this.detail = detail;
        }
    }

    // --- UI Event hierarchy ---
    class UIEvent extends Event {
        constructor(type, options = {}) {
            super(type, options);
            this.view = options.view || null;
            this.detail = options.detail || 0;
        }
        initUIEvent(type, bubbles = false, cancelable = false, view = null, detail = 0) {
            if (this.eventPhase !== 0) return;
            this.initEvent(type, bubbles, cancelable);
            this.view = view;
            this.detail = detail | 0;
        }
    }

    class MouseEvent extends UIEvent {
        constructor(type, options = {}) {
            super(type, options);
            this.screenX = options.screenX || 0;
            this.screenY = options.screenY || 0;
            this.clientX = options.clientX || 0;
            this.clientY = options.clientY || 0;
            this.pageX = options.pageX || this.clientX;
            this.pageY = options.pageY || this.clientY;
            this.offsetX = options.offsetX || 0;
            this.offsetY = options.offsetY || 0;
            this.button = options.button || 0;
            this.buttons = options.buttons || 0;
            this.ctrlKey = !!options.ctrlKey;
            this.shiftKey = !!options.shiftKey;
            this.altKey = !!options.altKey;
            this.metaKey = !!options.metaKey;
            this.relatedTarget = options.relatedTarget || null;
            this.movementX = options.movementX || 0;
            this.movementY = options.movementY || 0;
        }
        getModifierState(key) { return false; }
        initMouseEvent(type, bubbles = false, cancelable = false, view = null, detail = 0, screenX = 0, screenY = 0,
                       clientX = 0, clientY = 0, ctrlKey = false, altKey = false, shiftKey = false, metaKey = false,
                       button = 0, relatedTarget = null) {
            if (this.eventPhase !== 0) return;
            this.initUIEvent(type, bubbles, cancelable, view, detail);
            Object.assign(this, { screenX, screenY, clientX, clientY, pageX: clientX, pageY: clientY,
                ctrlKey: !!ctrlKey, altKey: !!altKey, shiftKey: !!shiftKey, metaKey: !!metaKey, button: button | 0, relatedTarget });
        }
    }

    class KeyboardEvent extends UIEvent {
        constructor(type, options = {}) {
            super(type, options);
            this.key = options.key || "";
            this.code = options.code || "";
            this.keyCode = options.keyCode || 0;
            this.charCode = options.charCode || 0;
            this.which = options.which || options.keyCode || 0;
            this.ctrlKey = !!options.ctrlKey;
            this.shiftKey = !!options.shiftKey;
            this.altKey = !!options.altKey;
            this.metaKey = !!options.metaKey;
            this.repeat = !!options.repeat;
            this.isComposing = !!options.isComposing;
            this.location = options.location || 0;
        }
        getModifierState(key) { return false; }
        initKeyboardEvent(type, bubbles = false, cancelable = false, view = null, key = "", location = 0,
                          ctrlKey = false, altKey = false, shiftKey = false, metaKey = false) {
            if (this.eventPhase !== 0) return;
            this.initUIEvent(type, bubbles, cancelable, view, 0);
            Object.assign(this, { key: String(key), location: location | 0, ctrlKey: !!ctrlKey, altKey: !!altKey, shiftKey: !!shiftKey, metaKey: !!metaKey });
        }
    }

    class InputEvent extends UIEvent {
        constructor(type, options = {}) {
            super(type, { bubbles: true, cancelable: false, ...options });
            this.data = options.data || null;
            this.inputType = options.inputType || "";
            this.isComposing = !!options.isComposing;
        }
    }

    class FocusEvent extends UIEvent {
        constructor(type, options = {}) {
            super(type, options);
            this.relatedTarget = options.relatedTarget || null;
        }
    }

    class PointerEvent extends MouseEvent {
        constructor(type, options = {}) {
            super(type, options);
            this.pointerId = options.pointerId || 0;
            this.width = options.width || 1;
            this.height = options.height || 1;
            this.pressure = options.pressure || 0;
            this.tangentialPressure = options.tangentialPressure || 0;
            this.tiltX = options.tiltX || 0;
            this.tiltY = options.tiltY || 0;
            this.twist = options.twist || 0;
            this.pointerType = options.pointerType || "mouse";
            this.isPrimary = options.isPrimary !== undefined ? options.isPrimary : true;
        }
    }

    class WheelEvent extends MouseEvent {
        constructor(type, options = {}) {
            super(type, options);
            this.deltaX = options.deltaX || 0;
            this.deltaY = options.deltaY || 0;
            this.deltaZ = options.deltaZ || 0;
            this.deltaMode = options.deltaMode || 0;
        }
        static DOM_DELTA_PIXEL = 0;
        static DOM_DELTA_LINE = 1;
        static DOM_DELTA_PAGE = 2;
    }

    class TouchEvent extends UIEvent {
        constructor(type, options = {}) {
            super(type, { bubbles: true, cancelable: true, ...options });
            this.touches = options.touches || [];
            this.targetTouches = options.targetTouches || [];
            this.changedTouches = options.changedTouches || [];
            this.ctrlKey = !!options.ctrlKey;
            this.shiftKey = !!options.shiftKey;
            this.altKey = !!options.altKey;
            this.metaKey = !!options.metaKey;
        }
    }

    class MessageEvent extends Event {
        constructor(type, options = {}) {
            super(type, options);
            this.data = options.data !== undefined ? options.data : null;
            this.origin = options.origin || "";
            this.lastEventId = options.lastEventId || "";
            this.source = options.source || null;
            this.ports = options.ports || [];
        }
    }

    class ErrorEvent extends Event {
        constructor(type, options = {}) {
            super(type, options);
            this.message = options.message || "";
            this.filename = options.filename || "";
            this.lineno = options.lineno || 0;
            this.colno = options.colno || 0;
            this.error = options.error || null;
        }
    }

    class ProgressEvent extends Event {
        constructor(type, options = {}) {
            super(type, options);
            this.lengthComputable = !!options.lengthComputable;
            this.loaded = options.loaded || 0;
            this.total = options.total || 0;
        }
    }

    class AnimationEvent extends Event {
        constructor(type, options = {}) {
            super(type, { bubbles: true, ...options });
            this.animationName = options.animationName || "";
            this.elapsedTime = options.elapsedTime || 0;
            this.pseudoElement = options.pseudoElement || "";
        }
    }

    class TransitionEvent extends Event {
        constructor(type, options = {}) {
            super(type, { bubbles: true, ...options });
            this.propertyName = options.propertyName || "";
            this.elapsedTime = options.elapsedTime || 0;
            this.pseudoElement = options.pseudoElement || "";
        }
    }

    class ClipboardEvent extends Event {
        constructor(type, options = {}) {
            super(type, { bubbles: true, cancelable: true, ...options });
            this.clipboardData = options.clipboardData || null;
        }
    }

    class PopStateEvent extends Event {
        constructor(type, options = {}) {
            super(type, options);
            this.state = options.state !== undefined ? options.state : null;
        }
    }

    class HashChangeEvent extends Event {
        constructor(type, options = {}) {
            super(type, options);
            this.oldURL = options.oldURL || "";
            this.newURL = options.newURL || "";
        }
    }

    class StorageEvent extends Event {
        constructor(type, options = {}) {
            super(type, options);
            this.key = options.key || null;
            this.oldValue = options.oldValue || null;
            this.newValue = options.newValue || null;
            this.url = options.url || "";
            this.storageArea = options.storageArea || null;
        }
    }

    class PageTransitionEvent extends Event {
        constructor(type, options = {}) {
            super(type, options);
            this.persisted = !!options.persisted;
        }
    }

    class BeforeUnloadEvent extends Event {
        constructor(type, options = {}) {
            super(type, { cancelable: true, ...options });
            this.returnValue = "";
        }
    }

    class DragEvent extends MouseEvent {
        constructor(type, options = {}) {
            super(type, options);
            this.dataTransfer = options.dataTransfer || null;
        }
    }

    // --- EventTarget core logic ---
    const _nodeListeners = new Map(); // nodeId → Map<eventType, [{callback, capture, once}]>
    let _objListeners = new WeakMap(); // object → Map<eventType, [{callback, capture, once}]>

    // Warm-reuse listener reaper — the events-side analogue of
    // `timer_bootstrap.js`'s `__cancelAllTimers()`. A pooled `Page`
    // (`PagePool` / `Page::navigate_warm`) keeps ONE `JsRuntime` alive across
    // navigations, so both registries above outlive the document they were
    // populated for. Two distinct failures follow:
    //
    //   * Leak. `_objListeners` is keyed by target *object*; listeners a page
    //     attaches to `window`/`globalThis` (analytics, scroll handlers, …)
    //     are keyed against the one global that is never collected for the
    //     life of the isolate, so those callbacks — and every closure
    //     variable they capture, which can be the page's whole object graph —
    //     are retained forever. `_nodeListeners` is worse: it is a *strong*
    //     Map that is never pruned at all. Measured at ~10 MB/page of live
    //     (non-GC-able) V8 heap on real product pages, unbounded.
    //   * Cross-page misfire. `_nodeListeners` is keyed by `nodeId`, and node
    //     IDs restart from zero when `replace_dom` swaps the document. The
    //     previous page's handler for node 42 therefore fires on the *new*
    //     page's node 42.
    //
    // Called from `Page::reset_for_reuse` alongside `__cancelAllTimers()`.
    // Non-enumerable so it does not widen `Object.getOwnPropertyNames(window)`.
    Object.defineProperty(globalThis, '__cancelAllListeners', {
        value: function __cancelAllListeners() {
            _nodeListeners.clear();
            _detachedListeners = new WeakMap();
            _sweepAt = 4096;
            // Reassign rather than clear: WeakMap has no `clear()`, and the
            // whole point is to drop the `window`-keyed entry.
            _objListeners = new WeakMap();
        },
        writable: true,
        configurable: true,
        enumerable: false,
    });

    const _getNodeIdOrMinusOne = (globalThis.__browser_oxide && globalThis.__browser_oxide._getNodeId)
        ? globalThis.__browser_oxide._getNodeId
        : (() => -1);

    // Listener maps of nodes that left the document, keyed by wrapper so
    // they live exactly as long as page code can still reach the node.
    // `_nodeListeners` (strong, by nodeId) never shrank before: every
    // listener ever added to a removed node — and the React fiber trees its
    // closure captures — stayed alive (FB: closed post dialogs, ~MBs each).
    let _detachedListeners = new WeakMap();
    let _sweepAt = 4096;
    let _wrapById = null;   // resolved lazily from the private script hook
    // The DOM reaper (dom_bootstrap.js _reapRun) frees unreachable removed subtrees;
    // drop their id-keyed listener maps too (nothing can dispatch to them any more).
    {
        const h = globalThis[Symbol.for('__browser_oxide_script_hook__')];
        if (h) {
            h.forgetNodes = (ids) => { for (let i = 0; i < ids.length; i++) { _nodeListeners.delete(ids[i]); _groupByRoot.delete(ids[i]); } };
            // Listener closures often capture their own node: while the map sits in the strong
            // id-keyed table the wrapper stays alive and the reaper cannot free the subtree.
            h.sweepListeners = () => _sweepListeners();
        }
    }
    function _sweepListeners() {
        _sweepAt = Math.max(4096, _nodeListeners.size * 2);
        if (!_wrapById) {
            const h = globalThis[Symbol.for('__browser_oxide_script_hook__')];
            _wrapById = (h && typeof h.wrap === 'function') ? h.wrap : null;
            if (!_wrapById) return;
        }
        for (const [id, m] of _nodeListeners) {
            if (id < 0) continue;                    // window / document pseudo ids
            let w = null;
            try { w = _wrapById(id); } catch (_) {}
            if (!w || w.isConnected !== false || w.nodeType === 9) continue;
            _nodeListeners.delete(id);
            let empty = true;
            for (const arr of m.values()) if (arr.length) { empty = false; break; }
            if (!empty) { _detachedListeners.set(w, m); _pinInGroup(id, w); }
        }
        _sweepAt = Math.max(4096, _nodeListeners.size * 2);
    }
    // Tree grouping (Blink keeps a detached tree's wrappers alive as one unit): a wrapper
    // whose node has listeners is pinned in its detached tree's group object, and every
    // live wrapper of that tree points at the group. Holding any node of the tree keeps the
    // listeners (and wrapper identity); holding none lets the whole group be collected,
    // after which the DOM reaper frees the Rust nodes.
    const _groupOf = new WeakMap();          // wrapper -> group {pins: Set}
    const _groupByRoot = new Map();          // detached root id -> WeakRef(group)
    function _pinInGroup(id, w) {
        const h = globalThis[Symbol.for('__browser_oxide_script_hook__')];
        if (!h || !h.detachedRoot) return;
        const root = h.detachedRoot(id);
        if (root < 0) return;
        let g = _groupByRoot.get(root);
        g = g && g.deref();
        if (!g) {
            g = { pins: new Set() };
            _groupByRoot.set(root, new WeakRef(g));
            // Point every currently-live wrapper of the tree at the group.
            const ids = h.subtreeIds(root);
            for (let i = 0; i < ids.length; i++) {
                const ww = h.liveWrapper(ids[i]);
                if (ww) _groupOf.set(ww, g);
            }
        }
        g.pins.add(w);
        _groupOf.set(w, g);
    }

    function _getListenersMap(target) {
        const nodeId = _getNodeIdOrMinusOne(target);
        // Node IDs: >0 for elements/text, 0 for document (sometimes), -999 for window.
        // We use the Map for any node that has a stable ID.
        if (nodeId !== -1) {
            let m = _nodeListeners.get(nodeId);
            if (!m) {
                m = _detachedListeners.get(target);
                if (m) _detachedListeners.delete(target); else m = new Map();
                _nodeListeners.set(nodeId, m);
                if (_nodeListeners.size > _sweepAt) _sweepListeners();
            }
            return m;
        } else {
            let m = _objListeners.get(target);
            if (!m) { m = new Map(); _objListeners.set(target, m); }
            return m;
        }
    }

    function _getListeners(target, type) {
        const nodeMap = _getListenersMap(target);
        let arr = nodeMap.get(type);
        if (!arr) { arr = []; nodeMap.set(type, arr); }
        return arr;
    }

    const _addEventListener = function addEventListener(type, callback, options) {
        if (typeof callback !== "function" && typeof callback !== "object") return;
        const capture = typeof options === "boolean" ? options : !!(options && options.capture);
        const once = typeof options === "object" && options ? !!options.once : false;
        const passive = typeof options === "object" && options ? !!options.passive : false;
        const listeners = _getListeners(this, type);
        // Prevent duplicate
        if (listeners.some(l => l.callback === callback && l.capture === capture)) return;
        listeners.push({ callback, capture, once, passive });
    };

    const _removeEventListener = function removeEventListener(type, callback, options) {
        const capture = typeof options === "boolean" ? options : !!(options && options.capture);
        const listeners = _getListeners(this, type);
        const idx = listeners.findIndex(l => l.callback === callback && l.capture === capture);
        if (idx !== -1) listeners.splice(idx, 1);
    };

    const _dispatchEvent = function dispatchEvent(event) {
        if (!(event instanceof Event)) {
            throw new TypeError("Failed to execute 'dispatchEvent' on 'EventTarget': parameter 1 is not of type 'Event'.");
        }
        event.target = this;
        const nodeId = _getNodeIdOrMinusOne(this);

        // Build propagation path (target → root) if it's a DOM node.
        // Real Chrome's EventTarget.prototype.dispatchEvent handles the
        // tree-walk automatically if 'this' is a Node.
        const path = [];
        if (nodeId !== -1 && this.parentNode !== undefined) {
            let current = this;
            while (current) {
                path.push(current);
                current = current.parentNode;
            }
            // The window is the last hop of every document-tree event except
            // `load` (HTML spec "get the parent" for Document).
            const top = path[path.length - 1];
            if (top && top.nodeType === 9 && event.type !== 'load' && top.defaultView) path.push(top.defaultView);
        }

        // Capture phase (root → target)
        if (path.length > 0 && !event._stopped) {
            for (let i = path.length - 1; i > 0; i--) {
                event.currentTarget = path[i];
                event.eventPhase = 1;
                _fireListeners(path[i], event, true);
                if (event._stopped) break;
            }
        }

        // Target phase
        if (!event._stopped) {
            event.currentTarget = this;
            event.eventPhase = 2;
            // Chrome (89+) runs capturing listeners at the target first.
            _fireListeners(this, event, true);
            _fireListeners(this, event, false);
        }

        // Bubble phase (target → root)
        if (path.length > 0 && !event._stopped && event.bubbles) {
            for (let i = 1; i < path.length; i++) {
                event.currentTarget = path[i];
                event.eventPhase = 3;
                _fireListeners(path[i], event, false);
                if (event._stopped) break;
            }
        }

        event.eventPhase = 0;
        event.currentTarget = null;
        return !event.defaultPrevented;
    };

    function _reportListenerError(e) {
        try { console.error(e); } catch (_) {}
        try {
            if (typeof globalThis.onerror === 'function') {
                globalThis.onerror(String(e && e.message || e), '', 0, 0, e);
            }
        } catch (_) {}
    }

    function _fireListeners(target, event, capturePhase) {
        // --- 1. Fire on* handler (Target phase only, not capture phase) ---
        if (!capturePhase && !event._stoppedImmediate) {
            const handlerName = `on${event.type}`;
            const handler = target[handlerName];
            if (typeof handler === "function") {
                try {
                    handler.call(target, event);
                } catch (e) {
                    console.error(e);
                }
            }
        }

        // --- 2. Fire registered listeners ---
        const listeners = _getListeners(target, event.type);
        const toRemove = [];
        for (let i = 0; i < listeners.length; i++) {
            const l = listeners[i];
            if (l.capture !== capturePhase) continue;
            if (event._stoppedImmediate) break;
            if (l.once) toRemove.push(i);
            // A throwing listener is reported and the dispatch goes on, as in
            // browsers ("report the exception"); it must not abort the others.
            if (l.passive) event.__inPassive = true;
            try {
                if (typeof l.callback === "function") {
                    l.callback.call(target, event);
                } else if (l.callback && typeof l.callback.handleEvent === "function") {
                    l.callback.handleEvent(event);
                }
            } catch (e) {
                _reportListenerError(e);
            } finally {
                if (l.passive) event.__inPassive = false;
            }
        }
        for (let i = toRemove.length - 1; i >= 0; i--) {
            listeners.splice(toRemove[i], 1);
        }
    }

    // Install on EventTarget.prototype — this is the canonical location.
    // Real Chrome has them as configurable/writable/enumerable=true.
    const _ET = globalThis.EventTarget;
    if (_ET && _ET.prototype) {
        const proto = _ET.prototype;
        Object.defineProperty(proto, 'addEventListener', {
            value: _addEventListener, writable: true, enumerable: true, configurable: true,
        });
        Object.defineProperty(proto, 'removeEventListener', {
            value: _removeEventListener, writable: true, enumerable: true, configurable: true,
        });
        Object.defineProperty(proto, 'dispatchEvent', {
            value: _dispatchEvent, writable: true, enumerable: true, configurable: true,
        });
    }

    // Ensure Node.prototype does NOT shadow these. Real Chrome's
    // Node.prototype does not have its own addEventListener.
    const origNodeProto = globalThis.Node.prototype;
    if (origNodeProto) {
        delete origNodeProto.addEventListener;
        delete origNodeProto.removeEventListener;
        delete origNodeProto.dispatchEvent;
    }

    // Native-code masking — some scripts run
    // `Function.prototype.toString.call(addEventListener)` against both
    // window-level and prototype-level methods. Each must serialize as
    // `function NAME() { [native code] }`, as in a real browser.
    if (typeof _maskFunction === 'function') {
        _maskFunction(_addEventListener, 'addEventListener');
        _maskFunction(_removeEventListener, 'removeEventListener');
        _maskFunction(_dispatchEvent, 'dispatchEvent');
    }

    // Window (globalThis) inheritance: real Chrome's Window inherits from
    // EventTarget via the prototype chain. Our Window setup (Window →
    // WindowProperties → EventTarget) should already handle this, but
    // we ensure the global aliases are correct.
    const _winProto = Object.getPrototypeOf(globalThis);
    if (_winProto && _winProto !== Object.prototype) {
        // Just ensure they are there if not inherited.
        if (!('addEventListener' in _winProto)) {
            Object.defineProperty(_winProto, 'addEventListener', {
                value: _addEventListener, writable: true, enumerable: true, configurable: true,
            });
        }
        if (!('removeEventListener' in _winProto)) {
            Object.defineProperty(_winProto, 'removeEventListener', {
                value: _removeEventListener, writable: true, enumerable: true, configurable: true,
            });
        }
        if (!('dispatchEvent' in _winProto)) {
            Object.defineProperty(_winProto, 'dispatchEvent', {
                value: _dispatchEvent, writable: true, enumerable: true, configurable: true,
            });
        }
    } else {
        globalThis.addEventListener = _addEventListener;
        globalThis.removeEventListener = _removeEventListener;
        globalThis.dispatchEvent = _dispatchEvent;
    }

    // Export all event classes
    // SecurityPolicyViolationEvent — what real Chrome dispatches on
    // `document` (and propagates to `window`) when a CSP rule blocks
    // a fetch. Sites can listen for `securitypolicyviolation` to log
    // their own violations; we must surface the same shape so that
    // analytics/telemetry code probing the event fires correctly.
    // Spec: https://www.w3.org/TR/CSP3/#securitypolicyviolationevent
    class SecurityPolicyViolationEvent extends Event {
        constructor(type, init) {
            super(type, init || {});
            const i = init || {};
            this.blockedURI = String(i.blockedURI ?? "");
            this.documentURI = String(i.documentURI ?? (typeof location !== 'undefined' ? location.href : ""));
            this.referrer = String(i.referrer ?? (typeof document !== 'undefined' && document.referrer ? document.referrer : ""));
            this.violatedDirective = String(i.violatedDirective ?? "");
            this.effectiveDirective = String(i.effectiveDirective ?? this.violatedDirective);
            this.originalPolicy = String(i.originalPolicy ?? "");
            this.disposition = String(i.disposition ?? "enforce");
            this.sample = String(i.sample ?? "");
            this.sourceFile = String(i.sourceFile ?? "");
            this.statusCode = +i.statusCode || 0;
            this.lineNumber = +i.lineNumber || 0;
            this.columnNumber = +i.columnNumber || 0;
        }
    }

    globalThis.Event = Event;
    globalThis.CustomEvent = CustomEvent;
    globalThis.SecurityPolicyViolationEvent = SecurityPolicyViolationEvent;
    globalThis.UIEvent = UIEvent;
    globalThis.MouseEvent = MouseEvent;
    globalThis.KeyboardEvent = KeyboardEvent;
    globalThis.InputEvent = InputEvent;
    globalThis.FocusEvent = FocusEvent;
    globalThis.PointerEvent = PointerEvent;
    globalThis.WheelEvent = WheelEvent;
    globalThis.TouchEvent = TouchEvent;
    globalThis.MessageEvent = MessageEvent;
    globalThis.ErrorEvent = ErrorEvent;
    globalThis.ProgressEvent = ProgressEvent;
    globalThis.AnimationEvent = AnimationEvent;
    globalThis.TransitionEvent = TransitionEvent;
    globalThis.ClipboardEvent = ClipboardEvent;
    globalThis.PopStateEvent = PopStateEvent;
    globalThis.HashChangeEvent = HashChangeEvent;
    globalThis.StorageEvent = StorageEvent;
    globalThis.PageTransitionEvent = PageTransitionEvent;
    globalThis.BeforeUnloadEvent = BeforeUnloadEvent;
    globalThis.DragEvent = DragEvent;
    // SubmitEvent: a constructible Event subclass (the interface stub threw
    // "Illegal constructor"); form.requestSubmit() dispatches one.
    class SubmitEvent extends Event {
        constructor(type, options = {}) {
            super(type, options);
            this.submitter = options.submitter !== undefined ? options.submitter : null;
        }
    }
    globalThis.SubmitEvent = SubmitEvent;
    if (typeof _maskFunction === 'function') _maskFunction(SubmitEvent, 'SubmitEvent');
    // EventTarget is already defined in dom_bootstrap.js as the base of
    // the Node prototype chain — do not reassign it here or the
    // `document instanceof EventTarget` check will break.

    // ---- Trusted input driver (fbgb fork) ------------------------------------
    // Drives the page the way a user's mouse and keyboard would: every event
    // is minted trusted, carries Chrome's field set, and the default actions
    // (focus on mousedown, checkbox toggle, link follow, form submission,
    // text insertion through the native value setter so React's value tracker
    // sees the change) run only when the page did not cancel the event.
    // Steps are spaced with human-like delays on the native timer; the host
    // pumps the event loop until `busy()` is false.
    // Reachable only through the Symbol-keyed hook dom_bootstrap.js installs.
    {
        const _hook = globalThis[Symbol.for('__browser_oxide_script_hook__')];
        if (_hook && typeof _hook._setTrust === 'function') _hook._setTrust(_markTrusted);
        const _st = globalThis.setTimeout;
        const _rnd = globalThis[Symbol.for('__browser_oxide_behavior_rand__')] || Math.random;
        const _between = (a, b) => a + Math.floor(_rnd() * (b - a + 1));
        const _focusProto = globalThis.HTMLElement && HTMLElement.prototype.focus;
        const _valueSetter = (el) => {
            for (let p = Object.getPrototypeOf(el); p; p = Object.getPrototypeOf(p)) {
                const d = Object.getOwnPropertyDescriptor(p, 'value');
                if (d && d.set) return d.set;
            }
            return null;
        };
        const _valueGetter = (el) => {
            for (let p = Object.getPrototypeOf(el); p; p = Object.getPrototypeOf(p)) {
                const d = Object.getOwnPropertyDescriptor(p, 'value');
                if (d && d.get) return d.get;
            }
            return null;
        };
        const _readValue = (el) => { const g = _valueGetter(el); return String(g ? g.call(el) : (el.value || '')); };
        const _writeValue = (el, v) => { const s = _valueSetter(el); if (s) s.call(el, v); else el.value = v; };

        const _queue = [];
        let _running = false;
        let _log = [];
        const _pump = () => {
            if (!_queue.length) { _running = false; return; }
            _running = true;
            const [fn, delay] = _queue.shift();
            _st(() => {
                try { fn(); } catch (e) { _log.push('step error: ' + (e && e.message || e)); }
                _pump();
            }, delay);
        };
        const _enqueue = (fn, delay) => { _queue.push([fn, delay]); if (!_running) _pump(); };

        const _mouse = { x: _between(300, 700), y: _between(200, 500), over: null, buttons: 0 };
        const _scr = () => ({ sx: (globalThis.screenX || 0), sy: (globalThis.screenY || 0) + Math.max(0, (globalThis.outerHeight || 0) - (globalThis.innerHeight || 0)) });

        const _mouseInit = (x, y, extra) => {
            const o = _scr();
            return Object.assign({
                bubbles: true, cancelable: true, composed: true, view: globalThis, detail: 0,
                clientX: x, clientY: y, screenX: x + o.sx, screenY: y + o.sy,
                pageX: x + (globalThis.scrollX || 0), pageY: y + (globalThis.scrollY || 0),
                button: 0, buttons: _mouse.buttons, relatedTarget: null,
            }, extra || {});
        };
        const _ptrInit = (x, y, extra) => Object.assign(_mouseInit(x, y), {
            pointerId: 1, pointerType: 'mouse', isPrimary: true, width: 1, height: 1,
            pressure: _mouse.buttons ? 0.5 : 0, tangentialPressure: 0, tiltX: 0, tiltY: 0, twist: 0,
        }, extra || {});
        const _fire = (target, ev) => { _markTrusted(ev); return target.dispatchEvent(ev); };
        const _PE = () => globalThis.PointerEvent || globalThis.MouseEvent;

        const _hover = (el, x, y) => {
            const prev = _mouse.over;
            if (prev === el) return;
            if (prev && prev.isConnected !== false) {
                _fire(prev, new (_PE())('pointerout', _ptrInit(x, y, { relatedTarget: el })));
                _fire(prev, new (_PE())('pointerleave', _ptrInit(x, y, { bubbles: false, cancelable: false, relatedTarget: el })));
                _fire(prev, new MouseEvent('mouseout', _mouseInit(x, y, { relatedTarget: el })));
                _fire(prev, new MouseEvent('mouseleave', _mouseInit(x, y, { bubbles: false, cancelable: false, relatedTarget: el })));
            }
            _mouse.over = el;
            _fire(el, new (_PE())('pointerover', _ptrInit(x, y, { relatedTarget: prev })));
            _fire(el, new (_PE())('pointerenter', _ptrInit(x, y, { bubbles: false, cancelable: false, relatedTarget: prev })));
            _fire(el, new MouseEvent('mouseover', _mouseInit(x, y, { relatedTarget: prev })));
            _fire(el, new MouseEvent('mouseenter', _mouseInit(x, y, { bubbles: false, cancelable: false, relatedTarget: prev })));
        };
        const _move = (el, x, y) => {
            const dx = x - _mouse.x, dy = y - _mouse.y;
            _mouse.x = x; _mouse.y = y;
            _hover(el, x, y);
            _fire(el, new (_PE())('pointermove', _ptrInit(x, y, { movementX: dx, movementY: dy })));
            _fire(el, new MouseEvent('mousemove', _mouseInit(x, y, { movementX: dx, movementY: dy })));
        };

        const _center = (el) => {
            let r = null;
            try { r = el.getBoundingClientRect(); } catch (_) {}
            const vw = globalThis.innerWidth || 1280, vh = globalThis.innerHeight || 800;
            let w = r && r.width > 0 ? r.width : 40, h = r && r.height > 0 ? r.height : 16;
            let x = r ? r.left + w * (0.35 + 0.3 * _rnd()) : vw / 2;
            let y = r ? r.top + h * (0.35 + 0.3 * _rnd()) : vh / 2;
            // Layout is approximate; keep the pointer inside the viewport.
            if (!(x >= 1 && x < vw)) x = _between(40, Math.max(41, vw - 40));
            if (!(y >= 1 && y < vh)) y = _between(40, Math.max(41, vh - 40));
            return [Math.round(x), Math.round(y)];
        };

        const _closest = (el, sel) => { try { return el.closest ? el.closest(sel) : null; } catch (_) { return null; } };
        const _focusTargetOf = (el) => {
            for (let cur = el; cur && cur.nodeType === 1; cur = cur.parentElement) {
                if (_hook && _hook.focusable && _hook.focusable(cur)) return cur;
            }
            return null;
        };
        const _isTextField = (el) => {
            if (!el) return false;
            if (el.tagName === 'TEXTAREA') return true;
            if (el.tagName === 'INPUT') {
                const t = (el.getAttribute('type') || 'text').toLowerCase();
                return ['text', 'email', 'password', 'search', 'tel', 'url', 'number', ''].includes(t);
            }
            return false;
        };
        const _submitterOf = (el) => {
            const b = _closest(el, 'button, input[type=submit], input[type=image]');
            if (!b || b.disabled) return null;
            if (b.tagName === 'BUTTON') {
                const t = (b.getAttribute('type') || 'submit').toLowerCase();
                if (t !== 'submit') return null;
            }
            return b;
        };
        const _formOf = (el) => {
            const id = el.getAttribute && el.getAttribute('form');
            if (id) { const f = document.getElementById(id); if (f) return f; }
            return _closest(el, 'form');
        };

        // Default action of a click that was not cancelled.
        const _activate = (target) => {
            const sub = _submitterOf(target);
            if (sub) { const f = _formOf(sub); if (f && typeof f.requestSubmit === 'function') f.requestSubmit(sub); return 'submit'; }
            const a = _closest(target, 'a[href]');
            if (a) {
                const href = a.getAttribute('href') || '';
                if (/^javascript:/i.test(href)) return 'js-link';
                if (href.startsWith('#')) { try { location.hash = href; } catch (_) {} return 'hash'; }
                try { location.href = new URL(href, location.href).href; } catch (_) {}
                return 'link';
            }
            const lab = _closest(target, 'label');
            if (lab) {
                const ctl = lab.htmlFor ? document.getElementById(lab.htmlFor) : lab.querySelector('input,textarea,select,button');
                if (ctl && ctl !== target) { _clickNow(ctl, true)(); return 'label'; }
            }
            return 'none';
        };

        const _toggleBefore = (target) => {
            if (target.tagName !== 'INPUT') return null;
            const t = (target.getAttribute('type') || '').toLowerCase();
            if (t !== 'checkbox' && t !== 'radio') return null;
            const was = !!target.checked;
            if (t === 'radio' && was) return null;
            target.checked = t === 'radio' ? true : !was;
            return { was, el: target };
        };

        // The press/release/click part (no movement), synchronous.
        const _clickNow = (el, synthetic) => {
            const [x, y] = [_mouse.x, _mouse.y];
            if (!synthetic) {
                _mouse.buttons = 1;
                _fire(el, new (_PE())('pointerdown', _ptrInit(x, y, { buttons: 1, pressure: 0.5 })));
                const md = _fire(el, new MouseEvent('mousedown', _mouseInit(x, y, { buttons: 1, detail: 1 })));
                if (md) {
                    const ft = _focusTargetOf(el);
                    if (ft) { if (_focusProto) _focusProto.call(ft); }
                    else if (document.activeElement && document.activeElement !== document.body && document.activeElement.blur) document.activeElement.blur();
                }
            }
            return () => {
                if (!synthetic) {
                    _mouse.buttons = 0;
                    _fire(el, new (_PE())('pointerup', _ptrInit(x, y, { buttons: 0, pressure: 0 })));
                    _fire(el, new MouseEvent('mouseup', _mouseInit(x, y, { buttons: 0, detail: 1 })));
                }
                const tog = _toggleBefore(el);
                const ok = _fire(el, new (_PE())('click', _ptrInit(x, y, { buttons: 0, detail: 1, pressure: 0 })));
                if (tog) {
                    if (!ok) { tog.el.checked = tog.was; }
                    else {
                        _fire(el, new Event('input', { bubbles: true, composed: true }));
                        _fire(el, new Event('change', { bubbles: true }));
                    }
                    return 'toggled';
                }
                return ok ? _activate(el) : 'cancelled';
            };
        };

        const _resolve = (t) => (typeof t === 'string') ? document.querySelector(t) : t;

        const click = (target) => {
            const el = _resolve(target);
            if (!el) return 'element not found';
            try { el.scrollIntoView && el.scrollIntoView({ block: 'center' }); } catch (_) {}
            const [tx, ty] = _center(el);
            const sx = _mouse.x, sy = _mouse.y;
            const steps = _between(8, 16);
            const body = document.body || document.documentElement;
            // Ease-in-out path with a little curvature and jitter.
            const bend = (_rnd() - 0.5) * 0.3;
            for (let i = 1; i <= steps; i++) {
                const t = i / steps, e = t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;
                const x = Math.round(sx + (tx - sx) * e + (ty - sy) * bend * Math.sin(Math.PI * t) + (i < steps ? _between(-1, 1) : 0));
                const y = Math.round(sy + (ty - sy) * e - (tx - sx) * bend * Math.sin(Math.PI * t) + (i < steps ? _between(-1, 1) : 0));
                const over = i > steps - 3 ? el : (_mouse.over || body);
                _enqueue(() => _move(over, x, y), _between(8, 22));
            }
            let release = null;
            _enqueue(() => { release = _clickNow(el, false); }, _between(40, 120));
            _enqueue(() => { const r = release ? release() : 'no press'; _log.push('click: ' + r); }, _between(60, 140));
            return 'queued';
        };

        const _KEYCODE = { ' ': [32, 'Space'], '.': [190, 'Period'], ',': [188, 'Comma'], '-': [189, 'Minus'], '_': [189, 'Minus', 1], '@': [50, 'Digit2', 1],
            '!': [49, 'Digit1', 1], '#': [51, 'Digit3', 1], '$': [52, 'Digit4', 1], '%': [53, 'Digit5', 1], '^': [54, 'Digit6', 1], '&': [55, 'Digit7', 1],
            '*': [56, 'Digit8', 1], '(': [57, 'Digit9', 1], ')': [48, 'Digit0', 1], '+': [187, 'Equal', 1], '=': [187, 'Equal'], '/': [191, 'Slash'],
            '?': [191, 'Slash', 1], ':': [186, 'Semicolon', 1], ';': [186, 'Semicolon'], "'": [222, 'Quote'], '"': [222, 'Quote', 1],
            '[': [219, 'BracketLeft'], ']': [221, 'BracketRight'], '{': [219, 'BracketLeft', 1], '}': [221, 'BracketRight', 1], '\\': [220, 'Backslash'],
            '|': [220, 'Backslash', 1], '<': [188, 'Comma', 1], '>': [190, 'Period', 1], '~': [192, 'Backquote', 1], '`': [192, 'Backquote'] };
        const _SPECIAL = { Enter: [13, 'Enter'], Tab: [9, 'Tab'], Backspace: [8, 'Backspace'], Escape: [27, 'Escape'], Delete: [46, 'Delete'],
            ArrowLeft: [37, 'ArrowLeft'], ArrowUp: [38, 'ArrowUp'], ArrowRight: [39, 'ArrowRight'], ArrowDown: [40, 'ArrowDown'],
            Home: [36, 'Home'], End: [35, 'End'], PageUp: [33, 'PageUp'], PageDown: [34, 'PageDown'], Shift: [16, 'ShiftLeft'] };
        const _keyInfo = (ch) => {
            if (_SPECIAL[ch]) return { key: ch, keyCode: _SPECIAL[ch][0], code: _SPECIAL[ch][1], shift: false, printable: false };
            if (/^[a-z]$/.test(ch)) return { key: ch, keyCode: ch.toUpperCase().charCodeAt(0), code: 'Key' + ch.toUpperCase(), shift: false, printable: true };
            if (/^[A-Z]$/.test(ch)) return { key: ch, keyCode: ch.charCodeAt(0), code: 'Key' + ch, shift: true, printable: true };
            if (/^[0-9]$/.test(ch)) return { key: ch, keyCode: 48 + +ch, code: 'Digit' + ch, shift: false, printable: true };
            const m = _KEYCODE[ch];
            if (m) return { key: ch, keyCode: m[0], code: m[1], shift: !!m[2], printable: true };
            return { key: ch, keyCode: 0, code: '', shift: false, printable: true };
        };
        const _mods = { shift: false };
        const _kInit = (k, extra) => Object.assign({ bubbles: true, cancelable: true, composed: true, view: globalThis, key: k.key, code: k.code,
            keyCode: k.keyCode, which: k.keyCode, charCode: 0, location: k.code === 'ShiftLeft' ? 1 : 0, repeat: false, isComposing: false,
            shiftKey: _mods.shift, ctrlKey: false, altKey: false, metaKey: false }, extra || {});

        const _focused = () => {
            const a = document.activeElement;
            return (a && a !== document.body) ? a : null;
        };
        const _insertText = (el, text, inputType) => {
            const bi = new InputEvent('beforeinput', { bubbles: true, cancelable: true, composed: true, data: text, inputType });
            if (!_fire(el, bi)) return false;
            if (_isTextField(el)) {
                const v = _readValue(el);
                if (inputType === 'deleteContentBackward') _writeValue(el, v.slice(0, -1));
                else {
                    const max = parseInt(el.getAttribute('maxlength') || '', 10);
                    if (max >= 0 && v.length >= max) return false;
                    _writeValue(el, v + text);
                }
            } else if (el.isContentEditable || (el.getAttribute && el.getAttribute('contenteditable') != null)) {
                if (inputType === 'deleteContentBackward') { const t = el.textContent || ''; el.textContent = t.slice(0, -1); }
                else el.appendChild(document.createTextNode(text));
            } else return false;
            _fire(el, new InputEvent('input', { bubbles: true, cancelable: false, composed: true, data: inputType === 'deleteContentBackward' ? null : text, inputType }));
            return true;
        };

        const _nextFocusable = (from, back) => {
            const all = Array.from(document.querySelectorAll('a[href],button,input,select,textarea,[tabindex],[contenteditable]'))
                .filter(e => _hook.focusable(e) && (e.getAttribute('tabindex') || '0') !== '-1');
            if (!all.length) return null;
            const i = all.indexOf(from);
            return all[(i + (back ? -1 : 1) + all.length) % all.length];
        };

        const _keyStroke = (k) => {
            const el = _focused() || document.body;
            const down = _fire(el, new KeyboardEvent('keydown', _kInit(k)));
            let result = 'key ' + k.key;
            if (down) {
                if (k.printable) {
                    const cc = k.key.charCodeAt(0);
                    const press = _fire(el, new KeyboardEvent('keypress', _kInit(k, { keyCode: cc, which: cc, charCode: cc })));
                    if (press) _insertText(el, k.key, 'insertText');
                } else if (k.key === 'Enter') {
                    const press = _fire(el, new KeyboardEvent('keypress', _kInit(k, { charCode: 13 })));
                    if (press) {
                        if (el.tagName === 'TEXTAREA' || el.isContentEditable) _insertText(el, '\n', 'insertLineBreak');
                        else if (_isTextField(el)) {
                            const f = _formOf(el);
                            if (f) {
                                const btn = f.querySelector('button:not([type]),button[type=submit],input[type=submit],input[type=image]');
                                if (btn) { const rel = _clickNow(btn, true); result = 'enter: ' + rel(); }
                                else if (typeof f.requestSubmit === 'function') { f.requestSubmit(); result = 'enter: submit'; }
                            }
                        } else if (el.tagName === 'BUTTON' || el.tagName === 'A') { const rel = _clickNow(el, true); result = 'enter: ' + rel(); }
                    }
                } else if (k.key === 'Backspace') {
                    _insertText(el, '', 'deleteContentBackward');
                } else if (k.key === 'Tab') {
                    const n = _nextFocusable(_focused(), _mods.shift);
                    if (n && _focusProto) _focusProto.call(n);
                }
            }
            _fire(_focused() || el, new KeyboardEvent('keyup', _kInit(k)));
            return result;
        };

        const typeText = (text) => {
            const el = _focused();
            if (!el) return 'nothing focused';
            let shiftPlanned = _mods.shift;
            for (const ch of String(text)) {
                const k = _keyInfo(ch);
                if (k.shift && !shiftPlanned) {
                    shiftPlanned = true;
                    _enqueue(() => { _mods.shift = true; _fire(_focused() || el, new KeyboardEvent('keydown', _kInit(_keyInfo('Shift'), { shiftKey: true }))); }, _between(30, 80));
                } else if (!k.shift && shiftPlanned) {
                    shiftPlanned = false;
                    _enqueue(() => { _mods.shift = false; _fire(_focused() || el, new KeyboardEvent('keyup', _kInit(_keyInfo('Shift'), { shiftKey: false }))); }, _between(20, 60));
                }
                _enqueue(() => { _keyStroke(k); }, _between(55, 170));
            }
            _enqueue(() => { if (_mods.shift) { _mods.shift = false; _fire(_focused() || el, new KeyboardEvent('keyup', _kInit(_keyInfo('Shift')))); } }, _between(20, 60));
            return 'queued ' + String(text).length;
        };

        const press = (key) => {
            const k = _keyInfo(key);
            _enqueue(() => { _log.push('press: ' + _keyStroke(k)); }, _between(60, 160));
            return 'queued';
        };

        // Mouse-wheel scrolling: uneven ~100 px ticks (like a real wheel /
        // CDP mouseWheel), each a trusted WheelEvent at the element under the
        // pointer; the viewport scrolls unless a listener cancelled it.
        const wheel = (totalPx) => {
            let left = Number(totalPx) || 0;
            if (!left) return 'nothing to scroll';
            const dir = left < 0 ? -1 : 1;
            if (!_mouse.x && !_mouse.y) {
                const vw = globalThis.innerWidth || 1280, vh = globalThis.innerHeight || 800;
                const x = _between(Math.round(vw * 0.35), Math.round(vw * 0.6)), y = _between(Math.round(vh * 0.35), Math.round(vh * 0.65));
                _enqueue(() => { const el = document.elementFromPoint(x, y) || document.body; _move(el, x, y); }, _between(40, 120));
            }
            let ticks = 0;
            while (Math.abs(left) > 0.5) {
                const step = dir * Math.min(Math.abs(left), [100, 100, 100, 120, 80, 200][_between(0, 5)]);
                left -= step; ticks++;
                _enqueue(() => {
                    const x = _mouse.x, y = _mouse.y;
                    const el = document.elementFromPoint(x, y) || document.body || document.documentElement;
                    const ev = new WheelEvent('wheel', Object.assign(_mouseInit(x, y), { deltaX: 0, deltaY: step, deltaZ: 0, deltaMode: 0, buttons: 0, composed: true }));
                    const ok = _fire(el, ev);
                    if (ok && _hook && _hook.viewportScroll) {
                        const [cx, cy] = _hook.viewportPos();
                        _hook.viewportScroll(cx, cy + step);
                    }
                }, _between(60, 160));
            }
            _enqueue(() => { _log.push('wheel: ' + totalPx + 'px, scrollY=' + Math.round(globalThis.scrollY || 0)); }, _between(20, 60));
            return 'queued ' + ticks + ' ticks';
        };

        const _markDriven = () => { try { if (_hook && !_hook.driven) Object.defineProperty(_hook, 'driven', { value: true, enumerable: false }); } catch (_) {} };
        const _input = {
            click: (t) => { _markDriven(); return click(t); },
            type: (t) => { _markDriven(); return typeText(t); },
            press: (k) => { _markDriven(); return press(k); },
            wheel: (px) => { _markDriven(); return wheel(px); },
            busy: () => _running || _queue.length > 0,
            takeLog: () => { const l = _log; _log = []; return l; },
            focused: () => { const a = _focused(); return a ? (a.tagName + (a.name ? '[name=' + a.name + ']' : '') + (a.id ? '#' + a.id : '')) : null; },
        };
        if (_hook) {
            try { Object.defineProperty(_hook, 'input', { value: Object.freeze(_input), enumerable: false, configurable: false, writable: false }); } catch (_) {}
        }
    }

    // Parsed <iframe src> elements get their documents once the HTML is in.
    try {
        const _hk2 = globalThis[Symbol.for('__browser_oxide_script_hook__')];
        if (_hk2 && typeof _hk2.scanParsedFrames === 'function' && globalThis.document) {
            globalThis.document.addEventListener('DOMContentLoaded', () => _hk2.scanParsedFrames());
        }
    } catch (_) {}

    // Privileged handoff of the trusted-event minter (behavioral E1/E2). Our
    // init scripts (humanize.js) capture this into a closure and `delete` it
    // synchronously at their top — before any page script runs — so page JS
    // never observes it. Non-enumerable to keep it off Object.keys scans even
    // in the brief window before capture.
    try {
        Object.defineProperty(globalThis, '__bo_mark_trusted', {
            value: _markTrusted,
            configurable: true,
            enumerable: false,
            writable: false,
        });
    } catch (_) { /* ignore */ }
})(globalThis);
