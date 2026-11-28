((globalThis) => {
    const ops = Deno && Deno.core && Deno.core.ops;
    // -- Per-page secure-context gating (Phase 7) --------------------
    // The V8 snapshot bootstraps with is_secure_context=true so all
    // [SecureContext]-only Web Platform APIs are baked in. On insecure
    // pages (data:/http:/about:blank) we strip them here to match real
    // Chrome.
    try {
        const _ops = Deno && Deno.core && Deno.core.ops;
        const _isSecure = _ops && _ops.op_is_secure_context && _ops.op_is_secure_context();
        if (!_isSecure) {
            // Methods + globals registered as values in the snapshot.
            // Navigator getters (mediaDevices, clipboard, ...) gate
            // themselves lazily so they don't need stripping.
            try { delete globalThis.Navigator.prototype.getBattery; } catch (_e) {}
            for (const k of ['caches', 'cookieStore', 'IdleDetector', 'EyeDropper', 'WebTransport']) {
                try { delete globalThis[k]; } catch (_e) {}
            }
            // Phase 7 — also strip the constructor *interfaces* for the
            // [SecureContext] APIs. Real Chrome 147 hides these from
            // `Object.getOwnPropertyNames(window)` on insecure pages.
            // Some scripts hash the global namespace.
            // Also: ApplePaySession, SharedArrayBuffer, webkitAudioContext,
            // DedicatedWorkerGlobalScope, WorkerGlobalScope, CSSPseudoElement
            // are absent from Chrome 147's globalThis on insecure pages —
            // verified against a real browser.
            for (const k of [
                "SharedArrayBuffer", "webkitAudioContext",
                "DedicatedWorkerGlobalScope", "WorkerGlobalScope",
                "CSSPseudoElement",
                "ApplePaySession", "AuthenticatorAssertionResponse",
                "AuthenticatorAttestationResponse", "AuthenticatorResponse",
                "BatteryManager", "Bluetooth", "CacheStorage", "CookieStore",
                "Credential", "CredentialsContainer", "DevicePosture",
                "FederatedCredential", "FileSystemDirectoryHandle",
                "FileSystemFileHandle", "FileSystemHandle",
                "FileSystemWritableFileStream", "IdentityCredential",
                "IdentityProvider", "Keyboard", "KeyboardLayoutMap",
                "MediaDevices", "PasswordCredential", "PaymentRequest",
                "Presentation", "PresentationConnection",
                "PublicKeyCredential", "ServiceWorker",
                "ServiceWorkerContainer", "StorageManager", "SubtleCrypto",
                "VirtualKeyboard", "XRSession", "XRSystem",
                // Generic Sensor API — also [SecureContext]
                "Sensor", "Accelerometer", "AbsoluteOrientationSensor",
                "GravitySensor", "Gyroscope", "LinearAccelerationSensor",
                "Magnetometer", "OrientationSensor",
                "RelativeOrientationSensor",
            ]) {
                try { delete globalThis[k]; } catch (_e) {}
            }
            // crypto.subtle + crypto.randomUUID are [SecureContext]. They
            // come from deno_core's crypto extension and are non-configurable
            // own properties. `delete` fails — replace `globalThis.crypto`
            // with a Proxy that hides those two keys.
            if (globalThis.crypto) {
                const _origCrypto = globalThis.crypto;
                const _maskedCrypto = new Proxy(_origCrypto, {
                    get(target, prop, receiver) {
                        if (prop === 'subtle' || prop === 'randomUUID') return undefined;
                        const v = Reflect.get(target, prop, receiver);
                        return typeof v === 'function' ? v.bind(target) : v;
                    },
                    has(target, prop) {
                        if (prop === 'subtle' || prop === 'randomUUID') return false;
                        return Reflect.has(target, prop);
                    },
                    ownKeys(target) {
                        return Reflect.ownKeys(target).filter(
                            (k) => k !== 'subtle' && k !== 'randomUUID',
                        );
                    },
                    getOwnPropertyDescriptor(target, prop) {
                        if (prop === 'subtle' || prop === 'randomUUID') return undefined;
                        return Reflect.getOwnPropertyDescriptor(target, prop);
                    },
                });
                try {
                    Object.defineProperty(globalThis, 'crypto', {
                        value: _maskedCrypto, configurable: true, enumerable: true, writable: true,
                    });
                } catch (_e) {}
            }
        }
    } catch (_e) { /* secure-context cleanup is best-effort */ }

    // -- Profile-conditional installs --------------------------------
    // These run AFTER the V8 startup snapshot is restored, so the
    // stealth profile is loaded and op-based reads return real values.
    // (Snapshot-time bootstraps see profile=None and would mis-gate.)
    try {
        const _hasProfile = ops && ops.op_has_stealth_profile && ops.op_has_stealth_profile();
        const _osName = (_hasProfile && ops.op_get_profile_value)
            ? (ops.op_get_profile_value("os_name") || "Linux")
            : "Linux";

        // ApplePaySession — present only on macOS Chrome AND only on
        // secure contexts (Apple Pay requires https). A missing constructor
        // on a macOS UA is a strong inconsistency versus a real browser.
        // Constructor + statics shaped to match
        // Chrome 147's ApplePaySession surface.
        const _ops2 = Deno && Deno.core && Deno.core.ops;
        const _isSecureForAP = _ops2 && _ops2.op_is_secure_context && _ops2.op_is_secure_context();
        if (_osName === "macOS" && _isSecureForAP && typeof globalThis.ApplePaySession === "undefined") {
            const _APP = function ApplePaySession(_version, _paymentRequest) {
                this.onvalidatemerchant = null;
                this.onpaymentauthorized = null;
                this.onpaymentmethodselected = null;
                this.onshippingcontactselected = null;
                this.onshippingmethodselected = null;
                this.oncouponcodechanged = null;
                this.oncancel = null;
            };
            _APP.prototype = {
                begin() {},
                abort() {},
                completeMerchantValidation() {},
                completePayment() {},
                completePaymentMethodSelection() {},
                completeShippingContactSelection() {},
                completeShippingMethodSelection() {},
                completeCouponCodeChange() {},
                addEventListener() {},
                removeEventListener() {},
            };
            _APP.STATUS_SUCCESS = 0;
            _APP.STATUS_FAILURE = 1;
            _APP.STATUS_INVALID_BILLING_POSTAL_ADDRESS = 2;
            _APP.STATUS_INVALID_SHIPPING_POSTAL_ADDRESS = 3;
            _APP.STATUS_INVALID_SHIPPING_CONTACT = 4;
            _APP.STATUS_PIN_REQUIRED = 5;
            _APP.STATUS_PIN_INCORRECT = 6;
            _APP.STATUS_PIN_LOCKOUT = 7;
            _APP.canMakePayments = function canMakePayments() { return true; };
            _APP.canMakePaymentsWithActiveCard = function canMakePaymentsWithActiveCard(_id) { return Promise.resolve(false); };
            _APP.openPaymentSetup = function openPaymentSetup(_id) { return Promise.resolve(false); };
            _APP.supportsVersion = function supportsVersion(version) { return version >= 1 && version <= 14; };
            Object.defineProperty(globalThis, 'ApplePaySession', {
                value: _APP,
                configurable: true,
                writable: true,
            });
        }

        // -- iOS Safari profile: strip 16 declined APIs + add iOS globals --
        // Per Apple's "16 web APIs declined for privacy" policy. The
        // single highest-ROI mobile patch — many leaks vanish at once.
        const _deviceClass = (_hasProfile && ops.op_get_profile_value)
            ? ops.op_get_profile_value("device_class")
            : "Desktop";
        if (_deviceClass === "MobileIOS") {
            // 1. Delete 16 declined APIs from globalThis
            const _iosDeleted = [
                "Bluetooth", "USB", "USBAlternateInterface", "USBConfiguration",
                "USBConnectionEvent", "USBDevice", "USBEndpoint",
                "USBInTransferResult", "USBInterface",
                "USBIsochronousInTransferPacket", "USBIsochronousInTransferResult",
                "USBIsochronousOutPacket", "USBIsochronousOutTransferResult",
                "USBOutTransferResult",
                "HID", "HIDConnectionEvent", "HIDDevice", "HIDInputReportEvent",
                "Serial", "SerialPort",
                "NetworkInformation", "BatteryManager",
                "IdleDetector", "EyeDropper",
                // Chrome-only interfaces real Safari does NOT expose.
                // A `'X' in window` check against an iOS UA would flag these.
                "UserActivation", "Scheduling",
                "Sensor", "Accelerometer", "AbsoluteOrientationSensor",
                "GravitySensor", "Gyroscope", "LinearAccelerationSensor",
                "Magnetometer", "OrientationSensor", "RelativeOrientationSensor",
                // WebGPU is feature-flagged on iOS 18+ but defaults off
                "GPU", "GPUAdapter", "GPUDevice", "GPUQueue", "GPUBuffer",
                "GPUTexture", "GPUSampler", "GPUBindGroup", "GPUBindGroupLayout",
                "GPUPipelineLayout", "GPUShaderModule", "GPURenderPipeline",
                "GPUComputePipeline", "GPUCommandEncoder", "GPUCommandBuffer",
                "GPURenderPassEncoder", "GPUComputePassEncoder",
                "GPURenderBundleEncoder", "GPURenderBundle", "GPUCanvasContext",
                "GPUColorWrite", "GPUMapMode", "GPUTextureUsage",
                "GPUBufferUsage", "GPUShaderStage",
                // Speech recognition has limited iOS support, but webkit-prefixed
                // is the only form Safari ships
                "SpeechRecognition", "SpeechRecognitionEvent",
                "SpeechRecognitionErrorEvent",
            ];
            for (const k of _iosDeleted) {
                try { delete globalThis[k]; } catch (_e) {}
            }

            // 2. Strip Navigator.prototype methods/getters that iOS doesn't have.
            // Defense in depth: window_bootstrap.js W1.5 gate avoids
            // installing these on iOS profiles, but we also delete here in
            // case any prior pass re-installed them. Use `delete` (not
            // redefine-with-undefined-getter) so `'X' in navigator` returns
            // false — the descriptor must not be present.
            const _NavProto = globalThis.Navigator && globalThis.Navigator.prototype;
            if (_NavProto) {
                for (const k of [
                    "bluetooth", "usb", "serial", "hid", "requestMIDIAccess",
                    "getBattery", "connection", "getInstalledRelatedApps",
                    "scheduling", "userActivation",
                    // userAgentData absent on Safari (no UA-CH at all)
                    "userAgentData",
                    // deviceMemory absent on Safari
                    "deviceMemory",
                ]) {
                    try { delete _NavProto[k]; } catch (_e) {}
                }
            }

            // 3. PaymentRequest.prototype.hasEnrolledInstrument is Chrome/Edge-only
            //    Safari MUST NOT have it.
            if (globalThis.PaymentRequest && globalThis.PaymentRequest.prototype) {
                try { delete globalThis.PaymentRequest.prototype.hasEnrolledInstrument; } catch (_e) {}
            }

            // 4. window.orientation — legacy iOS-only property. Desktop browsers
            //    do NOT have this. Setting to 0 = portrait.
            try {
                Object.defineProperty(globalThis, "orientation", {
                    get: function() { return 0; },
                    configurable: true, enumerable: true,
                });
            } catch (_e) {}

            // 5. ontouchstart on window — every detection script's cheapest
            //    mobile-vs-desktop check
            try {
                Object.defineProperty(globalThis, "ontouchstart", {
                    value: null, configurable: true, writable: true, enumerable: true,
                });
            } catch (_e) {}

            // 6. DeviceMotionEvent.requestPermission + DeviceOrientationEvent.requestPermission
            //    iOS 13+ requires user-gesture-gated permission for these. The presence
            //    of these static methods is itself a strong iOS signal — Android does NOT
            //    expose these statics.
            if (globalThis.DeviceMotionEvent
                && typeof globalThis.DeviceMotionEvent.requestPermission !== "function") {
                try {
                    globalThis.DeviceMotionEvent.requestPermission =
                        function requestPermission() { return Promise.resolve("denied"); };
                } catch (_e) {}
            }
            if (globalThis.DeviceOrientationEvent
                && typeof globalThis.DeviceOrientationEvent.requestPermission !== "function") {
                try {
                    globalThis.DeviceOrientationEvent.requestPermission =
                        function requestPermission() { return Promise.resolve("denied"); };
                } catch (_e) {}
            }

            // 7. Sec-CH-UA-* JS surface absent on Safari — already handled
            //    above via userAgentData getter returning undefined.

            // 8. window.chrome must be absent on iOS Safari. Some scripts
            //    explicitly probe `typeof window.chrome` — Chrome
            //    returns "object", Safari "undefined". A positive hit under
            //    an iOS UA is a strong inconsistency.
            try { delete globalThis.chrome; } catch (_e) {}

            // 8b. navigator.permissions.query() — Safari 18 supports a much
            //     narrower permission name set than Chrome. Per WebKit:
            //     allowed = notifications, push, camera, microphone,
            //               geolocation, persistent-storage.
            //     Chrome-only names (midi, accelerometer, gyroscope,
            //     magnetometer, ambient-light-sensor, background-fetch,
            //     background-sync, clipboard-read, clipboard-write,
            //     display-capture, screen-wake-lock, system-wake-lock,
            //     window-management) must reject with TypeError on Safari
            //     to match real WebKit behavior. PLAN W1.5 (Plan §0 #6).
            try {
                if (globalThis.navigator && globalThis.navigator.permissions) {
                    const _safariAllowed = new Set([
                        'notifications', 'push', 'camera', 'microphone',
                        'geolocation', 'persistent-storage',
                    ]);
                    const _PProto = globalThis.navigator.permissions
                        && Object.getPrototypeOf(globalThis.navigator.permissions);
                    if (_PProto && typeof _PProto.query === 'function') {
                        const _origQuery = _PProto.query;
                        const safariQuery = function query(desc) {
                            const name = desc && typeof desc === 'object' ? desc.name : undefined;
                            if (typeof name !== 'string' || !_safariAllowed.has(name)) {
                                return Promise.reject(new TypeError(
                                    "Failed to execute 'query' on 'Permissions': "
                                    + (typeof name === 'string'
                                        ? "The provided value '" + name + "' is not a valid enum value of type PermissionName."
                                        : "parameter 1 is not of type 'PermissionDescriptor'.")
                                ));
                            }
                            return _origQuery.call(this, desc);
                        };
                        Object.defineProperty(_PProto, 'query', {
                            value: safariQuery, writable: true, enumerable: false, configurable: true,
                        });
                        // Preserve native-shape Function.prototype.toString output
                        // via the _nativeTag symbol installed by stealth_bootstrap.js.
                        const _tag = globalThis._nativeTag;
                        if (_tag) {
                            try { Object.defineProperty(safariQuery, _tag, { value: 'query', configurable: true }); } catch (_e) {}
                            try { Object.defineProperty(safariQuery, 'name', { value: 'query', configurable: true }); } catch (_e) {}
                        }
                    }
                }
            } catch (_e) {}

            // 9. navigator.plugins / navigator.mimeTypes empty on iOS
            //    (PluginArray length 0 is the canonical mobile-Safari shape).
            try {
                if (globalThis.navigator) {
                    const _emptyPlugins = Object.create(globalThis.PluginArray ? globalThis.PluginArray.prototype : null);
                    Object.defineProperty(_emptyPlugins, 'length', { get: () => 0, enumerable: true });
                    Object.defineProperty(_emptyPlugins, 'item', {
                        value: function item() { return null; },
                        writable: true, enumerable: false, configurable: true,
                    });
                    Object.defineProperty(_emptyPlugins, 'namedItem', {
                        value: function namedItem() { return null; },
                        writable: true, enumerable: false, configurable: true,
                    });
                    Object.defineProperty(_emptyPlugins, 'refresh', {
                        value: function refresh() {},
                        writable: true, enumerable: false, configurable: true,
                    });
                    Object.defineProperty(_emptyPlugins, Symbol.iterator, {
                        value: function* () {},
                        writable: true, enumerable: false, configurable: true,
                    });
                    Object.defineProperty(_NavProto, 'plugins', {
                        get: function() { return _emptyPlugins; },
                        configurable: true, enumerable: false,
                    });
                    const _emptyMimeTypes = Object.create(globalThis.MimeTypeArray ? globalThis.MimeTypeArray.prototype : null);
                    Object.defineProperty(_emptyMimeTypes, 'length', { get: () => 0, enumerable: true });
                    Object.defineProperty(_emptyMimeTypes, 'item', {
                        value: function item() { return null; },
                        writable: true, enumerable: false, configurable: true,
                    });
                    Object.defineProperty(_emptyMimeTypes, 'namedItem', {
                        value: function namedItem() { return null; },
                        writable: true, enumerable: false, configurable: true,
                    });
                    Object.defineProperty(_NavProto, 'mimeTypes', {
                        get: function() { return _emptyMimeTypes; },
                        configurable: true, enumerable: false,
                    });
                    // pdfViewerEnabled is false on mobile (no integrated PDF viewer)
                    Object.defineProperty(_NavProto, 'pdfViewerEnabled', {
                        get: function() { return false; },
                        configurable: true, enumerable: false,
                    });
                }
            } catch (_e) {}
        }
    } catch (_e) { /* profile-conditional installs are best-effort */ }

    // -- native-source masking of Web Platform constructors --------
    // Some scripts dump `String(globalThis.<ctor>)` for a
    // rotating list of Web Platform constructors/functions and feed
    // the result into a browser-fingerprint score. Without masking,
    // many probed names leak our polyfill source —
    // raw `class Worker {…}` / `function(input, init){…}` bodies, or
    // the wrong native name (constructors that extend our internal
    // EventTarget reported `function EventTarget() { [native code] }`,
    // `clearTimeout` reported `clearInterval`). Real Chrome returns
    // `function <Name>() { [native code] }` for every one of these.
    //
    // This MUST run here, not in stealth_bootstrap.js: the constructors
    // are defined by interfaces/shared_apis/streams/window/worker
    // bootstraps that are concatenated AFTER stealth_bootstrap.js (and
    // shared_apis/worker run at runtime, after the snapshot). This is
    // the universal last pass — it runs always for the page (even from
    // snapshot) and last for workers — and `_maskFunction` is still on
    // globalThis here (the `internals` purge below removes it after).
    try {
        const _mask = globalThis._maskFunction;
        if (typeof _mask === 'function') {
            // De-alias Chrome-distinct pairs our impl points at one
            // object. The fresh /tl `sfc` probe caught these: real
            // Chrome has clearTimeout!==clearInterval,
            // scroll!==scrollTo, DOMMatrix!==DOMMatrixReadOnly — each
            // is its own named native, so a single shared object can't
            // satisfy `String(globalThis[name])` for both names. We
            // split the secondary into a distinct delegator/subclass
            // (more Chrome-faithful; zero behavior change).
            try {
                if (typeof globalThis.clearTimeout === 'function'
                    && globalThis.clearInterval === globalThis.clearTimeout) {
                    const _ct = globalThis.clearTimeout;
                    globalThis.clearInterval = { clearInterval(id) { return _ct(id); } }.clearInterval;
                }
                if (typeof globalThis.scrollTo === 'function'
                    && globalThis.scroll === globalThis.scrollTo) {
                    const _st = globalThis.scrollTo;
                    globalThis.scroll = { scroll() { return _st.apply(this, arguments); } }.scroll;
                }
                if (typeof globalThis.DOMMatrix === 'function'
                    && globalThis.DOMMatrixReadOnly === globalThis.DOMMatrix) {
                    globalThis.DOMMatrixReadOnly = class DOMMatrixReadOnly extends globalThis.DOMMatrix {};
                }
            } catch (_e) {}

            // Native NON-constructor functions must have NO own
            // `prototype` and must be non-constructable (`new fetch()`
            // throws in Chrome). A CLEAN production probe
            // (the challenge-vendor native-fn-shape clean probe — no capture shim)
            // confirmed setTimeout/setInterval/clearTimeout/
            // clearInterval/queueMicrotask/structuredClone are plain
            // `function` decls → carry `.prototype` + are
            // constructable (a real-browser inconsistency).
            // `function f(){}`'s `.prototype` is non-configurable so
            // `delete` fails — the only fix is to REPLACE with a
            // method-shorthand (`{[k](){}}[k]`): no `.prototype`,
            // non-constructable, name===k. Forwarding wrapper
            // preserves behavior (none use `this`/`new`). Only the
            // probe-confirmed-broken set is touched; already-correct
            // async/shorthand natives (fetch/atob/btoa/scrollTo/
            // reportError/console.*) are left alone.
            const _natMethod = (holder, key, nm) => {
                try {
                    const o = holder && holder[key];
                    if (typeof o !== 'function') return;
                    if (!Object.prototype.hasOwnProperty.call(o, 'prototype')) {
                        _mask(o, nm || key);
                        return;
                    }
                    const w = { [key]() { return o.apply(this, arguments); } }[key];
                    _mask(w, nm || key);
                    try { holder[key] = w; } catch (_e2) {}
                } catch (_e2) {}
            };
            for (const _k of ['setTimeout', 'setInterval', 'clearTimeout',
                'clearInterval', 'queueMicrotask', 'structuredClone']) {
                _natMethod(globalThis, _k);
            }
            try {
                const _ca = globalThis.chrome && globalThis.chrome.app;
                if (_ca) {
                    for (const _m of ['getDetails', 'getIsInstalled',
                        'installState', 'runningState']) {
                        _natMethod(_ca, _m);
                    }
                }
            } catch (_e) {}

            // (chrome.app.* are handled by _natMethod above — it both
            // native-masks toString [otherwise a probe would leak
            // "function getDetails() { return null; }"] and removes the
            // illegal `.prototype`/constructability.)
            // The commonly probed names, plus adjacent
            // standard constructors — all are
            // genuinely `[native code]` in real Chrome, so masking any
            // that exist on this profile is correct (missing ones are a
            // safe no-op via `_maskFunction`'s `if (!fn) return`).
            // [globalKey, maskName]. maskName differs from globalKey
            // only for the legacy webkit-prefixed aliases: in real
            // Chrome `webkitAudioContext === AudioContext` (same object),
            // so `String(webkitAudioContext)` is
            // `function AudioContext() { [native code] }`. Masking them
            // to their prefixed key would itself be a divergence.
            const _sfcNames = [
                ['webkitMediaStream', 'MediaStream'],
                ['webkitAudioContext', 'AudioContext'],
                ['webkitRTCPeerConnection', 'RTCPeerConnection'],
                'fetch', 'clearTimeout', 'clearInterval', 'setTimeout',
                'setInterval', 'TouchEvent', 'AudioContext', 'OffscreenCanvas',
                'Bluetooth', 'StorageManager', 'scrollTo', 'scroll', 'scrollBy',
                'Worker', 'SharedWorker', 'ServiceWorker', 'WorkerGlobalScope',
                'DedicatedWorkerGlobalScope', 'FileReader', 'ImageBitmap',
                'DOMMatrix', 'DOMMatrixReadOnly', 'PerformanceObserver',
                'PerformanceEntry', 'ReportingObserver', 'ReadableStream',
                'WritableStream', 'TransformStream', 'ReadableStreamDefaultReader',
                'WritableStreamDefaultWriter', 'ReadableStreamDefaultController',
                'BroadcastChannel', 'MessagePort', 'MessageChannel',
                'EventSource', 'CompressionStream', 'DecompressionStream',
                'Crypto', 'SubtleCrypto', 'CloseEvent', 'AbortController',
                'AbortSignal', 'DOMException', 'URL', 'URLSearchParams',
                'FormData', 'Blob', 'File', 'FileList', 'RTCPeerConnection',
                'PressureObserver', 'InputDeviceCapabilities', 'MediaSession',
                'Touch', 'TouchList', 'EyeDropper', 'XMLHttpRequest',
                'XMLHttpRequestUpload', 'WebSocket', 'Notification', 'Image',
                'Audio', 'Headers', 'Request', 'Response', 'createImageBitmap',
                'structuredClone', 'queueMicrotask', 'reportError', 'atob',
                'btoa', 'ResizeObserver', 'IntersectionObserver',
                'MutationObserver', 'TextEncoder', 'TextDecoder', 'EventTarget',
                'Event', 'CustomEvent', 'MediaStream', 'MediaStreamTrack',
                'MediaRecorder', 'DOMRect', 'DOMRectReadOnly', 'DOMPoint',
                'DOMPointReadOnly', 'DOMQuad',
                // The WebGL/Canvas context
                // constructor OBJECTS themselves. Their prototype methods are
                // masked by the universal sweep, but String(WebGLRenderingContext)
                // is commonly enumerated and must be `[native code]`.
                'WebGLRenderingContext', 'WebGL2RenderingContext',
                'CanvasRenderingContext2D', 'WebGLContextEvent',
                // Event-subclass constructor
                // objects. event_bootstrap.js defines them as JS classes, so
                // String(MouseEvent) leaked `class MouseEvent extends ...`,
                // which differs from real Chrome. Masking sets `[native code]`
                // + the correct own `.name`. Real Chrome: every one is native.
                'UIEvent', 'MouseEvent', 'KeyboardEvent', 'InputEvent',
                'FocusEvent', 'PointerEvent', 'WheelEvent', 'MessageEvent',
                'ErrorEvent', 'ProgressEvent', 'AnimationEvent',
                'TransitionEvent', 'ClipboardEvent', 'PopStateEvent',
                'HashChangeEvent', 'StorageEvent', 'PageTransitionEvent',
                'BeforeUnloadEvent', 'DragEvent', 'SecurityPolicyViolationEvent',
                'CompositionEvent', 'DeviceMotionEvent', 'DeviceOrientationEvent',
            ];
            for (const _e of _sfcNames) {
                try {
                    const _key = Array.isArray(_e) ? _e[0] : _e;
                    const _nm = Array.isArray(_e) ? _e[1] : _e;
                    const _fn = globalThis[_key];
                    if (typeof _fn === 'function') _mask(_fn, _nm);
                } catch (_e2) {}
            }
        }
    } catch (_e) { /* sfc masking is best-effort */ }

    // -- Universal prototype mask sweep ----------
    // Many scripts inspect Function.prototype.toString
    // on patched prototype methods (Headers/Request/Response, XHR,
    // Observers, Streams, Event subclasses, IDB, Range, etc.). Walk
    // every globalThis constructor that has a .prototype, mask every
    // own-function method to `function NAME() { [native code] }`.
    // Runs AFTER all bootstraps (interfaces / shared_apis / streams /
    // events / canvas / window / worker) so it covers every prototype
    // installed by them — including bootstraps that run post-snapshot.
    // Safe on real V8 natives: `_maskAsNative` is idempotent — sets the
    // Symbol(__browser_oxide_native__) tag; if the function was already
    // native-toString-ing it stays so.
    try {
        const _mask = globalThis._maskAsNative;
        if (typeof _mask === 'function') {
            const _SKIP = new Set([Object.prototype, Function.prototype]);
            for (const _gname of Object.getOwnPropertyNames(globalThis)) {
                let _v;
                try { _v = globalThis[_gname]; } catch (_e) { continue; }
                if (typeof _v !== 'function') continue;
                const _p = _v.prototype;
                if (!_p || _SKIP.has(_p)) continue;
                const _methods = [];
                let _ns;
                try { _ns = Object.getOwnPropertyNames(_p); } catch (_e) { continue; }
                for (const _n of _ns) {
                    if (_n === 'constructor') continue;
                    let _d;
                    try { _d = Object.getOwnPropertyDescriptor(_p, _n); } catch (_e) { continue; }
                    // Collect ACCESSOR props too
                    // (get/set), not just data-value methods. _maskAsNative
                    // already masks desc.get/desc.set (stealth_bootstrap.js:94),
                    // but the sweep previously skipped accessor-only props, so
                    // ~15 injected getters/setters (Request.signal, Response.*,
                    // ReadableStream.locked, MessagePort.onmessage,
                    // URLSearchParams.size, WebSocket.*) leaked JS source under
                    // `getOwnPropertyDescriptor(proto,name).get.toString()` —
                    // a Function.toString integrity tell ~11 vendors probe.
                    if (
                        _d &&
                        (typeof _d.value === 'function' ||
                            typeof _d.get === 'function' ||
                            typeof _d.set === 'function')
                    ) {
                        _methods.push(_n);
                    }
                }
                if (_methods.length) {
                    try { _mask(_p, ..._methods); } catch (_e) {}
                }
            }
        }
    } catch (_e) { /* universal mask sweep is best-effort */ }

    // -- Native-shape pass (fbgb fork) --------------------------------
    // Real natives that are methods, getters or setters have NO own
    // `prototype` and cannot be constructed; ours were plain `function`
    // expressions, so `hasOwnProperty(fn, 'prototype')` exposed ~150 of them
    // (rebrowser "nativeTampering", CreepJS lies). The `prototype` property
    // of an ordinary function cannot be deleted, so each one is replaced by
    // an equivalent method/accessor-shorthand forwarder (prototype-less, not
    // constructible, same `length`) and masked native under the same name.
    // Also masks window's own accessors that still showed their JS source.
    // Runs on the FIRST execution only (this file runs again after the page's
    // scripts; page-defined functions must keep their identity).
    // The warm-reuse helpers move onto the hidden hook at the end of the first
    // run (see below), so "first run" is decided from there.
    const _boHook = globalThis[Symbol.for('__browser_oxide_script_hook__')];
    const _boSecondRun = !!(_boHook && typeof _boHook.__resetPageGlobals === 'function')
        || typeof globalThis.__resetPageGlobals === 'function';
    if (!_boSecondRun) try {
        const _mask = globalThis._maskFunction;
        const _tag = Symbol.for('__browser_oxide_native__');
        const _hasProto = (fn) => typeof fn === 'function' && Object.prototype.hasOwnProperty.call(fn, 'prototype');
        const _isCtorLike = (fn) => {
            try {
                if (/^class[\s{]/.test(Function.prototype.toString.call(fn))) return true;
                const p = fn.prototype;
                return !!(p && Object.getOwnPropertyNames(p).some((n) => n !== 'constructor'));
            } catch (_e) { return true; }
        };
        const _tagOf = (fn, fallback) => { try { return fn[_tag] || fallback; } catch (_e) { return fallback; } };
        const _fix = (fn, key, kind) => {
            const k = typeof key === 'symbol' ? key : String(key);
            let w;
            if (kind === 'get') w = Object.getOwnPropertyDescriptor({ get [k]() { return fn.call(this); } }, k).get;
            else if (kind === 'set') w = Object.getOwnPropertyDescriptor({ set [k](v) { fn.call(this, v); } }, k).set;
            else w = ({ [k](...args) { return fn.apply(this, args); } })[k];
            try { Object.defineProperty(w, 'length', { value: fn.length, configurable: true }); } catch (_e) {}
            const label = typeof key === 'symbol' ? '' : (kind ? kind + ' ' : '') + String(key);
            if (typeof _mask === 'function') _mask(w, _tagOf(fn, label));
            return w;
        };
        const _visit = (obj, keyFilter) => {
            let keys;
            try { keys = Reflect.ownKeys(obj); } catch (_e) { return; }
            for (const key of keys) {
                if (key === 'constructor') continue;
                if (keyFilter && !keyFilter(key)) continue;
                let d;
                try { d = Object.getOwnPropertyDescriptor(obj, key); } catch (_e) { continue; }
                if (!d || !d.configurable) continue;
                let changed = false;
                const nd = Object.assign({}, d);
                if (_hasProto(d.get) && !_isCtorLike(d.get)) { nd.get = _fix(d.get, key, 'get'); changed = true; }
                if (_hasProto(d.set) && !_isCtorLike(d.set)) { nd.set = _fix(d.set, key, 'set'); changed = true; }
                if (typeof key === 'string' && /^[A-Z]/.test(key)) { /* constructors stay */ }
                else if (_hasProto(d.value) && !_isCtorLike(d.value)) { nd.value = _fix(d.value, key, ''); changed = true; }
                if (changed) { try { Object.defineProperty(obj, key, nd); } catch (_e) {} }
                // unmasked accessors on window: give them the native name tag
                if (!changed && typeof _mask === 'function') {
                    for (const [f, kind] of [[d.get, 'get'], [d.set, 'set']]) {
                        if (typeof f === 'function' && !_tagOf(f, null) && typeof key === 'string') _mask(f, kind + ' ' + key);
                    }
                }
            }
        };
        const _SKIP = new Set([Object.prototype, Function.prototype]);
        for (const _gname of Object.getOwnPropertyNames(globalThis)) {
            let _v;
            try { _v = globalThis[_gname]; } catch (_e) { continue; }
            if (typeof _v !== 'function' || !/^[A-Z]/.test(_gname)) continue;
            const _p = _v.prototype;
            if (_p && !_SKIP.has(_p)) _visit(_p);
        }
        for (const o of [Date.prototype, Intl.DateTimeFormat.prototype, Intl.NumberFormat.prototype, Intl.Collator && Intl.Collator.prototype,
                         Intl.PluralRules && Intl.PluralRules.prototype, Intl.RelativeTimeFormat && Intl.RelativeTimeFormat.prototype]) {
            if (o) _visit(o);
        }
        // Interface objects (constructors) are non-enumerable own properties of
        // window in Chrome: Object.keys(window) lists none of them.
        for (const k of Object.getOwnPropertyNames(globalThis)) {
            if (!/^[A-Z]/.test(k)) continue;
            const d = Object.getOwnPropertyDescriptor(globalThis, k);
            if (d && d.enumerable && d.configurable && typeof d.value === 'function') {
                try { Object.defineProperty(globalThis, k, Object.assign({}, d, { enumerable: false })); } catch (_e) {}
            }
        }
        // window's own members (lowercase names; skip engine internals and frame indices)
        _visit(globalThis, (key) => typeof key === 'string' && /^[a-z]/.test(key) && !key.startsWith('on'));
    } catch (_e) { /* native-shape pass is best-effort */ }

    const internals = [
        'Deno',
        'ops',
        '_maskFunction',
        '_maskAsNative',
        '_nativeTag',
        '_customElementsRegistry',
        '__bootstrap',
        '__browser_oxide',
        '__syncCookiesFromNet',
        '__documentReadyState',
        '__drainCspViolations',
        '__onNodeInserted',
        '__errors',
    ];

    // -- Worker Scope Isolation (Phase 8) ---------------------------
    // Real Chrome Web Workers (DedicatedWorkerGlobalScope) have a very
    // clean namespace. They do NOT expose DOM, CSSOM, or Hardware APIs.
    // If we're in a worker, purge the illegal globals.
    const _isWorker = typeof DedicatedWorkerGlobalScope !== 'undefined' && 
                      globalThis instanceof DedicatedWorkerGlobalScope;
    if (_isWorker) {
        const _workerPurge = [
            'window', 'document', 'history', 'locationbar', 'menubar', 
            'personalbar', 'scrollbars', 'statusbar', 'toolbar', 'frames', 
            'parent', 'top', 'opener', 'frameElement', 'styleMedia', 
            'getComputedStyle', 'getSelection', 'matchMedia', 'alert', 
            'confirm', 'prompt', 'print', 'stop', 'open', 'close', 
            'focus', 'blur', 'moveBy', 'moveTo', 'resizeBy', 'resizeTo', 
            'scroll', 'scrollBy', 'scrollTo', 'requestAnimationFrame', 
            'cancelAnimationFrame', 'requestIdleCallback', 'cancelIdleCallback',
            // Constructors
            'Node', 'Element', 'HTMLElement', 'HTMLDocument', 'Document', 
            'CharacterData', 'Text', 'Comment', 'CDATASection', 'DocumentFragment', 
            'DocumentType', 'NamedNodeMap', 'Attr', 'NodeList', 'HTMLCollection', 
            'HTMLAllCollection', 'DOMTokenList', 'DOMImplementation', 'Range', 
            'Selection', 'DOMParser', 'XMLSerializer', 'XPathEvaluator', 
            'XPathExpression', 'XPathResult', 'XSLTProcessor', 'MutationObserver', 
            'MutationRecord', 'IntersectionObserver', 'ResizeObserver', 
            'PermissionStatus', 'Screen', 'ScreenOrientation', 'VisualViewport',
            'ViewTransition', 'Highlight', 'HighlightRegistry',
            // Hardware/Media (not allowed in workers)
            'Bluetooth', 'USB', 'HID', 'Serial', 'Gamepad', 'GamepadButton', 
            'GamepadEvent', 'GamepadHapticActuator', 'MediaStream', 'MediaStreamTrack', 
            'MediaRecorder', 'RTCPeerConnection', 'RTCDataChannel', 'RTCSessionDescription', 
            'RTCIceCandidate', 'RTCCertificate', 'Presentation', 'PresentationRequest',
            // CSS classes (100+)
            'CSS', 'CSSStyleSheet', 'CSSRule', 'CSSStyleRule', 'CSSMediaRule', 
            'CSSImportRule', 'CSSFontFaceRule', 'CSSPageRule', 'CSSKeyframesRule', 
            'CSSKeyframeRule', 'CSSNamespaceRule', 'CSSSupportsRule', 'CSSCounterStyleRule',
            // ... and all HTML*Element subclasses
        ];
        for (const k of Object.keys(globalThis)) {
            if (k.startsWith('HTML') || k.startsWith('SVG') || k.startsWith('CSS') || _workerPurge.includes(k)) {
                try { delete globalThis[k]; } catch (_) {}
            }
        }
    }

    if (ops && ops.op_cross_origin_isolated && !ops.op_cross_origin_isolated()) {
        internals.push('SharedArrayBuffer');
    }

    // -- Warm-reuse global-namespace reset ---------------------------
    // The last retention source for a pooled `Page`: properties page
    // scripts hang straight off the global (`window.__APP_STATE = …`,
    // `window.onscroll = …`, framework singletons). `globalThis` is the
    // same object for the whole life of the `JsRuntime`, so on the warm
    // path every one of those — and everything they transitively
    // reference — survives into the next navigation. A real browser gives
    // each navigation a fresh global; this is the closest equivalent that
    // keeps the expensive bootstrap intact.
    //
    // `__markGlobalsBaseline()` snapshots the engine-owned key set;
    // `__resetPageGlobals()` deletes everything added since. Rust re-marks
    // the baseline once more after it installs the post-bootstrap
    // instrumentation (`__cookieWrites` / `__scriptErrors` / the fetch +
    // XHR wrappers), which is why those names are also allowlisted below —
    // construction paths that skip the re-mark must not lose them.
    // Note `window === globalThis` here (dom_bootstrap.js), so scrubbing
    // the global object covers both.
    // Guarded: this file is executed TWICE per page — once from
    // `BrowserJsRuntime`'s constructor (before any page script) and again
    // from `build_page_with_scripts_*` after the document's scripts have
    // run. Only the first execution may seed the baseline; re-running the
    // definitions would also reset the closure variable and throw the real
    // baseline away.
    if (!_boSecondRun) {
        let _globalsBaseline = null;
        let _onHandlerBaseline = null;
        const _BASELINE_ALWAYS = [
            '_browser_oxide', '__cookieWrites', '__scriptErrors',
            '__bo_input_events', '__jsCookies',
        ];

        // `on*` handlers need value-level treatment, not just key-level.
        // `onscroll`, `onerror`, … already EXIST as own properties of the
        // global at bootstrap (default `null`), so a page that assigns
        // `window.onscroll = fn` mutates a baseline key rather than adding
        // one — the key-set diff below cannot see it, and the closure (plus
        // everything it captures) survives the navigation.
        //
        // Blanket-nulling them is wrong: the engine itself installs
        // `window.onerror` as its script-error instrumentation, once, and
        // does NOT re-install it on the warm path. So snapshot the values
        // at baseline and RESTORE them, which nulls page assignments while
        // preserving the engine's.
        const _snapshotOnHandlers = (target) => {
            const m = new Map();
            if (!target) return m;
            let names;
            try { names = Object.getOwnPropertyNames(target); } catch (_e) { return m; }
            for (const k of names) {
                if (!k.startsWith('on')) continue;
                try { m.set(k, target[k]); } catch (_e) {}
            }
            return m;
        };
        const _restoreOnHandlers = (target, baseline) => {
            if (!target || !baseline) return;
            let names;
            try { names = Object.getOwnPropertyNames(target); } catch (_e) { return; }
            for (const k of names) {
                if (!k.startsWith('on')) continue;
                try {
                    if (typeof target[k] !== 'function') continue;
                    const orig = baseline.get(k);
                    // Already the engine's own handler ⇒ leave it alone.
                    if (orig === target[k]) continue;
                    target[k] = (typeof orig === 'function') ? orig : null;
                } catch (_e) {}
            }
        };

        Object.defineProperty(globalThis, '__markGlobalsBaseline', {
            value: function __markGlobalsBaseline() {
                const seen = new Set(_BASELINE_ALWAYS);
                for (const k of Object.getOwnPropertyNames(globalThis)) seen.add(k);
                for (const s of Object.getOwnPropertySymbols(globalThis)) seen.add(s);
                _globalsBaseline = seen;
                // `document` is a singleton that survives `replace_dom`, so
                // `document.onclick = fn` persists exactly like the window
                // case and needs the same treatment.
                _onHandlerBaseline = {
                    global: _snapshotOnHandlers(globalThis),
                    document: _snapshotOnHandlers(globalThis.document),
                };
            },
            writable: true, configurable: true, enumerable: false,
        });
        Object.defineProperty(globalThis, '__resetPageGlobals', {
            value: function __resetPageGlobals() {
                // No baseline ⇒ nothing to compare against; deleting on a
                // guess would strip the engine's own globals.
                if (!_globalsBaseline) return 0;
                let removed = 0;
                const keys = Object.getOwnPropertyNames(globalThis)
                    .concat(Object.getOwnPropertySymbols(globalThis));
                for (const k of keys) {
                    if (_globalsBaseline.has(k)) continue;
                    // Best-effort: a page can install a non-configurable
                    // property, and `delete` cannot remove those.
                    try { if (delete globalThis[k]) removed++; } catch (_e) {}
                }
                if (_onHandlerBaseline) {
                    _restoreOnHandlers(globalThis, _onHandlerBaseline.global);
                    _restoreOnHandlers(globalThis.document, _onHandlerBaseline.document);
                }
                return removed;
            },
            writable: true, configurable: true, enumerable: false,
        });
        // Seed the baseline on this first execution: it runs as the last
        // bootstrap, before anything page-authored, so the global namespace
        // is exactly the engine's. Rust re-marks once more after installing
        // the post-bootstrap instrumentation. The `internals` purge below
        // only ever REMOVES keys, so marking before it is safe.
        globalThis.__markGlobalsBaseline();
    }

    // Engine helpers that page scripts must not see in
    // Object.getOwnPropertyNames(window): keep them on the Symbol-keyed hook.
    if (!_boSecondRun && _boHook) {
        for (const n of ['__cancelAllTimers', '__bgSetTimeout', '__resetDomRegistries', '__cancelAllListeners',
                         '__resetCustomElements', '__markGlobalsBaseline', '__resetPageGlobals']) {
            if (typeof globalThis[n] !== 'function') continue;
            try { Object.defineProperty(_boHook, n, { value: globalThis[n], enumerable: false, configurable: true, writable: true }); } catch (_e) {}
            try { delete globalThis[n]; } catch (_e) {}
        }
    }


    // -- Chrome surface alignment (generated from a real-Chrome diff) ----
    // Removes non-Chrome globals, adds Chrome-only ones FingerprintJS'
    // isChromium()/tampering models probe, and hides engine plumbing from
    // own-key reflection on window.
    if (!_isWorker && !(_boHook && _boHook.__surfaceAligned)) {
        try {
            if (_boHook) Object.defineProperty(_boHook, '__surfaceAligned', { value: true, enumerable: false, configurable: true });
            const g = globalThis;
            const mk = globalThis._maskFunction;
            const M = (f, n) => { try { if (typeof mk === 'function') mk(f, n); } catch (_e) {} return f; };
            const defV = (o, k, v) => { try { Object.defineProperty(o, k, { value: v, writable: true, enumerable: false, configurable: true }); } catch (_e) {} };
            // 1. Not present in desktop Chrome.
            for (const k of ['WorkerGlobalScope', 'DedicatedWorkerGlobalScope', 'ApplePaySession', 'webkitAudioContext',
                             'XSLTProcessor', 'defaultStatus', 'Magnetometer', 'SharedStorage', 'SharedStorageWorklet',
                             'SpeechRecognitionAlternative', 'USBIsochronousOutPacket']) {
                try { delete g[k]; } catch (_e) {}
            }
            const NP = g.Navigator && g.Navigator.prototype;
            for (const k of ['msDoNotTrack', 'loadPurpose', 'sayswho', 'mozGetUserMedia']) {
                try { if (NP) delete NP[k]; } catch (_e) {}
                try { delete g.navigator[k]; } catch (_e) {}
            }
            try { delete g.Document.prototype.documentMode; } catch (_e) {}
            try { delete g.document.documentMode; } catch (_e) {}
            // 2. Chrome-only window members.
            const quota = (name) => {
                const Q = M(function DeprecatedStorageQuota() { throw new TypeError('Illegal constructor'); }, 'DeprecatedStorageQuota');
                const q = Object.create(Q.prototype);
                Object.defineProperty(Q.prototype, Symbol.toStringTag, { value: 'DeprecatedStorageQuota', configurable: true });
                defV(Q.prototype, 'queryUsageAndQuota', M(function queryUsageAndQuota(ok) {
                    if (typeof ok === 'function') setTimeout(() => ok(0, name === 'temp' ? 299977904946 : 0), 0);
                }, 'queryUsageAndQuota'));
                defV(Q.prototype, 'requestQuota', M(function requestQuota(n, ok) {
                    if (typeof ok === 'function') setTimeout(() => ok(name === 'temp' ? 0 : n), 0);
                }, 'requestQuota'));
                return q;
            };
            if (NP) {
                const tq = quota('temp'), pq = quota('pers');
                for (const [k, v] of [['webkitTemporaryStorage', tq], ['webkitPersistentStorage', pq]]) {
                    if (!(k in NP)) {
                        try { Object.defineProperty(NP, k, { get: M(function () { return v; }, 'get ' + k), enumerable: true, configurable: true }); } catch (_e) {}
                    }
                }
            }
            const fsErr = (cb) => { if (typeof cb === 'function') setTimeout(() => { try { cb(new DOMException('An attempt was made to break through the security policy of the user agent.', 'SecurityError')); } catch (_e) {} }, 0); };
            const fns = {
                webkitRequestFileSystem: function webkitRequestFileSystem(t, s, ok, err) { fsErr(err); },
                webkitResolveLocalFileSystemURL: function webkitResolveLocalFileSystemURL(u, ok, err) { fsErr(err); },
                blur: function blur() {}, focus: function focus() {},
                captureEvents: function captureEvents() {}, releaseEvents: function releaseEvents() {},
                find: function find() { return false; },
                moveBy: function moveBy() {}, moveTo: function moveTo() {},
                resizeBy: function resizeBy() {}, resizeTo: function resizeTo() {},
            };
            for (const k in fns) if (!(k in g)) defV(g, k, M(fns[k], k));
            if (!('webkitURL' in g) && g.URL) defV(g, 'webkitURL', g.URL);
            const ill = (n) => { const C = M(({ [n]: function () { throw new TypeError('Illegal constructor'); } })[n], n);
                Object.defineProperty(C.prototype, Symbol.toStringTag, { value: n, configurable: true }); return C; };
            for (const n of ['SpeechRecognition', 'SpeechGrammar', 'SpeechGrammarList', 'SpeechRecognitionErrorEvent', 'SpeechRecognitionEvent']) {
                const w = 'webkit' + n.replace('ErrorEvent', 'Error');
                if (!(w in g)) defV(g, w, g[n] || ill(w));
            }
            for (const n of ['PictureInPictureWindow', 'PermissionsPolicy', 'MediaStreamTrack', 'MediaStream',
                             'HTMLDirectoryElement', 'HTMLDialogElement', 'HTMLDetailsElement', 'HTMLDataListElement',
                             'HTMLDataElement', 'HTMLDListElement', 'HTMLCollection', 'HTMLBaseElement', 'HTMLBRElement',
                             'HTMLAreaElement', 'FontFaceSet', 'MIDIAccess', 'MIDIConnectionEvent', 'MIDIInput', 'MIDIInputMap',
                             'MIDIMessageEvent', 'MIDIOutput', 'MIDIOutputMap', 'MIDIPort', 'VideoDecoder', 'VideoEncoder',
                             'BluetoothCharacteristicProperties', 'BluetoothDevice', 'BluetoothRemoteGATTCharacteristic',
                             'BluetoothRemoteGATTDescriptor', 'BluetoothRemoteGATTServer', 'BluetoothRemoteGATTService',
                             'BluetoothUUID', 'USB', 'USBIsochronousOutTransferPacket']) {
                if (!(n in g)) {
                    const C = ill(n);
                    if (/^HTML.*Element$/.test(n) && g.HTMLElement) Object.setPrototypeOf(C.prototype, g.HTMLElement.prototype);
                    defV(g, n, C);
                }
            }
            // 3. Event-handler IDL attributes (null by default, wired to listeners).
            const handlerAttr = (proto, k) => {
                if (!proto || Object.prototype.hasOwnProperty.call(proto, k)) return;
                const store = new WeakMap(); const type = k.slice(2);
                Object.defineProperty(proto, k, {
                    get: M(function () { return store.has(this) ? store.get(this).h : null; }, 'get ' + k),
                    set: M(function (v) {
                        const prev = store.get(this);
                        if (prev && prev.l && this.removeEventListener) this.removeEventListener(type, prev.l);
                        if (typeof v === 'function' || (v && typeof v === 'object')) {
                            const self = this;
                            const l = function (e) { const h = store.get(self); if (h && typeof h.h === 'function') return h.h.call(self, e); };
                            store.set(this, { h: v, l });
                            if (this.addEventListener) this.addEventListener(type, l);
                        } else store.delete(this);
                    }, 'set ' + k),
                    enumerable: true, configurable: true,
                });
            };
            for (const k of ['onerror', 'ondevicemotion', 'ondeviceorientation', 'ondeviceorientationabsolute']) {
                if (!(k in g)) handlerAttr(g, k);
            }
            const DP = g.Document && g.Document.prototype;
            const docHandlers = ['onabort','onanimationcancel','onanimationend','onanimationiteration','onanimationstart','onauxclick','onbeforecopy','onbeforecut','onbeforeinput','onbeforematch','onbeforepaste','onbeforetoggle','onbeforexrselect','onblur','oncancel','oncanplay','oncanplaythrough','onchange','onclick','onclose','oncommand','oncontentvisibilityautostatechange','oncontextlost','oncontextmenu','oncontextrestored','oncopy','oncuechange','oncut','ondblclick','ondrag','ondragend','ondragenter','ondragleave','ondragover','ondragstart','ondrop','ondurationchange','onemptied','onended','onerror','onfocus','onformdata','onfreeze','onfullscreenchange','onfullscreenerror','ongotpointercapture','oninput','oninvalid','onkeydown','onkeypress','onkeyup','onload','onloadeddata','onloadedmetadata','onloadstart','onlostpointercapture','onmousedown','onmouseenter','onmouseleave','onmousemove','onmouseout','onmouseover','onmouseup','onmousewheel','onpaste','onpause','onplay','onplaying','onpointercancel','onpointerdown','onpointerenter','onpointerleave','onpointerlockchange','onpointerlockerror','onpointermove','onpointerout','onpointerover','onpointerrawupdate','onpointerup','onprerenderingchange','onprogress','onratechange','onreadystatechange','onreset','onresize','onresume','onscroll','onscrollend','onscrollsnapchange','onscrollsnapchanging','onsearch','onsecuritypolicyviolation','onseeked','onseeking','onselect','onselectionchange','onselectstart','onslotchange','onstalled','onsubmit','onsuspend','ontimeupdate','ontoggle','ontransitioncancel','ontransitionend','ontransitionrun','ontransitionstart','onvisibilitychange','onvolumechange','onwaiting','onwebkitanimationend','onwebkitanimationiteration','onwebkitanimationstart','onwebkitfullscreenchange','onwebkitfullscreenerror','onwebkittransitionend','onwheel'];
            if (DP) {
                for (const k of docHandlers) {
                    // Move instance-level handler props (if any) to the prototype shape.
                    try { if (Object.prototype.hasOwnProperty.call(g.document, k) && g.document[k] == null) delete g.document[k]; } catch (_e) {}
                    if (!(k in g.document)) handlerAttr(DP, k);
                }
                const acc = (k, get, set) => { if (k in DP) return; try { Object.defineProperty(DP, k, { get: M(get, 'get ' + k), set: set && M(set, 'set ' + k), enumerable: true, configurable: true }); } catch (_e) {} };
                const sv = new WeakMap(); const sget = (k, d) => function () { const o = sv.get(this); return o && k in o ? o[k] : d; };
                const sset = (k) => function (v) { let o = sv.get(this); if (!o) { o = {}; sv.set(this, o); } o[k] = String(v); };
                for (const k of ['alinkColor', 'bgColor', 'fgColor', 'linkColor', 'vlinkColor']) acc(k, sget(k, ''), sset(k));
                acc('designMode', sget('designMode', 'off'), sset('designMode'));
                acc('dir', sget('dir', ''), sset('dir'));
                acc('inputEncoding', function () { return this.characterSet || 'UTF-8'; });
                acc('fullscreen', function () { return false; });
                acc('lastModified', function () {
                    const d = new Date(); const p = (n) => String(n).padStart(2, '0');
                    return `${p(d.getMonth() + 1)}/${p(d.getDate())}/${d.getFullYear()} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
                });
                const sheets = new WeakMap();
                acc('adoptedStyleSheets', function () { if (!sheets.has(this)) sheets.set(this, []); return sheets.get(this); },
                    function (v) { sheets.set(this, Array.from(v || [])); });
                for (const [k, f] of [['captureEvents', function captureEvents() {}], ['releaseEvents', function releaseEvents() {}],
                                      ['clear', function clear() {}], ['getAnimations', function getAnimations() { return []; }],
                                      ['exitPictureInPicture', function exitPictureInPicture() { return Promise.reject(new DOMException('There is no Picture-in-Picture element in this document.', 'InvalidStateError')); }],
                                      ['hasUnpartitionedCookieAccess', function hasUnpartitionedCookieAccess() { return Promise.resolve(true); }]]) {
                    if (!(k in DP)) defV(DP, k, M(f, k));
                }
            }
            // 2b. Remaining Chrome members (document / navigator / window).
            if (DP) {
                const acc2 = (k, get) => { if (!(k in DP)) try { Object.defineProperty(DP, k, { get: M(get, 'get ' + k), enumerable: true, configurable: true }); } catch (_e) {} };
                acc2('scrollingElement', function () { return this.documentElement || null; });
                acc2('rootElement', function () { return null; });
                acc2('xmlEncoding', function () { return null; });
                acc2('xmlVersion', function () { return null; });
                acc2('xmlStandalone', function () { return false; });
                acc2('pictureInPictureElement', function () { return null; });
                acc2('pictureInPictureEnabled', function () { return true; });
                acc2('webkitFullscreenElement', function () { return null; });
                acc2('webkitCurrentFullScreenElement', function () { return null; });
                acc2('activeViewTransition', function () { return null; });
                acc2('prerendering', function () { return false; });
                acc2('wasDiscarded', function () { return false; });
                acc2('plugins', function () { return this.embeds || []; });
                acc2('applets', function () { return []; });
                const ff = (() => {
                    const C = ill('FontFaceSet'); const o = Object.create((g.FontFaceSet || C).prototype);
                    const ready = Promise.resolve(o);
                    for (const [k, v] of [['ready', ready], ['status', 'loaded'], ['size', 0], ['onloading', null], ['onloadingdone', null], ['onloadingerror', null]]) defV(o, k, v);
                    defV(o, 'check', M(function check() { return true; }, 'check'));
                    defV(o, 'load', M(function load() { return Promise.resolve([]); }, 'load'));
                    defV(o, 'add', M(function add() { return o; }, 'add'));
                    defV(o, 'delete', M(function () { return false; }, 'delete'));
                    defV(o, 'has', M(function has() { return false; }, 'has'));
                    defV(o, 'clear', M(function clear() {}, 'clear'));
                    defV(o, 'forEach', M(function forEach() {}, 'forEach'));
                    defV(o, 'addEventListener', M(function addEventListener() {}, 'addEventListener'));
                    defV(o, 'removeEventListener', M(function removeEventListener() {}, 'removeEventListener'));
                    defV(o, Symbol.iterator, M(function values() { return [][Symbol.iterator](); }, 'values'));
                    return o;
                })();
                acc2('fonts', function () { return ff; });
                const docFns = {
                    queryCommandState: function queryCommandState() { return false; },
                    queryCommandValue: function queryCommandValue() { return ''; },
                    queryCommandIndeterm: function queryCommandIndeterm() { return false; },
                    webkitCancelFullScreen: function webkitCancelFullScreen() {},
                    webkitExitFullscreen: function webkitExitFullscreen() {},
                    caretRangeFromPoint: function caretRangeFromPoint() { return null; },
                    getElementsByTagNameNS: function getElementsByTagNameNS(ns, n) { return this.getElementsByTagName(n); },
                    createAttributeNS: function createAttributeNS(ns, n) { return this.createAttribute(String(n).split(':').pop()); },
                    createCDATASection: function createCDATASection() { throw new DOMException('This operation is not supported for HTML documents.', 'NotSupportedError'); },
                    createProcessingInstruction: function createProcessingInstruction(t, d) { return this.createComment(d); },
                    createNSResolver: function createNSResolver(n) { return n; },
                };
                for (const k in docFns) if (!(k in DP)) defV(DP, k, M(docFns[k], k));
                // [LegacyUnforgeable] location: own property of the document.
                try {
                    const ld = Object.getOwnPropertyDescriptor(DP, 'location');
                    if (ld && !Object.prototype.hasOwnProperty.call(g.document, 'location')) {
                        Object.defineProperty(g.document, 'location', { get: ld.get, set: ld.set, enumerable: true, configurable: false });
                        delete DP.location;
                    }
                } catch (_e) {}
            }
            if (NP) {
                const navObj = (k, iface) => { if (!(k in NP)) { const o = Object.create(ill(iface).prototype);
                    try { Object.defineProperty(NP, k, { get: M(function () { return o; }, 'get ' + k), enumerable: true, configurable: true }); } catch (_e) {} } };
                navObj('ink', 'Ink'); navObj('login', 'NavigatorLogin'); navObj('presentation', 'Presentation');
                navObj('xr', 'XRSystem'); navObj('storageBuckets', 'StorageBucketManager'); navObj('managed', 'NavigatorManagedData');
                if (!('cpuPerformance' in NP)) try { Object.defineProperty(NP, 'cpuPerformance', { get: M(function () { return 0; }, 'get cpuPerformance'), enumerable: true, configurable: true }); } catch (_e) {}
                if (!('requestMIDIAccess' in NP)) defV(NP, 'requestMIDIAccess', M(function requestMIDIAccess() { return Promise.reject(new DOMException('Permission denied.', 'NotAllowedError')); }, 'requestMIDIAccess'));
                if (!('install' in NP)) defV(NP, 'install', M(function install() { return Promise.reject(new DOMException('Not allowed.', 'NotAllowedError')); }, 'install'));
            }
            const rej = (name, msg) => function () { return Promise.reject(new DOMException(msg, name)); };
            const wfns = {
                queryLocalFonts: rej('SecurityError', "Failed to execute 'queryLocalFonts' on 'Window': Must be handling a user gesture to show a permission request."),
                showOpenFilePicker: rej('SecurityError', "Failed to execute 'showOpenFilePicker' on 'Window': Must be handling a user gesture to show a file picker."),
                showSaveFilePicker: rej('SecurityError', "Failed to execute 'showSaveFilePicker' on 'Window': Must be handling a user gesture to show a file picker."),
                showDirectoryPicker: rej('SecurityError', "Failed to execute 'showDirectoryPicker' on 'Window': Must be handling a user gesture to show a file picker."),
                getScreenDetails: rej('NotAllowedError', 'Permission denied.'),
            };
            for (const k in wfns) if (!(k in g)) defV(g, k, M(Object.defineProperty(wfns[k], 'name', { value: k }), k));
            if (!('event' in g)) try { Object.defineProperty(g, 'event', { get: M(function () { return undefined; }, 'get event'), set: M(function () {}, 'set event'), enumerable: true, configurable: true }); } catch (_e) {}
            if (!('oninstallresult' in g)) handlerAttr(g, 'oninstallresult');
            // 2c. Chrome-ordered Navigator.prototype, document members on the
            // prototype, Audio() named constructor.
            if (NP) {
                const ORDER = ["maxTouchPoints", "scheduling", "userActivation", "geolocation", "ink", "doNotTrack", "mediaCapabilities", "permissions", "webkitTemporaryStorage", "webkitPersistentStorage", "windowControlsOverlay", "hardwareConcurrency", "cookieEnabled", "appCodeName", "appName", "appVersion", "platform", "product", "productSub", "userAgent", "vendor", "vendorSub", "language", "languages", "onLine", "webdriver", "plugins", "mimeTypes", "pdfViewerEnabled", "connection", "getGamepads", "javaEnabled", "sendBeacon", "vibrate", "mediaSession", "constructor", "cpuPerformance", "bluetooth", "clipboard", "credentials", "keyboard", "managed", "mediaDevices", "serviceWorker", "virtualKeyboard", "wakeLock", "deviceMemory", "userAgentData", "locks", "storage", "gpu", "login", "devicePosture", "hid", "presentation", "serial", "usb", "xr", "storageBuckets", "canShare", "share", "clearAppBadge", "getBattery", "getUserMedia", "requestMIDIAccess", "requestMediaKeySystemAccess", "setAppBadge", "webkitGetUserMedia", "getInstalledRelatedApps", "install", "registerProtocolHandler", "unregisterProtocolHandler"];
                const descs = {};
                for (const k of Object.getOwnPropertyNames(NP)) descs[k] = Object.getOwnPropertyDescriptor(NP, k);
                const rest = Object.keys(descs).filter((k) => !ORDER.includes(k) && k !== 'constructor');
                for (const k of [...ORDER, ...rest]) {
                    const dsc = descs[k];
                    if (!dsc || !dsc.configurable) continue;
                    try { delete NP[k]; Object.defineProperty(NP, k, dsc); } catch (_e) {}
                }
            }
            if (DP) {
                for (const k of Object.getOwnPropertyNames(g.document)) {
                    if (k === 'location') continue;
                    const dsc = Object.getOwnPropertyDescriptor(g.document, k);
                    if (!dsc || !dsc.configurable) continue;
                    if (Object.prototype.hasOwnProperty.call(DP, k)) { try { delete g.document[k]; } catch (_e) {} continue; }
                    try {
                        if ('value' in dsc && typeof dsc.value !== 'function') {
                            const v = dsc.value;
                            Object.defineProperty(DP, k, { get: M(function () { return v; }, 'get ' + k), enumerable: true, configurable: true });
                        } else {
                            Object.defineProperty(DP, k, Object.assign({}, dsc, { enumerable: true }));
                        }
                        delete g.document[k];
                    } catch (_e) {}
                }
            }
            if (g.HTMLAudioElement && g.document) {
                const A = M(function Audio(src) {
                    if (!new.target) throw new TypeError("Failed to construct 'Audio': Please use the 'new' operator, this DOM object constructor cannot be called as a function.");
                    const el = g.document.createElement('audio');
                    try { el.preload = 'auto'; } catch (_e) {}
                    if (src !== undefined) el.src = String(src);
                    return el;
                }, 'Audio');
                A.prototype = g.HTMLAudioElement.prototype;
                defV(g, 'Audio', A);
            }
            globalThis[Symbol.for('__bo_system_colors')] = {"accentcolor": "rgb(0, 117, 255)", "accentcolortext": "rgb(255, 255, 255)", "activetext": "rgb(255, 0, 0)", "activeborder": "rgb(0, 0, 0)", "activecaption": "rgb(255, 255, 255)", "appworkspace": "rgb(255, 255, 255)", "background": "rgb(255, 255, 255)", "buttonhighlight": "rgb(239, 239, 239)", "buttonshadow": "rgb(239, 239, 239)", "buttonborder": "rgb(0, 0, 0)", "buttonface": "rgb(239, 239, 239)", "buttontext": "rgb(0, 0, 0)", "canvas": "rgb(255, 255, 255)", "canvastext": "rgb(0, 0, 0)", "captiontext": "rgb(0, 0, 0)", "field": "rgb(255, 255, 255)", "fieldtext": "rgb(0, 0, 0)", "graytext": "rgb(128, 128, 128)", "highlight": "rgba(128, 188, 254, 0.6)", "highlighttext": "rgb(0, 0, 0)", "inactiveborder": "rgb(0, 0, 0)", "inactivecaption": "rgb(255, 255, 255)", "inactivecaptiontext": "rgb(128, 128, 128)", "infobackground": "rgb(255, 255, 255)", "infotext": "rgb(0, 0, 0)", "linktext": "rgb(0, 0, 238)", "mark": "rgb(255, 255, 0)", "marktext": "rgb(0, 0, 0)", "menu": "rgb(255, 255, 255)", "menutext": "rgb(0, 0, 0)", "scrollbar": "rgb(255, 255, 255)", "threeddarkshadow": "rgb(0, 0, 0)", "threedface": "rgb(239, 239, 239)", "threedhighlight": "rgb(0, 0, 0)", "threedlightshadow": "rgb(0, 0, 0)", "threedshadow": "rgb(0, 0, 0)", "visitedtext": "rgb(85, 26, 139)", "window": "rgb(255, 255, 255)", "windowframe": "rgb(0, 0, 0)", "windowtext": "rgb(0, 0, 0)", "selecteditem": "rgb(179, 215, 255)", "selecteditemtext": "rgb(0, 0, 0)"};
            // 4. Hide engine plumbing from own-key reflection on window.
            const HIDE = new Set(['_browser_oxide', '__pendingNavigation', '__bo_input_events']);
            for (const k of HIDE) {
                const d = Object.getOwnPropertyDescriptor(g, k);
                if (d && d.configurable) { try { Object.defineProperty(g, k, Object.assign({}, d, { enumerable: false })); } catch (_e) {} }
            }
            const isG = (o) => o === g;
            const _gopn = Object.getOwnPropertyNames, _keys = Object.keys, _own = Reflect.ownKeys,
                  _gopd = Object.getOwnPropertyDescriptor, _gopds = Object.getOwnPropertyDescriptors,
                  _hop = Object.prototype.hasOwnProperty, _hasOwn = Object.hasOwn;
            const filt = (a) => a.filter((k) => !HIDE.has(k));
            const P = { getOwnPropertyNames(o) { const r = _gopn(o); return isG(o) ? filt(r) : r; },
                        keys(o) { const r = _keys(o); return isG(o) ? filt(r) : r; },
                        ownKeys(o) { const r = _own(o); return isG(o) ? filt(r) : r; },
                        getOwnPropertyDescriptor(o, k) { return isG(o) && HIDE.has(k) ? undefined : _gopd(o, k); },
                        getOwnPropertyDescriptors(o) { const r = _gopds(o); if (isG(o)) for (const k of HIDE) delete r[k]; return r; },
                        hasOwnProperty(k) { return isG(this) && HIDE.has(k) ? false : _hop.call(this, k); },
                        hasOwn(o, k) { return isG(o) && HIDE.has(k) ? false : _hasOwn(o, k); } };
            defV(Object, 'getOwnPropertyNames', M(P.getOwnPropertyNames, 'getOwnPropertyNames'));
            defV(Object, 'keys', M(P.keys, 'keys'));
            defV(Reflect, 'ownKeys', M(P.ownKeys, 'ownKeys'));
            defV(Object, 'getOwnPropertyDescriptor', M(P.getOwnPropertyDescriptor, 'getOwnPropertyDescriptor'));
            defV(Object, 'getOwnPropertyDescriptors', M(P.getOwnPropertyDescriptors, 'getOwnPropertyDescriptors'));
            defV(Object.prototype, 'hasOwnProperty', M(P.hasOwnProperty, 'hasOwnProperty'));
            if (_hasOwn) defV(Object, 'hasOwn', M(P.hasOwn, 'hasOwn'));
        } catch (_e) { /* best-effort */ }
    }

    for (const name of internals) {
        [globalThis, globalThis.window].forEach(obj => {
            if (!obj || !(name in obj)) return;
            try {
                const success = delete obj[name];
                if (!success) {
                    Object.defineProperty(obj, name, { enumerable: false, configurable: true });
                }
            } catch (e) {
                try {
                    Object.defineProperty(obj, name, { enumerable: false, configurable: true });
                } catch (e2) {}
            }
        });
    }

})(globalThis);
