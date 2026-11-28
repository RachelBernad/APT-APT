//! Out-of-process-style iframes (fbgb fork).
//!
//! An `<iframe>` whose `src` is an http(s) URL gets a real document of its
//! own: a separate V8 runtime with its own DOM, storage and location, built
//! from the fetched HTML. Frames can nest (a frame may create frames).
//!
//! All frames of a tab live in a thread-local registry. The TOP page's
//! event loop services them from `run_until_idle`: it builds requested
//! frames, runs each one for a short slice, and routes `postMessage` and
//! `load` events between documents. A guard keeps nested event loops (the
//! frames' own, and the legacy build-time children) from servicing.
//!
//! JS side (dom_bootstrap.js): `op_frame_request` when an http(s) iframe is
//! connected, `op_frame_post` for `postMessage`, `op_frame_detach` when it
//! is removed. Delivery calls `hook.deliverMessage` / `hook.frameLoaded` in
//! the receiving document. Frame id 0 is the top document.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use deno_core::op2;

use crate::event_loop::BrowserEventLoop;
use crate::js_runtime::runtime::BrowserRuntimeOptions;
use crate::js_runtime::BrowserJsRuntime;

/// Live frames per tab (a runaway page must not spawn isolates forever).
const MAX_FRAMES: usize = 8;
/// Nesting depth limit (top = 0).
const MAX_DEPTH: u8 = 4;
/// Per-service slice each frame's event loop gets.
const FRAME_SLICE: Duration = Duration::from_millis(15);

struct DynFrame {
    parent: u32,
    depth: u8,
    url: String,
    event_loop: BrowserEventLoop,
}

struct Request {
    id: u32,
    parent: u32,
    url: String,
    referer: String,
}

struct Message {
    target: u32,
    source: u32,
    data_json: String,
    origin: String,
    target_origin: String,
}

#[derive(Default)]
struct Queues {
    next_id: u32,
    requests: VecDeque<Request>,
    messages: VecDeque<Message>,
    /// (parent id, frame id) — the parent fires `load` on the element.
    loaded: VecDeque<(u32, u32)>,
    detach: Vec<u32>,
    /// frame id -> (parent, depth, url) for frames requested or live; used
    /// for depth limits and origin checks before the frame exists.
    meta: BTreeMap<u32, (u32, u8, String)>,
}

thread_local! {
    static QUEUES: RefCell<Queues> = RefCell::new(Queues { next_id: 1, ..Default::default() });
    /// Live frames. Taken out while one of them runs (so a frame's own ops
    /// can touch QUEUES freely).
    static FRAMES: RefCell<BTreeMap<u32, DynFrame>> = const { RefCell::new(BTreeMap::new()) };
    /// Frame whose JS is running right now (0 = top document).
    static CURRENT: Cell<u32> = const { Cell::new(0) };
    /// Set while frames are being serviced or a legacy child is built.
    static SERVICING: Cell<bool> = const { Cell::new(false) };
    static PROFILE: RefCell<Option<crate::stealth::StealthProfile>> = const { RefCell::new(None) };
    static TOP_URL: RefCell<String> = const { RefCell::new(String::new()) };
}

/// RAII: block frame servicing (nested event loops must not service).
pub struct NoService(bool);
impl NoService {
    pub fn new() -> Self {
        NoService(SERVICING.with(|s| s.replace(true)))
    }
}
impl Default for NoService {
    fn default() -> Self {
        Self::new()
    }
}
impl Drop for NoService {
    fn drop(&mut self) {
        let prev = self.0;
        SERVICING.with(|s| s.set(prev));
    }
}

/// Drop a frame's runtime. Frame isolates are not left entered after they
/// are built (see build_frame), and rusty_v8 exits the isolate on drop, so
/// enter it first: the enter/exit pair stays balanced and the thread's
/// entered-isolate stack is not corrupted when frames die out of creation
/// order (which happens whenever an outer iframe is removed).
fn dispose(mut frame: DynFrame) {
    let iso: *mut deno_core::v8::OwnedIsolate = frame.event_loop.runtime_mut().inner().v8_isolate();
    unsafe { (**iso).enter() };
    drop(frame);
}

fn dispose_all() {
    let all: Vec<DynFrame> = FRAMES.with(|f| std::mem::take(&mut *f.borrow_mut()).into_values().collect());
    for fr in all {
        dispose(fr);
    }
}

/// Engine thread is exiting: dispose every frame isolate now (entered, in
/// order) instead of leaving them to thread-local destructors, which run
/// after the top-level isolate is gone and abort the process with
/// "thread local panicked on drop" (closing a second Browser killed all).
pub fn shutdown() {
    dispose_all();
    QUEUES.with(|q| *q.borrow_mut() = Queues { next_id: 1, ..Default::default() });
}

/// A new top-level document: drop every frame of the previous one.
pub fn begin_document(profile: &crate::stealth::StealthProfile, url: &str) {
    if SERVICING.with(|s| s.get()) {
        return;
    }
    dispose_all();
    QUEUES.with(|q| {
        let mut q = q.borrow_mut();
        let next = q.next_id;
        *q = Queues { next_id: next, ..Default::default() };
    });
    PROFILE.with(|p| *p.borrow_mut() = Some(profile.clone()));
    TOP_URL.with(|t| *t.borrow_mut() = url.to_string());
    CURRENT.with(|c| c.set(0));
}

/// Number of live frames (document order of creation).
pub fn count() -> usize {
    FRAMES.with(|f| f.borrow().len())
}

/// Evaluate JS in the `index`-th live frame.
pub fn evaluate(index: usize, js: &str) -> Option<Result<String, deno_core::error::AnyError>> {
    let id = FRAMES.with(|f| f.borrow().keys().nth(index).copied())?;
    let mut frame = FRAMES.with(|f| f.borrow_mut().remove(&id))?;
    let prev = CURRENT.with(|c| c.replace(id));
    let res = frame.event_loop.execute_script(js);
    CURRENT.with(|c| c.set(prev));
    FRAMES.with(|f| f.borrow_mut().insert(id, frame));
    Some(res)
}

fn origin_of(url: &str) -> String {
    url::Url::parse(url)
        .map(|u| u.origin().ascii_serialization())
        .unwrap_or_else(|_| "null".to_string())
}

fn url_of(id: u32) -> String {
    if id == 0 {
        return TOP_URL.with(|t| t.borrow().clone());
    }
    QUEUES.with(|q| q.borrow().meta.get(&id).map(|m| m.2.clone()).unwrap_or_default())
}

/// Is any frame work queued?
pub fn has_work() -> bool {
    QUEUES.with(|q| {
        let q = q.borrow();
        !q.requests.is_empty() || !q.messages.is_empty() || !q.loaded.is_empty() || !q.detach.is_empty()
    })
}

// ---- ops -----------------------------------------------------------------

/// Ask for a real document in an iframe. Returns its frame id (0 = refused).
#[op2(fast)]
pub fn op_frame_request(#[string] url: String, #[string] referer: String) -> u32 {
    let parent = CURRENT.with(|c| c.get());
    QUEUES.with(|q| {
        let mut q = q.borrow_mut();
        let depth = if parent == 0 { 0 } else { q.meta.get(&parent).map(|m| m.1).unwrap_or(0) };
        if depth + 1 > MAX_DEPTH {
            return 0;
        }
        let live = q.meta.len();
        if live >= MAX_FRAMES {
            return 0;
        }
        let id = q.next_id;
        q.next_id += 1;
        q.meta.insert(id, (parent, depth + 1, url.clone()));
        q.requests.push_back(Request { id, parent, url, referer });
        id
    })
}

/// `postMessage(data, targetOrigin)` from the running document to frame `target`.
#[op2(fast)]
pub fn op_frame_post(target: u32, #[string] data_json: String, #[string] target_origin: String) {
    let source = CURRENT.with(|c| c.get());
    let origin = origin_of(&url_of(source));
    QUEUES.with(|q| {
        q.borrow_mut().messages.push_back(Message { target, source, data_json, origin, target_origin })
    });
}

/// The iframe element was removed: drop its document (and its descendants).
#[op2(fast)]
pub fn op_frame_detach(id: u32) {
    QUEUES.with(|q| q.borrow_mut().detach.push(id));
}

/// Id of the frame whose JS is running (0 = top).
#[op2(fast)]
pub fn op_frame_self() -> u32 {
    CURRENT.with(|c| c.get())
}

/// CSP `script-src-elem` decision for a script the page inserted (not
/// parser-inserted, so 'strict-dynamic' trust applies). "" = allowed,
/// otherwise the violated directive.
#[op2]
#[string]
pub fn op_csp_script_check(#[string] url: String, #[string] nonce: String) -> String {
    let Ok(u) = url::Url::parse(&url) else { return String::new() };
    let nonce = if nonce.is_empty() { None } else { Some(nonce.as_str()) };
    match crate::js_runtime::extensions::fetch_ext::check_csp(crate::net::csp::Directive::ScriptSrcElem, &u, nonce, false) {
        Ok(()) => String::new(),
        Err(d) => d.to_string(),
    }
}

deno_core::extension!(
    frames_extension,
    ops = [op_frame_request, op_frame_post, op_frame_detach, op_frame_self, op_csp_script_check],
);

// ---- servicing -------------------------------------------------------------

/// Run in `target`'s document (top or frame) with CURRENT set accordingly.
fn run_in(target: u32, top: &mut BrowserJsRuntime, js: &str) {
    if target == 0 {
        let prev = CURRENT.with(|c| c.replace(0));
        let _ = top.execute_script(js, None);
        CURRENT.with(|c| c.set(prev));
        return;
    }
    let Some(mut frame) = FRAMES.with(|f| f.borrow_mut().remove(&target)) else { return };
    let prev = CURRENT.with(|c| c.replace(target));
    let csp = crate::js_runtime::extensions::fetch_ext::take_csp();
    let _ = frame.event_loop.execute_script(js);
    crate::js_runtime::extensions::fetch_ext::restore_csp(csp);
    CURRENT.with(|c| c.set(prev));
    FRAMES.with(|f| f.borrow_mut().insert(target, frame));
}

fn descendants_of(root: u32) -> Vec<u32> {
    QUEUES.with(|q| {
        let q = q.borrow();
        let mut out = vec![root];
        let mut i = 0;
        while i < out.len() {
            let p = out[i];
            for (id, m) in q.meta.iter() {
                if m.0 == p && !out.contains(id) {
                    out.push(*id);
                }
            }
            i += 1;
        }
        out
    })
}

/// Service all frames once. Returns true if any work was done (the caller
/// keeps looping instead of reporting idle).
pub async fn service(top: &mut BrowserJsRuntime) -> bool {
    if SERVICING.with(|s| s.get()) {
        return false;
    }
    if !has_work() && FRAMES.with(|f| f.borrow().is_empty()) {
        return false;
    }
    let _guard = NoService::new();
    let mut worked = false;

    // 1. detach
    let detach: Vec<u32> = QUEUES.with(|q| std::mem::take(&mut q.borrow_mut().detach));
    for id in detach {
        // innermost first
        for d in descendants_of(id).into_iter().rev() {
            if let Some(fr) = FRAMES.with(|f| f.borrow_mut().remove(&d)) {
                dispose(fr);
            }
            QUEUES.with(|q| {
                let mut q = q.borrow_mut();
                q.meta.remove(&d);
                q.requests.retain(|r| r.id != d);
                q.messages.retain(|m| m.target != d);
            });
        }
        worked = true;
    }

    // 2. build requested frames
    loop {
        let req = QUEUES.with(|q| q.borrow_mut().requests.pop_front());
        let Some(req) = req else { break };
        worked = true;
        let depth = QUEUES.with(|q| q.borrow().meta.get(&req.id).map(|m| m.1).unwrap_or(1));
        if dbg() { eprintln!("[frames] build #{} (parent {}) {}", req.id, req.parent, req.url); }
        let prev = CURRENT.with(|c| c.replace(req.id));
        let csp = crate::js_runtime::extensions::fetch_ext::take_csp();
        let built = build_frame(req.id, req.parent, &req.url, &req.referer).await;
        crate::js_runtime::extensions::fetch_ext::restore_csp(csp);
        CURRENT.with(|c| c.set(prev));
        match built {
            Ok((event_loop, final_url)) => {
                QUEUES.with(|q| {
                    let mut q = q.borrow_mut();
                    if let Some(m) = q.meta.get_mut(&req.id) {
                        m.2 = final_url.clone();
                    }
                    q.loaded.push_back((req.parent, req.id));
                });
                FRAMES.with(|f| {
                    f.borrow_mut().insert(req.id, DynFrame { parent: req.parent, depth, url: final_url, event_loop })
                });
            }
            Err(e) => {
                if std::env::var("BO_FRAME_DEBUG").is_ok() {
                    eprintln!("[frames] {} failed: {}", req.url, e);
                }
                // A failed navigation still fires load on the element (Chrome
                // shows an error page); the frame stays without a document.
                QUEUES.with(|q| q.borrow_mut().loaded.push_back((req.parent, req.id)));
            }
        }
    }

    // 3. run every frame for a slice
    let ids: Vec<u32> = FRAMES.with(|f| f.borrow().keys().copied().collect());
    for id in ids {
        let Some(mut frame) = FRAMES.with(|f| f.borrow_mut().remove(&id)) else { continue };
        if dbg() { eprintln!("[frames] run #{id}"); }
        let prev = CURRENT.with(|c| c.replace(id));
        let csp = crate::js_runtime::extensions::fetch_ext::take_csp();
        if let Ok(crate::event_loop::IdleReason::Timeout) = frame.event_loop.run_until_idle(FRAME_SLICE).await {
            worked = true;
        }
        crate::js_runtime::extensions::fetch_ext::restore_csp(csp);
        CURRENT.with(|c| c.set(prev));
        FRAMES.with(|f| f.borrow_mut().insert(id, frame));
    }

    // 4. deliver load events
    let loaded: Vec<(u32, u32)> = QUEUES.with(|q| q.borrow_mut().loaded.drain(..).collect());
    for (parent, id) in loaded {
        worked = true;
        if dbg() { eprintln!("[frames] load #{id} -> #{parent}"); }
        run_in(
            parent,
            top,
            &format!("(globalThis[Symbol.for('__browser_oxide_script_hook__')] || {{}}).frameLoaded?.({id})"),
        );
    }

    // 5. deliver messages
    let msgs: Vec<Message> = QUEUES.with(|q| q.borrow_mut().messages.drain(..).collect());
    for m in msgs {
        worked = true;
        if dbg() { eprintln!("[frames] msg #{} -> #{} ({} bytes, to {})", m.source, m.target, m.data_json.len(), m.target_origin); }
        // targetOrigin: "*" = any, "/" = same origin as the sender, else exact.
        let target_origin = origin_of(&url_of(m.target));
        let wanted = if m.target_origin == "/" { m.origin.clone() } else { m.target_origin.clone() };
        if wanted != "*" && origin_of(&wanted) != target_origin && wanted != target_origin {
            continue; // dropped, as in browsers
        }
        let js = format!(
            "(globalThis[Symbol.for('__browser_oxide_script_hook__')] || {{}}).deliverMessage?.({}, {}, {})",
            serde_json::to_string(&m.data_json).unwrap_or_else(|_| "\"null\"".into()),
            serde_json::to_string(&m.origin).unwrap_or_else(|_| "\"null\"".into()),
            m.source
        );
        run_in(m.target, top, &js);
    }

    worked
}

/// Fetch `url` as an iframe document and build its runtime.
async fn build_frame(
    id: u32,
    parent: u32,
    url: &str,
    referer: &str,
) -> Result<(BrowserEventLoop, String), deno_core::error::AnyError> {
    let profile = PROFILE
        .with(|p| p.borrow().clone())
        .unwrap_or_else(crate::stealth::presets::chrome_148_macos);
    let client = crate::net::HttpClient::shared(&profile)
        .map_err(|e| deno_core::error::AnyError::msg(e.to_string()))?;

    // An <iframe src> navigation in Chrome 158's wire order: dest iframe,
    // no sec-fetch-user, referer = parent document, PP-filtered client hints.
    let resp = client
        .get_frame(url, referer, 10)
        .await
        .map_err(|e| deno_core::error::AnyError::msg(e.to_string()))?;
    let final_url = resp.url.clone();
    let html = resp.text();

    let dom = crate::html_parser::parse_html(&html);
    let scripts = crate::script_runner::find_scripts(&dom);
    let css_entries = crate::stylesheet_collector::find_stylesheets(&dom);
    let mut stylesheets = Vec::new();
    for entry in &css_entries {
        match entry {
            crate::stylesheet_collector::StylesheetEntry::Inline(css) => stylesheets.push(css.clone()),
            crate::stylesheet_collector::StylesheetEntry::External(href) => {
                if let Some(full) = url::Url::parse(&final_url).ok().and_then(|b| b.join(href).ok()) {
                    if let Ok(r) = client
                        .get_subresource(full.as_str(), &final_url, crate::net::headers::Subresource::Stylesheet, 5)
                        .await
                    {
                        if r.ok() {
                            stylesheets.push(r.text());
                        }
                    }
                }
            }
        }
    }

    let parent_url = url_of(parent);
    let top_url = url_of(0);
    let init = format!(
        "(globalThis[Symbol.for('__browser_oxide_script_hook__')] || {{}}).initFrame?.({id}, {parent}, {}, {})",
        serde_json::to_string(&origin_of(&parent_url)).unwrap_or_default(),
        serde_json::to_string(&origin_of(&top_url)).unwrap_or_default(),
    );
    let runtime = BrowserJsRuntime::with_options(
        dom,
        BrowserRuntimeOptions {
            stealth_profile: Some(profile.clone()),
            stylesheets,
            base_url: url::Url::parse(&final_url).ok(),
            is_secure_context: crate::page::is_secure_url(&final_url),
            ..Default::default()
        },
    );
    let mut runtime = runtime;
    // rusty_v8 enters an isolate at construction and only exits it on drop;
    // a frame must not stay entered (it would sit on the thread's entry
    // stack above the page's isolate). Every later use enters/exits through
    // IsolateEnterGuard; dispose() re-enters before the drop.
    unsafe { runtime.inner().v8_isolate().exit() };
    let mut event_loop = BrowserEventLoop::new(runtime);
    if dbg() { eprintln!("[frames] #{id} runtime up, {} scripts", scripts.len()); }
    crate::js_runtime::extensions::fetch_ext::set_fetch_client(client.clone());
    let url_js = final_url.replace('\\', "\\\\").replace('\'', "\\'");
    let _ = event_loop.execute_script(&format!("location.href = '{url_js}';"));
    event_loop.reset_nav_pending();
    let _ = event_loop.execute_script(&init);
    let _ = event_loop.execute_script("globalThis._browser_oxide.__documentReadyState = 'loading';");

    for script in &scripts {
        let hook_set = format!(
            "globalThis[Symbol.for('__browser_oxide_script_hook__')].set(globalThis[Symbol.for('__browser_oxide_script_hook__')].wrap({}))",
            script.node_id
        );
        if script.is_module {
            if let Some(src) = &script.src {
                if let Some(full) = url::Url::parse(&final_url).ok().and_then(|b| b.join(src).ok()) {
                    let _ = tokio::time::timeout(Duration::from_secs(5), event_loop.eval_module_url(full.as_str())).await;
                }
            }
            continue;
        }
        let (code, name) = if let Some(src) = &script.src {
            if let Some(code) = crate::page::decode_data_url(src) {
                (code, final_url.clone())
            } else {
                let Some(full) = url::Url::parse(&final_url).ok().and_then(|b| b.join(src).ok()) else { continue };
                match client
                    .get_subresource(full.as_str(), &final_url, script.request_kind(), 5)
                    .await
                {
                    Ok(r) if r.ok() => (r.text(), full.to_string()),
                    _ => continue,
                }
            }
        } else {
            (script.code.clone(), final_url.clone())
        };
        if code.trim().is_empty() {
            continue;
        }
        let _ = event_loop.execute_script(&hook_set);
        let _ = event_loop.execute_script_with_name(&code, &name);
        let _ = event_loop.execute_script("globalThis[Symbol.for('__browser_oxide_script_hook__')].set(null)");
        let _ = event_loop.run_until_idle(Duration::from_millis(20)).await;
    }
    let _ = event_loop.execute_script(
        "try { globalThis._browser_oxide.__documentReadyState = 'interactive'; } catch (_e) {}
         document.dispatchEvent(new Event('readystatechange'));
         document.dispatchEvent(new Event('DOMContentLoaded', {bubbles: true}));
         setTimeout(() => { try { globalThis._browser_oxide.__documentReadyState = 'complete'; } catch (_e) {}
             document.dispatchEvent(new Event('readystatechange')); window.dispatchEvent(new Event('load')); }, 0);",
    );
    let _ = event_loop.run_until_idle(Duration::from_millis(300)).await;
    event_loop.reset_nav_pending();
    Ok((event_loop, final_url))
}

fn dbg() -> bool {
    std::env::var("BO_FRAME_DEBUG").is_ok()
}

/// eTLD+1 approximation (last two labels) for Sec-Fetch-Site.
fn registrable(host: &str) -> String {
    let parts: Vec<&str> = host.rsplit('.').take(2).collect();
    parts.into_iter().rev().collect::<Vec<_>>().join(".")
}
