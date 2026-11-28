//! Python bindings for browser_oxide (PyO3).
//!
//! Idiomatic Python surface over [`browser_oxide_host`]: `Browser`, `Page`,
//! `Profile`. The engine is `!Send` (per-thread V8); `browser_oxide_host` runs
//! it on a dedicated thread, so the `Browser`/`Page` objects are ordinary
//! GIL-friendly Python objects and the GIL is released during navigation.

use std::sync::Arc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use browser_oxide::host::{stealth::presets, EngineHandle, PageSnapshot, StealthProfile};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

fn to_pyerr<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// A browser identity (TLS + headers + navigator + GPU + fingerprint seeds).
#[pyclass(module = "browser_oxide._native", from_py_object)]
#[derive(Clone)]
struct Profile {
    inner: StealthProfile,
}

#[pymethods]
impl Profile {
    /// Chrome 148 on macOS (the default).
    #[staticmethod]
    fn chrome() -> Self {
        Self {
            inner: presets::chrome_148_macos(),
        }
    }
    /// Firefox 135 on macOS (real NSS ClientHello).
    #[staticmethod]
    fn firefox() -> Self {
        Self {
            inner: presets::firefox_135_macos(),
        }
    }
    /// Safari 18 on iPhone 15 Pro.
    #[staticmethod]
    fn iphone() -> Self {
        Self {
            inner: presets::iphone_15_pro_safari_18(),
        }
    }
    /// Chrome 148 on Pixel 9 Pro (Android).
    #[staticmethod]
    fn pixel() -> Self {
        Self {
            inner: presets::pixel_9_pro_chrome_148(),
        }
    }
    /// Load a custom profile from a YAML or JSON file.
    #[staticmethod]
    fn from_file(path: &str) -> PyResult<Self> {
        StealthProfile::load_from_file(path)
            .map(|inner| Self { inner })
            .map_err(to_pyerr)
    }

    fn __repr__(&self) -> String {
        format!("Profile(user_agent={:?})", self.inner.user_agent)
    }
}

/// A stealth headless browser. Spawns the engine thread on construction; the
/// thread is shut down when the object (and any pages from it) are dropped.
#[pyclass(module = "browser_oxide._native")]
struct Browser {
    engine: Arc<EngineHandle>,
    profile: StealthProfile,
}

#[pymethods]
impl Browser {
    #[new]
    #[pyo3(signature = (profile=None))]
    fn new(profile: Option<Profile>) -> Self {
        Browser {
            engine: Arc::new(EngineHandle::spawn()),
            profile: profile
                .map(|p| p.inner)
                .unwrap_or_else(presets::chrome_148_macos),
        }
    }

    /// Navigate to `url` and return a `Page`. Releases the GIL while the engine
    /// thread does the work.
    /// `init_scripts` run before every page's own scripts (like
    /// `Page.addScriptToEvaluateOnNewDocument`), e.g. to install error listeners.
    #[pyo3(signature = (url, max_iterations=5, init_scripts=None))]
    fn navigate(
        &self,
        py: Python<'_>,
        url: &str,
        max_iterations: u8,
        init_scripts: Option<Vec<String>>,
    ) -> PyResult<Page> {
        let engine = Arc::clone(&self.engine);
        let profile = self.profile.clone();
        let url = url.to_string();
        let init_scripts = init_scripts.unwrap_or_default();
        let snap = py
            .detach(move || engine.navigate(&url, profile, max_iterations, init_scripts))
            .map_err(to_pyerr)?;
        Ok(Page {
            engine: Arc::clone(&self.engine),
            snap,
        })
    }

    /// Load the navigation the current page started (a followed link,
    /// `location = …`, a submitted form). Returns the new `Page`, or `None`
    /// when the page has not asked to navigate.
    #[pyo3(signature = (max_iterations=5, init_scripts=None))]
    fn follow(
        &self,
        py: Python<'_>,
        max_iterations: u8,
        init_scripts: Option<Vec<String>>,
    ) -> PyResult<Option<Page>> {
        let engine = Arc::clone(&self.engine);
        let profile = self.profile.clone();
        let init_scripts = init_scripts.unwrap_or_default();
        let snap = py
            .detach(move || engine.follow(profile, max_iterations, init_scripts))
            .map_err(to_pyerr)?;
        Ok(snap.map(|snap| Page { engine: Arc::clone(&self.engine), snap }))
    }

    /// The Cookie header the shared jar sends to `url` (HttpOnly cookies included).
    fn cookies(&self, py: Python<'_>, url: &str) -> PyResult<Option<String>> {
        let engine = Arc::clone(&self.engine);
        let url = url.to_string();
        py.detach(move || engine.cookies(&url)).map_err(to_pyerr)
    }

    /// Save the shared cookie jar to `path` (JSON). Reload it in a later process
    /// with the environment variable BROWSER_OXIDE_COOKIE_JAR=path.
    fn save_cookies(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        let engine = Arc::clone(&self.engine);
        let path = path.to_string();
        py.detach(move || engine.save_cookies(&path)).map_err(to_pyerr)
    }

    /// Evaluate JS against the most recently navigated page.
    fn evaluate(&self, py: Python<'_>, js: &str) -> PyResult<String> {
        let engine = Arc::clone(&self.engine);
        let js = js.to_string();
        py.detach(move || engine.evaluate(&js)).map_err(to_pyerr)
    }

    /// Number of child frames (iframes) the engine materialized for the
    /// current page.
    /// Keep the current page running for up to `ms` (timers, fetches, rAF),
    /// returning earlier if the page goes idle.
    #[pyo3(signature = (ms=1000))]
    fn pump(&self, py: Python<'_>, ms: u64) -> PyResult<String> {
        py.detach(|| self.engine.pump(ms)).map_err(to_pyerr)
    }

    /// Console lines (`[LEVEL] message`) logged by the page since the last call.
    fn console(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        py.detach(|| self.engine.console()).map_err(to_pyerr)
    }

    /// Engine-side response tap (our addition; see kb/engine-changes.md#network-tap).
    ///
    /// Records responses whose final URL contains any of `patterns` (plain substrings): fetch()/XHR with their
    /// request body, and navigation documents. The page can't observe it (no JS wrapping). Bodies are capped at
    /// `max_kb`. An empty list turns it off. Process-wide, and it clears anything queued.
    #[pyo3(signature = (patterns, max_kb=4096))]
    fn tap(&self, patterns: Vec<String>, max_kb: usize) {
        browser_oxide::net::tap::configure(patterns, max_kb.saturating_mul(1024));
    }

    /// Drain the tap: `(records, dropped)`. Each record is a dict with kind ("document"|"fetch"), method, url,
    /// status, content_type, request_body, body, truncated, ts_ms. `dropped` counts records lost to the
    /// 512-entry queue cap since the last call.
    fn take_responses<'py>(&self, py: Python<'py>) -> PyResult<(Vec<Bound<'py, pyo3::types::PyDict>>, u64)> {
        let (recs, dropped) = browser_oxide::net::tap::take();
        let mut out = Vec::with_capacity(recs.len());
        for r in recs {
            let d = pyo3::types::PyDict::new(py);
            d.set_item("kind", r.kind)?;
            d.set_item("method", r.method)?;
            d.set_item("url", r.url)?;
            d.set_item("status", r.status)?;
            d.set_item("content_type", r.content_type)?;
            d.set_item("request_body", r.request_body)?;
            d.set_item("body", r.body)?;
            d.set_item("truncated", r.truncated)?;
            d.set_item("ts_ms", r.ts_ms)?;
            out.push(d);
        }
        Ok((out, dropped))
    }

    /// Uncaught page errors (exceptions in callbacks, unhandled rejections)
    /// recorded since the last call.
    fn page_errors(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        py.detach(|| self.engine.page_errors()).map_err(to_pyerr)
    }
    fn frame_count(&self, py: Python<'_>) -> PyResult<usize> {
        let engine = Arc::clone(&self.engine);
        py.detach(move || engine.frame_count()).map_err(to_pyerr)
    }

    /// Evaluate JS inside a child frame's own realm, by document order.
    ///
    /// The page realm cannot do this itself: a cross-origin frame's
    /// `contentWindow.document` raises `SecurityError`, and a same-origin one
    /// is still a separate V8 context. This is the equivalent of CDP's
    /// `executionContextId` routing, using the child isolate the engine builds
    /// at navigation time.
    fn frame_evaluate(&self, py: Python<'_>, index: usize, js: &str) -> PyResult<String> {
        let engine = Arc::clone(&self.engine);
        let js = js.to_string();
        py.detach(move || engine.frame_evaluate(index, &js))
            .map_err(to_pyerr)
    }

    /// `querySelector(selector).textContent` against the current page.
    fn query_text(&self, py: Python<'_>, selector: &str) -> PyResult<Option<String>> {
        let engine = Arc::clone(&self.engine);
        let selector = selector.to_string();
        py.detach(move || engine.query_text(&selector))
            .map_err(to_pyerr)
    }

    /// Idempotent; the engine thread is also stopped on drop.
    fn close(&self) {}

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        _exc_type: Option<Py<pyo3::PyAny>>,
        _exc_value: Option<Py<pyo3::PyAny>>,
        _traceback: Option<Py<pyo3::PyAny>>,
    ) -> bool {
        false
    }
}

/// A rendered page. Accessors are properties; `evaluate`/`query_text` run
/// against the engine's current page.
#[pyclass(module = "browser_oxide._native")]
struct Page {
    engine: Arc<EngineHandle>,
    snap: PageSnapshot,
}

#[pymethods]
impl Page {
    #[getter]
    fn url(&self) -> &str {
        &self.snap.url
    }
    #[getter]
    fn title(&self) -> &str {
        &self.snap.title
    }
    #[getter]
    fn html(&self) -> &str {
        &self.snap.html
    }
    #[getter]
    fn text(&self) -> &str {
        &self.snap.text
    }
    /// One of: pass / thin-shell / render-incomplete / edge-block / sensor-fail
    /// / challenge-incomplete (compares equal to the Python `Verdict` enum).
    #[getter]
    fn verdict(&self) -> &str {
        &self.snap.verdict
    }
    #[getter]
    fn is_challenge(&self) -> bool {
        self.snap.is_challenge
    }

    fn evaluate(&self, py: Python<'_>, js: &str) -> PyResult<String> {
        let engine = Arc::clone(&self.engine);
        let js = js.to_string();
        py.detach(move || engine.evaluate(&js)).map_err(to_pyerr)
    }

    /// Number of child frames (iframes) the engine materialized for this page.
    /// Keep the page running for up to `ms`, returning earlier if it goes idle.
    #[pyo3(signature = (ms=1000))]
    fn pump(&self, py: Python<'_>, ms: u64) -> PyResult<String> {
        py.detach(|| self.engine.pump(ms)).map_err(to_pyerr)
    }

    /// Console lines (`[LEVEL] message`) logged by the page since the last call.
    fn console(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        py.detach(|| self.engine.console()).map_err(to_pyerr)
    }

    /// Uncaught page errors (exceptions in callbacks, unhandled rejections)
    /// recorded since the last call.
    fn page_errors(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        py.detach(|| self.engine.page_errors()).map_err(to_pyerr)
    }
    fn frame_count(&self, py: Python<'_>) -> PyResult<usize> {
        let engine = Arc::clone(&self.engine);
        py.detach(move || engine.frame_count()).map_err(to_pyerr)
    }

    /// Evaluate JS inside a child frame's own realm, by document order.
    ///
    /// `page.evaluate()` cannot reach frame code: a cross-origin frame's
    /// `contentWindow.document` raises `SecurityError`, and even a same-origin
    /// one is a separate V8 context. This is the per-frame execution context
    /// Chrome exposes via CDP's `executionContextId`.
    fn frame_evaluate(&self, py: Python<'_>, index: usize, js: &str) -> PyResult<String> {
        let engine = Arc::clone(&self.engine);
        let js = js.to_string();
        py.detach(move || engine.frame_evaluate(index, &js))
            .map_err(to_pyerr)
    }

    fn query_text(&self, py: Python<'_>, selector: &str) -> PyResult<Option<String>> {
        let engine = Arc::clone(&self.engine);
        let selector = selector.to_string();
        py.detach(move || engine.query_text(&selector))
            .map_err(to_pyerr)
    }

    fn __repr__(&self) -> String {
        format!(
            "Page(url={:?}, verdict={:?}, bytes={})",
            self.snap.url,
            self.snap.verdict,
            self.snap.html.len()
        )
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Browser>()?;
    m.add_class::<Page>()?;
    m.add_class::<Profile>()?;
    Ok(())
}
