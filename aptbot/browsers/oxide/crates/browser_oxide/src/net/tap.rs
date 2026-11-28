//! Engine-side response tap: an embedder (the scraper) sees selected responses without touching page JS.
//!
//! A real browser exposes this through the DevTools protocol, and the page can't observe it. Before the tap, the
//! research scrapers used an init script that wrapped `XMLHttpRequest`/`fetch`, and page code can detect that. The tap
//! records on the Rust side after a response arrives. It changes nothing the page can see: no globals, timing only
//! by the copy.
//!
//! Off by default (one relaxed atomic load per response). `configure(patterns, max_body)` turns it on. A response
//! is kept when its final URL contains any pattern (plain substring). Bodies over `max_body` bytes are truncated
//! and flagged. Records queue up to `MAX_QUEUED` and the oldest drop first, so a forgotten tap can't grow without
//! bound. `take()` drains the queue.
//!
//! Process-global: every page and frame in the process shares one tap. The aptbot worker runs one `Browser` per
//! process.
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

const MAX_QUEUED: usize = 512;

#[derive(Clone, Debug, serde::Serialize)]
pub struct Captured {
    /// "document" (a navigation's HTML) or "fetch" (fetch() and XHR).
    pub kind: &'static str,
    pub method: String,
    pub url: String,
    pub status: u16,
    pub content_type: String,
    /// The request body for fetch/XHR (graphql params such as `fb_api_req_friendly_name`), lossy UTF-8.
    pub request_body: String,
    pub body: String,
    pub truncated: bool,
    /// Milliseconds since the Unix epoch, when recorded.
    pub ts_ms: u64,
}

struct Tap {
    patterns: Vec<String>,
    max_body: usize,
    queue: VecDeque<Captured>,
    dropped: u64,
}

static ON: AtomicBool = AtomicBool::new(false);
static TAP: Mutex<Option<Tap>> = Mutex::new(None);

/// Turn the tap on with these URL substrings (empty = off) and clear anything queued.
pub fn configure(patterns: Vec<String>, max_body: usize) {
    let patterns: Vec<String> = patterns.into_iter().filter(|p| !p.is_empty()).collect();
    let on = !patterns.is_empty();
    let mut g = TAP.lock().unwrap_or_else(|e| e.into_inner());
    *g = on.then(|| Tap { patterns, max_body: max_body.max(1024), queue: VecDeque::new(), dropped: 0 });
    ON.store(on, Ordering::Release);
}

pub fn enabled() -> bool {
    ON.load(Ordering::Relaxed)
}

fn lossy(b: &[u8], max: usize) -> (String, bool) {
    if b.len() <= max {
        return (String::from_utf8_lossy(b).into_owned(), false);
    }
    let mut end = max;
    while end > 0 && (b[end] & 0xC0) == 0x80 {
        end -= 1; // don't split a UTF-8 sequence
    }
    (String::from_utf8_lossy(&b[..end]).into_owned(), true)
}

/// Record a response if the tap is on and the URL matches. Cheap when off.
pub fn record(kind: &'static str, method: &str, url: &str, status: u16, content_type: &str,
              request_body: Option<&[u8]>, body: &[u8]) {
    if !enabled() {
        return;
    }
    let mut g = TAP.lock().unwrap_or_else(|e| e.into_inner());
    let Some(t) = g.as_mut() else { return };
    if !t.patterns.iter().any(|p| url.contains(p.as_str())) {
        return;
    }
    let (body, truncated) = lossy(body, t.max_body);
    let (request_body, _) = lossy(request_body.unwrap_or_default(), 64 * 1024);
    let ts_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    if t.queue.len() >= MAX_QUEUED {
        t.queue.pop_front();
        t.dropped += 1;
    }
    t.queue.push_back(Captured {
        kind,
        method: method.to_ascii_uppercase(),
        url: url.to_string(),
        status,
        content_type: content_type.to_string(),
        request_body,
        body,
        truncated,
        ts_ms,
    });
}

/// Drain everything recorded so far, oldest first, plus how many records were dropped (queue full) since the last take.
pub fn take() -> (Vec<Captured>, u64) {
    let mut g = TAP.lock().unwrap_or_else(|e| e.into_inner());
    match g.as_mut() {
        Some(t) => (t.queue.drain(..).collect(), std::mem::take(&mut t.dropped)),
        None => (Vec::new(), 0),
    }
}

/// `content-type` from a response header map (header names are lowercase in oxide, but don't rely on it).
pub fn content_type(headers: &std::collections::HashMap<String, String>) -> &str {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-type"))
        .map(|(_, v)| v.as_str())
        .unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_truncates_and_drains() {
        configure(vec!["/api/graphql/".into()], 2048);
        record("fetch", "post", "https://x.test/other", 200, "text/html", None, b"no");
        let big = "\u{05d0}".repeat(2000); // 4000 bytes of 2-byte chars
        record("fetch", "post", "https://x.test/api/graphql/", 200, "application/json", Some(b"a=1"), big.as_bytes());
        let (v, dropped) = take();
        assert_eq!(dropped, 0);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].method, "POST");
        assert_eq!(v[0].request_body, "a=1");
        assert!(v[0].truncated);
        assert!(v[0].body.chars().all(|c| c == '\u{05d0}'), "no split UTF-8");
        assert!(take().0.is_empty());
        for _ in 0..(MAX_QUEUED + 3) {
            record("fetch", "GET", "https://x.test/api/graphql/", 200, "", None, b"{}");
        }
        let (v, dropped) = take();
        assert_eq!((v.len(), dropped), (MAX_QUEUED, 3));
        configure(vec![], 0);
        record("fetch", "GET", "https://x.test/api/graphql/", 200, "", None, b"{}");
        assert!(take().0.is_empty());
    }
}
