//! Stealth HTTP client with Chrome TLS/HTTP2 fingerprint impersonation.
//!
//! Built directly on tokio TCP + boring2 (BoringSSL) + http2 crate, replacing wreq.
//! Uses quinn+h3 for HTTP/3 (QUIC) with automatic Alt-Svc discovery and fallback.

pub mod alt_svc;
pub mod blink_order;
pub mod blocker;
pub mod compression;
pub mod cookies;
pub mod csp;
pub mod error;
pub mod h1_client;
pub mod h2_client;
pub mod h3_request;
pub mod headers;
// JA4H is patent-pending under FoxIO License 1.1 (non-commercial). The
// computer is test-gated so it never reaches a release binary, fitting the
// "internal testing/evaluation" carve-out. See ja4h.rs and LICENSE-NOTE.md.
#[cfg(test)]
pub(crate) mod ja4h;
pub mod pool;
pub mod proxy;
pub mod quic;
pub mod tap;
pub mod tcp;
pub mod tls;

use crate::stealth::StealthProfile;
use alt_svc::AltSvcCache;
use boring2::ssl::SslConnector;
use bytes::Bytes;
use cookies::CookieJar;
use error::NetError;
use http2::client::SendRequest;
use pool::ConnectionPool;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use url::Url;

#[derive(Debug, Clone)]
pub enum Method {
    Get,
    Post(Vec<u8>),
}

/// HTTP response.
#[derive(Debug, Clone, Default)]
pub struct TimingStats {
    pub dns_start_ms: f64,
    pub dns_end_ms: f64,
    pub connect_start_ms: f64,
    pub connect_end_ms: f64,
    pub tls_start_ms: f64,
    pub tls_end_ms: f64,
    pub request_start_ms: f64,
    pub response_start_ms: f64,
    pub response_end_ms: f64,
}

pub struct Response {
    pub status: u16,
    pub status_text: String,
    pub headers: HashMap<String, String>,
    /// All Set-Cookie header values, preserved separately because HTTP responses
    /// can contain multiple Set-Cookie headers and a HashMap would collapse them.
    pub set_cookies: Vec<String>,
    pub body: Vec<u8>,
    pub url: String,
    /// Whether this response taught the client Accept-CH for the first time.
    /// Drives reloads in the navigation loop (Wildberries parity).
    pub accept_ch_upgrade: bool,
    pub timings: TimingStats,
}

impl Response {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }

    pub fn ok(&self) -> bool {
        self.status >= 200 && self.status < 300
    }
}

/// Stealth HTTP client configured with a browser fingerprint profile.
/// Supports HTTP/1.1, HTTP/2 (via boring2/http2) and HTTP/3 (via quinn/rustls).
/// Pool for QUIC connections, keyed by (host, port).
struct QuicPool {
    inner: Arc<Mutex<HashMap<(String, u16), quinn::Connection>>>,
}

impl Clone for QuicPool {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Default for QuicPool {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl QuicPool {
    async fn get(&self, host: &str, port: u16) -> Option<quinn::Connection> {
        let pool = self.inner.lock().await;
        let key = (host.to_string(), port);
        pool.get(&key).cloned()
    }

    async fn put(&self, host: &str, port: u16, conn: quinn::Connection) {
        let mut pool = self.inner.lock().await;
        pool.insert((host.to_string(), port), conn);
    }
}

/// Process-wide shared browser-session state. One cookie jar, one DNS
/// cache, one Alt-Svc cache, one Accept-CH origin set — shared across
/// every [`HttpClient`] built via [`HttpClient::shared`]. This mimics
/// a real user with one persistent browser profile across all tabs.
///
/// Without shared state, sites that fingerprint on "browsing history"
/// (amazon, yandex, homedepot, leboncoin, quora, adidas, …) flag a
/// fresh cold-jar client as a bot on first visit and serve a stub or
/// challenge page. Measured 2026-05-23 same-IP A/B: 8 sites flip
/// pass↔fail depending on whether the cookie jar carries history
/// from prior navigations.
#[derive(Clone)]
pub struct SharedSession {
    pub cookies: Arc<Mutex<CookieJar>>,
    pub accept_ch: AcceptChStore,
    pub dns: tcp::DnsCache,
    pub alt_svc: AltSvcCache,
}

/// Client hints an origin requested (`Accept-CH`) and the ones its
/// `Permissions-Policy` takes away again.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AcceptChState {
    pub hints: headers::Hints,
    pub pp_disabled: headers::Hints,
}

/// Per-host `Accept-CH` state, shared between the clients of one browsing session.
pub type AcceptChStore = Arc<Mutex<HashMap<String, AcceptChState>>>;

static SHARED_SESSION: std::sync::OnceLock<SharedSession> = std::sync::OnceLock::new();

/// Get (lazily initialize) the process-wide shared session. The cookie
/// jar can be seeded from disk via `BROWSER_OXIDE_COOKIE_JAR=<path>` (same env
/// var the legacy per-client `HttpClient::new` honors). Otherwise the
/// jar starts empty and grows as navigations execute.
pub fn shared_session() -> SharedSession {
    SHARED_SESSION
        .get_or_init(|| {
            let initial_jar = if let Ok(path) = std::env::var("BROWSER_OXIDE_COOKIE_JAR") {
                match CookieJar::load_from_file(&std::path::PathBuf::from(&path)) {
                    Ok(j) => {
                        eprintln!(
                            "[cookies] shared session loaded persisted jar from {}",
                            path
                        );
                        j
                    }
                    Err(e) => {
                        eprintln!(
                            "[cookies] shared session: failed to load {}: {} (starting fresh)",
                            path, e
                        );
                        CookieJar::new()
                    }
                }
            } else {
                CookieJar::new()
            };
            SharedSession {
                cookies: Arc::new(Mutex::new(initial_jar)),
                accept_ch: Arc::new(Mutex::new(HashMap::new())),
                dns: tcp::DnsCache::new(),
                alt_svc: AltSvcCache::new(),
            }
        })
        .clone()
}

/// Synchronously write a `document.cookie`-style raw cookie string into the
/// process-wide shared-session jar.
///
/// The nav loop's reload/refetch reads cookies from a `shared()` client (the
/// global session jar), but a `document.cookie` write from JS lands in the
/// per-thread `FETCH_CLIENT`, which can diverge from the global jar — so the
/// token a challenge deposits via `document.cookie` (e.g. AWS-WAF
/// `aws-waf-token`) never reached the reload. Writing here too guarantees the
/// session jar (and thus the reload GET) sees it. Honors deletions via
/// [`CookieJar::set_cookies`]. No-op under `BROWSER_OXIDE_NO_SHARED_SESSION`.
/// Uses `try_lock` (the jar is never held across an await during synchronous
/// JS execution); returns `false` only under genuine contention.
pub fn set_shared_cookie_sync(url: &Url, raw: &str) -> bool {
    if std::env::var("BROWSER_OXIDE_NO_SHARED_SESSION").is_ok() {
        return false;
    }
    let cookies = shared_session().cookies;
    let wrote = match cookies.try_lock() {
        Ok(mut jar) => {
            jar.set_cookies(url, &[raw.to_string()]);
            true
        }
        Err(_) => false,
    };
    wrote
}

#[derive(Clone)]
pub struct HttpClient {
    tls_connector: Arc<SslConnector>,
    profile: StealthProfile,
    cookies: Arc<Mutex<CookieJar>>,
    pool: ConnectionPool,
    quic_pool: QuicPool,
    dns_cache: tcp::DnsCache,
    quic_client: Option<quic::QuicClient>,
    alt_svc_cache: AltSvcCache,
    /// Per-host client-hint state learned from `Accept-CH` /
    /// `Permissions-Policy` response headers. Chrome sends exactly the hints an
    /// origin asked for (minus the ones its policy disables) on every
    /// same-origin request, and only `sec-ch-ua*` low-entropy hints otherwise.
    accept_ch_origins: AcceptChStore,
    /// Hosts whose TLS ALPN negotiated `http/1.1` instead of `h2`. The h2
    /// connect attempt to such a host always fails the ALPN check and we then
    /// open a *second* TLS connection for the h1 fallback — so remember h1-only
    /// hosts (some CDNs / challenge endpoints serve h1 only) and skip the doomed
    /// h2 attempt on subsequent requests, halving the per-request handshake cost
    /// when a page makes many requests to such a host.
    h1_only_hosts: Arc<Mutex<HashSet<String>>>,
    /// Resolved proxy config. `BROWSER_OXIDE_PROXY` env var overrides
    /// `profile.proxy`. None = direct connect (the existing path). T1C.
    proxy: Option<proxy::ProxyConfig>,
}

impl HttpClient {
    /// Borrow this client's stealth profile (read-only). Useful for
    /// callers that need to spawn auxiliary clients with the same UA /
    /// locale / TLS profile (e.g., the sync-fetch op which builds a
    /// fresh client to avoid a connection-pool deadlock with the main
    /// runtime).
    pub fn profile(&self) -> &StealthProfile {
        &self.profile
    }

    pub fn cookies(&self) -> Arc<Mutex<CookieJar>> {
        self.cookies.clone()
    }

    pub fn accept_ch_origins(&self) -> AcceptChStore {
        self.accept_ch_origins.clone()
    }

    pub fn dns_cache(&self) -> tcp::DnsCache {
        self.dns_cache.clone()
    }

    pub fn alt_svc_cache(&self) -> AltSvcCache {
        self.alt_svc_cache.clone()
    }

    /// Connect TCP and apply profile-specific TCP fingerprinting (TTL).
    pub(crate) async fn connect_tcp(
        &self,
        host: &str,
        port: u16,
    ) -> Result<tokio::net::TcpStream, NetError> {
        let tcp_stream = tcp::connect_via_proxy(
            host,
            port,
            std::time::Duration::from_secs(10),
            Some(&self.dns_cache),
            self.proxy.as_ref(),
        )
        .await?;

        // Set TCP TTL to match claimed OS (Linux=64, Windows=128, macOS=64)
        // Advanced anti-bot systems check TCP SYN TTL vs User-Agent. Gap #8.
        let ttl = match self.profile.os_name.as_str() {
            "Windows" => 128,
            _ => 64,
        };
        let _ = tcp_stream.set_ttl(ttl);

        Ok(tcp_stream)
    }

    /// Create a new client with the given stealth profile.
    pub fn new(profile: &StealthProfile) -> Result<Self, NetError> {
        let connector = tls::chrome_connector(profile)?;

        // Create QUIC client for HTTP/3 (non-fatal if it fails)
        let quic_client = quic::QuicClient::new().ok();

        // Optionally load a persisted cookie jar so per-origin trust
        // accumulates across runs. Set BROWSER_OXIDE_COOKIE_JAR to the desired
        // file path. Without this env var, behavior is the same as before
        // (fresh in-memory jar each run).
        let initial_jar = if let Ok(path) = std::env::var("BROWSER_OXIDE_COOKIE_JAR") {
            let p = std::path::PathBuf::from(&path);
            match CookieJar::load_from_file(&p) {
                Ok(jar) => {
                    eprintln!("[cookies] loaded persisted jar from {}", path);
                    jar
                }
                Err(e) => {
                    eprintln!("[cookies] failed to load {}: {} (starting fresh)", path, e);
                    CookieJar::new()
                }
            }
        } else {
            CookieJar::new()
        };

        Ok(Self {
            tls_connector: Arc::new(connector),
            profile: profile.clone(),
            cookies: Arc::new(Mutex::new(initial_jar)),
            pool: ConnectionPool::new(),
            quic_pool: QuicPool::default(),
            dns_cache: tcp::DnsCache::new(),
            quic_client,
            alt_svc_cache: AltSvcCache::new(),
            accept_ch_origins: Arc::new(Mutex::new(HashMap::new())),
            h1_only_hosts: Arc::new(Mutex::new(HashSet::new())),
            // Resolve proxy: BROWSER_OXIDE_PROXY env override, then profile.proxy.
            // Bad proxy URLs are non-fatal — log and continue without proxy.
            proxy: match proxy::ProxyConfig::resolve(profile.proxy.as_deref()) {
                Ok(p) => {
                    if let Some(ref pc) = p {
                        eprintln!(
                            "[proxy] active: scheme={}",
                            match pc {
                                proxy::ProxyConfig::Http { tls: true, .. } => "https",
                                proxy::ProxyConfig::Http { tls: false, .. } => "http",
                                proxy::ProxyConfig::Socks5 { .. } => "socks5",
                            }
                        );
                    }
                    p
                }
                Err(e) => {
                    eprintln!("[proxy] WARN: failed to parse proxy URL: {e} (running direct)");
                    None
                }
            },
        })
    }

    /// Build a client that participates in the process-wide
    /// [`SharedSession`] — cookies, DNS cache, Accept-CH origins, and
    /// Alt-Svc cache all persist across navigations.
    ///
    /// This is the production / benchmarking model: one user, one
    /// cookie jar, accumulating browsing history across page loads.
    /// Real browsers do this; without it, sites like amazon / yandex /
    /// homedepot / leboncoin / quora / adidas flag a fresh no-cookie
    /// client as a bot and serve a stub or a challenge page.
    ///
    /// The shared session is lazily initialized on first call. A
    /// `BROWSER_OXIDE_COOKIE_JAR=<path>` env var seeds the cookie jar from a
    /// previously-persisted file (preserved from the legacy `new()`
    /// path).
    pub fn shared(profile: &StealthProfile) -> Result<Self, NetError> {
        // A/B toggles for the SharedSession-bleed hypothesis on x-com
        // (THIN-BODY 69 mid-sweep, L3-RENDERED 274KB in isolation):
        //   BROWSER_OXIDE_NO_SHARED_SESSION=1     — fully isolated client
        //   BROWSER_OXIDE_NO_SHARED_COOKIES=1     — isolated cookies, shared accept_ch
        //   BROWSER_OXIDE_NO_SHARED_ACCEPT_CH=1   — shared cookies, isolated accept_ch
        if std::env::var("BROWSER_OXIDE_NO_SHARED_SESSION").is_ok() {
            return Self::new(profile);
        }
        let s = shared_session();
        let cookies = if std::env::var("BROWSER_OXIDE_NO_SHARED_COOKIES").is_ok() {
            Arc::new(Mutex::new(CookieJar::new()))
        } else {
            s.cookies
        };
        let accept_ch = if std::env::var("BROWSER_OXIDE_NO_SHARED_ACCEPT_CH").is_ok() {
            Arc::new(Mutex::new(HashMap::new()))
        } else {
            s.accept_ch
        };
        // Share the cookie jar and the Accept-CH origin set, but keep
        // DNS cache and Alt-Svc cache per-client. Real browsers share
        // cookies and Client-Hints opt-ins across tabs but DNS / Alt-Svc
        // are connection-level state that can leak per-IP routing across
        // unrelated origins (a challenge vendor flagged leboncoin when a shared
        // Alt-Svc cache routed us to a previously-throttled CDN node).
        Self::new_with_shared_state(
            profile,
            cookies,
            accept_ch,
            tcp::DnsCache::new(),
            AltSvcCache::new(),
        )
    }

    /// Create a new client that shares session state (cookies, DNS/H3
    /// caches, Accept-CH origins) with an existing one, but has its own
    /// connection pool to avoid deadlocks in synchronous contexts.
    pub fn new_with_shared_state(
        profile: &StealthProfile,
        cookies: Arc<Mutex<CookieJar>>,
        accept_ch: AcceptChStore,
        dns: tcp::DnsCache,
        alt_svc: AltSvcCache,
    ) -> Result<Self, NetError> {
        let connector = tls::chrome_connector(profile)?;
        let quic_client = quic::QuicClient::new().ok();

        Ok(Self {
            tls_connector: Arc::new(connector),
            profile: profile.clone(),
            cookies,
            pool: ConnectionPool::new(),
            quic_pool: QuicPool::default(),
            dns_cache: dns,
            quic_client,
            alt_svc_cache: alt_svc,
            accept_ch_origins: accept_ch,
            h1_only_hosts: Arc::new(Mutex::new(HashSet::new())),
            proxy: proxy::ProxyConfig::resolve(profile.proxy.as_deref()).unwrap_or_default(),
        })
    }

    /// Record which client hints `host` requested through `Accept-CH`, and
    /// which of them its `Permissions-Policy` disables for the documents it
    /// serves. Returns `true` if the stored state changed.
    async fn learn_accept_ch(&self, host: &str, headers: &HashMap<String, String>) -> bool {
        let header = |name: &str| -> Option<&String> {
            let key = headers.keys().find(|k| k.eq_ignore_ascii_case(name))?;
            headers.get(key)
        };
        let requested = header("accept-ch").map(|v| headers::Hints::parse_accept_ch(v));
        let disabled = header("permissions-policy").map(|v| headers::Hints::parse_permissions_policy(v));
        if requested.is_none() && disabled.is_none() {
            return false;
        }
        let mut store = self.accept_ch_origins.lock().await;
        let entry = store.entry(host.to_string()).or_default();
        let before = entry.clone();
        // A fresh `Accept-CH` replaces the origin's previous list.
        if let Some(h) = requested {
            entry.hints = h.union(headers::Hints::LOW_ENTROPY);
        }
        if let Some(d) = disabled {
            entry.pp_disabled = d;
        }
        let changed = before != *entry;
        if entry.hints.is_empty() && entry.pp_disabled.is_empty() {
            store.remove(host);
        }
        changed
    }

    /// Return `true` if `host` has previously sent `Accept-CH`.
    pub async fn has_accept_ch(&self, host: &str) -> bool {
        self.accept_ch_origins.lock().await.contains_key(host)
    }

    /// Client hints Chrome would put on a request for `target`.
    ///
    /// `initiator` is the URL of the document that started the request (`None`
    /// for address-bar style navigations). Top-level navigations carry every
    /// hint the origin asked for, including the frame hints, whether or not
    /// its policy disables them. Anything else that stays same-origin carries
    /// the requested hints minus the disabled ones, and cross-origin requests
    /// (no delegation) only the low-entropy `sec-ch-ua*` set.
    pub async fn hints_for(&self, target: &Url, initiator: Option<&Url>, top_level: bool) -> headers::Hints {
        let store = self.accept_ch_origins.lock().await;
        let low = headers::Hints(headers::Hints::LOW_ENTROPY);
        let Some(state) = store.get(target.host_str().unwrap_or("")) else {
            return low;
        };
        let requested = state.hints.union(headers::Hints::LOW_ENTROPY);
        if initiator.is_none() || top_level {
            return requested;
        }
        if !same_document_origin(initiator.unwrap(), target) {
            return low;
        }
        requested.without(state.pp_disabled.0).without(headers::Hints::FRAME)
    }

    /// Headers for a same-page subresource load started by the engine itself
    /// (script, stylesheet, image): Chrome 158 wire order, no cookies yet.
    pub async fn subresource_headers(&self, url: &str, initiator: &str, kind: headers::Subresource) -> Vec<(String, String)> {
        let target = Url::parse(url).unwrap_or_else(|_| Url::parse("about:blank").expect("about:blank parses"));
        let init = Url::parse(initiator).ok();
        let hints = self.hints_for(&target, init.as_ref(), false).await;
        let opts = headers::FetchOpts {
            referer: init.as_ref().map(|i| referrer_for(i, &target)),
            sec_fetch_site: fetch_site(init.as_ref(), &target),
            storage_access: storage_access(init.as_ref(), &target),
            // CORS-mode module scripts carry `Origin` (Chrome puts it first).
            origin: (kind == headers::Subresource::ModuleScript)
                .then(|| init.as_ref().map(|i| i.origin().ascii_serialization()))
                .flatten(),
            ..kind.fetch_opts()
        };
        headers::fetch_headers_chrome158(&self.profile, url, hints, &[], &opts)
    }

    /// Load a subresource of the document at `initiator`.
    pub async fn get_subresource(
        &self,
        url: &str,
        initiator: &str,
        kind: headers::Subresource,
        max_redirects: u8,
    ) -> Result<Response, NetError> {
        let hdrs = self.subresource_headers(url, initiator, kind).await;
        let r = self.get_follow_exact_headers(url, &hdrs, max_redirects).await;
        net_log("GET", url, None, &r);
        r
    }

    /// Navigate a frame (iframe navigation headers: `sec-fetch-dest: iframe`,
    /// hints filtered by the origin's policy, `Referer` of the parent document).
    pub async fn get_frame(&self, url: &str, initiator: &str, max_redirects: u8) -> Result<Response, NetError> {
        let hdrs = self.frame_headers(url, initiator).await;
        let r = self.get_follow_exact_headers(url, &hdrs, max_redirects).await;
        net_log("GET", url, None, &r);
        r
    }

    /// Iframe navigation headers for a frame `url` inside the document `initiator`.
    pub async fn frame_headers(&self, url: &str, initiator: &str) -> Vec<(String, String)> {
        let target = Url::parse(url).unwrap_or_else(|_| Url::parse("about:blank").expect("about:blank parses"));
        let init = Url::parse(initiator).ok();
        let hints = self.hints_for(&target, init.as_ref(), false).await;
        let mut hdrs = headers::nav_headers_chrome158(
            &self.profile,
            url,
            hints,
            &headers::NavOpts {
                referer: init.as_ref().map(|i| referrer_for(i, &target)),
                sec_fetch_site: Some(fetch_site(init.as_ref(), &target)),
                dest: "iframe",
                top_level: false,
                ..headers::NavOpts::default()
            },
        );
        if let Some(idx) = hdrs.iter().position(|(k, _)| k == "sec-fetch-dest") {
            let sa = storage_access(init.as_ref(), &target);
            if let Some(sa) = sa {
                hdrs.insert(idx + 1, ("sec-fetch-storage-access".to_string(), sa.to_string()));
            }
        }
        hdrs
    }

    /// Build the headers of a `fetch()` / XHR request started by a script.
    async fn fetch_request_headers(
        &self,
        url: &str,
        author: &[(String, String)],
        initiator: Option<&str>,
        method: &str,
        beacon: bool,
    ) -> Vec<(String, String)> {
        let target = Url::parse(url).unwrap_or_else(|_| Url::parse("about:blank").expect("about:blank parses"));
        let init = initiator.and_then(|i| Url::parse(i).ok());
        let hints = self.hints_for(&target, init.as_ref(), false).await;
        // Scripts cannot set these; Chrome silently drops them.
        let author: Vec<(String, String)> = author
            .iter()
            .filter(|(k, _)| !is_forbidden_request_header(k))
            .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
            .collect();
        // Chrome adds `Origin` to a CORS-mode request unless it is a same-origin
        // GET/HEAD (Fetch spec "append a request Origin header").
        let site = fetch_site(init.as_ref(), &target);
        let simple = method.eq_ignore_ascii_case("GET") || method.eq_ignore_ascii_case("HEAD");
        let origin = init
            .as_ref()
            .filter(|_| !(simple && site == "same-origin"))
            .map(|i| i.origin().ascii_serialization());
        let mut opts = headers::FetchOpts {
            origin,
            referer: init.as_ref().map(|i| referrer_for(i, &target)),
            sec_fetch_site: fetch_site(init.as_ref(), &target),
            storage_access: storage_access(init.as_ref(), &target),
            ..headers::FetchOpts::default()
        };
        if beacon {
            // navigator.sendBeacon: keepalive, lowest priority; no-cors when the
            // body's content type is CORS-safelisted (`Origin` stays: it is a POST).
            opts.priority = Some("u=4, i");
            let safelisted = author
                .iter()
                .find(|(k, _)| k == "content-type")
                .map(|(_, v)| {
                    let mime = v.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
                    matches!(mime.as_str(), "text/plain" | "application/x-www-form-urlencoded" | "multipart/form-data")
                })
                .unwrap_or(true);
            if safelisted {
                opts.sec_fetch_mode = "no-cors";
            }
        }
        headers::fetch_headers_chrome158(&self.profile, url, hints, &author, &opts)
    }

    /// Add the jar's cookies for `url` as one `cookie` header per cookie, at
    /// Chrome's position (after `accept-language`, before `priority`).
    async fn add_cookies(&self, hdrs: &mut Vec<(String, String)>, url: &Url) {
        if has_header(hdrs, "cookie") {
            return;
        }
        let jar = self.cookies.lock().await;
        for (name, value) in jar.cookie_list(url) {
            insert_before_priority(hdrs, name, value);
        }
    }

    /// Per W3 Client Hints Reliability spec
    /// (https://wicg.github.io/client-hints-infrastructure/#critical-ch),
    /// when a server sends `Critical-CH`, the browser MUST retry the
    /// request with the listed hints BEFORE rendering the response.
    /// Without this retry, the server treats the client as non-conformant
    /// (one challenge vendor serves the captcha; another
    /// returns 403; a third downgrades the response).
    fn needs_critical_ch_retry(headers: &HashMap<String, String>) -> bool {
        headers
            .keys()
            .any(|k| k.eq_ignore_ascii_case("critical-ch"))
    }

    /// Fetch the challenge vendor's `/mfc` endpoint for `host` if we have a
    /// session with a known tenant prefix and don't yet have an fc token.
    /// On success, stores `x-kpsdk-fc` from the response in the session.
    /// Previously raced the page's ips.js by fetching /mfc from Rust with a
    /// hardcoded `x-kpsdk-dt` literal. That token is per-session and derived
    /// inside the ips.js VM; hardcoding it caused the vendor to refuse to issue
    /// `x-kpsdk-fc` (and exposed an obvious bot signature: every session
    /// presented the same dt value).
    ///
    /// The page's own ips.js fetches /mfc with the correct session-derived
    /// headers via window.fetch(), and our `learn_from_headers` already
    /// extracts `x-kpsdk-fc` from any response that carries it. So the right
    /// behaviour is to do nothing here — let the page run.
    /// Try HTTP/3 for an HTTPS URL. Returns Ok if successful, Err to fall back.
    async fn try_h3_request(
        &self,
        url: &str,
        method: Method,
        extra_headers: &[(String, String)],
    ) -> Result<Response, NetError> {
        // Belt-and-suspenders: even if something populates the cache, never
        // emit a QUIC handshake when allow_http3=false. See learn_alt_svc()
        // for the full rationale.
        if !self.profile.allow_http3 {
            return Err(NetError::Quic("h3 disabled by profile".into()));
        }
        let parsed = Url::parse(url).map_err(|e| NetError::Quic(e.to_string()))?;
        if parsed.scheme() != "https" {
            return Err(NetError::Quic("not HTTPS".into()));
        }

        let host = parsed
            .host_str()
            .ok_or_else(|| NetError::Quic("no host".into()))?;
        let cached_port = self.alt_svc_cache.lookup(host).await;
        let port = cached_port.ok_or_else(|| NetError::Quic("not in alt-svc cache".into()))?;

        let quic = self
            .quic_client
            .as_ref()
            .ok_or_else(|| NetError::Quic("no quic client".into()))?;

        // Try pooled connection first, then create new
        let conn = if let Some(conn) = self.quic_pool.get(host, port).await {
            conn
        } else {
            let conn =
                tokio::time::timeout(std::time::Duration::from_secs(3), quic.connect(host, port))
                    .await
                    .map_err(|_| NetError::Quic("connect timeout".into()))?
                    .map_err(|e| NetError::Quic(e.to_string()))?;
            self.quic_pool.put(host, port, conn.clone()).await;
            conn
        };

        let (resp, alt_svc) =
            h3_request::h3_request(conn, &parsed, method, &self.profile, extra_headers).await?;

        // Update cache from response
        if let Some(alt_svc_header) = &alt_svc {
            if let Some((port, max_age)) = alt_svc::parse_alt_svc(alt_svc_header) {
                self.alt_svc_cache.insert(host, port, max_age).await;
            }
        }

        Ok(resp)
    }

    /// Learn h3 support from a response's Alt-Svc header.
    ///
    /// When `profile.allow_http3 = false` (the default)
    /// we DO NOT cache the h3 alternative. Reason: vanilla `quinn-proto 0.11`
    /// emits transport_parameters in a *random* order with a *random* GREASE
    /// TP per handshake. Real Chrome uses a deterministic fixed order — so
    /// upgrading to QUIC with our current stack would emit a uniquely-
    /// distinguishable browser_oxide signature. Until we vendor-fork
    /// quinn-proto with a Chrome-fixed-order patch, advertising h3 is worse
    /// than not speaking it at all.
    async fn learn_alt_svc(&self, url: &str, resp_headers: &HashMap<String, String>) {
        if !self.profile.allow_http3 {
            return;
        }
        if let Some(alt_svc_header) = resp_headers.get("alt-svc") {
            if let Some((port, max_age)) = alt_svc::parse_alt_svc(alt_svc_header) {
                if let Ok(parsed) = Url::parse(url) {
                    if let Some(host) = parsed.host_str() {
                        self.alt_svc_cache.insert(host, port, max_age).await;
                    }
                }
            }
        }
    }

    /// Inject cookies from external sources (e.g., JS document.cookie).
    pub async fn inject_cookies(&self, url: &Url, cookies: &[String]) {
        let mut jar = self.cookies.lock().await;
        jar.set_cookies(url, cookies);
    }

    /// Connect TCP+TLS and perform HTTP/2 handshake, returning a sender.
    /// Also spawns the connection driver task.
    async fn connect_h2(&self, host: &str, port: u16) -> Result<SendRequest<Bytes>, NetError> {
        let tcp_stream = self.connect_tcp(host, port).await?;

        let tls_stream =
            tls::connect_tls(&self.tls_connector, &self.profile, host, tcp_stream).await?;

        // Check ALPN
        let alpn = tls::negotiated_alpn(&tls_stream);
        if alpn != Some(b"h2") {
            // HTTP/1.1 fallback — the caller's request method opens its own h1
            // connection on this error. Remember the host so future requests
            // skip the doomed h2 attempt (and its wasted TLS handshake).
            self.h1_only_hosts.lock().await.insert(host.to_string());
            return Err(NetError::Http("ALPN negotiated http/1.1, not h2".into()));
        }

        let (sender, conn) = h2_client::handshake(tls_stream, &self.profile).await?;

        // Spawn the connection driver
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("HTTP/2 connection error: {e}");
            }
        });

        // Store in pool for reuse
        self.pool.put(host, port, sender.clone()).await;

        Ok(sender)
    }

    /// Pre-establish a TCP+TLS+HTTP/2 connection to a host.
    /// The connection is stored in the pool for future requests.
    pub async fn preconnect(&self, host: &str, port: u16) -> Result<(), NetError> {
        if self.pool.get(host, port).await.is_some() {
            return Ok(());
        }
        self.connect_h2(host, port).await?;
        Ok(())
    }

    /// Get or create an HTTP/2 sender for the given host.
    async fn get_sender(&self, host: &str, port: u16) -> Result<SendRequest<Bytes>, NetError> {
        // Check pool first
        if let Some(sender) = self.pool.get(host, port).await {
            self.pool.touch(host, port).await;
            return Ok(sender);
        }
        // Known h1-only host: skip the doomed h2 connect (which would do a full
        // TLS handshake only to fail the ALPN check). Fail fast so the caller
        // goes straight to its h1 fallback path.
        if self.h1_only_hosts.lock().await.contains(host) {
            return Err(NetError::Http("host is http/1.1 only (cached)".into()));
        }
        // Create new connection
        self.connect_h2(host, port).await
    }

    /// Perform a GET request. Tries HTTP/3 if available, falls back to HTTP/2.
    pub async fn get(&self, url: &str) -> Result<Response, NetError> {
        self.get_with_headers(url, &[]).await
    }

    /// Fetch/XHR-style GET in Chrome 158 wire order: the header map (author
    /// headers, the client hints this origin asked for, `user-agent`) is
    /// emitted in Blink `HTTPHeaderMap` iteration order, followed by the fixed
    /// fetch suffix. `initiator` is the URL of the document making the request
    /// (`None` = no-origin context).
    pub async fn fetch_get(
        &self,
        url: &str,
        author_headers: &[(String, String)],
        initiator: Option<&str>,
    ) -> Result<Response, NetError> {
        let hdrs = self.fetch_request_headers(url, author_headers, initiator, "GET", false).await;
        self.get_with_exact_headers(url, &hdrs).await
    }

    /// Fetch/XHR-style POST with raw bytes (same header model as `fetch_get`).
    pub async fn fetch_post_bytes(
        &self,
        url: &str,
        body: &[u8],
        author_headers: &[(String, String)],
        initiator: Option<&str>,
    ) -> Result<Response, NetError> {
        let hdrs = self.fetch_request_headers(url, author_headers, initiator, "POST", false).await;
        self.post_bytes_with_exact_headers(url, body, &hdrs).await
    }

    /// `navigator.sendBeacon` POST (Chrome's beacon request shape).
    pub async fn fetch_beacon(
        &self,
        url: &str,
        body: &[u8],
        author_headers: &[(String, String)],
        initiator: Option<&str>,
    ) -> Result<Response, NetError> {
        let hdrs = self.fetch_request_headers(url, author_headers, initiator, "POST", true).await;
        self.post_bytes_with_exact_headers(url, body, &hdrs).await
    }

    /// Header list for a top-level navigation (GET or form POST) in Chrome 158
    /// wire order. `extra_headers` may carry a `referer`, which marks a
    /// script-initiated navigation: it is placed where Chrome puts it and
    /// dropped from `rest`; without one the request looks browser-initiated.
    async fn navigation_headers(
        &self,
        url: &str,
        extra_headers: &[(String, String)],
        rest: &mut Vec<(String, String)>,
    ) -> Vec<(String, String)> {
        *rest = extra_headers.to_vec();
        let referer = rest
            .iter()
            .position(|(k, _)| k.eq_ignore_ascii_case("referer"))
            .map(|i| rest.remove(i).1)
            .filter(|v| v != "\u{1}remove");
        let target = Url::parse(url).ok();
        let initiator = referer.as_deref().and_then(|r| Url::parse(r).ok());
        let hints = match &target {
            Some(t) => self.hints_for(t, initiator.as_ref(), true).await,
            None => headers::Hints(headers::Hints::LOW_ENTROPY),
        };
        let opts = headers::NavOpts {
            referer,
            browser_initiated: initiator.is_none(),
            sec_fetch_site: match (&initiator, &target) {
                (Some(i), Some(t)) => Some(fetch_site(Some(i), t)),
                _ => None,
            },
            ..Default::default()
        };
        headers::nav_headers_chrome158(&self.profile, url, hints, &opts)
    }

    /// GET with the caller's exact header set — NO chrome_headers overlay.
    /// Used for "reload" flavors where sec-fetch-user must be omitted
    /// (chrome_headers always adds it). The caller is responsible for
    /// providing user-agent, accept, etc. Cookies are still auto-injected
    /// from the jar unless the caller already included a Cookie header.
    async fn get_with_exact_headers_inner(
        &self,
        url: &str,
        headers: &[(String, String)],
    ) -> Result<Response, NetError> {
        let parsed = Url::parse(url)?;
        let host = parsed
            .host_str()
            .ok_or_else(|| NetError::Http(format!("no host in URL: {url}")))?;
        let port = parsed.port().unwrap_or(443);

        let mut hdrs: Vec<(String, String)> = headers
            .iter()
            .filter(|(k, _)| {
                let lower = k.to_ascii_lowercase();
                !lower.starts_with(':') && lower != "host" && lower != "connection"
            })
            .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
            .collect();

        self.add_cookies(&mut hdrs, &parsed).await;

        let response = 'h2: {
            for attempt in 0..2 {
                let sender_res = self.get_sender(host, port).await;
                let mut sender = match sender_res {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("[net] H2 connection failed for {}: {}", host, e);
                        break 'h2 None;
                    }
                };
                let uri = parsed.as_str();
                match h2_client::send_get(&mut sender, uri, host, &hdrs).await {
                    Ok((parts, body)) => {
                        let resp = self.build_response(parts, body, url).await?;
                        break 'h2 Some(resp);
                    }
                    Err(e) if attempt == 0 && is_stale_conn_error(&e) => {
                        self.pool.evict(host, port).await;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
            None
        };
        let response = match response {
            Some(r) => r,
            None => {
                let tcp_stream = tcp::connect_via_proxy(
                    host,
                    port,
                    std::time::Duration::from_secs(10),
                    Some(&self.dns_cache),
                    self.proxy.as_ref(),
                )
                .await?;
                let mut tls_stream =
                    tls::connect_tls(&self.tls_connector, &self.profile, host, tcp_stream).await?;
                let path = if parsed.query().is_some() {
                    format!("{}?{}", parsed.path(), parsed.query().unwrap())
                } else {
                    parsed.path().to_string()
                };
                if url.contains("/mfc")
                    || url.contains("/akam/13")
                    || url.contains("/tl")
                    || url.contains("/r")
                {
                    eprintln!(
                        "[net] sending H1 request to {} with headers: {:?}",
                        url, hdrs
                    );
                }
                let raw = h1_client::send_get(&mut tls_stream, host, &path, &hdrs).await?;
                self.build_response_from_raw(raw, url).await?
            }
        };
        self.learn_alt_svc(url, &response.headers).await;
        // Chrome keeps Accept-CH only from top-level document responses
        // (not subresources, fetch()/XHR or iframes).
        let upgrade = if is_top_level_document(&hdrs) {
            self.learn_accept_ch(host, &response.headers).await
        } else {
            false
        };
        self.store_set_cookies(&parsed, &response.set_cookies).await;

        let mut final_response = response;
        final_response.accept_ch_upgrade = upgrade;
        Ok(final_response)
    }

    /// GET follow for exact-header requests.
    pub async fn get_follow_exact_headers(
        &self,
        url: &str,
        headers: &[(String, String)],
        max_redirects: u8,
    ) -> Result<Response, NetError> {
        let mut current_url = url.to_string();
        for _ in 0..max_redirects {
            let resp = self.get_with_exact_headers(&current_url, headers).await?;
            if matches!(resp.status, 301 | 302 | 303 | 307 | 308) {
                if let Some(loc) = resp.headers.get("location") {
                    current_url = resolve_redirect(&current_url, loc)?;
                    continue;
                }
            }
            return Ok(resp);
        }
        self.get_with_exact_headers(&current_url, headers).await
    }

    /// GET with caller-provided extra headers (e.g., from JS fetch init.headers).
    /// Extra headers override any matching profile headers (case-insensitive match).
    async fn get_with_headers_inner(
        &self,
        url: &str,
        extra_headers: &[(String, String)],
    ) -> Result<Response, NetError> {
        // Try HTTP/3 first
        if let Ok(resp) = self.try_h3_request(url, Method::Get, extra_headers).await {
            return Ok(resp);
        }

        let parsed = Url::parse(url)?;
        let host = parsed
            .host_str()
            .ok_or_else(|| NetError::Http(format!("no host in URL: {url}")))?;
        let port = parsed.port().unwrap_or(443);

        // Top-level navigation in Chrome 158 wire order. Client hints: the
        // full list this origin asked for (top-level navigations ignore the
        // Permissions-Policy restrictions that apply to subresources).
        let accept_ch_upgraded = self.has_accept_ch(host).await;
        let mut rest: Vec<(String, String)> = Vec::new();
        let mut hdrs = self.navigation_headers(url, extra_headers, &mut rest).await;
        merge_headers(&mut hdrs, &rest);

        // Add cookies (unless caller already supplied one)
        self.add_cookies(&mut hdrs, &parsed).await;

        // Try HTTP/2 with automatic stale-connection recovery. If the pooled
        // connection has been closed by the server (GOAWAY), retry once with
        // a fresh connection.
        let response = 'h2: {
            for attempt in 0..2 {
                let sender_res = self.get_sender(host, port).await;
                let mut sender = match sender_res {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("[net] H2 connection failed for {}: {}", host, e);
                        break 'h2 None;
                    }
                };
                let uri = parsed.as_str();
                match h2_client::send_get(&mut sender, uri, host, &hdrs).await {
                    Ok((parts, body)) => {
                        let resp = self.build_response(parts, body, url).await?;
                        break 'h2 Some(resp);
                    }
                    Err(e) if attempt == 0 && is_stale_conn_error(&e) => {
                        // Evict the dead connection from the pool and try once more.
                        self.pool.evict(host, port).await;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
            None
        };

        let response = match response {
            Some(r) => r,
            None => {
                // HTTP/1.1 fallback
                let tcp_stream = tcp::connect_via_proxy(
                    host,
                    port,
                    std::time::Duration::from_secs(10),
                    Some(&self.dns_cache),
                    self.proxy.as_ref(),
                )
                .await?;
                let mut tls_stream =
                    tls::connect_tls(&self.tls_connector, &self.profile, host, tcp_stream).await?;
                let path = if parsed.query().is_some() {
                    format!("{}?{}", parsed.path(), parsed.query().unwrap())
                } else {
                    parsed.path().to_string()
                };
                if url.contains("/mfc")
                    || url.contains("/akam/13")
                    || url.contains("/tl")
                    || url.contains("/r")
                {
                    eprintln!(
                        "[net] sending H1 request to {} with headers: {:?}",
                        url, hdrs
                    );
                }
                let raw = h1_client::send_get(&mut tls_stream, host, &path, &hdrs).await?;
                self.build_response_from_raw(raw, url).await?
            }
        };

        // Learn Alt-Svc from response
        self.learn_alt_svc(url, &response.headers).await;

        // Learn the challenge vendor's session from the response (look for x-kpsdk-cr + x-kpsdk-st).
        let upgrade = self.learn_accept_ch(host, &response.headers).await;

        // Store Set-Cookie from response into jar
        self.store_set_cookies(&parsed, &response.set_cookies).await;

        // Critical-CH per W3 spec: when the server demands immediate-retry
        // hints AND we weren't already sending full CH, retry the same
        // URL once with the high-entropy hints. Some challenge vendors
        // (e.g. on udemy.com, yelp/leboncoin/etsy/wsj)
        // both send Critical-CH; without an immediate retry they treat
        // us as non-Chrome and serve the captcha. One-shot to avoid loops.
        if !accept_ch_upgraded && Self::needs_critical_ch_retry(&response.headers) {
            return Box::pin(self.get_with_headers(url, extra_headers)).await;
        }

        let mut final_response = response;
        final_response.accept_ch_upgrade = upgrade;
        Ok(final_response)
    }

    /// Store all Set-Cookie headers from a response into the cookie jar.
    async fn store_set_cookies(&self, url: &Url, set_cookies: &[String]) {
        if set_cookies.is_empty() {
            return;
        }
        let mut jar = self.cookies.lock().await;
        jar.set_cookies(url, set_cookies);
        // Persist if BROWSER_OXIDE_COOKIE_JAR is set. Atomic write (tempfile +
        // rename) so concurrent runs don't tear the file.
        if let Ok(path) = std::env::var("BROWSER_OXIDE_COOKIE_JAR") {
            let p = std::path::PathBuf::from(&path);
            if let Err(e) = jar.save_to_file(&p) {
                eprintln!("[cookies] save_to_file({}) failed: {}", path, e);
            }
        }
    }

    /// GET with explicit redirect following.
    /// Perform a GET request, following redirects up to `max_redirects`.
    /// Set `BROWSER_OXIDE_DEBUG_REDIRECTS=1` for hop-by-hop tracing.
    pub async fn get_follow(&self, url: &str, max_redirects: u8) -> Result<Response, NetError> {
        let debug = std::env::var("BROWSER_OXIDE_DEBUG_REDIRECTS").is_ok();
        let mut current_url = url.to_string();
        for hop in 0..max_redirects {
            if debug {
                eprintln!("[redirect] hop={} GET {}", hop, current_url);
            }
            let resp = self.get(&current_url).await?;
            if debug {
                let body_len = resp.body.len();
                let cookies: Vec<&str> = resp
                    .set_cookies
                    .iter()
                    .map(|s| s.split(';').next().unwrap_or("").trim())
                    .collect();
                eprintln!(
                    "[redirect]   <- status={} body_len={} location={:?} set-cookies={:?}",
                    resp.status,
                    body_len,
                    resp.headers.get("location"),
                    cookies
                );
            }

            if matches!(resp.status, 301 | 302 | 303 | 307 | 308) {
                if let Some(loc) = resp.headers.get("location") {
                    let next_url = resolve_redirect(&current_url, loc)?;
                    // For 301, 302, 303 redirects from a GET, we just continue with another GET.
                    // For 307, 308, we MUST preserve the method (GET), which self.get() does.
                    current_url = next_url;
                    continue;
                }
            }
            return Ok(resp);
        }
        if debug {
            eprintln!(
                "[redirect] hit max_redirects={}, final GET {}",
                max_redirects, current_url
            );
        }
        self.get(&current_url).await
    }

    /// Perform a GET request, following redirects, with extra headers.
    pub async fn get_follow_with_headers(
        &self,
        url: &str,
        extra_headers: &[(String, String)],
        max_redirects: u8,
    ) -> Result<Response, NetError> {
        let mut current_url = url.to_string();
        for _ in 0..max_redirects {
            // Re-apply headers to each hop
            let resp = self.get_with_headers(&current_url, extra_headers).await?;

            if matches!(resp.status, 301 | 302 | 303 | 307 | 308) {
                if let Some(loc) = resp.headers.get("location") {
                    current_url = resolve_redirect(&current_url, loc)?;
                    continue;
                }
            }
            return Ok(resp);
        }
        self.get_with_headers(&current_url, extra_headers).await
    }

    /// Perform a POST request, following redirects.
    /// DDoS-Guard (ozon.ru) returns 307 on POST /abt/result, which requires
    /// re-POSTing the body to the new location.
    pub async fn post_follow(
        &self,
        url: &str,
        body: &str,
        max_redirects: u8,
    ) -> Result<Response, NetError> {
        self.post_bytes_follow(url, body.as_bytes(), &[], max_redirects)
            .await
    }

    /// POST with raw bytes and redirects.
    pub async fn post_bytes_follow(
        &self,
        url: &str,
        body: &[u8],
        extra_headers: &[(String, String)],
        max_redirects: u8,
    ) -> Result<Response, NetError> {
        let mut current_url = url.to_string();
        for _ in 0..max_redirects {
            let resp = self
                .post_bytes_with_headers(&current_url, body, extra_headers)
                .await?;

            if matches!(resp.status, 301 | 302 | 303 | 307 | 308) {
                if let Some(loc) = resp.headers.get("location") {
                    let next_url = resolve_redirect(&current_url, loc)?;
                    if matches!(resp.status, 307 | 308) {
                        // 307/308: MUST re-POST the same body to the new location.
                        current_url = next_url;
                        continue;
                    } else {
                        // 301/302/303: Standard behavior is to switch to GET.
                        return self.get_follow(&next_url, max_redirects - 1).await;
                    }
                }
            }
            return Ok(resp);
        }
        self.post_bytes_with_headers(&current_url, body, extra_headers)
            .await
    }

    /// Perform a POST request.
    pub async fn post(&self, url: &str, body: &str) -> Result<Response, NetError> {
        self.post_with_headers(url, body, &[]).await
    }

    /// POST with caller-provided extra headers (e.g., Content-Type from JS fetch).
    pub async fn post_with_headers(
        &self,
        url: &str,
        body: &str,
        extra_headers: &[(String, String)],
    ) -> Result<Response, NetError> {
        self.post_bytes_with_headers(url, body.as_bytes(), extra_headers)
            .await
    }

    pub async fn post_bytes_with_exact_headers_direct(
        &self,
        url: &str,
        body: &[u8],
        headers: &[(String, String)],
    ) -> Result<Response, NetError> {
        let parsed = Url::parse(url)?;
        let host = parsed
            .host_str()
            .ok_or_else(|| NetError::Http(format!("no host in URL: {url}")))?;
        let port = parsed.port().unwrap_or(443);
        let path = if let Some(q) = parsed.query() {
            format!("{}?{}", parsed.path(), q)
        } else {
            parsed.path().to_string()
        };

        let mut hdrs: Vec<(String, String)> = headers
            .iter()
            .filter(|(k, _)| {
                let lower = k.to_ascii_lowercase();
                !lower.starts_with(':') && lower != "host" && lower != "connection"
            })
            .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
            .collect();

        self.add_cookies(&mut hdrs, &parsed).await;

        let tcp_stream = self.connect_tcp(host, port).await?;
        let connector = tls::chrome_connector(&self.profile)?;
        let mut tls_stream = tls::connect_tls(&connector, &self.profile, host, tcp_stream).await?;

        let raw = h1_client::send_post(&mut tls_stream, host, &path, &hdrs, body).await?;
        self.build_response_from_raw(raw, url).await
    }

    /// POST with a raw byte body and ONLY the caller-provided headers plus cookies.
    async fn post_bytes_with_exact_headers_inner(
        &self,
        url: &str,
        body: &[u8],
        headers: &[(String, String)],
    ) -> Result<Response, NetError> {
        let parsed = Url::parse(url)?;
        let host = parsed
            .host_str()
            .ok_or_else(|| NetError::Http(format!("no host in URL: {url}")))?;
        let port = parsed.port().unwrap_or(443);

        let mut hdrs: Vec<(String, String)> = headers
            .iter()
            .filter(|(k, _)| {
                let lower = k.to_ascii_lowercase();
                !lower.starts_with(':') && lower != "host" && lower != "connection"
            })
            .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
            .collect();

        self.add_cookies(&mut hdrs, &parsed).await;

        // Env-gated POST body dump
        if let Ok(dir) = std::env::var("BROWSER_OXIDE_DUMP_POST_DIR") {
            use std::io::Write;
            let _ = std::fs::create_dir_all(&dir);
            let counter_path = format!("{}/.counter", dir);
            let next: usize = std::fs::read_to_string(&counter_path)
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(0)
                + 1;
            let _ = std::fs::write(&counter_path, next.to_string());
            let stem = format!("{}/{:03}", dir, next);
            if let Ok(mut f) = std::fs::File::create(format!("{stem}.body")) {
                let _ = f.write_all(body);
            }
            let mut meta = String::new();
            meta.push_str("{\n");
            meta.push_str(&format!(
                "  \"url\": {},\n",
                serde_json::to_string(url).unwrap_or_else(|_| "\"\"".into())
            ));
            meta.push_str(&format!("  \"body_len\": {},\n", body.len()));
            meta.push_str("  \"headers\": {\n");
            for (i, (k, v)) in hdrs.iter().enumerate() {
                let trailing = if i + 1 == hdrs.len() { "" } else { "," };
                meta.push_str(&format!(
                    "    {}: {}{}\n",
                    serde_json::to_string(k).unwrap_or_else(|_| "\"\"".into()),
                    serde_json::to_string(v).unwrap_or_else(|_| "\"\"".into()),
                    trailing
                ));
            }
            meta.push_str("  }\n}\n");
            let _ = std::fs::write(format!("{stem}.meta.json"), meta);
        }

        let response = 'h2: {
            for attempt in 0..2 {
                let sender_res = self.get_sender(host, port).await;
                let mut sender = match sender_res {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("[net] H2 connection failed for {}: {}", host, e);
                        break 'h2 None;
                    }
                };
                let uri = parsed.as_str();
                // Debug only (BO_NET_LOG=1), and never with secrets: this used
                // to print every POST's headers — cookies included — to stderr
                // unconditionally (`contains("/r")` matched nearly all URLs).
                if std::env::var_os("BO_NET_LOG").is_some() {
                    let safe: Vec<(String, String)> = hdrs
                        .iter()
                        .map(|(k, v)| {
                            let lk = k.to_ascii_lowercase();
                            if lk == "cookie" || lk == "authorization" || lk.starts_with("x-fb-") || lk.contains("token") {
                                (k.to_string(), format!("<redacted {} bytes>", v.len()))
                            } else {
                                (k.to_string(), v.to_string())
                            }
                        })
                        .collect();
                    eprintln!("[net] sending H2 POST to {} with headers: {:?}", uri, safe);
                }
                match h2_client::send_post(&mut sender, uri, host, &hdrs, body).await {
                    Ok((parts, resp_body)) => {
                        let resp = self.build_response(parts, resp_body, url).await?;
                        break 'h2 Some(resp);
                    }
                    Err(e) if attempt == 0 && is_stale_conn_error(&e) => {
                        self.pool.evict(host, port).await;
                        continue;
                    }
                    Err(e) => {
                        eprintln!("[net] H2 POST failed for {}: {}", uri, e);
                    }
                }
            }
            None
        };

        let response = match response {
            Some(r) => r,
            None => {
                let tcp_stream = tcp::connect_via_proxy(
                    host,
                    port,
                    std::time::Duration::from_secs(10),
                    Some(&self.dns_cache),
                    self.proxy.as_ref(),
                )
                .await?;
                let connector = tls::chrome_connector(&self.profile)?;
                let mut tls_stream =
                    tls::connect_tls(&connector, &self.profile, host, tcp_stream).await?;
                let path = if parsed.query().is_some() {
                    format!("{}?{}", parsed.path(), parsed.query().unwrap())
                } else {
                    parsed.path().to_string()
                };
                if url.contains("/mfc")
                    || url.contains("/akam/13")
                    || url.contains("/tl")
                    || url.contains("/r")
                {
                    eprintln!(
                        "[net] sending H1 request to {} with headers: {:?}",
                        url, hdrs
                    );
                }
                let raw = h1_client::send_post(&mut tls_stream, host, &path, &hdrs, body).await?;
                self.build_response_from_raw(raw, url).await?
            }
        };

        // Chrome keeps Accept-CH only from top-level document responses
        // (not subresources, fetch()/XHR or iframes).
        let upgrade = if is_top_level_document(&hdrs) {
            self.learn_accept_ch(host, &response.headers).await
        } else {
            false
        };
        self.store_set_cookies(&parsed, &response.set_cookies).await;

        let mut final_response = response;
        final_response.accept_ch_upgrade = upgrade;

        Ok(final_response)
    }

    /// POST with a raw byte body and caller-provided headers. The binary-
    /// safe variant of `post_with_headers` — preserves the byte payload
    /// exactly instead of treating it as UTF-8.
    async fn post_bytes_with_headers_inner(
        &self,
        url: &str,
        body: &[u8],
        extra_headers: &[(String, String)],
    ) -> Result<Response, NetError> {
        // Try HTTP/3 first
        if let Ok(resp) = self
            .try_h3_request(url, Method::Post(body.to_vec()), extra_headers)
            .await
        {
            return Ok(resp);
        }

        let parsed = Url::parse(url)?;
        let host = parsed
            .host_str()
            .ok_or_else(|| NetError::Http(format!("no host in URL: {url}")))?;
        let port = parsed.port().unwrap_or(443);

        // Top-level form-POST navigation in Chrome 158 wire order (same header
        // model as GET navigations; see `get_with_headers_inner`).
        let mut rest: Vec<(String, String)> = Vec::new();
        let mut hdrs = self.navigation_headers(url, extra_headers, &mut rest).await;
        merge_headers(&mut hdrs, &rest);

        // Env-gated POST body dump (for sensor-payload diffing). Writes one
        // file per POST into BROWSER_OXIDE_DUMP_POST_DIR with a numeric index, plus
        // a sidecar .meta.json holding the URL and request headers.
        if let Ok(dir) = std::env::var("BROWSER_OXIDE_DUMP_POST_DIR") {
            use std::io::Write;
            let _ = std::fs::create_dir_all(&dir);
            let counter_path = format!("{}/.counter", dir);
            let next: usize = std::fs::read_to_string(&counter_path)
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(0)
                + 1;
            let _ = std::fs::write(&counter_path, next.to_string());
            let stem = format!("{}/{:03}", dir, next);
            if let Ok(mut f) = std::fs::File::create(format!("{stem}.body")) {
                let _ = f.write_all(body);
            }
            let mut meta = String::new();
            meta.push_str("{\n");
            meta.push_str(&format!(
                "  \"url\": {},\n",
                serde_json::to_string(url).unwrap_or_else(|_| "\"\"".into())
            ));
            meta.push_str(&format!("  \"body_len\": {},\n", body.len()));
            meta.push_str("  \"headers\": {\n");
            for (i, (k, v)) in hdrs.iter().enumerate() {
                let trailing = if i + 1 == hdrs.len() { "" } else { "," };
                meta.push_str(&format!(
                    "    {}: {}{}\n",
                    serde_json::to_string(k).unwrap_or_else(|_| "\"\"".into()),
                    serde_json::to_string(v).unwrap_or_else(|_| "\"\"".into()),
                    trailing
                ));
            }
            meta.push_str("  }\n}\n");
            let _ = std::fs::write(format!("{stem}.meta.json"), meta);
        }

        self.add_cookies(&mut hdrs, &parsed).await;

        // Same stale-connection recovery as GET.
        let response = 'h2: {
            for attempt in 0..2 {
                let sender_res = self.get_sender(host, port).await;
                let mut sender = match sender_res {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("[net] H2 connection failed for {}: {}", host, e);
                        break 'h2 None;
                    }
                };
                let uri = parsed.as_str();
                match h2_client::send_post(&mut sender, uri, host, &hdrs, body).await {
                    Ok((parts, resp_body)) => {
                        let resp = self.build_response(parts, resp_body, url).await?;
                        break 'h2 Some(resp);
                    }
                    Err(e) if attempt == 0 && is_stale_conn_error(&e) => {
                        self.pool.evict(host, port).await;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
            None
        };

        let response = match response {
            Some(r) => r,
            None => {
                let tcp_stream = tcp::connect_via_proxy(
                    host,
                    port,
                    std::time::Duration::from_secs(10),
                    Some(&self.dns_cache),
                    self.proxy.as_ref(),
                )
                .await?;
                let mut tls_stream =
                    tls::connect_tls(&self.tls_connector, &self.profile, host, tcp_stream).await?;
                let path = match parsed.query() {
                    Some(q) => format!("{}?{}", parsed.path(), q),
                    None => parsed.path().to_string(),
                };
                let raw = h1_client::send_post(&mut tls_stream, host, &path, &hdrs, body).await?;
                self.build_response_from_raw(raw, url).await?
            }
        };

        // Store Set-Cookie from POST response — some challenge vendors set a
        // session token here (e.g. KP_UIDz / akm_bmfp_b2 session cookies).
        // Also learn the vendor session: the /tl POST returns x-kpsdk-cr/st here.
        let upgrade = self.learn_accept_ch(host, &response.headers).await;
        self.store_set_cookies(&parsed, &response.set_cookies).await;

        let mut final_response = response;
        final_response.accept_ch_upgrade = upgrade;
        Ok(final_response)
    }

    /// Snapshot all cookies for a URL as a "name=value; name2=value2" string.
    /// Used by document.cookie getter to unify JS-visible cookies with the network jar.
    pub async fn get_with_exact_headers(
        &self,
        url: &str,
        headers: &[(String, String)],
    ) -> Result<Response, NetError> {
        let r = self.get_with_exact_headers_inner(url, headers).await;
        net_log("GET", url, None, &r);
        r
    }

    pub async fn get_with_headers(
        &self,
        url: &str,
        extra_headers: &[(String, String)],
    ) -> Result<Response, NetError> {
        let r = self.get_with_headers_inner(url, extra_headers).await;
        net_log("GET", url, None, &r);
        r
    }

    pub async fn post_bytes_with_exact_headers(
        &self,
        url: &str,
        body: &[u8],
        headers: &[(String, String)],
    ) -> Result<Response, NetError> {
        let r = self.post_bytes_with_exact_headers_inner(url, body, headers).await;
        net_log("POST", url, Some(body), &r);
        r
    }

    pub async fn post_bytes_with_headers(
        &self,
        url: &str,
        body: &[u8],
        extra_headers: &[(String, String)],
    ) -> Result<Response, NetError> {
        let r = self.post_bytes_with_headers_inner(url, body, extra_headers).await;
        net_log("POST", url, Some(body), &r);
        r
    }

    pub async fn cookies_for_url(&self, url: &Url) -> Option<String> {
        let jar = self.cookies.lock().await;
        jar.cookies_for(url)
    }

    /// Evict any pooled HTTP/2 connection to the given host. The next request
    /// will create a fresh TCP+TLS+H2 handshake. Used by challenge retries
    /// where the solver POSTs may have used an H2 session the server is now
    /// done with, or where the session has accumulated GOAWAY.
    pub async fn evict_connection(&self, host: &str, port: u16) {
        self.pool.evict(host, port).await;
    }

    /// Set cookies for a URL from a raw Set-Cookie-style string.
    /// Used by document.cookie setter.
    pub async fn set_cookie_str(&self, url: &Url, raw: &str) {
        let mut jar = self.cookies.lock().await;
        jar.set_cookies(url, &[raw.to_string()]);
    }

    /// Synchronous cookie write. The
    /// `document.cookie` setter must persist into the jar SYNCHRONOUSLY:
    /// challenge.js deposits the `aws-waf-token` via `document.cookie` in the
    /// last microtasks before `location.reload()`, and the previous
    /// fire-and-forget async `op_cookie_set` future was torn down before it
    /// ran — so the reload re-fetched without the token (verified on imdb:
    /// document.cookie had the token but the shared jar was empty → 202 stub).
    ///
    /// The jar mutex is never held across an await during synchronous JS
    /// execution, so `try_lock` succeeds in the common case. Returns `false`
    /// only under genuine contention, where the caller falls back to the
    /// async op. No new blocking in the async runtime.
    pub fn set_cookie_str_sync(&self, url: &Url, raw: &str) -> bool {
        match self.cookies.try_lock() {
            Ok(mut jar) => {
                jar.set_cookies(url, &[raw.to_string()]);
                true
            }
            Err(_) => false,
        }
    }

    /// Drop every cookie whose stored-domain is a host-suffix match of
    /// `target_domain`. Returns the number of (domain → cookie-map)
    /// buckets evicted. Used for the x.com / twitter.com rebrand-
    /// collision band-aid where a fresh nav to one
    /// identity must NOT inherit cookies from the sister identity.
    pub async fn clear_cookies_for_domain(&self, target_domain: &str) -> usize {
        let mut jar = self.cookies.lock().await;
        jar.clear_for_domain(target_domain)
    }

    /// Build a Response from HTTP/2 response parts and body.
    async fn build_response(
        &self,
        parts: http::response::Parts,
        body: Vec<u8>,
        url: &str,
    ) -> Result<Response, NetError> {
        let status = parts.status.as_u16();
        let status_text = parts.status.canonical_reason().unwrap_or("").to_string();

        // Split Set-Cookie out of the regular header map so multi-value
        // Set-Cookie headers aren't collapsed (HashMap would overwrite).
        let mut resp_headers = HashMap::new();
        let mut set_cookies = Vec::new();
        for (key, value) in &parts.headers {
            if let Ok(v) = value.to_str() {
                if key.as_str().eq_ignore_ascii_case("set-cookie") {
                    set_cookies.push(v.to_string());
                } else {
                    resp_headers.insert(key.to_string(), v.to_string());
                }
            }
        }

        // Decompress body
        let encoding = resp_headers
            .get("content-encoding")
            .map(|s| s.as_str())
            .unwrap_or("");
        let decompressed = compression::decompress(&body, encoding)?;

        Ok(Response {
            status,
            status_text,
            headers: resp_headers,
            set_cookies,
            body: decompressed,
            url: url.to_string(),
            accept_ch_upgrade: false,
            timings: TimingStats::default(),
        })
    }

    /// Build a Response from an HTTP/1.1 raw response.
    async fn build_response_from_raw(
        &self,
        raw: h1_client::RawResponse,
        url: &str,
    ) -> Result<Response, NetError> {
        let mut resp_headers = HashMap::new();
        let mut set_cookies = Vec::new();
        for (name, value) in &raw.headers {
            if name.eq_ignore_ascii_case("set-cookie") {
                set_cookies.push(value.clone());
            } else {
                resp_headers.insert(name.clone(), value.clone());
            }
        }

        let encoding = resp_headers
            .get("content-encoding")
            .map(|s| s.as_str())
            .unwrap_or("");
        let decompressed = compression::decompress(&raw.body, encoding)?;

        Ok(Response {
            status: raw.status,
            status_text: raw.status_text,
            headers: resp_headers,
            set_cookies,
            body: decompressed,
            url: url.to_string(),
            accept_ch_upgrade: false,
            timings: TimingStats::default(),
        })
    }
}

/// Detect whether an error indicates a stale/closed pooled connection that can
/// be safely retried by evicting from the pool and reconnecting. This catches
/// HTTP/2 GOAWAY ("not a result of an error"), broken pipe, and ResetStream.
fn is_stale_conn_error(e: &NetError) -> bool {
    let msg = e.to_string();
    msg.contains("not a result of an error")       // h2 GOAWAY / NO_ERROR
        || msg.contains("broken pipe")
        || msg.contains("connection closed")
        || msg.contains("ResetStream")
        || msg.contains("stream was reset")
        || msg.contains("HTTP/2 not ready")
}

/// Check if a header name is already present (case-insensitive).
fn has_header(hdrs: &[(String, String)], name: &str) -> bool {
    hdrs.iter().any(|(k, _)| k.eq_ignore_ascii_case(name))
}

/// `true` when both URLs have the same scheme, host and effective port.
fn same_document_origin(a: &Url, b: &Url) -> bool {
    a.origin() == b.origin()
}

/// `Sec-Fetch-Site` as Chrome computes it for a request to `target` started
/// by `initiator` (`None` = the browser itself, e.g. the address bar).
/// `true` for a top-level document request (`sec-fetch-dest: document`).
fn is_top_level_document(hdrs: &[(String, String)]) -> bool {
    hdrs.iter()
        .any(|(k, v)| k.eq_ignore_ascii_case("sec-fetch-dest") && v.eq_ignore_ascii_case("document"))
}

fn fetch_site(initiator: Option<&Url>, target: &Url) -> &'static str {
    match initiator {
        None => "none",
        Some(init) => {
            if same_document_origin(init, target) {
                "same-origin"
            } else if headers::same_site(init, target) {
                "same-site"
            } else {
                "cross-site"
            }
        }
    }
}

/// `Referer` under Chrome's default policy, `strict-origin-when-cross-origin`.
fn referrer_for(initiator: &Url, target: &Url) -> String {
    if same_document_origin(initiator, target) {
        let mut u = initiator.clone();
        u.set_fragment(None);
        u.to_string()
    } else {
        format!("{}/", initiator.origin().ascii_serialization())
    }
}

/// Chrome sends `Sec-Fetch-Storage-Access: active` on cross-site requests made
/// from a first-party document (storage access granted).
fn storage_access(initiator: Option<&Url>, target: &Url) -> Option<&'static str> {
    match initiator {
        Some(init) if !same_document_origin(init, target) => Some("active"),
        _ => None,
    }
}

/// Header names script may not set on a request (Fetch "forbidden request
/// header"), plus the engine's own `x-browser-oxide-*` control names.
fn is_forbidden_request_header(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    matches!(
        n.as_str(),
        "accept-charset"
            | "accept-encoding"
            | "access-control-request-headers"
            | "access-control-request-method"
            | "connection"
            | "content-length"
            | "cookie"
            | "cookie2"
            | "date"
            | "dnt"
            | "expect"
            | "host"
            | "keep-alive"
            | "origin"
            | "referer"
            | "set-cookie"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "via"
    ) || n.starts_with("sec-")
        || n.starts_with("proxy-")
        || n.starts_with("x-browser-oxide-")
}

fn insert_before_priority(hdrs: &mut Vec<(String, String)>, name: String, value: String) {
    if let Some(pos) = hdrs
        .iter()
        .position(|(k, _)| k.eq_ignore_ascii_case("priority"))
    {
        hdrs.insert(pos, (name, value));
    } else {
        hdrs.push((name, value));
    }
}

/// Merge extra headers into the base list. Existing headers with the same name
/// (case-insensitive) are replaced in place so order is preserved.
fn merge_headers(base: &mut Vec<(String, String)>, extra: &[(String, String)]) {
    for (k, v) in extra {
        // Skip pseudo-headers and forbidden fetch headers that would corrupt H2.
        let lower = k.to_ascii_lowercase();
        if lower.starts_with(':') || lower == "host" || lower == "connection" {
            continue;
        }
        // Engine-internal marker: drop a default header (e.g. Origin on a
        // no-cors <script> load).
        if v == "\u{1}remove" {
            base.retain(|(bk, _)| !bk.eq_ignore_ascii_case(k));
            continue;
        }
        if let Some(slot) = base.iter_mut().find(|(bk, _)| bk.eq_ignore_ascii_case(k)) {
            slot.1 = v.clone();
        } else {
            base.push((lower, v.clone()));
        }
    }
}

/// Resolve a redirect Location header to an absolute URL.
fn resolve_redirect(current_url: &str, location: &str) -> Result<String, NetError> {
    // RFC 3986 §5.2 — resolve `location` against `current_url` as base.
    // Url::join correctly handles all three cases:
    //   - absolute URL ("https://b.com/x")
    //   - root-relative ("/x")
    //   - relative ("x.html", "../y")
    // The previous impl returned `location` verbatim for the third case,
    // which then failed downstream "no host in URL" — caught on iphey.com
    // (holistic sweep 2026-05-10).
    let base = Url::parse(current_url).map_err(|e| NetError::Request(e.to_string()))?;
    let resolved = base.join(location).map_err(|e| {
        NetError::Request(format!(
            "redirect resolve: {e} (base={current_url}, loc={location})"
        ))
    })?;
    Ok(resolved.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_creates_successfully() {
        let profile = crate::stealth::chrome_148_linux();
        let client = HttpClient::new(&profile);
        assert!(client.is_ok());
    }

    #[test]
    fn redirect_resolve_handles_all_three_rfc3986_cases() {
        // Absolute — pass through.
        assert_eq!(
            resolve_redirect("https://a.com/x", "https://b.com/y").unwrap(),
            "https://b.com/y"
        );
        // Root-relative — replaces path on same origin.
        assert_eq!(
            resolve_redirect("https://a.com/x/y", "/z").unwrap(),
            "https://a.com/z"
        );
        // Relative-no-leading-slash — resolves against current path's
        // directory. THIS WAS THE IPHEY BUG (returned "z.html" verbatim
        // → "no host in URL" downstream).
        assert_eq!(
            resolve_redirect("https://a.com/x/y", "z.html").unwrap(),
            "https://a.com/x/z.html"
        );
        // Dot segments per RFC 3986 §5.2.4.
        assert_eq!(
            resolve_redirect("https://a.com/x/y/", "../z.html").unwrap(),
            "https://a.com/x/z.html"
        );
        // Scheme-relative.
        assert_eq!(
            resolve_redirect("https://a.com/x", "//b.com/y").unwrap(),
            "https://b.com/y"
        );
        // Query-only — preserves path.
        assert_eq!(
            resolve_redirect("https://a.com/x?old=1", "?new=2").unwrap(),
            "https://a.com/x?new=2"
        );
    }

    #[test]
    fn response_text() {
        let resp = Response {
            status: 200,
            status_text: "OK".into(),
            headers: HashMap::new(),
            set_cookies: Vec::new(),
            body: b"Hello world".to_vec(),
            url: "https://example.com".into(),
            accept_ch_upgrade: false,
            timings: TimingStats::default(),
        };
        assert_eq!(resp.text(), "Hello world");
        assert!(resp.ok());
    }

    #[test]
    fn response_not_ok() {
        let resp = Response {
            status: 404,
            status_text: "Not Found".into(),
            headers: HashMap::new(),
            set_cookies: Vec::new(),
            body: vec![],
            url: "https://example.com/missing".into(),
            accept_ch_upgrade: false,
            timings: TimingStats::default(),
        };
        assert!(!resp.ok());
    }

    #[tokio::test]
    #[ignore]
    async fn get_request() {
        let profile = crate::stealth::chrome_148_linux();
        let client = HttpClient::new(&profile).unwrap();
        let resp = client.get("https://httpbin.org/get").await.unwrap();
        assert_eq!(resp.status, 200);
        assert!(resp.text().contains("httpbin"));
    }

    #[tokio::test]
    #[ignore]
    async fn get_ipv6_example_com() {
        let profile = crate::stealth::chrome_148_linux();
        let client = HttpClient::new(&profile).unwrap();
        let resp = client.get("https://example.com").await.unwrap();
        assert_eq!(resp.status, 200);
        assert!(resp.text().contains("Example Domain"));
    }

    #[tokio::test]
    #[ignore]
    async fn get_hacker_news() {
        let profile = crate::stealth::chrome_148_linux();
        let client = HttpClient::new(&profile).unwrap();
        let resp = client.get("https://news.ycombinator.com").await.unwrap();
        assert_eq!(resp.status, 200);
        assert!(resp.text().contains("Hacker News"));
    }

    #[tokio::test]
    #[ignore]
    async fn headers_include_ua() {
        let profile = crate::stealth::chrome_148_windows();
        let client = HttpClient::new(&profile).unwrap();
        let resp = client.get("https://httpbin.org/headers").await.unwrap();
        let body = resp.text();
        assert!(
            body.contains("Chrome/130"),
            "Response should show our UA: {}",
            body
        );
    }

    #[tokio::test]
    async fn accept_ch_starts_false_then_true_after_learn() {
        let profile = crate::stealth::chrome_148_windows();
        let client = HttpClient::new(&profile).unwrap();

        // No response seen yet → no Accept-CH for this origin.
        assert!(!client.has_accept_ch("example.com").await);

        // Simulate a response that includes Accept-CH.
        let mut headers = HashMap::new();
        headers.insert(
            "accept-ch".to_string(),
            "Sec-CH-UA-Full-Version-List, Sec-CH-UA-Platform-Version".to_string(),
        );
        client.learn_accept_ch("example.com", &headers).await;

        assert!(client.has_accept_ch("example.com").await);
        // Other origins are not affected.
        assert!(!client.has_accept_ch("other.com").await);
    }

    #[tokio::test]
    async fn accept_ch_header_name_is_case_insensitive() {
        let profile = crate::stealth::chrome_148_linux();
        let client = HttpClient::new(&profile).unwrap();

        // Mixed-case header name (e.g. from an HTTP/1.1 server that sends
        // it with canonical capitalisation).
        let mut headers = HashMap::new();
        headers.insert("Accept-CH".to_string(), "Sec-CH-UA-Arch".to_string());
        client.learn_accept_ch("site.example", &headers).await;

        assert!(client.has_accept_ch("site.example").await);
    }

    #[tokio::test]
    async fn response_without_accept_ch_does_not_upgrade_origin() {
        let profile = crate::stealth::chrome_148_linux();
        let client = HttpClient::new(&profile).unwrap();

        let mut headers = HashMap::new();
        headers.insert("content-type".to_string(), "text/html".to_string());
        client.learn_accept_ch("boring.example", &headers).await;

        assert!(!client.has_accept_ch("boring.example").await);
    }
}


/// `BO_NET_LOG=1`: one line per request (method, status, url, and for POST
/// bodies only the field NAMES — never values, so credentials typed into
/// forms cannot leak into logs).
/// BO_NET_LOG_HDR=<url substring>: print the request headers (cookie value
/// omitted) of matching exact-header GETs (navigations).
pub(crate) fn net_log_headers(url: &str, headers: &[(String, String)]) {
    let Ok(pat) = std::env::var("BO_NET_LOG_HDR") else { return };
    if !url.contains(&pat) { return; }
    eprintln!("[nethdr] == {}", url.split('?').next().unwrap_or(url));
    for (k, v) in headers {
        let v = if k.eq_ignore_ascii_case("cookie") { "<omitted>".to_string() } else { v.chars().take(160).collect() };
        eprintln!("[nethdr] {k}: {v}");
    }
}

fn net_log(method: &str, url: &str, body: Option<&[u8]>, r: &Result<Response, NetError>) {
    if std::env::var("BO_NET_LOG").is_err() {
        return;
    }
    let status = match r { Ok(x) => x.status.to_string(), Err(e) => format!("ERR {e}") };
    let mut fields = String::new();
    if let Some(b) = body {
        let t = String::from_utf8_lossy(b);
        let names: Vec<String> = if t.trim_start().starts_with('{') {
            serde_json::from_str::<serde_json::Value>(&t).ok()
                .and_then(|v| v.as_object().map(|o| o.keys().cloned().collect())).unwrap_or_default()
        } else {
            t.split('&').filter_map(|kv| kv.split('=').next()).map(|k| k.to_string()).collect()
        };
        fields = format!(" [{}B fields: {}]", b.len(), names.join(","));
    }
    let short: String = url.chars().take(220).collect();
    let loc = match r {
        Ok(x) => x.headers.get("location").map(|l| format!(" -> {}", l.chars().take(160).collect::<String>())).unwrap_or_default(),
        _ => String::new(),
    };
    eprintln!("[netlog] {method} {status} {short}{fields}{loc}");
}
