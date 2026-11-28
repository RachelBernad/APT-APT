//! Top-level headless browser API.
//!
//! Provides `Page` — parse HTML, execute JavaScript, extract content.

// --- merged engine modules ---
pub mod canvas;
pub mod css_cascade;
pub mod css_parser;
pub mod css_selectors;
pub mod css_values;
pub mod dom;
pub mod event_loop;
pub mod host;
pub mod html_parser;
pub mod js_runtime;
pub mod layout;
pub mod net;
pub mod protocol;
pub mod stealth;
pub mod workers;

pub mod challenge;
pub mod classify;
pub mod csp_collector;
pub mod frames;
pub mod iframe;
mod page;
pub mod parallel;
pub mod pool;
mod script_runner;
pub mod stylesheet_collector;

pub use challenge::{ChallengeKind, ChallengeSolver, SolveOutcome};
pub use classify::{engine_classify, EngineClass};
pub use page::{ChallengeVerdict, Page};
pub use parallel::{NavigateResult, ParallelPager};
pub use pool::PagePool;

/// A JS string literal (with quotes) holding `s`, for building engine-side scripts. JSON encoding is valid JS
/// (ES2019+) and escapes quotes, backslashes and control characters. Never interpolate raw text into scripts.
pub(crate) fn js_string_literal(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".into())
}
