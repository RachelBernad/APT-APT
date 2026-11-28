# Vendored: BrowserOxide fork

- Upstream: https://github.com/yfedoseev/browser_oxide @ `8714506` (v0.1.3, "expose child-frame evaluation").
  License: MIT OR Apache-2.0 (`LICENSE*` here).
- Vendored 2026-11-28 from our research checkout (`~/aptbot-browser-research/browseroxide/oxide`, branch `poc`).
  That checkout still holds the upstream git history. `git diff 8714506` there shows our changes against upstream.
- **What's here:** `crates/browser_oxide` (engine), `crates/browser_oxide_py` (pyo3 binding, standalone cargo
  workspace), `python/` (the `browser_oxide` package), `pyproject.toml` (maturin), the root `Cargo.toml` (workspace
  trimmed to the engine; `browser_oxide_mcp` dropped), `Cargo.lock`, `.cargo/config.toml`.
- **What's not:** upstream CI, docs, the MCP crate, python tests and examples, and any build output.
- **Our changes** (every one has its file, what and why): [kb/engine-changes.md](kb/engine-changes.md).
- **To update from upstream:** merge upstream into the research checkout, rebuild and run the selftests
  (`tools/reap_selftest.py`, `tools/cdp_selftest.py`, `tools/leak_selftest.py`), then re-copy the same paths here.
