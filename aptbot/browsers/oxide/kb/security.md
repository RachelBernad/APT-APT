# Security and account safety

[← index](README.md) · related: [facebook](facebook.md#rules), [env-vars](env-vars.md)

## Session files {#session}

- `profiles/fbtest_state.json` (Playwright storageState) and `profiles/fbtest_jar.json` (oxide jar) hold the test
  account's **live session**. Treat them like a password: chmod 600, local only, never commit, never print.
- How they were made: one oxide login attempt reached FB's Arkose check (the same one Chrome gets). A human logged in
  in the Playwright Chrome, and its cookies were converted to oxide's jar format:
  `{cookies:{domain:{name:{name,value,path,creation,domain|null,secure,http_only,expires}}},seq}`.
- Runs use a **copy** (`/tmp/fbjar_run.json`, chmod 600) because oxide persists the jar it loads.
- Credentials: only through stdin (`type @stdin:N`). Never in argv, env, files or logs. Ask the user before every
  live login attempt; never retry automatically.

## Logs {#logs}

- Before 2026-11-27 oxide printed **every POST's full headers, cookies included, to stderr**
  (`[net] sending H2 request ...`; the URL filter `contains("/r")` matched nearly everything). Fixed: it now only logs with
  `BO_NET_LOG=1`, and redacts `cookie`/`authorization`/`x-fb-*`/token values. A 403/429 reload response dump
  now redacts `set-cookie`.
- Still assume stderr can contain URLs with ids. Read stdout (`## ` lines in drive.py), and grep stderr only for
  specific markers (`panicked`, `fatal`).
- `BROWSER_OXIDE_DUMP_POST_DIR` writes requests **with cookies** to disk. Debug only; delete the files afterwards.
- The live view PNG (`/tmp/oxlive`), heap snapshots (`/tmp/*.heapsnapshot`) and CSV/log outputs contain private
  page content or tokens (snapshots contain cookies and `fb_dtsg`). Keep them local and delete them when done.

## Account

- Stop on any checkpoint or block page, and tell the user (fb_deep_session's `blocked()`).
- Don't automate the Arkose check.
- Keep volume low. A 200-post deep session is a research exception that the user asked for.
- The public GitHub repo (`APT-APT`) must never receive any of these files. Scan before any push.
