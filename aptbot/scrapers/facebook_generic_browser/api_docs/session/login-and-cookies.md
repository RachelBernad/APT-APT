# Login & session persistence

Last verified: 2026-11-24

- Login is **manual**: `explore/open_profile.py` opens Chrome on `~/.fbgb/profile`, the user logs in (incl. 2FA,
  "trust this device"), then closes the window. The script polls only for the cookie *names* `c_user` and `xs` (never values).
- The session survived 5 later launches (2026-11-24) with no re-login. Cookies are encrypted with the profile's
  `--password-store=basic --use-mock-keychain` key, so keep those flags or the profile is logged out.
- Relevant cookie names: `c_user` (account id), `xs` (session), `datr` (browser id, long-lived: the device identity),
  `fr`, `sb`. Treat the whole profile dir as a secret. It never enters the repo.
- Always close the context cleanly (`async with open_context`), because killing Chrome can lose recent cookie writes.
- Moving machines (Mac → Pi) = a new profile + a new login (by hand or `login --stdin`) (see `../stealth/pi-headless.md`).
