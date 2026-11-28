"""Convert a Chrome session into oxide's cookie jar (BROWSER_OXIDE_COOKIE_JAR). Prints cookie NAMES only.

    python chrome_to_jar.py --state storage_state.json OUT_JAR          # Playwright/Patchright storageState file
    python chrome_to_jar.py --profile ~/.fbgb/profile OUT_JAR           # fbgb's Chrome profile (opens Chrome via
                                                                        # fbgb's validated launcher, no navigation)

Only facebook.com cookies are kept. OUT_JAR is written atomically with mode 600.
**One browser per session:** once oxide uses the jar, never run Chrome on that profile again at the same time
(the two would rotate the session's cookies against each other, and FB sees two devices).

Jar format (oxide net/cookies.rs): {"cookies": {domain: {name: {name, value, path, creation, domain, secure,
http_only, expires}}}, "seq": N}. domain = "facebook.com" for a domain cookie (no leading dot) or null for
host-only; expires = unix seconds, or null for a session cookie; creation = an increasing sequence number.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import tempfile

KEEP = ("facebook.com",)
NEEDED = {"c_user", "xs"}


def to_jar(cookies: list[dict]) -> dict:
    jar: dict[str, dict] = {}
    seq = 0
    for c in cookies:
        dom = (c.get("domain") or "").lower()
        bare = dom.lstrip(".")
        if not any(bare == k or bare.endswith("." + k) for k in KEEP):
            continue
        seq += 1
        exp = c.get("expires")
        jar.setdefault(bare, {})[c["name"]] = {
            "name": c["name"], "value": c["value"], "path": c.get("path") or "/", "creation": seq,
            "domain": bare if dom.startswith(".") else None,
            "secure": bool(c.get("secure")), "http_only": bool(c.get("httpOnly", c.get("http_only"))),
            "expires": int(exp) if isinstance(exp, (int, float)) and exp > 0 else None,
        }
    return {"cookies": jar, "seq": seq}


async def _from_profile(profile: str) -> list[dict]:
    from aptbot.scrapers.facebook_generic_browser.browser import open_context
    from pathlib import Path
    async with open_context(Path(profile)) as ctx:
        return await ctx.cookies("https://www.facebook.com")


def write_jar(jar: dict, out: str) -> None:
    d = os.path.dirname(os.path.abspath(out))
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".jar-")
    with os.fdopen(fd, "w") as f:
        json.dump(jar, f)
    os.chmod(tmp, 0o600)
    os.replace(tmp, out)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--state", help="storageState JSON")
    src.add_argument("--profile", help="Chrome user-data-dir (fbgb profile)")
    ap.add_argument("out")
    a = ap.parse_args()
    if a.state:
        with open(a.state) as f:
            cookies = json.load(f).get("cookies", [])
    else:
        cookies = asyncio.run(_from_profile(a.profile))
    jar = to_jar(cookies)
    names = {n for d in jar["cookies"].values() for n in d}
    if not NEEDED <= names:
        print(f"refusing: no logged-in session (missing {sorted(NEEDED - names)})", file=sys.stderr)
        return 1
    write_jar(jar, a.out)
    print(f"wrote {a.out} (600): {len(names)} cookies: {', '.join(sorted(names))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
