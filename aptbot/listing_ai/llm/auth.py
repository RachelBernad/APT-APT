"""ChatGPT-subscription OAuth: where the token store lives, and how to log in.

The store is langchain-openai's file format (``_FileChatGPTOAuthTokenProvider``:
0600 file, thread lock + ``flock``, refresh 5 min before expiry). It is ours alone:
never ``~/.codex/auth.json``, because two clients rotating one refresh token log
each other out. It must live OUTSIDE the repo (default ``~/.aptbot/``).

Login flows:
- :func:`device_login`: headless (the Pi). Speaks Codex CLI's device protocol; the
  langchain-openai 1.8.5 ``login_chatgpt_device`` sends a form body with the wrong
  fields and is rejected (HTTP 400), so we don't use it.
- :func:`browser_login`: langchain's loopback PKCE flow on port 1455 (needs a local
  browser; on a remote host tunnel it with ``ssh -L 1455:localhost:1455``).

Nothing here prints or logs a token.
"""
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Callable, Optional

DEFAULT_STORE = Path.home() / ".aptbot" / "chatgpt-auth.json"
ISSUER = "https://auth.openai.com"
DEVICE_PAGE = f"{ISSUER}/codex/device"
_PENDING = (403, 404)            # device poll: not approved yet


def store_path() -> Path:
    raw = os.getenv("AI_AUTH_PATH", "").strip()
    return Path(raw).expanduser() if raw else DEFAULT_STORE


def provider(path: Optional[Path] = None):
    """The refresh-aware token provider for the store (raises if langchain-openai is missing)."""
    from langchain_openai.chatgpt_oauth import _FileChatGPTOAuthTokenProvider
    return _FileChatGPTOAuthTokenProvider(path=path or store_path())


def device_login(path: Optional[Path] = None, *, say: Callable[[str], None] = print,
                 timeout: float = 15 * 60):
    """Device-code login: prints a URL + code to approve in any browser, then saves the token."""
    import httpx
    from langchain_openai import chatgpt_oauth as co

    with httpx.Client(timeout=30) as c:
        r = c.post(f"{ISSUER}/api/accounts/deviceauth/usercode", json={"client_id": co.CHATGPT_CLIENT_ID})
        _check(r, "device code request")
        start = r.json()
        dev_id = start["device_auth_id"]
        code = start.get("user_code") or start.get("usercode")
        interval = float(start.get("interval") or 5)
        say(f"Open {DEVICE_PAGE} and enter the code:  {code}")
        deadline = time.monotonic() + timeout
        grant = None
        while time.monotonic() < deadline:
            time.sleep(interval)
            p = c.post(f"{ISSUER}/api/accounts/deviceauth/token",
                       json={"device_auth_id": dev_id, "user_code": code})
            if p.status_code in _PENDING:
                continue
            _check(p, "device poll")
            grant = p.json()
            break
        if grant is None:
            raise TimeoutError("Timed out waiting for the device code to be approved.")
        t = c.post(co.CHATGPT_TOKEN_URL, data={
            "grant_type": "authorization_code", "code": grant["authorization_code"],
            "redirect_uri": co.CHATGPT_DEVICE_REDIRECT_URI, "client_id": co.CHATGPT_CLIENT_ID,
            "code_verifier": grant["code_verifier"]})
        _check(t, "token exchange")
    prov = provider(path)
    prov.save(co._token_from_response(t.json()))
    return prov


def browser_login(path: Optional[Path] = None):
    from langchain_openai.chatgpt_oauth import login_chatgpt
    return login_chatgpt(store_path=path or store_path())


def status(path: Optional[Path] = None) -> str:
    """One safe line: plan + expiry, never a token. Refreshes if due."""
    p = path or store_path()
    if not p.exists():
        return f"not logged in (no token store at {p})"
    tok = provider(p).get_token()
    return f"logged in: plan={tok.plan_type} access token valid until {tok.expires_at:%Y-%m-%d %H:%M} UTC ({p})"


def _check(resp, what: str) -> None:
    if resp.status_code >= 400:
        # OAuth error bodies carry no secrets, but keep it short.
        raise RuntimeError(f"ChatGPT {what} failed: HTTP {resp.status_code} {resp.text[:200]}")
