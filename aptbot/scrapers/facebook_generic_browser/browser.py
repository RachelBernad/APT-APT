"""Shared browser launcher: one persistent, headful, real-Chrome context.

Rules (see api_docs/stealth/browser-setup.md for why):
- headful only. On a GUI-less Linux box run under Xvfb (DISPLAY must be set).
- real Google Chrome on macOS (channel="chrome"); system Chromium on Linux/arm64.
- no custom user agent, headers, viewport, locale or fingerprint injection.
- the profile lives OUTSIDE the repo (default ~/.fbgb/profile). Logged in once (`fbgb login`, by hand or --stdin).

Env overrides:
  FBGB_HOME          base dir for profile/captures/validation (default ~/.fbgb)
  FBGB_PROFILE_DIR   Chrome user-data-dir (default $FBGB_HOME/profile)
  FBGB_BROWSER       explicit browser executable path
  FBGB_CHROME_ARGS   extra Chrome switches, space-separated (deployment only, e.g. in Docker:
                     "--disable-namespace-sandbox" = use the setuid sandbox; see api_docs/stealth/pi-headless.md)
"""
from __future__ import annotations

import os
import platform
import shutil
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

HOME = Path(os.environ.get("FBGB_HOME", Path.home() / ".fbgb"))
PROFILE_DIR = Path(os.environ.get("FBGB_PROFILE_DIR", HOME / "profile"))
CAPTURE_DIR = HOME / "captures"
VALIDATION_DIR = HOME / "validation"

_LINUX_CHROMIUM = ("/usr/bin/chromium", "/usr/bin/chromium-browser", "/snap/bin/chromium")

# Playwright default switches that a normal Chrome never has. Matched as exact strings, so this list
# is tied to the pinned patchright version (requirements.txt). validate.py's storagePartitioning check
# catches drift after an upgrade. Kept on purpose: --password-store=basic / --use-mock-keychain (the
# profile's cookies are encrypted with that key; changing it later logs the profile out).
_PW_DISABLED_FEATURES = (
    "AvoidUnnecessaryBeforeUnloadCheckSync,DestroyProfileOnBrowserClose,DialMediaRouteProvider,"
    "GlobalMediaControls,HttpsUpgrades,LensOverlay,PaintHolding,ThirdPartyStoragePartitioning,"
    "BlockOriginHeaderModificationOnRedirect,AvoidCorsURLLoaderRestartOnRedirect,Translate,AutoDeElevate,"
    "OptimizationHints,NetworkTimeServiceQuerying,AimEnabled,msForceBrowserSignIn,"
    "msEdgeUpdateLaunchServicesPreferredVersion"
)
IGNORED_DEFAULT_ARGS = [
    "--disable-features=" + _PW_DISABLED_FEATURES,  # 3rd-party storage partitioning off = detectable
    "--disable-background-timer-throttling",  # hidden tabs must throttle like a real Chrome
    "--disable-backgrounding-occluded-windows",
    "--disable-renderer-backgrounding",
    "--force-color-profile=srgb",  # real Macs report their display's color profile / gamut
]


def browser_kwargs() -> dict:
    """How to pick the browser binary: real Chrome where it exists, distro Chromium on Linux."""
    exe = os.environ.get("FBGB_BROWSER")
    if exe:
        return {"executable_path": exe}
    if platform.system() == "Linux":
        for path in _LINUX_CHROMIUM:
            if Path(path).exists():
                return {"executable_path": path}
        if shutil.which("google-chrome"):
            return {"channel": "chrome"}
        raise RuntimeError("No Chromium found; apt install chromium (Google Chrome has no linux-arm64 build)")
    return {"channel": "chrome"}


def _check_display() -> None:
    if platform.system() == "Linux" and not os.environ.get("DISPLAY"):
        raise RuntimeError("No DISPLAY. Run under Xvfb (see api_docs/stealth/pi-headless.md); never headless.")


@asynccontextmanager
async def open_context(profile_dir: Path = PROFILE_DIR, *, engine: str = "patchright") -> AsyncIterator:
    """Yield a persistent BrowserContext. `engine` is "patchright" (default) or "playwright"
    (vanilla, used only by validate.py as the control group)."""
    _check_display()
    profile_dir.mkdir(parents=True, exist_ok=True)
    if engine == "patchright":
        from patchright.async_api import async_playwright
    elif engine == "playwright":
        from playwright.async_api import async_playwright
    else:
        raise ValueError(engine)
    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(
            str(profile_dir),
            headless=False,
            no_viewport=True,
            ignore_default_args=IGNORED_DEFAULT_ARGS if engine == "patchright" else None,
            chromium_sandbox=True,
            args=os.environ.get("FBGB_CHROME_ARGS", "").split() or None,  # Playwright defaults to --no-sandbox; never browse Facebook unsandboxed
            # focus_control=True turns OFF Playwright's focus emulation, which otherwise makes every tab
            # report hasFocus()=true and visibilityState="visible" at the same time.
            **({"focus_control": True} if engine == "patchright" else {}),
            **browser_kwargs(),
        )
        try:
            yield ctx
        finally:
            await ctx.close()
