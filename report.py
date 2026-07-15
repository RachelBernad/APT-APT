# report.py
"""Render a user's current apartment matches into a self-contained, mobile-first
HTML page (RTL-aware, inline CSS/images-by-URL). Used by the daily report feature.
"""
from __future__ import annotations

import html
from typing import Any, Dict, List, Optional

from models import FEATURES


def _esc(v: Any) -> str:
    return html.escape(str(v if v is not None else ""), quote=True)


def _price_int(listing: Dict) -> Optional[int]:
    p = listing.get("price")
    try:
        return int(p) if p not in (None, "", 0) else None
    except (TypeError, ValueError):
        return None


def _price_label(listing: Dict) -> str:
    p = _price_int(listing)
    return f"₪{p:,}" if p else "מחיר לא צוין"


def _sort_key(listing: Dict):
    p = _price_int(listing)
    return (0, p) if p else (1, 0)   # priced first (ascending), then unpriced


def _feature_chips(listing: Dict) -> str:
    chips = []
    for key in listing.get("features") or []:
        spec = FEATURES.get(key)
        if spec:
            chips.append(f'<span class="chip">{spec[3]} {_esc(spec[2])}</span>')
    if listing.get("is_mamad") is True and "mamad" not in (listing.get("features") or []):
        chips.append('<span class="chip">🛡 ממ״ד</span>')
    return "".join(chips)


def _card(listing: Dict) -> str:
    img = ""
    images = listing.get("images") or []
    if images:
        img = f'<div class="thumb" style="background-image:url(\'{_esc(images[0])}\')"></div>'

    loc = _esc(listing.get("location") or listing.get("hood") or listing.get("city") or "")
    meta = []
    if listing.get("property_type"):
        meta.append(_esc(listing["property_type"]))
    if listing.get("rooms") not in (None, "", "0"):
        meta.append(f'{_esc(listing["rooms"])} חד׳')
    if listing.get("size") not in (None, "", "0", ""):
        meta.append(f'{_esc(listing["size"])} מ״ר')
    floor = listing.get("floor")
    if floor not in (None, "", "0"):
        meta.append(f'קומה {_esc(floor)}')
    meta_html = " · ".join(meta)

    url = _esc(listing.get("apartment_page_url") or "#")
    chips = _feature_chips(listing)
    return (
        '<article class="card">'
        f'{img}'
        '<div class="body">'
        f'<div class="row"><span class="price">{_price_label(listing)}</span></div>'
        f'<div class="loc">📍 {loc}</div>'
        + (f'<div class="meta">{meta_html}</div>' if meta_html else "")
        + (f'<div class="chips">{chips}</div>' if chips else "")
        + f'<a class="btn" href="{url}" target="_blank" rel="noopener">צפייה במודעה ↗</a>'
        '</div></article>'
    )


def render(listings: List[Dict], when: str, title: str = "הדירות שלי היום") -> str:
    ordered = sorted(listings, key=_sort_key)
    cards = "\n".join(_card(l) for l in ordered)
    count = len(ordered)
    priced = [p for p in (_price_int(l) for l in ordered) if p]
    price_range = f"₪{min(priced):,}–₪{max(priced):,}" if priced else ""

    return f"""<!doctype html>
<html lang="he" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>{_esc(title)}</title>
<style>
  :root {{ color-scheme: light dark; }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; padding: 0 12px 40px;
    font-family: -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    background: #f4f5f7; color: #1a1a1a;
  }}
  @media (prefers-color-scheme: dark) {{
    body {{ background: #0e0f13; color: #e9e9ee; }}
    .card {{ background: #1a1c22 !important; box-shadow: none !important; border: 1px solid #262932; }}
    .meta, .loc {{ color: #b5b8c2 !important; }}
    .chip {{ background: #262932 !important; color: #cfd2db !important; }}
    header .sub {{ color: #9aa0ad !important; }}
  }}
  header {{ padding: 22px 4px 12px; max-width: 720px; margin: 0 auto; }}
  header h1 {{ font-size: 22px; margin: 0 0 4px; }}
  header .sub {{ color: #6b7280; font-size: 14px; }}
  .wrap {{ max-width: 720px; margin: 0 auto; display: grid; gap: 14px; }}
  .card {{
    background: #fff; border-radius: 16px; overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,.08), 0 6px 18px rgba(0,0,0,.05);
  }}
  .thumb {{ height: 190px; background-size: cover; background-position: center; background-color:#dfe1e6; }}
  .body {{ padding: 14px 16px 16px; }}
  .row {{ display: flex; align-items: center; justify-content: space-between; gap: 8px; }}
  .price {{ font-size: 21px; font-weight: 800; letter-spacing: -.3px; }}
  .badge {{ color:#fff; font-size: 11px; font-weight: 700; padding: 3px 9px; border-radius: 999px; white-space: nowrap; }}
  .loc {{ margin-top: 6px; font-size: 15px; color:#374151; }}
  .meta {{ margin-top: 6px; font-size: 13px; color:#6b7280; }}
  .chips {{ margin-top: 10px; display: flex; flex-wrap: wrap; gap: 6px; }}
  .chip {{ background:#eef0f4; color:#3a3f4b; font-size: 12px; padding: 4px 9px; border-radius: 999px; }}
  .btn {{
    margin-top: 14px; display: block; text-align: center; text-decoration: none;
    background: linear-gradient(135deg,#6c5ce7,#4834d4); color: #fff; font-weight: 700;
    padding: 12px; border-radius: 12px; font-size: 15px;
  }}
  footer {{ max-width:720px; margin: 26px auto 0; text-align:center; color:#9aa0ad; font-size:12px; }}
</style>
</head>
<body>
  <header>
    <h1>🏠 {_esc(title)}</h1>
    <div class="sub">{count} דירות{(' · ' + price_range) if price_range else ''} · עודכן {_esc(when)}</div>
  </header>
  <main class="wrap">
    {cards if cards else '<p style="text-align:center;color:#888">אין תוצאות כרגע.</p>'}
  </main>
  <footer>נוצר על ידי בוט הדירות</footer>
</body>
</html>"""
