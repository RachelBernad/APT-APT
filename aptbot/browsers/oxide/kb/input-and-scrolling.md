# Input, scrolling, focus

[← index](README.md) · related: [caveats](caveats.md), [facebook](facebook.md), [engine-changes](engine-changes.md)

All input goes through a **private hook** that pages can't see or enumerate:

```js
const H = globalThis[Symbol.for('__browser_oxide_script_hook__')];
H.input.click(elOrSelector)   // queue a human pointer path + press/release/click on the element
H.input.type(text)            // queue trusted keystrokes into document.activeElement
H.input.press(key)            // queue one key: 'Enter', 'Escape', 'Tab', 'ArrowDown', ...
H.input.wheel(px)             // queue a wheel scroll (ours)
H.input.busy()                // true while queued input remains
H.input.takeLog()             // array of what happened ("click: ...", "wheel: 1300px, scrollY=...")
H.input.focused()             // current focus target
H.viewportScroll(x, y)        // programmatic window scroll (= what scrollTo does), ours
H.viewportPos()               // [scrollX, scrollY]
```

Events are **trusted** (`isTrusted === true`): they're minted inside event_bootstrap (`_markTrusted`). Each input call returns
`'queued'`, and the events then fire on timers with human gaps, **so you must pump**:

```python
b.evaluate(f"{H}.input.click(el)")
while b.evaluate(f"{H}.input.busy()") == "true": b.pump(40)
```

## click

The pointer moves to the element's center along an eased, slightly curved path (8–16 `pointermove`/`mousemove` steps,
8–22 ms apart), then pointerdown/mousedown, a 40–120 ms hold, then pointerup/mouseup/click, plus default actions
(focus, link navigation, form submit). Before that it calls `el.scrollIntoView({block:'center'})`, which is now a
real viewport scroll (it used to be a no-op).

- Pass an **element** (from a query inside the same `evaluate`) rather than a selector when the selector is ambiguous:
  `(() => { const b = [...d.querySelectorAll('[role=button]')].find(x => x.textContent.trim() === 'See more'); return H.input.click(b); })()`
- Don't mark DOM nodes with attributes to find them again: that's page-visible. Find them by content each time.

## wheel

`wheel(px)` (ours). If the pointer has never moved, it first moves to a random point mid-viewport. Then it sends ticks of
100/120/80/200 px with random 60–160 ms gaps. Each tick dispatches a trusted
`WheelEvent('wheel', {deltaY, deltaMode: 0, composed: true})` at `document.elementFromPoint(pointer)` and, unless
the event is cancelled, scrolls the viewport. Pages see the same `wheel` → `scroll` sequence as with a real mouse.

Pacing that worked on FB: 900–1600 px per gesture, then pump 2.5–4.5 s. When re-scrolling past content you've already
handled: 2000–3400 px, then 1.2–2.2 s.

## Scroll model (ours)

Before our changes oxide had no scroll: rects never moved and `scrollY` was a fake counter. Now:
- One viewport scroll position (`_vpX/_vpY` in dom_bootstrap), clamped to `documentElement` size − `innerWidth/Height`.
- `getBoundingClientRect()` is layout position minus the scroll offset, so it moves when you scroll.
- `scrollX/scrollY/pageXOffset/pageYOffset`, `document.documentElement.scrollTop/Left` (get and set), `scrollTo`,
  `scrollBy` and `scrollIntoView` all use it.
- A change fires one trusted `scroll` event at `document` (it bubbles to `window`) after 16 ms, coalesced like Chrome's
  once-per-frame scroll events.
- **Element scroll containers are not modelled.** `el.scrollTop` on a normal element is stored and read back,
  but it doesn't move that element's children, and `scrollHeight`/`clientHeight` are just `offsetHeight`. Pages that
  use inner scrollers (FB's detached `#scrollview` layer) see a flat box ([caveat](caveats.md#inner-scrollers)).
- `scrollIntoView(arg)` supports `block`/`inline` = start | center | end | nearest, plus the boolean form. It's instant
  (smooth scrolling isn't animated) and viewport-only.

## IntersectionObserver (ours, layout-based)

It now computes real intersections against the viewport (or `root`) with `rootMargin` and the thresholds.
Before, it reported everything as visible, so FB loaded 10 feed pages without any scrolling (a bot tell, and +400 MB).
It re-checks 16 ms after DOM mutations, scrolls and resizes, with no idle polling. Entries are delivered only when an
element's threshold index changes; the first entry for each target comes on `setTimeout(0)`.

Consequence: oxide lays some pages out differently from Chrome, so **what is "on screen" can differ**. On the FB home
page the left nav is stacked above the feed, so the first feed sentinel sits at y≈1335: no posts load until the first
scroll ([caveat](caveats.md#layout-stacking)).

## Focus

- `el.focus(options)` reads `options.focusVisible` and then `options.preventScroll`, in WebIDL order, like Chrome.
  Pages feature-detect `preventScroll` with a getter (FB's FocusManager does). Without `preventScroll` the element
  is scrolled into view (`nearest`).
- `document.activeElement` follows Chrome's **focus fixup**: if the focused element leaves the document, focus falls
  back to `<body>`. Focus is held weakly ([why](memory.md#leaks-fixed)).
- Never focus or type into FB comment boxes ([facebook](facebook.md#rules)).

## Keys

`press('Escape')` closes FB dialogs about 90% of the time. When it doesn't (focus is inside a nested layer), click the
dialog's `[aria-label=Close]` instead ([caveat](caveats.md#escape-swallowed)).
