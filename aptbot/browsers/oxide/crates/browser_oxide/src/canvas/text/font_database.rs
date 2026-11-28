//! Global font database backed by `fontdb`.
//!
//! Loads the bundled TTF faces at startup (once per process) and
//! exposes a `query()` that maps a CSS font-family request + weight +
//! italic to a face ID plus the raw face bytes. Chrome-style family
//! aliases (Arial → Liberation Sans, Times → Liberation Serif, etc.)
//! are set up so a site asking for Arial gets plausible Liberation
//! Sans metrics rather than the DejaVu Sans fallback.
//!
//! The database is built from in-binary font data only — NEVER from
//! the host's system fonts — so output is reproducible across
//! machines and matches the StealthProfile's nominal Chrome-on-X
//! behaviour rather than the developer's laptop.

use fontdb::{Database, Family, Query, Stretch, Style, Weight, ID};
use std::sync::OnceLock;

pub struct FontDatabase {
    inner: Database,
}

/// Bundled font data. `include_bytes!` keeps these in the final binary
/// so the fingerprint is hermetic — no filesystem lookups at runtime.
///
/// The bundled set covers all four Latin style combinations (regular,
/// bold, italic, bold-italic) for sans, serif, and monospace — twelve
/// Liberation faces total, mirroring what Linux Chrome gets from
/// fontconfig. DejaVu Sans is kept as the ultimate-fallback face, and
/// Noto Sans Regular handles Cyrillic / Greek / extended Latin
/// coverage so measurements of Russian / Greek strings don't fall off
/// the Liberation glyph set.
const LIBERATION_SANS_REGULAR: &[u8] = include_bytes!("../fonts/LiberationSans-Regular.ttf");
const LIBERATION_SANS_BOLD: &[u8] = include_bytes!("../fonts/LiberationSans-Bold.ttf");
const LIBERATION_SANS_ITALIC: &[u8] = include_bytes!("../fonts/LiberationSans-Italic.ttf");
const LIBERATION_SANS_BOLD_ITALIC: &[u8] = include_bytes!("../fonts/LiberationSans-BoldItalic.ttf");
const LIBERATION_SERIF_REGULAR: &[u8] = include_bytes!("../fonts/LiberationSerif-Regular.ttf");
const LIBERATION_SERIF_BOLD: &[u8] = include_bytes!("../fonts/LiberationSerif-Bold.ttf");
const LIBERATION_SERIF_ITALIC: &[u8] = include_bytes!("../fonts/LiberationSerif-Italic.ttf");
const LIBERATION_SERIF_BOLD_ITALIC: &[u8] =
    include_bytes!("../fonts/LiberationSerif-BoldItalic.ttf");
const LIBERATION_MONO_REGULAR: &[u8] = include_bytes!("../fonts/LiberationMono-Regular.ttf");
const LIBERATION_MONO_BOLD: &[u8] = include_bytes!("../fonts/LiberationMono-Bold.ttf");
const LIBERATION_MONO_ITALIC: &[u8] = include_bytes!("../fonts/LiberationMono-Italic.ttf");
const LIBERATION_MONO_BOLD_ITALIC: &[u8] = include_bytes!("../fonts/LiberationMono-BoldItalic.ttf");
const DEJAVU_SANS: &[u8] = include_bytes!("../fonts/DejaVuSans.ttf");
const NOTO_SANS_REGULAR: &[u8] = include_bytes!("../fonts/NotoSans-Regular.ttf");

/// Number of faces we bundle and advertise via `document.fonts.size`.
/// Keep in sync with the `load_font_data` calls in `init_bundled`.
pub const BUNDLED_FACE_COUNT: usize = 14;

impl FontDatabase {
    pub fn get() -> &'static FontDatabase {
        static INSTANCE: OnceLock<FontDatabase> = OnceLock::new();
        INSTANCE.get_or_init(Self::init_bundled)
    }

    fn init_bundled() -> FontDatabase {
        let mut db = Database::new();
        // Load bundled faces as binary sources so fontdb can hand back
        // the raw bytes later via `face()`. The order matters for
        // default resolution — the first matching face wins when
        // multiple families carry the same name.
        db.load_font_data(LIBERATION_SANS_REGULAR.to_vec());
        db.load_font_data(LIBERATION_SANS_BOLD.to_vec());
        db.load_font_data(LIBERATION_SANS_ITALIC.to_vec());
        db.load_font_data(LIBERATION_SANS_BOLD_ITALIC.to_vec());
        db.load_font_data(LIBERATION_SERIF_REGULAR.to_vec());
        db.load_font_data(LIBERATION_SERIF_BOLD.to_vec());
        db.load_font_data(LIBERATION_SERIF_ITALIC.to_vec());
        db.load_font_data(LIBERATION_SERIF_BOLD_ITALIC.to_vec());
        db.load_font_data(LIBERATION_MONO_REGULAR.to_vec());
        db.load_font_data(LIBERATION_MONO_BOLD.to_vec());
        db.load_font_data(LIBERATION_MONO_ITALIC.to_vec());
        db.load_font_data(LIBERATION_MONO_BOLD_ITALIC.to_vec());
        db.load_font_data(DEJAVU_SANS.to_vec());
        db.load_font_data(NOTO_SANS_REGULAR.to_vec());

        // Chrome-on-Linux family aliases. When a site asks for Arial
        // (which is a Microsoft-licensed face we don't bundle), Linux
        // Chrome falls back to Liberation Sans under the hood. Matching
        // that alias chain here means `measureText("W", "16px Arial")`
        // gets realistic widths out of the box.
        //
        // Chrome's `sans-serif` / `serif` / `monospace` generics also
        // route through fontconfig on Linux to Liberation. We mirror
        // that explicitly rather than trusting fontdb's defaults.
        db.set_sans_serif_family("Liberation Sans");
        db.set_serif_family("Liberation Serif");
        db.set_monospace_family("Liberation Mono");
        db.set_cursive_family("Liberation Sans");
        db.set_fantasy_family("Liberation Sans");

        FontDatabase { inner: db }
    }

    /// Look up a face by family name + weight + italic style. Uses
    /// fontdb's selection algorithm which includes weight-closeness and
    /// style-closeness fallbacks.
    pub fn query(&self, family: &str, weight: u16, italic: bool, os_name: &str) -> Option<ID> {
        let style = if italic { Style::Italic } else { Style::Normal };
        let families = resolve_family(family, os_name);
        let query = Query {
            families: &families,
            weight: Weight(weight),
            stretch: Stretch::Normal,
            style,
        };
        if let Some(id) = self.inner.query(&query) {
            return Some(id);
        }
        // Final fallback: sans-serif generic. We configured this to
        // point at Liberation Sans above so a misspelled family or an
        // exotic one still renders something plausible.
        let fallback = [Family::SansSerif];
        self.inner.query(&Query {
            families: &fallback,
            weight: Weight(weight),
            stretch: Stretch::Normal,
            style,
        })
    }

    /// Strict per-family lookup that does NOT fall back to sans-serif.
    /// Used by `query_chain` so the user-supplied fallback chain is
    /// honoured — `("Wingdings", "serif")` must reach `serif` instead of
    /// short-circuiting on Wingdings's outer fallback.
    fn query_strict(&self, family: &str, weight: u16, italic: bool, os_name: &str) -> Option<ID> {
        let style = if italic { Style::Italic } else { Style::Normal };
        let families = resolve_family(family, os_name);
        self.inner.query(&Query {
            families: &families,
            weight: Weight(weight),
            stretch: Stretch::Normal,
            style,
        })
    }

    /// First-match query across a family fallback chain. Tries each
    /// user-specified family in order; only after the entire chain is
    /// exhausted does the global sans-serif fallback fire.
    pub fn query_chain(
        &self,
        families: &[String],
        weight: u16,
        italic: bool,
        os_name: &str,
    ) -> Option<ID> {
        for fam in families {
            if let Some(id) = self.query_strict(fam, weight, italic, os_name) {
                return Some(id);
            }
        }
        // Whole chain unresolvable → final fallback to sans-serif so
        // shaping still renders something plausible (e.g. all-emoji
        // strings on a build without an emoji face).
        let style = if italic { Style::Italic } else { Style::Normal };
        let fallback = [Family::SansSerif];
        self.inner.query(&Query {
            families: &fallback,
            weight: Weight(weight),
            stretch: Stretch::Normal,
            style,
        })
    }

    /// Return the raw face bytes + face index for a given face ID.
    /// Returns `None` for faces backed by a file source (we only ever
    /// load binary sources, so this is effectively infallible, but the
    /// `fontdb::Source` enum forces us to handle both).
    pub fn face_data(&self, id: ID) -> Option<(&[u8], u32)> {
        let face = self.inner.face(id)?;
        // `init_bundled` only loads binary sources, so `face.source`
        // is always `Binary`. The destructure is still necessary to
        // extract the Arc'd bytes.
        let fontdb::Source::Binary(data) = &face.source;
        // `data` is an `Arc<dyn AsRef<[u8]> + Send + Sync>`. The Arc's
        // contents are immutable for the process lifetime, so the
        // slice is safe to hand out with the database's lifetime.
        let slice: &[u8] = (**data).as_ref();
        Some((slice, face.index))
    }
}

/// Resolve a CSS family name to a list of fontdb `Family` entries,
/// handling generic keywords and Chrome-style aliases.
///
/// Unknown families return ONLY the literal name — no implicit
/// `Family::SansSerif` fallback. The outer fallback path (in `query`)
/// catches the case where the user-supplied chain is fully
/// unresolvable; `query_chain` relies on the per-family failure to
/// proceed to the user's next explicit fallback (e.g. `"Wingdings",
/// serif` should resolve to a serif face, not silently to sans-serif).
/// Without this, challenge-vendor-style font detection probes
/// (`Math.abs(measure("X, serif") - measure("serif")) > 0`) would
/// register every unknown family as installed.
/// Real-Chrome (macOS) vertical metrics per family:
/// (name, DOM line-height @100px, DOM line-height @16px,
///  canvas fontBoundingBoxAscent @100px, fontBoundingBoxDescent @100px).
/// Our bundled stand-in faces have different vertical metrics, so line
/// boxes and canvas font boxes use these instead.
const MACOS_VMETRICS: &[(&str, f32, f32, f32, f32)] = &[
    ("serif", 115.0000f32, 18.5000f32, 90.0000f32, 25.0000f32),
    ("sans-serif", 115.0000f32, 18.5000f32, 92.0000f32, 23.0000f32),
    ("monospace", 115.0000f32, 18.5000f32, 90.0000f32, 25.0000f32),
    ("system-ui", 117.5000f32, 19.0000f32, 96.7000f32, 21.1000f32),
    ("arial unicode ms", 134.0000f32, 21.5000f32, 106.9000f32, 27.1000f32),
    ("gill sans", 115.0000f32, 18.0000f32, 91.8000f32, 23.0500f32),
    ("helvetica neue", 116.5000f32, 18.5000f32, 95.2000f32, 21.3000f32),
    ("menlo", 116.5000f32, 19.0000f32, 92.8000f32, 23.6000f32),
    ("arial", 111.5000f32, 18.0000f32, 90.5500f32, 21.2000f32),
    ("arial black", 141.0000f32, 22.5000f32, 110.0500f32, 30.9500f32),
    ("arial narrow", 114.5000f32, 18.5000f32, 93.5500f32, 21.2000f32),
    ("arial rounded mt bold", 115.5000f32, 18.5000f32, 94.6500f32, 21.1000f32),
    ("helvetica", 115.0000f32, 18.5000f32, 92.0000f32, 23.0000f32),
    ("times", 115.0000f32, 18.5000f32, 90.0000f32, 25.0000f32),
    ("times new roman", 110.5000f32, 18.0000f32, 89.1000f32, 21.6500f32),
    ("courier", 115.0000f32, 18.5000f32, 90.4000f32, 24.6000f32),
    ("courier new", 113.5000f32, 18.5000f32, 83.2500f32, 30.0500f32),
    ("georgia", 113.5000f32, 18.0000f32, 91.7000f32, 21.9000f32),
    ("verdana", 121.5000f32, 19.5000f32, 100.5500f32, 21.0000f32),
    ("tahoma", 120.5000f32, 19.5000f32, 100.0500f32, 20.6500f32),
    ("trebuchet ms", 116.0000f32, 18.5000f32, 93.9000f32, 22.2000f32),
    ("impact", 122.0000f32, 19.5000f32, 100.9000f32, 21.1000f32),
    ("comic sans ms", 139.0000f32, 22.0000f32, 110.2000f32, 29.1500f32),
    ("palatino", 110.0000f32, 17.5000f32, 82.3000f32, 27.7500f32),
    ("optima", 119.0000f32, 19.0000f32, 91.9000f32, 26.8000f32),
    ("futura", 130.0000f32, 20.5000f32, 103.8500f32, 26.0000f32),
    ("avenir", 136.5000f32, 22.0000f32, 100.0000f32, 36.6000f32),
    ("avenir next", 136.5000f32, 22.0000f32, 100.0000f32, 36.6000f32),
    ("baskerville", 114.5000f32, 18.5000f32, 89.8000f32, 24.6000f32),
    ("didot", 124.0000f32, 20.0000f32, 94.1000f32, 29.9000f32),
    ("american typewriter", 115.5000f32, 18.5000f32, 90.4000f32, 25.0000f32),
    ("andale mono", 112.5000f32, 18.0000f32, 90.7000f32, 21.8000f32),
    ("monaco", 125.0000f32, 20.0000f32, 100.0000f32, 25.0000f32),
    ("geneva", 125.0000f32, 20.0000f32, 100.0000f32, 25.0000f32),
    ("lucida grande", 117.5000f32, 19.0000f32, 96.7000f32, 21.1000f32),
    ("hoefler text", 100.0000f32, 16.0000f32, 72.1000f32, 27.9000f32),
    ("copperplate", 101.5000f32, 16.0000f32, 76.3000f32, 24.8000f32),
    ("papyrus", 154.5000f32, 24.5000f32, 94.0000f32, 60.3000f32),
    ("chalkboard", 126.5000f32, 20.0000f32, 98.0000f32, 28.3000f32),
    ("marker felt", 109.0000f32, 17.5000f32, 86.8000f32, 21.8000f32),
    ("noteworthy", 160.0000f32, 25.5000f32, 128.0000f32, 32.0000f32),
    ("skia", 100.0000f32, 16.0000f32, 77.7000f32, 22.3000f32),
    ("zapfino", 338.0000f32, 54.0000f32, 187.5000f32, 150.2500f32),
    ("apple chancery", 158.5000f32, 25.5000f32, 111.7500f32, 46.5000f32),
    ("big caslon", 119.0000f32, 19.0000f32, 93.4000f32, 25.7000f32),
    ("bodoni 72", 120.0000f32, 19.5000f32, 93.6000f32, 26.6000f32),
    ("brush script mt", 123.0000f32, 19.5000f32, 88.8500f32, 33.8000f32),
    ("cochin", 114.5000f32, 18.5000f32, 89.7000f32, 25.0000f32),
    ("herculanum", 100.0000f32, 16.0000f32, 79.5000f32, 20.5000f32),
    ("iowan old style", 136.5000f32, 22.0000f32, 103.6000f32, 32.9000f32),
    ("charter", 122.0000f32, 19.5000f32, 98.0000f32, 24.0000f32),
    ("rockwell", 100.0000f32, 16.0000f32, 67.9000f32, 32.1000f32),
    ("seravek", 122.5000f32, 20.0000f32, 92.5000f32, 30.2000f32),
    ("superclarendon", 120.5000f32, 19.0000f32, 98.0000f32, 22.5000f32),
    ("trattatello", 181.0000f32, 29.0000f32, 115.0000f32, 66.2000f32),
    ("phosphate", 120.0000f32, 19.0000f32, 94.0000f32, 26.0000f32),
    ("luminari", 134.0000f32, 21.0000f32, 98.3000f32, 35.6000f32),
    ("kefa", 120.0000f32, 19.0000f32, 98.0000f32, 22.0000f32),
    ("pingfang sc", 140.0000f32, 22.5000f32, 106.0000f32, 34.0000f32),
    ("hiragino sans", 100.0000f32, 16.0000f32, 88.0000f32, 12.0000f32),
    ("apple sd gothic neo", 120.0000f32, 19.5000f32, 90.0000f32, 30.0000f32),
    ("symbol", 100.0000f32, 16.0000f32, 70.1000f32, 29.9000f32),
    ("webdings", 100.0000f32, 16.0000f32, 80.0000f32, 20.0000f32),
    ("wingdings", 111.0000f32, 18.0000f32, 89.9000f32, 21.1000f32),
    ("stixgeneral", 151.0000f32, 24.5000f32, 105.5000f32, 45.5000f32),
    ("pt sans", 117.5000f32, 19.0000f32, 90.0000f32, 27.6000f32),
    ("pt serif", 129.5000f32, 21.0000f32, 101.8000f32, 27.6000f32),
    ("pt mono", 112.0000f32, 18.0000f32, 88.5000f32, 23.5000f32),
    ("stix two text", 100.0000f32, 16.0000f32, 76.2000f32, 23.8000f32),
];

/// Vertical metrics for the first family in `families` that resolves on
/// macOS: (dom_line_height_px, canvas_font_ascent_px, canvas_font_descent_px).
pub fn mac_vmetrics(families: &[String], size: f32, os_name: &str) -> Option<(f32, f32, f32)> {
    if os_name != "macOS" {
        return None;
    }
    for f in families {
        let l = f.to_ascii_lowercase();
        let key = match l.as_str() {
            "-apple-system" | "blinkmacsystemfont" => "system-ui",
            "ui-monospace" => "monospace",
            other => other,
        };
        if let Some(&(_, h100, h16, a, d)) = MACOS_VMETRICS.iter().find(|r| r.0 == key) {
            let lh = if (size - 16.0).abs() < 0.01 { h16 } else { h100 * size / 100.0 };
            return Some((lh, (a * size / 100.0).round(), (d * size / 100.0).round()));
        }
        // Installed family without captured metrics: Chrome would use it,
        // so don't fall through to the next family's metrics.
        if MACOS_FAMILIES.iter().any(|(n, _)| *n == key) {
            return None;
        }
    }
    None
}

/// Font families present on a stock macOS install, as detected by real
/// Chrome (width-probe vs monospace/sans-serif/serif). Category picks the
/// bundled stand-in face: A = sans, S = serif, M = mono.
const MACOS_FAMILIES: &[(&str, u8)] = &[
    ("arial unicode ms", b'A'),
    ("gill sans", b'A'),
    ("helvetica neue", b'A'),
    ("menlo", b'M'),
    ("arial", b'A'),
    ("arial black", b'A'),
    ("arial narrow", b'A'),
    ("arial rounded mt bold", b'A'),
    ("helvetica", b'A'),
    ("times", b'S'),
    ("times new roman", b'S'),
    ("courier", b'M'),
    ("courier new", b'M'),
    ("georgia", b'S'),
    ("verdana", b'A'),
    ("tahoma", b'A'),
    ("trebuchet ms", b'A'),
    ("impact", b'A'),
    ("comic sans ms", b'A'),
    ("palatino", b'S'),
    ("optima", b'A'),
    ("futura", b'A'),
    ("avenir", b'A'),
    ("avenir next", b'A'),
    ("baskerville", b'S'),
    ("didot", b'S'),
    ("american typewriter", b'A'),
    ("andale mono", b'M'),
    ("monaco", b'M'),
    ("geneva", b'A'),
    ("lucida grande", b'A'),
    ("hoefler text", b'S'),
    ("copperplate", b'S'),
    ("papyrus", b'A'),
    ("chalkboard", b'A'),
    ("marker felt", b'A'),
    ("noteworthy", b'A'),
    ("skia", b'A'),
    ("zapfino", b'A'),
    ("apple chancery", b'S'),
    ("big caslon", b'S'),
    ("bodoni 72", b'S'),
    ("brush script mt", b'A'),
    ("cochin", b'S'),
    ("herculanum", b'S'),
    ("iowan old style", b'S'),
    ("charter", b'S'),
    ("rockwell", b'S'),
    ("seravek", b'A'),
    ("superclarendon", b'S'),
    ("trattatello", b'S'),
    ("phosphate", b'A'),
    ("luminari", b'A'),
    ("kefa", b'A'),
    ("pingfang sc", b'A'),
    ("hiragino sans", b'A'),
    ("apple sd gothic neo", b'A'),
    ("symbol", b'A'),
    ("webdings", b'A'),
    ("wingdings", b'A'),
    ("stixgeneral", b'S'),
    ("al bayan", b'A'),
    ("ayuthaya", b'A'),
    ("farah", b'A'),
    ("gurmukhi mn", b'A'),
    ("mshtakan", b'A'),
    ("sathu", b'A'),
    ("silom", b'A'),
    ("thonburi", b'A'),
    ("wingdings 2", b'A'),
    ("wingdings 3", b'A'),
    ("bradley hand", b'A'),
    ("chalkduster", b'A'),
    ("snell roundhand", b'A'),
    ("savoye let", b'A'),
    ("party let", b'A'),
    ("plantagenet cherokee", b'A'),
    ("euphemia ucas", b'A'),
    ("krungthep", b'A'),
    ("gujarati sangam mn", b'A'),
    ("stsong", b'S'),
    ("songti sc", b'S'),
    ("heiti sc", b'A'),
    ("apple color emoji", b'A'),
    ("apple symbols", b'A'),
    ("academy engraved let", b'A'),
    ("bangla mn", b'A'),
    ("diwan kufi", b'A'),
    ("mishafi", b'A'),
    ("sana", b'A'),
    ("tamil mn", b'A'),
    ("telugu mn", b'A'),
    ("inaimathi", b'A'),
    ("khmer mn", b'A'),
    ("lao mn", b'A'),
    ("malayalam mn", b'A'),
    ("myanmar mn", b'A'),
    ("oriya mn", b'A'),
    ("sinhala mn", b'A'),
    ("kannada mn", b'A'),
    ("devanagari mt", b'A'),
    ("gujarati mt", b'A'),
    ("corsiva hebrew", b'A'),
    ("raanana", b'A'),
    ("arial hebrew", b'A'),
    ("nadeem", b'A'),
    ("waseem", b'A'),
    ("muna", b'A'),
    ("baghdad", b'A'),
    ("beirut", b'A'),
    ("decotype naskh", b'A'),
    ("kufistandardgk", b'A'),
    ("athelas", b'S'),
    ("avenir next condensed", b'A'),
    ("marion", b'S'),
    ("sukhumvit set", b'A'),
    ("hiragino mincho pron", b'S'),
    ("hiragino maru gothic pron", b'A'),
    ("applemyungjo", b'S'),
    ("applegothic", b'A'),
    ("mukta mahee", b'A'),
    ("kohinoor devanagari", b'A'),
    ("pt sans", b'A'),
    ("pt serif", b'S'),
    ("pt mono", b'M'),
    ("stix two text", b'S'),
];

fn resolve_family<'a>(name: &'a str, os_name: &str) -> Vec<Family<'a>> {
    let lower = name.to_ascii_lowercase();
    match lower.as_str() {
        "sans-serif" | "system-ui" | "-apple-system" | "blinkmacsystemfont" => return vec![Family::SansSerif],
        "serif" => return vec![Family::Serif],
        "monospace" | "ui-monospace" => return vec![Family::Monospace],
        "cursive" => return vec![Family::Cursive],
        "fantasy" => return vec![Family::Fantasy],
        _ => {}
    }
    if os_name == "macOS" {
        // Only families a real Mac has resolve; everything else must stay
        // unresolvable so font-probe detectors see it as absent.
        return match MACOS_FAMILIES.iter().find(|(n, _)| *n == lower) {
            Some((_, b'S')) => vec![Family::Name("Liberation Serif"), Family::Serif],
            Some((_, b'M')) => vec![Family::Name("Liberation Mono"), Family::Monospace],
            Some(_) => vec![Family::Name("Liberation Sans"), Family::SansSerif],
            None => vec![Family::Name(name)],
        };
    }
    // Chrome substitution table for families we don't bundle (Windows/Linux).
    match lower.as_str() {
        "arial" | "helvetica" | "helvetica neue" | "tahoma" | "verdana" | "segoe ui"
        | "calibri" => {
            if os_name == "Windows" || os_name == "Linux" {
                vec![Family::Name("Liberation Sans"), Family::SansSerif]
            } else {
                vec![Family::Name(name)]
            }
        }
        "times" | "times new roman" | "georgia" => {
            if os_name == "Windows" || os_name == "Linux" {
                vec![Family::Name("Liberation Serif"), Family::Serif]
            } else {
                vec![Family::Name(name)]
            }
        }
        "courier" | "courier new" | "consolas" | "menlo" | "monaco" => {
            if os_name == "Windows" || os_name == "Linux" {
                vec![Family::Name("Liberation Mono"), Family::Monospace]
            } else {
                vec![Family::Name(name)]
            }
        }
        _ => vec![Family::Name(name)],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_sans_serif_generic() {
        let db = FontDatabase::get();
        assert!(db.query("sans-serif", 400, false, "Linux").is_some());
    }

    #[test]
    fn resolves_arial_alias_to_liberation_sans() {
        let db = FontDatabase::get();
        let arial_id = db
            .query("Arial", 400, false, "Linux")
            .expect("Arial should resolve");
        let libsans_id = db
            .query("Liberation Sans", 400, false, "Linux")
            .expect("Liberation Sans should resolve");
        assert_eq!(
            arial_id, libsans_id,
            "Arial alias should map to the bundled Liberation Sans face"
        );
    }

    #[test]
    fn resolves_times_alias_to_liberation_serif() {
        let db = FontDatabase::get();
        let id = db
            .query("Times New Roman", 400, false, "Linux")
            .expect("Times should resolve");
        let libserif_id = db
            .query("Liberation Serif", 400, false, "Linux")
            .expect("Liberation Serif should resolve");
        assert_eq!(id, libserif_id);
    }

    #[test]
    fn resolves_bold_weight() {
        let db = FontDatabase::get();
        let reg = db.query("Arial", 400, false, "Linux").unwrap();
        let bold = db.query("Arial", 700, false, "Linux").unwrap();
        assert_ne!(
            reg, bold,
            "regular and bold Arial should map to different faces"
        );
    }

    #[test]
    fn fallback_when_family_unknown() {
        let db = FontDatabase::get();
        // Should still return something via the sans-serif fallback.
        assert!(db
            .query("NonexistentFamily42", 400, false, "Linux")
            .is_some());
    }

    #[test]
    fn face_data_returns_bytes() {
        let db = FontDatabase::get();
        let id = db.query("Arial", 400, false, "Linux").unwrap();
        let (bytes, _idx) = db.face_data(id).expect("bundled face has binary source");
        assert!(
            bytes.len() > 1000,
            "face bytes suspiciously small: {}",
            bytes.len()
        );
        // TTF magic is either 0x00010000 (TrueType) or "OTTO" (CFF).
        assert!(
            matches!(&bytes[0..4], b"\x00\x01\x00\x00" | b"OTTO" | b"true"),
            "not a TTF: first 4 bytes = {:?}",
            &bytes[0..4]
        );
    }

    #[test]
    fn resolves_italic_sans() {
        let db = FontDatabase::get();
        let reg = db.query("Arial", 400, false, "Linux").unwrap();
        let italic = db.query("Arial", 400, true, "Linux").unwrap();
        assert_ne!(
            reg, italic,
            "italic Arial should map to Liberation Sans Italic"
        );
    }

    #[test]
    fn resolves_bold_italic_serif() {
        let db = FontDatabase::get();
        let reg = db.query("Times New Roman", 400, false, "Linux").unwrap();
        let bi = db.query("Times New Roman", 700, true, "Linux").unwrap();
        assert_ne!(
            reg, bi,
            "bold-italic Times should map to Liberation Serif Bold Italic"
        );
    }

    #[test]
    fn bundled_face_count_matches_advertised() {
        // Sanity check against the constant document.fonts.size reports.
        let db = FontDatabase::get();
        let loaded = db.inner.faces().count();
        assert_eq!(
            loaded, BUNDLED_FACE_COUNT,
            "fontdb loaded {} faces, advertised {}",
            loaded, BUNDLED_FACE_COUNT
        );
    }

    #[test]
    fn query_chain_walks_fallback() {
        let db = FontDatabase::get();
        let chain = vec![
            "NonexistentA".to_string(),
            "NonexistentB".to_string(),
            "Arial".to_string(),
        ];
        let id = db.query_chain(&chain, 400, false, "Linux").unwrap();
        let arial_id = db.query("Arial", 400, false, "Linux").unwrap();
        assert_eq!(id, arial_id);
    }
}
