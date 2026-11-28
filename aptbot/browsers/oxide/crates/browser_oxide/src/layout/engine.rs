use crate::canvas::text::font_shorthand::ParsedFont;
use crate::css_cascade::ComputedStyle;
use crate::css_values::property::{CssValue, PropertyId};
use crate::css_values::types::display::Display;
use crate::dom::node::{NodeData, NodeId};
use crate::dom::Dom;
use crate::layout::query::DOMRect;
use crate::layout::resolve::ResolveContext;
use crate::layout::style_map::computed_to_taffy;
use crate::layout::viewport::Viewport;
use std::collections::{HashMap, HashSet};
use taffy::prelude::*;

/// Step limit for the iterative DOM walk in `build_node`. A correct DOM has
/// at most `nodes.len()` unique ids; if the walker takes more steps than this
/// it is iterating a cycle and we panic with a clear message rather than
/// running until OS abort.
const LAYOUT_BUILD_LIMIT: usize = 100_000;

/// Inherited text properties (CSS inheritance for the font/white-space
/// subset that drives text measurement).
#[derive(Clone, Debug)]
struct TextStyle {
    size: f32,
    families: Vec<String>,
    weight: u16,
    italic: bool,
    nowrap: bool,
    /// Explicit line-height in px (None = `normal` → font metrics).
    line_height: Option<f32>,
    /// Cumulative CSS `zoom` factor (product of ancestors' zoom).
    zoom: f32,
    /// Size derives from the `medium` default (no explicit absolute size):
    /// Chrome then uses 13px instead of 16px for generic `monospace`.
    size_default: bool,
}

impl TextStyle {
    fn eff_size(&self) -> f32 {
        let mono = self.families.len() == 1 && self.families[0].eq_ignore_ascii_case("monospace");
        if mono && self.size_default { self.size * 13.0 / 16.0 } else { self.size }
    }
}

impl Default for TextStyle {
    fn default() -> Self {
        Self {
            size: 16.0,
            families: vec!["Times New Roman".to_string()],
            weight: 400,
            italic: false,
            nowrap: false,
            line_height: None,
            zoom: 1.0,
            size_default: true,
        }
    }
}

/// Display class after applying UA defaults.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Kind {
    None,
    Block,
    Inline,
    InlineBlock,
    Flex,
    Grid,
    Br,
}

/// Per-node info computed top-down (pre-order) so inheritance works in the
/// post-order taffy build.
struct NodeInfo {
    kind: Kind,
    text: TextStyle,
    style: Option<ComputedStyle>,
}

/// Measure context attached to text leaves.
pub struct TextLeaf {
    text: String,
    font: ParsedFont,
    nowrap: bool,
    line_height: f32,
}

/// The layout engine. Converts a DOM + styles into positioned elements.
pub struct LayoutEngine {
    tree: TaffyTree<TextLeaf>,
    dom_to_taffy: HashMap<u32, taffy::NodeId>,
    viewport: Viewport,
    dirty: bool,
    root_taffy: Option<taffy::NodeId>,
    os_name: String,
    info: HashMap<u32, NodeInfo>,
    /// Viewport sizes for detached roots that act as frame documents.
    root_viewports: HashMap<u32, (f32, f32)>,
}

fn ua_kind(tag: &str) -> Kind {
    match tag {
        "head" | "script" | "style" | "meta" | "link" | "title" | "template" | "noscript"
        | "base" | "param" | "datalist" | "source" | "track" => Kind::None,
        "br" => Kind::Br,
        "span" | "a" | "b" | "i" | "em" | "strong" | "small" | "big" | "code" | "kbd"
        | "samp" | "var" | "abbr" | "cite" | "q" | "sub" | "sup" | "label" | "font" | "u"
        | "s" | "strike" | "mark" | "time" | "data" | "bdi" | "bdo" | "tt" | "wbr" | "ins"
        | "del" | "dfn" | "nobr" | "slot"
        // MathML presentation elements lay out inline (math is display:inline).
        | "math" | "mrow" | "mi" | "mo" | "mn" | "ms" | "mtext" | "mspace" | "msup" | "msub" | "msubsup"
        | "munder" | "mover" | "munderover" | "mmultiscripts" | "mprescripts" | "none" | "mfrac"
        | "msqrt" | "mroot" | "mstyle" | "mpadded" | "mphantom" | "menclose" | "semantics" => Kind::Inline,
        "img" | "input" | "button" | "select" | "textarea" | "canvas" | "svg" | "iframe"
        | "video" | "audio" | "object" | "embed" | "meter" | "progress" => Kind::InlineBlock,
        _ => Kind::Block,
    }
}

/// Split a style attribute into lower-cased (name, value) pairs.
fn decls(style: &str) -> Vec<(String, String)> {
    style
        .split(';')
        .filter_map(|d| {
            let (k, v) = d.split_once(':')?;
            let v = v.trim().trim_end_matches("!important").trim();
            Some((k.trim().to_ascii_lowercase(), v.to_string()))
        })
        .collect()
}

fn parse_len(v: &str, parent_px: f32, root_px: f32) -> Option<f32> {
    let v = v.trim().to_ascii_lowercase();
    let num = |s: &str| s.trim().parse::<f32>().ok();
    if let Some(n) = v.strip_suffix("px") { return num(n); }
    if let Some(n) = v.strip_suffix("rem") { return num(n).map(|x| x * root_px); }
    if let Some(n) = v.strip_suffix("em") { return num(n).map(|x| x * parent_px); }
    if let Some(n) = v.strip_suffix('%') { return num(n).map(|x| x * parent_px / 100.0); }
    if let Some(n) = v.strip_suffix("pt") { return num(n).map(|x| x * 4.0 / 3.0); }
    match v.as_str() {
        "xx-small" => Some(9.0),
        "x-small" => Some(10.0),
        "small" => Some(13.0),
        "medium" => Some(16.0),
        "large" => Some(18.0),
        "x-large" => Some(24.0),
        "xx-large" => Some(32.0),
        "xxx-large" => Some(48.0),
        "smaller" => Some(parent_px / 1.2),
        "larger" => Some(parent_px * 1.2),
        _ => num(&v).filter(|_| v == "0"),
    }
}

fn parse_families(v: &str) -> Vec<String> {
    v.split(',')
        .map(|f| f.trim().trim_matches(|c| c == '"' || c == '\'').to_string())
        .filter(|f| !f.is_empty())
        .collect()
}

fn ua_text(tag: &str, t: &mut TextStyle) {
    let em = t.size;
    match tag {
        "h1" => { t.size = em * 2.0; t.weight = 700; }
        "h2" => { t.size = em * 1.5; t.weight = 700; }
        "h3" => { t.size = em * 1.17; t.weight = 700; }
        "h4" => { t.weight = 700; }
        "h5" => { t.size = em * 0.83; t.weight = 700; }
        "h6" => { t.size = em * 0.67; t.weight = 700; }
        "b" | "strong" | "th" => t.weight = 700,
        "i" | "em" | "cite" | "var" | "dfn" | "address" => t.italic = true,
        "small" => t.size = em / 1.2,
        "big" => t.size = em * 1.2,
        "sub" | "sup" => t.size = em * 0.83,
        "code" | "kbd" | "samp" | "tt" | "pre" | "textarea" => {
            t.families = vec!["monospace".to_string()];
            // 13px comes from TextStyle::eff_size (generic monospace at a
            // medium-derived size), not from scaling here.
            let _ = em;
        }
        "input" | "button" | "select" => {
            t.size = 13.333;
            t.size_default = false;
            t.families = vec!["system-ui".to_string(), "sans-serif".to_string()];
            t.weight = 400;
            t.italic = false;
        }
        _ => {}
    }
    if matches!(tag, "pre" | "nobr" | "textarea") {
        t.nowrap = true;
    }
}

/// Collapse CSS `white-space: normal` runs to single spaces.
fn collapse_ws(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_ws = false;
    for ch in s.chars() {
        if ch.is_whitespace() {
            if !prev_ws {
                out.push(' ');
            }
            prev_ws = true;
        } else {
            out.push(ch);
            prev_ws = false;
        }
    }
    out
}

impl LayoutEngine {
    pub fn new(viewport: Viewport) -> Self {
        Self {
            tree: { let mut t = TaffyTree::new(); t.disable_rounding(); t },
            dom_to_taffy: HashMap::new(),
            viewport,
            dirty: true,
            root_taffy: None,
            os_name: "macOS".to_string(),
            info: HashMap::new(),
            root_viewports: HashMap::new(),
        }
    }

    /// Profile OS name used for font resolution (matches canvas text).
    pub fn set_os_name(&mut self, os: &str) {
        if self.os_name != os {
            self.os_name = os.to_string();
            self.dirty = true;
        }
    }

    /// Register the viewport of a detached subtree root that is an iframe
    /// document (`<html>` of an about:blank/srcdoc frame).
    pub fn set_root_viewport(&mut self, root: NodeId, w: f32, h: f32) {
        self.root_viewports.insert(root.to_raw(), (w, h));
        self.dirty = true;
    }

    /// Lay out the detached subtree that contains `node_id`, on demand.
    /// Nodes outside the main document (iframe documents, detached
    /// elements) still get real geometry relative to their own root.
    fn ensure_node(&mut self, dom: &Dom, node_id: NodeId) {
        self.ensure_computed(dom);
        if self.dom_to_taffy.contains_key(&node_id.to_raw()) {
            return;
        }
        // Find the root ancestor.
        let mut root = node_id;
        let mut guard = 0;
        while let Some(p) = dom.get(root).and_then(|n| n.parent) {
            root = p;
            guard += 1;
            if guard > LAYOUT_BUILD_LIMIT {
                return;
            }
        }
        if root == NodeId::DOCUMENT {
            return; // inside the document but not rendered (display:none)
        }
        let Some(&(vw, vh)) = self.root_viewports.get(&root.to_raw()) else {
            // Plain detached elements are not rendered: Chrome reports 0.
            return;
        };
        let ctx = ResolveContext {
            font_size: 16.0,
            root_font_size: 16.0,
            viewport_w: vw,
            viewport_h: vh,
        };
        // Wrap in a synthetic document box of the frame's width.
        let Some(r) = self.build_node(dom, root, &ctx) else { return };
        let doc_style = taffy::Style {
            display: taffy::Display::Block,
            size: taffy::Size { width: Dimension::length(vw), height: Dimension::auto() },
            ..Default::default()
        };
        let Ok(doc) = self.tree.new_with_children(doc_style, &[r]) else { return };
        let avail = taffy::Size {
            width: AvailableSpace::Definite(vw),
            height: AvailableSpace::Definite(vh),
        };
        let os = self.os_name.clone();
        let mut cache: HashMap<(String, u64), f32> = HashMap::new();
        self.tree
            .compute_layout_with_measure(doc, avail, |known, avail, _id, ctx, _style| {
                measure_text(known, avail, ctx, &os, &mut cache)
            })
            .ok();
    }

    /// Mark layout as dirty (needs recomputation).
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Compute layout for the entire DOM tree.
    pub fn compute(&mut self, dom: &Dom) {
        self.tree = { let mut t = TaffyTree::new(); t.disable_rounding(); t };
        self.dom_to_taffy.clear();
        self.info.clear();

        let ctx = ResolveContext {
            font_size: 16.0,
            root_font_size: 16.0,
            viewport_w: self.viewport.width,
            viewport_h: self.viewport.height,
        };

        let root = self.build_node(dom, NodeId::DOCUMENT, &ctx);
        self.root_taffy = root;

        if let Some(root_id) = self.root_taffy {
            let avail = taffy::Size {
                width: AvailableSpace::Definite(self.viewport.width),
                height: AvailableSpace::Definite(self.viewport.height),
            };
            let os = self.os_name.clone();
            let mut cache: HashMap<(String, u64), f32> = HashMap::new();
            self.tree
                .compute_layout_with_measure(root_id, avail, |known, avail, _id, ctx, _style| {
                    measure_text(known, avail, ctx, &os, &mut cache)
                })
                .ok();
        }

        self.dirty = false;
    }

    /// Ensure layout is computed (lazy).
    pub fn ensure_computed(&mut self, dom: &Dom) {
        if self.dirty {
            self.compute(dom);
        }
    }

    /// Get the bounding rect of a node.
    pub fn get_bounding_rect(&mut self, dom: &Dom, node_id: NodeId) -> DOMRect {
        self.ensure_node(dom, node_id);
        let taffy_id = match self.dom_to_taffy.get(&node_id.to_raw()) {
            Some(id) => *id,
            None => return DOMRect::default(),
        };
        let layout = match self.tree.layout(taffy_id) {
            Ok(l) => *l,
            Err(_) => return DOMRect::default(),
        };
        let (abs_x, abs_y) = self.absolute_position(taffy_id);
        DOMRect::new(
            abs_x as f64,
            abs_y as f64,
            layout.size.width as f64,
            layout.size.height as f64,
        )
    }

    /// Get offsetWidth (width including padding + border).
    pub fn get_offset_width(&mut self, dom: &Dom, node_id: NodeId) -> f64 {
        self.ensure_node(dom, node_id);
        self.taffy_size(node_id).0
    }

    /// Get offsetHeight.
    pub fn get_offset_height(&mut self, dom: &Dom, node_id: NodeId) -> f64 {
        self.ensure_node(dom, node_id);
        self.taffy_size(node_id).1
    }

    /// Get offsetTop (position relative to offsetParent).
    pub fn get_offset_top(&mut self, dom: &Dom, node_id: NodeId) -> f64 {
        self.ensure_node(dom, node_id);
        self.taffy_position(node_id).1
    }

    /// Get offsetLeft.
    pub fn get_offset_left(&mut self, dom: &Dom, node_id: NodeId) -> f64 {
        self.ensure_node(dom, node_id);
        self.taffy_position(node_id).0
    }

    // --- Internal ---

    /// Pre-order: compute kind + inherited text style for `node_id`.
    fn visit_info(&mut self, dom: &Dom, node_id: NodeId, parent: Option<NodeId>) {
        let ptext = parent
            .and_then(|p| self.info.get(&p.to_raw()))
            .map(|i| i.text.clone())
            .unwrap_or_default();
        let pkind_none = parent
            .and_then(|p| self.info.get(&p.to_raw()))
            .map(|i| i.kind == Kind::None)
            .unwrap_or(false);
        let Some(node) = dom.get(node_id) else { return };
        let info = match &node.data {
            NodeData::Element(elem) => {
                let tag = elem.name.local.to_ascii_lowercase();
                let mut kind = ua_kind(&tag);
                let mut text = ptext.clone();
                ua_text(&tag, &mut text);
                let style_attr = elem
                    .attrs
                    .iter()
                    .find(|a| a.name.local == "style")
                    .map(|a| a.value.clone())
                    .unwrap_or_default();
                if elem.attrs.iter().any(|a| a.name.local == "hidden") {
                    kind = Kind::None;
                }
                let mut style = None;
                if !style_attr.is_empty() {
                    for (k, v) in decls(&style_attr) {
                        let lv = v.to_ascii_lowercase();
                        match k.as_str() {
                            "display" => {
                                kind = match lv.as_str() {
                                    "none" => Kind::None,
                                    "inline" => Kind::Inline,
                                    "inline-block" | "inline-table" => Kind::InlineBlock,
                                    "flex" | "inline-flex" => Kind::Flex,
                                    "grid" | "inline-grid" => Kind::Grid,
                                    "contents" => kind,
                                    _ => Kind::Block,
                                }
                            }
                            "font-size" => {
                                if let Some(px) = parse_len(&lv, ptext.size, 16.0) {
                                    text.size = px;
                                    let rel = lv.ends_with("em") || lv.ends_with('%') || lv == "smaller" || lv == "larger";
                                    text.size_default = if lv == "medium" { true } else { rel && ptext.size_default };
                                }
                            }
                            "font-family" => {
                                let f = parse_families(&v);
                                if !f.is_empty() {
                                    text.families = f;
                                }
                            }
                            "font-weight" => {
                                text.weight = match lv.as_str() {
                                    "bold" | "bolder" => 700,
                                    "normal" | "lighter" => 400,
                                    n => n.parse().unwrap_or(text.weight),
                                }
                            }
                            "font-style" => text.italic = lv.starts_with("italic") || lv.starts_with("oblique"),
                            "white-space" | "text-wrap" | "text-wrap-mode" => {
                                text.nowrap = matches!(lv.as_str(), "nowrap" | "pre");
                            }
                            "line-height" => {
                                text.line_height = if lv == "normal" {
                                    None
                                } else if let Ok(n) = lv.parse::<f32>() {
                                    Some(n * text.size)
                                } else {
                                    parse_len(&lv, text.size, 16.0)
                                };
                            }
                            "zoom" => {
                                let z = if let Some(n) = lv.strip_suffix('%') {
                                    n.trim().parse::<f32>().ok().map(|x| x / 100.0)
                                } else if lv == "normal" || lv == "reset" {
                                    Some(1.0)
                                } else {
                                    lv.parse::<f32>().ok()
                                };
                                if let Some(z) = z.filter(|z| *z > 0.0) {
                                    text.zoom = ptext.zoom * z;
                                }
                            }
                            "font" => {
                                if let Some(pf) = ParsedFont::parse(&v) {
                                    text.size = pf.size_px;
                                    text.size_default = false;
                                    text.families = pf.families;
                                    text.weight = pf.weight;
                                    text.italic = pf.italic;
                                }
                            }
                            _ => {}
                        }
                    }
                    let map = self.parse_inline_style(elem);
                    style = Some(ComputedStyle::resolve(&map, None));
                }
                NodeInfo { kind, text, style }
            }
            NodeData::Text(_) => NodeInfo {
                kind: if pkind_none { Kind::None } else { Kind::Inline },
                text: ptext,
                style: None,
            },
            _ => NodeInfo { kind: Kind::Block, text: ptext, style: None },
        };
        // display:none hides descendants too.
        let info = if pkind_none { NodeInfo { kind: Kind::None, ..info } } else { info };
        self.info.insert(node_id.to_raw(), info);
    }

    fn build_node(
        &mut self,
        dom: &Dom,
        root: NodeId,
        ctx: &ResolveContext,
    ) -> Option<taffy::NodeId> {
        enum Work {
            Visit(NodeId, Option<NodeId>),
            Finish(NodeId),
        }
        let mut stack: Vec<Work> = vec![Work::Visit(root, None)];
        let mut visited: HashSet<NodeId> = HashSet::with_capacity(64);
        let mut steps: usize = 0;
        while let Some(work) = stack.pop() {
            match work {
                Work::Visit(node_id, parent) => {
                    if !visited.insert(node_id) {
                        continue;
                    }
                    steps += 1;
                    if steps > LAYOUT_BUILD_LIMIT {
                        panic!(
                            "Layout build cycle from {:?} — visited {} unique nodes",
                            root,
                            visited.len()
                        );
                    }
                    self.visit_info(dom, node_id, parent);
                    stack.push(Work::Finish(node_id));
                    let kids = dom.children(node_id);
                    for c in kids.into_iter().rev() {
                        stack.push(Work::Visit(c, Some(node_id)));
                    }
                }
                Work::Finish(node_id) => {
                    self.finish_node(dom, node_id, ctx);
                }
            }
        }
        self.dom_to_taffy.get(&root.to_raw()).copied()
    }

    /// Style of an (anonymous) line box: a wrapping row of inline items.
    fn line_box_style(&self, t: &TextStyle) -> taffy::Style {
        taffy::Style {
            display: taffy::Display::Flex,
            flex_direction: taffy::FlexDirection::Row,
            flex_wrap: if t.nowrap { taffy::FlexWrap::NoWrap } else { taffy::FlexWrap::Wrap },
            align_items: Some(taffy::AlignItems::FLEX_START),
            align_content: Some(taffy::AlignContent::FLEX_START),
            ..Default::default()
        }
    }

    fn kind_of(&self, id: NodeId) -> Kind {
        self.info.get(&id.to_raw()).map(|i| i.kind).unwrap_or(Kind::Block)
    }

    fn line_height_for(&self, t: &TextStyle) -> f32 {
        if let Some(lh) = t.line_height {
            return lh * t.zoom;
        }
        let font = to_font(t);
        if let Some(lh) = crate::canvas::text::normal_line_height(&font, &self.os_name) {
            return lh;
        }
        let m = crate::canvas::text::measure_text_metrics("x", &font, &self.os_name);
        let lh = m.font_bounding_box_ascent + m.font_bounding_box_descent;
        if lh > 0.0 { lh } else { t.eff_size() * t.zoom * 1.15 }
    }

    fn finish_node(&mut self, dom: &Dom, node_id: NodeId, base: &ResolveContext) {
        let node = match dom.get(node_id) {
            Some(n) => n,
            None => return,
        };
        let kind = self.kind_of(node_id);
        if kind == Kind::None {
            return;
        }
        let text_style = self.info.get(&node_id.to_raw()).map(|i| i.text.clone()).unwrap_or_default();

        // Children (already built) and whether this box holds inline content.
        let child_ids = dom.children(node_id);
        let has_inline = child_ids.iter().any(|c| {
            let k = self.kind_of(*c);
            if !self.dom_to_taffy.contains_key(&c.to_raw()) {
                return false;
            }
            match dom.get(*c).map(|n| &n.data) {
                Some(NodeData::Text(t)) => !t.trim().is_empty(),
                _ => matches!(k, Kind::Inline | Kind::InlineBlock | Kind::Br),
            }
        });
        let mut children: Vec<taffy::NodeId> = Vec::new();
        let mut kept: Vec<NodeId> = Vec::new();
        let last_idx = child_ids.len().saturating_sub(1);
        for (i, cid) in child_ids.iter().enumerate() {
            let Some(tid) = self.dom_to_taffy.get(&cid.to_raw()).copied() else { continue };
            if let Some(NodeData::Text(t)) = dom.get(*cid).map(|n| &n.data) {
                let ws_only = t.trim().is_empty();
                // Whitespace between blocks is not rendered; at the edges of an
                // inline run it collapses away.
                if ws_only && (!has_inline || i == 0 || i == last_idx) {
                    continue;
                }
            }
            children.push(tid);
            kept.push(*cid);
        }

        let taffy_id = match &node.data {
            NodeData::Document | NodeData::DocumentFragment => {
                let style = taffy::Style {
                    display: taffy::Display::Block,
                    size: taffy::Size {
                        width: Dimension::length(base.viewport_w),
                        height: Dimension::auto(),
                    },
                    ..Default::default()
                };
                match self.tree.new_with_children(style, &children) {
                    Ok(id) => id,
                    Err(_) => return,
                }
            }
            NodeData::Element(elem) => {
                let tag = elem.name.local.to_ascii_lowercase();
                let ctx = ResolveContext {
                    font_size: text_style.size,
                    root_font_size: base.root_font_size,
                    viewport_w: base.viewport_w,
                    viewport_h: base.viewport_h,
                };
                let computed = self
                    .info
                    .get(&node_id.to_raw())
                    .and_then(|i| i.style.clone())
                    .unwrap_or_else(|| ComputedStyle::resolve(&HashMap::new(), None));
                let mut ts = computed_to_taffy(&computed, &ctx);
                // `border-width` initial is `medium` (3px) but `border-style`
                // initial is `none`, which computes the used width to 0.
                // Only honour widths when the style declares a visible border.
                let style_txt = elem
                    .attrs
                    .iter()
                    .find(|a| a.name.local == "style")
                    .map(|a| a.value.to_ascii_lowercase())
                    .unwrap_or_default();
                // Border widths straight from the declarations (shorthand and
                // longhands, in order). Used width is 0 unless the side has a
                // visible border-style (initial style is `none`).
                {
                    let mut w = [3.0f32; 4]; // top right bottom left (initial: medium)
                    let mut st = [false; 4];
                    let em = text_style.size;
                    let bw = |t: &str| -> Option<f32> {
                        match t {
                            "thin" => Some(1.0),
                            "medium" => Some(3.0),
                            "thick" => Some(5.0),
                            _ => parse_len(t, em, 16.0),
                        }
                    };
                    let is_style = |t: &str| matches!(t, "none" | "hidden" | "solid" | "dashed" | "dotted" | "double" | "groove" | "ridge" | "inset" | "outset");
                    let visible = |t: &str| is_style(t) && !matches!(t, "none" | "hidden");
                    let sides = |n: &str| -> Vec<usize> {
                        match n { "top" => vec![0], "right" => vec![1], "bottom" => vec![2], "left" => vec![3], _ => vec![0, 1, 2, 3] }
                    };
                    for (k, v) in decls(&style_txt) {
                        let lv = v.to_ascii_lowercase();
                        let toks: Vec<&str> = lv.split_whitespace().collect();
                        let k = k.as_str();
                        if k == "border" || (k.starts_with("border-") && ["top", "right", "bottom", "left"].contains(&&k[7..])) {
                            let idx = if k == "border" { vec![0, 1, 2, 3] } else { sides(&k[7..]) };
                            let mut ww = 3.0; let mut ss = false;
                            for t in &toks { if is_style(t) { ss = visible(t); } else if let Some(x) = bw(t) { ww = x; } }
                            for i in idx { w[i] = ww; st[i] = ss; }
                        } else if k == "border-width" {
                            let vals: Vec<f32> = toks.iter().filter_map(|t| bw(t)).collect();
                            let q = match vals.len() { 1 => [vals[0]; 4], 2 => [vals[0], vals[1], vals[0], vals[1]],
                                3 => [vals[0], vals[1], vals[2], vals[1]], 4 => [vals[0], vals[1], vals[2], vals[3]], _ => w };
                            w = q;
                        } else if k == "border-style" {
                            let vals: Vec<bool> = toks.iter().filter(|t| is_style(t)).map(|t| visible(t)).collect();
                            st = match vals.len() { 1 => [vals[0]; 4], 2 => [vals[0], vals[1], vals[0], vals[1]],
                                3 => [vals[0], vals[1], vals[2], vals[1]], 4 => [vals[0], vals[1], vals[2], vals[3]], _ => st };
                        } else if let Some(rest) = k.strip_prefix("border-") {
                            if let Some(side) = rest.strip_suffix("-width") { if let Some(x) = toks.first().and_then(|t| bw(t)) { for i in sides(side) { w[i] = x; } } }
                            if let Some(side) = rest.strip_suffix("-style") { if let Some(t) = toks.first() { for i in sides(side) { st[i] = visible(t); } } }
                        }
                    }
                    // Chrome snaps border widths to device pixels (min 1 device px).
                    let dpr = 2.0f32;
                    let snap = |x: f32| if x <= 0.0 { 0.0 } else { ((x * dpr).floor().max(1.0)) / dpr };
                    let used: Vec<f32> = (0..4).map(|i| if st[i] { snap(w[i]) } else { 0.0 }).collect();
                    ts.border = taffy::Rect {
                        top: LengthPercentage::length(used[0]),
                        right: LengthPercentage::length(used[1]),
                        bottom: LengthPercentage::length(used[2]),
                        left: LengthPercentage::length(used[3]),
                    };
                }
                // UA margins.
                let has_margin = |p: &str| {
                    elem.attrs.iter().any(|a| a.name.local == "style" && a.value.contains(p))
                };
                if !has_margin("margin") {
                    let em = text_style.size;
                    let vm = match tag.as_str() {
                        "body" => Some((8.0, 8.0)),
                        "p" | "blockquote" | "pre" | "ul" | "ol" | "dl" | "figure" | "fieldset" => Some((em, 0.0)),
                        "h1" => Some((0.67 * em, 0.0)),
                        "h2" => Some((0.83 * em, 0.0)),
                        "h3" | "h4" => Some((em, 0.0)),
                        "h5" => Some((1.67 * em, 0.0)),
                        "h6" => Some((2.33 * em, 0.0)),
                        _ => None,
                    };
                    if let Some((v, h)) = vm {
                        let v = if tag == "body" { 8.0 } else { v };
                        ts.margin.top = LengthPercentageAuto::length(v);
                        ts.margin.bottom = LengthPercentageAuto::length(v);
                        if h > 0.0 {
                            ts.margin.left = LengthPercentageAuto::length(h);
                            ts.margin.right = LengthPercentageAuto::length(h);
                        }
                    }
                }
                // Replaced-element intrinsic sizes.
                let attr_px = |n: &str| {
                    elem.attrs
                        .iter()
                        .find(|a| a.name.local == n)
                        .and_then(|a| a.value.trim().trim_end_matches("px").parse::<f32>().ok())
                };
                match tag.as_str() {
                    "iframe" | "canvas" | "video" | "object" | "embed" => {
                        if ts.size.width == Dimension::auto() {
                            ts.size.width = Dimension::length(attr_px("width").unwrap_or(300.0));
                        }
                        if ts.size.height == Dimension::auto() {
                            ts.size.height = Dimension::length(attr_px("height").unwrap_or(150.0));
                        }
                    }
                    "img" => {
                        if ts.size.width == Dimension::auto() {
                            ts.size.width = Dimension::length(attr_px("width").unwrap_or(0.0));
                        }
                        if ts.size.height == Dimension::auto() {
                            ts.size.height = Dimension::length(attr_px("height").unwrap_or(0.0));
                        }
                    }
                    _ => {}
                }
                match kind {
                    Kind::Br => {
                        ts = taffy::Style {
                            flex_basis: Dimension::percent(1.0),
                            size: taffy::Size { width: Dimension::percent(1.0), height: Dimension::length(0.0) },
                            ..Default::default()
                        };
                    }
                    Kind::Inline => {
                        // Inline boxes shrink-wrap their content; width/height
                        // do not apply. Lay their content out as a wrapping row.
                        ts.display = taffy::Display::Flex;
                        ts.flex_direction = taffy::FlexDirection::Row;
                        ts.flex_wrap = if text_style.nowrap { taffy::FlexWrap::NoWrap } else { taffy::FlexWrap::Wrap };
                        ts.align_items = Some(taffy::AlignItems::FLEX_START);
                        ts.size = taffy::Size { width: Dimension::auto(), height: Dimension::auto() };
                        ts.flex_shrink = if text_style.nowrap { 0.0 } else { 1.0 };
                        ts.margin.top = LengthPercentageAuto::length(0.0);
                        ts.margin.bottom = LengthPercentageAuto::length(0.0);
                        if !has_inline && children.is_empty() {
                            // Empty inline still has a line-height strut.
                            ts.min_size.height = Dimension::length(self.line_height_for(&text_style));
                        }
                    }
                    Kind::InlineBlock | Kind::Block if has_inline => {
                        let has_block_child = kept.iter().any(|c| {
                            matches!(self.kind_of(*c), Kind::Block | Kind::Flex | Kind::Grid)
                        });
                        if has_block_child {
                            // CSS anonymous block boxes: wrap each run of
                            // inline-level children in an anonymous line box.
                            let mut out: Vec<taffy::NodeId> = Vec::new();
                            let mut run: Vec<taffy::NodeId> = Vec::new();
                            for (c, t) in kept.iter().zip(children.iter()) {
                                if matches!(self.kind_of(*c), Kind::Block | Kind::Flex | Kind::Grid) {
                                    if !run.is_empty() {
                                        if let Ok(anon) = self.tree.new_with_children(self.line_box_style(&text_style), &run) {
                                            out.push(anon);
                                        }
                                        run.clear();
                                    }
                                    out.push(*t);
                                } else {
                                    run.push(*t);
                                }
                            }
                            if !run.is_empty() {
                                if let Ok(anon) = self.tree.new_with_children(self.line_box_style(&text_style), &run) {
                                    out.push(anon);
                                }
                            }
                            children = out;
                        } else {
                            let lb = self.line_box_style(&text_style);
                            ts.display = lb.display;
                            ts.flex_direction = lb.flex_direction;
                            ts.flex_wrap = lb.flex_wrap;
                            ts.align_items = lb.align_items;
                            ts.align_content = lb.align_content;
                        }
                        if kind == Kind::InlineBlock {
                            ts.flex_shrink = 0.0;
                        }
                    }
                    Kind::InlineBlock => {
                        ts.flex_shrink = 0.0;
                    }
                    _ => {}
                }
                if (text_style.zoom - 1.0).abs() > f32::EPSILON {
                    zoom_style(&mut ts, text_style.zoom);
                }
                match self.tree.new_with_children(ts, &children) {
                    Ok(id) => id,
                    Err(_) => return,
                }
            }
            NodeData::Text(text) => {
                let collapsed = if text_style.nowrap && false { text.to_string() } else { collapse_ws(text) };
                let leaf = TextLeaf {
                    text: collapsed,
                    font: to_font(&text_style),
                    nowrap: text_style.nowrap,
                    line_height: self.line_height_for(&text_style),
                };
                let style = taffy::Style {
                    flex_shrink: if text_style.nowrap { 0.0 } else { 1.0 },
                    ..Default::default()
                };
                match self.tree.new_leaf_with_context(style, leaf) {
                    Ok(id) => id,
                    Err(_) => return,
                }
            }
            _ => return,
        };
        self.dom_to_taffy.insert(node_id.to_raw(), taffy_id);
    }

    fn parse_inline_style(
        &self,
        elem: &crate::dom::node::ElementData,
    ) -> HashMap<PropertyId, CssValue> {
        let mut map = HashMap::new();
        let style_attr = elem.attrs.iter().find(|a| a.name.local == "style");
        if let Some(attr) = style_attr {
            let (decls, _) = crate::css_parser::parse_declaration_list(&attr.value);
            for decl in &decls {
                if let Ok(props) =
                    crate::css_values::parse_property(decl.name, &decl.value, decl.important)
                {
                    for prop in props {
                        map.insert(prop.property, prop.value);
                    }
                }
            }
        }
        map
    }

    fn absolute_position(&self, taffy_id: taffy::NodeId) -> (f32, f32) {
        let mut x = 0.0f32;
        let mut y = 0.0f32;
        let mut current = taffy_id;
        loop {
            if let Ok(layout) = self.tree.layout(current) {
                x += layout.location.x;
                y += layout.location.y;
            }
            match self.tree.parent(current) {
                Some(parent) => current = parent,
                None => break,
            }
        }
        (x, y)
    }

    fn taffy_size(&self, node_id: NodeId) -> (f64, f64) {
        match self.dom_to_taffy.get(&node_id.to_raw()) {
            Some(taffy_id) => match self.tree.layout(*taffy_id) {
                Ok(layout) => (
                    crate::layout::layout_unit::LayoutUnit::from_taffy_f32(layout.size.width)
                        .to_f64_px(),
                    crate::layout::layout_unit::LayoutUnit::from_taffy_f32(layout.size.height)
                        .to_f64_px(),
                ),
                Err(_) => (0.0, 0.0),
            },
            None => (0.0, 0.0),
        }
    }

    fn taffy_position(&self, node_id: NodeId) -> (f64, f64) {
        match self.dom_to_taffy.get(&node_id.to_raw()) {
            Some(taffy_id) => match self.tree.layout(*taffy_id) {
                Ok(layout) => (
                    crate::layout::layout_unit::LayoutUnit::from_taffy_f32(layout.location.x)
                        .to_f64_px(),
                    crate::layout::layout_unit::LayoutUnit::from_taffy_f32(layout.location.y)
                        .to_f64_px(),
                ),
                Err(_) => (0.0, 0.0),
            },
            None => (0.0, 0.0),
        }
    }
}

/// Scale every absolute length in a taffy style by a CSS `zoom` factor.
fn zoom_style(ts: &mut taffy::Style, z: f32) {
    use taffy::CompactLength as CL;
    fn d(v: Dimension, z: f32) -> Dimension {
        let r = v.into_raw();
        if r.tag() == CL::LENGTH_TAG { Dimension::length(r.value() * z) } else { v }
    }
    fn lp(v: LengthPercentage, z: f32) -> LengthPercentage {
        let r = v.into_raw();
        if r.tag() == CL::LENGTH_TAG { LengthPercentage::length(r.value() * z) } else { v }
    }
    fn lpa(v: LengthPercentageAuto, z: f32) -> LengthPercentageAuto {
        let r = v.into_raw();
        if r.tag() == CL::LENGTH_TAG { LengthPercentageAuto::length(r.value() * z) } else { v }
    }
    ts.size.width = d(ts.size.width, z);
    ts.size.height = d(ts.size.height, z);
    ts.min_size.width = d(ts.min_size.width, z);
    ts.min_size.height = d(ts.min_size.height, z);
    ts.max_size.width = d(ts.max_size.width, z);
    ts.max_size.height = d(ts.max_size.height, z);
    ts.flex_basis = d(ts.flex_basis, z);
    for r in [&mut ts.margin.left, &mut ts.margin.right, &mut ts.margin.top, &mut ts.margin.bottom,
              &mut ts.inset.left, &mut ts.inset.right, &mut ts.inset.top, &mut ts.inset.bottom] {
        *r = lpa(*r, z);
    }
    for r in [&mut ts.padding.left, &mut ts.padding.right, &mut ts.padding.top, &mut ts.padding.bottom,
              &mut ts.border.left, &mut ts.border.right, &mut ts.border.top, &mut ts.border.bottom] {
        *r = lp(*r, z);
    }
    ts.gap.width = lp(ts.gap.width, z);
    ts.gap.height = lp(ts.gap.height, z);
}

fn to_font(t: &TextStyle) -> ParsedFont {
    ParsedFont {
        weight: t.weight,
        italic: t.italic,
        size_px: t.eff_size() * t.zoom,
        families: t.families.clone(),
    }
}

fn font_key(f: &ParsedFont) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    f.weight.hash(&mut h);
    f.italic.hash(&mut h);
    f.size_px.to_bits().hash(&mut h);
    f.families.hash(&mut h);
    h.finish()
}

/// Taffy measure function for text leaves: shaped widths from the bundled
/// fonts, greedy word wrap at the available width.
fn measure_text(
    known: taffy::Size<Option<f32>>,
    avail: taffy::Size<AvailableSpace>,
    leaf: Option<&mut TextLeaf>,
    os: &str,
    cache: &mut HashMap<(String, u64), f32>,
) -> taffy::Size<f32> {
    let Some(leaf) = leaf else { return taffy::Size::ZERO };
    let fk = font_key(&leaf.font);
    let mut w_of = |s: &str| -> f32 {
        if let Some(v) = cache.get(&(s.to_string(), fk)) {
            return *v;
        }
        let v = crate::canvas::text::measure_text_width_dom(s, &leaf.font, os) as f32;
        cache.insert((s.to_string(), fk), v);
        v
    };
    let lh = leaf.line_height;
    let full = w_of(&leaf.text);
    let (w, lines) = if leaf.nowrap {
        (full, 1usize)
    } else {
        let limit = match (known.width, avail.width) {
            (Some(w), _) => w,
            (None, AvailableSpace::Definite(w)) => w,
            (None, AvailableSpace::MinContent) => 0.0,
            (None, AvailableSpace::MaxContent) => f32::INFINITY,
        };
        if full <= limit {
            (full, 1)
        } else {
            let space = w_of(" ");
            let mut max_w: f32 = 0.0;
            let mut line_w: f32 = 0.0;
            let mut lines = 1usize;
            let mut first = true;
            for word in leaf.text.split(' ').filter(|w| !w.is_empty()) {
                let ww = w_of(word);
                if first {
                    line_w = ww;
                    first = false;
                } else if line_w + space + ww <= limit {
                    line_w += space + ww;
                } else {
                    max_w = max_w.max(line_w);
                    line_w = ww;
                    lines += 1;
                }
            }
            max_w = max_w.max(line_w);
            (max_w, lines)
        }
    };
    taffy::Size {
        width: known.width.unwrap_or(w),
        height: known.height.unwrap_or(lh * lines as f32),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dom::node::{Attribute, QualName};

    fn make_dom_with_styled_div(style: &str) -> Dom {
        let mut dom = Dom::new();
        let html = dom.create_element(QualName::new("html"), vec![]);
        dom.append_child(NodeId::DOCUMENT, html);
        let body = dom.create_element(QualName::new("body"), vec![]);
        dom.append_child(html, body);
        let div = dom.create_element(
            QualName::new("div"),
            vec![Attribute {
                name: QualName::new("style"),
                value: style.to_string(),
            }],
        );
        dom.append_child(body, div);
        dom
    }

    #[test]
    fn layout_basic_div() {
        let dom = make_dom_with_styled_div("width: 200px; height: 100px");
        let viewport = Viewport::new(1920.0, 1080.0);
        let mut engine = LayoutEngine::new(viewport);
        engine.compute(&dom);

        // Find the div (it's the child of body, which is child of html, which is child of document)
        let html = dom.child_elements(NodeId::DOCUMENT)[0];
        let body = dom.child_elements(html)[0];
        let div = dom.child_elements(body)[0];

        let rect = engine.get_bounding_rect(&dom, div);
        // Width includes border (default 3px medium border on each side)
        // Content: 200px + border: 3+3 = 206px (content-box)
        assert!(
            rect.width >= 200.0,
            "width should be >= 200, got {}",
            rect.width
        );
        assert!(
            rect.height >= 100.0,
            "height should be >= 100, got {}",
            rect.height
        );
    }

    #[test]
    fn layout_text_node_has_size() {
        let mut dom = Dom::new();
        let html = dom.create_element(QualName::new("html"), vec![]);
        dom.append_child(NodeId::DOCUMENT, html);
        let body = dom.create_element(QualName::new("body"), vec![]);
        dom.append_child(html, body);
        let text = dom.create_text("Hello world".to_string());
        dom.append_child(body, text);

        let viewport = Viewport::new(1920.0, 1080.0);
        let mut engine = LayoutEngine::new(viewport);
        engine.compute(&dom);

        let (w, h) = engine.taffy_size(text);
        assert!(w > 0.0, "text width should be > 0, got {}", w);
        assert!(h > 0.0, "text height should be > 0, got {}", h);
    }

    #[test]
    fn layout_offset_width() {
        let dom = make_dom_with_styled_div("width: 300px; height: 150px");
        let viewport = Viewport::new(1920.0, 1080.0);
        let mut engine = LayoutEngine::new(viewport);

        let html = dom.child_elements(NodeId::DOCUMENT)[0];
        let body = dom.child_elements(html)[0];
        let div = dom.child_elements(body)[0];

        let w = engine.get_offset_width(&dom, div);
        assert!(w >= 300.0, "offsetWidth should be >= 300, got {}", w);
        let h = engine.get_offset_height(&dom, div);
        assert!(h >= 150.0, "offsetHeight should be >= 150, got {}", h);
    }

    #[test]
    fn dirty_tracking() {
        let dom = make_dom_with_styled_div("width: 100px");
        let viewport = Viewport::new(1920.0, 1080.0);
        let mut engine = LayoutEngine::new(viewport);

        assert!(engine.dirty);
        engine.compute(&dom);
        assert!(!engine.dirty);
        engine.mark_dirty();
        assert!(engine.dirty);
    }

    #[test]
    fn dom_rect_from_layout() {
        let layout = taffy::Layout::new();
        let rect = DOMRect::from_taffy_layout(&layout);
        assert_eq!(rect.width, 0.0);
    }
}
