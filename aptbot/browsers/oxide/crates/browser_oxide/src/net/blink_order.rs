//! Emulation of Blink's `HTTPHeaderMap` iteration order (Chromium 158).
//!
//! Every non-navigation request (script, stylesheet, image, fetch, XHR, ...)
//! gets its headers from a
//! `WTF::HashMap<AtomicString, AtomicString, DeprecatedCaseFoldingHashTraits>`
//! and they go on the wire in that map's bucket order. Reproducing it needs:
//!
//! * the key hash: `HashString24<CaseFoldingHashReader<LChar>>` — rapidhash v3
//!   over the case-folded name expanded to UTF-16LE, masked to 24 bits
//!   (`x_string_hasher.h`, `x_case_folding_hash.h`, `rapidhash.h`);
//! * `WTF::HashTable` geometry: bucket = `hash & (size - 1)`, triangular
//!   probing, doubling from 8 at 50 % load, rehash in old-table order,
//!   iteration from bucket 0 upwards (`x_hash_table.h`).

const RAPID_SEED: u64 = 0xbdd89aa982704029;
const RAPID_SECRET: [u64; 3] = [0x2d358dccaa6c78a5, 0x8bb84b93962eacc9, 0x4b33a62ed433d4a3];
const MIN_TABLE: usize = 8;

#[inline]
fn mul128(a: u64, b: u64) -> (u64, u64) {
    let p = (a as u128).wrapping_mul(b as u128);
    (p as u64, (p >> 64) as u64)
}

#[inline]
fn rapid_mix(a: u64, b: u64) -> u64 {
    let (lo, hi) = mul128(a, b);
    lo ^ hi
}

/// rapidhash v3 over UTF-16 code units, as `rapidhash_internal` reads them
/// through `CaseFoldingHashReader<LChar>` (`x = 1`, `y = 2`: every offset in the
/// algorithm is expressed in expanded bytes, i.e. twice the unit index).
fn rapidhash_utf16(units: &[u16], seed: u64) -> u64 {
    let [s0, s1, s2] = RAPID_SECRET;
    let mut seed = seed;
    let r64 = |o: usize| -> u64 {
        (units[o] as u64)
            | ((units[o + 1] as u64) << 16)
            | ((units[o + 2] as u64) << 32)
            | ((units[o + 3] as u64) << 48)
    };
    let r32 = |o: usize| -> u64 { (units[o] as u64) | ((units[o + 1] as u64) << 16) };

    let len = (units.len() * 2) as u64;
    seed ^= rapid_mix(seed ^ s0, s1) ^ len;
    let mut p = 0usize;
    let (mut a, mut b);
    if len <= 16 {
        if len >= 4 {
            let plast = units.len() - 2;
            a = (r32(0) << 32) | r32(plast);
            let delta = (((len & 24) >> (len >> 3)) / 2) as usize;
            b = (r32(delta) << 32) | r32(plast - delta);
        } else if len > 0 {
            a = ((units[0] as u64) << 56) | ((units[(units.len() >> 1)] as u64) << 32) | units[units.len() - 1] as u64;
            b = 0;
        } else {
            a = 0;
            b = 0;
        }
    } else {
        let mut i = len as usize;
        if i > 48 {
            let (mut see1, mut see2) = (seed, seed);
            loop {
                seed = rapid_mix(r64(p) ^ s0, r64(p + 4) ^ seed);
                see1 = rapid_mix(r64(p + 8) ^ s1, r64(p + 12) ^ see1);
                see2 = rapid_mix(r64(p + 16) ^ s2, r64(p + 20) ^ see2);
                p += 24;
                i -= 48;
                if i < 48 {
                    break;
                }
            }
            seed ^= see1 ^ see2;
        }
        if i > 16 {
            seed = rapid_mix(r64(p) ^ s2, r64(p + 4) ^ seed ^ s1);
            if i > 32 {
                seed = rapid_mix(r64(p + 8) ^ s2, r64(p + 12) ^ seed);
            }
        }
        let half = i / 2;
        a = r64(p + half - 8);
        b = r64(p + half - 4);
    }
    a ^= s1;
    b ^= seed;
    let (a, b) = mul128(a, b);
    rapid_mix(a ^ s0 ^ len, b ^ s1)
}

/// The 24-bit hash Chromium computes for a (Latin-1) header name.
pub fn name_hash(name: &str) -> u32 {
    let units: Vec<u16> = name
        .bytes()
        .map(|c| {
            let folded = if c.is_ascii_uppercase() { c + 32 } else { c };
            folded as u16
        })
        .collect();
    let h = rapidhash_utf16(&units, RAPID_SEED) as u32 & 0x00ff_ffff;
    if h == 0 {
        0x800000
    } else {
        h
    }
}

/// A `WTF::HashTable` keyed by hash, grown exactly like Blink's map.
struct Table {
    size: usize,
    slots: Vec<Option<usize>>,
}

impl Table {
    fn new() -> Self {
        Self { size: MIN_TABLE, slots: vec![None; MIN_TABLE] }
    }

    fn place(&mut self, hash: u32, idx: usize) {
        let mask = (self.size - 1) as u32;
        let mut i = (hash & mask) as usize;
        let mut probe = 0usize;
        while self.slots[i].is_some() {
            probe += 1;
            i = (i + probe) & (self.size - 1);
        }
        self.slots[i] = Some(idx);
    }

    /// `Expand()` + `RehashTo()`: double, re-insert in old-table order.
    fn grow(&mut self, hashes: &[u32]) {
        let mut next = Table { size: self.size * 2, slots: vec![None; self.size * 2] };
        for slot in self.slots.iter() {
            if let Some(idx) = *slot {
                next.place(hashes[idx], idx);
            }
        }
        *self = next;
    }
}

/// Indices of `hashes` in map iteration order when inserted in sequence.
pub fn iteration_order(hashes: &[u32]) -> Vec<usize> {
    let mut table = Table::new();
    for (count, &h) in hashes.iter().enumerate() {
        table.place(h, count);
        // `ShouldExpand()`: (key_count + deleted_count) * kMaxLoad >= table_size
        if (count + 1) * 2 >= table.size {
            table.grow(hashes);
        }
    }
    table.slots.into_iter().flatten().collect()
}

/// Reorder `(name, value)` pairs given in Blink's insertion order into the
/// order the map iterates them (i.e. the order they go on the wire).
pub fn map_order<V>(entries: Vec<(String, V)>) -> Vec<(String, V)> {
    let hashes: Vec<u32> = entries.iter().map(|(n, _)| name_hash(n)).collect();
    let order = iteration_order(&hashes);
    let mut slots: Vec<Option<(String, V)>> = entries.into_iter().map(Some).collect();
    order
        .into_iter()
        .filter_map(|i| slots[i].take())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(name: &str) -> u32 {
        name_hash(name)
    }

    #[test]
    fn hash_matches_chromium_values() {
        // Reference values computed from Chromium's own hash implementation.
        assert_eq!(h("sec-ch-ua"), 3403785);
        assert_eq!(h("sec-ch-ua-full-version-list"), 876930);
        assert_eq!(h("sec-ch-ua-platform"), 16760515);
        assert_eq!(h("sec-ch-ua-platform-version"), 7004351);
        assert_eq!(h("sec-ch-ua-model"), 16110796);
        assert_eq!(h("sec-ch-ua-mobile"), 10176653);
        assert_eq!(h("sec-ch-prefers-color-scheme"), 11745848);
        assert_eq!(h("x-fb-friendly-name"), 12545836);
        assert_eq!(h("x-asbd-id"), 1348720);
        assert_eq!(h("x-fb-entry-nonce"), 1351028);
        assert_eq!(h("x-fb-lsd"), 1518934);
        assert_eq!(h("content-type"), 8182171);
        assert_eq!(h("user-agent"), 12718456);
        assert_eq!(h("accept"), 5841433);
        assert_eq!(h("accept-language"), 5376040);
        assert_eq!(h("origin"), 4796162);
        assert_eq!(h("sec-purpose"), 2213130);
        assert_eq!(h("pragma"), 11921065);
        assert_eq!(h("cache-control"), 13747749);
        // Case-insensitive, like the map's traits.
        assert_eq!(h("Content-Type"), h("content-type"));
        assert_eq!(h("X-FB-LSd"), h("x-fb-lsd"));
    }

    #[test]
    fn facebook_login_request_order() {
        // Wire order of the real login XHR as captured from Chrome 158.
        let names = [
            "sec-ch-ua-full-version-list",
            "sec-ch-ua-platform",
            "sec-ch-ua",
            "x-fb-friendly-name",
            "sec-ch-ua-mobile",
            "sec-ch-ua-model",
            "x-asbd-id",
            "x-fb-entry-nonce",
            "x-fb-lsd",
            "sec-ch-prefers-color-scheme",
            "user-agent",
            "content-type",
            "sec-ch-ua-platform-version",
            // `accept` (Blink-generated) follows the map: appended by
            // `fetch_headers_chrome158`, not part of the hash order.
        ];
        // Blink's insertion order: author headers (sorted), then hints, then
        // user-agent / content-type.
        let inserted = [
            "content-type",
            "x-asbd-id",
            "x-fb-entry-nonce",
            "x-fb-friendly-name",
            "x-fb-lsd",
            "sec-ch-ua",
            "sec-ch-ua-mobile",
            "sec-ch-ua-platform",
            "sec-ch-ua-platform-version",
            "sec-ch-ua-model",
            "sec-ch-ua-full-version-list",
            "sec-ch-prefers-color-scheme",
            "user-agent",
        ];
        let got: Vec<String> =
            map_order(inserted.iter().map(|n| (n.to_string(), ())).collect()).into_iter().map(|(n, _)| n).collect();
        let want: Vec<String> = names.iter().map(|n| n.to_string()).collect();
        assert_eq!(got, want);
    }

    #[test]
    fn no_hints_request_order() {
        // Real Chrome 158, origin without Accept-CH, GET fetch() with no author
        // headers (capture `none.localhost /r/fetch_get`):
        // sec-ch-ua-platform | user-agent | sec-ch-ua | sec-ch-ua-mobile | accept
        // (`accept` is appended after the map by `fetch_headers_chrome158`).
        let got: Vec<String> = map_order(vec![
            ("sec-ch-ua".into(), ()),
            ("sec-ch-ua-mobile".into(), ()),
            ("sec-ch-ua-platform".into(), ()),
            ("user-agent".into(), ()),
        ])
        .into_iter()
        .map(|(n, _)| n)
        .collect();
        assert_eq!(got, ["sec-ch-ua-platform", "user-agent", "sec-ch-ua", "sec-ch-ua-mobile"]);
    }
}
