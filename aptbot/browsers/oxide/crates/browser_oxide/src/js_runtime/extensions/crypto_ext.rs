//! Web Crypto API ops — digest, random bytes, HMAC. Backs the JS-side
//! `crypto.subtle` stub so scripts that hash payloads via
//! `crypto.subtle.digest("SHA-256", ...)` see a real result.

use deno_core::op2;
use sha1::Sha1;
use sha2::{Digest, Sha256, Sha384, Sha512};

#[op2]
#[buffer]
pub fn op_crypto_digest(#[string] algorithm: String, #[buffer] data: &[u8]) -> Vec<u8> {
    let alg = algorithm.to_ascii_uppercase();
    match alg.as_str() {
        "SHA-1" | "SHA1" => {
            let mut h = Sha1::new();
            h.update(data);
            h.finalize().to_vec()
        }
        "SHA-256" | "SHA256" => {
            let mut h = Sha256::new();
            h.update(data);
            h.finalize().to_vec()
        }
        "SHA-384" | "SHA384" => {
            let mut h = Sha384::new();
            h.update(data);
            h.finalize().to_vec()
        }
        "SHA-512" | "SHA512" => {
            let mut h = Sha512::new();
            h.update(data);
            h.finalize().to_vec()
        }
        _ => Vec::new(),
    }
}

#[op2(fast)]
pub fn op_crypto_random_fill(#[buffer] out: &mut [u8]) {
    use rand::Rng;
    rand::rng().fill_bytes(out);
}

// ---- SubtleCrypto backends (ring) ------------------------------------------
// Results carry a status byte so an empty output stays distinguishable from a
// failure: [1, bytes...] on success, [0] on failure (the JS side throws
// OperationError).

fn ok(v: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() + 1);
    out.push(1);
    out.extend_from_slice(&v);
    out
}

/// AES-GCM with a 96-bit IV and a 128-bit tag (what ring supports; also what
/// every real-world caller we have seen uses). Key: 16 or 32 bytes.
#[op2]
#[buffer]
pub fn op_crypto_aes_gcm(
    #[buffer] key: &[u8],
    #[buffer] iv: &[u8],
    #[buffer] aad: &[u8],
    #[buffer] data: &[u8],
    encrypt: bool,
) -> Vec<u8> {
    use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_128_GCM, AES_256_GCM};
    let alg = match key.len() {
        16 => &AES_128_GCM,
        32 => &AES_256_GCM,
        _ => return vec![0],
    };
    let Ok(unbound) = UnboundKey::new(alg, key) else { return vec![0] };
    let k = LessSafeKey::new(unbound);
    let Ok(nonce) = Nonce::try_assume_unique_for_key(iv) else { return vec![0] };
    let mut buf = data.to_vec();
    if encrypt {
        match k.seal_in_place_append_tag(nonce, Aad::from(aad), &mut buf) {
            Ok(()) => ok(buf),
            Err(_) => vec![0],
        }
    } else {
        match k.open_in_place(nonce, Aad::from(aad), &mut buf) {
            Ok(pt) => ok(pt.to_vec()),
            Err(_) => vec![0],
        }
    }
}

fn hmac_alg(hash: &str) -> Option<ring::hmac::Algorithm> {
    Some(match hash.to_ascii_uppercase().replace('_', "-").as_str() {
        "SHA-1" | "SHA1" => ring::hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY,
        "SHA-256" | "SHA256" => ring::hmac::HMAC_SHA256,
        "SHA-384" | "SHA384" => ring::hmac::HMAC_SHA384,
        "SHA-512" | "SHA512" => ring::hmac::HMAC_SHA512,
        _ => return None,
    })
}

#[op2]
#[buffer]
pub fn op_crypto_hmac(#[string] hash: String, #[buffer] key: &[u8], #[buffer] data: &[u8]) -> Vec<u8> {
    match hmac_alg(&hash) {
        Some(alg) => ok(ring::hmac::sign(&ring::hmac::Key::new(alg, key), data).as_ref().to_vec()),
        None => vec![0],
    }
}

#[op2]
#[buffer]
pub fn op_crypto_pbkdf2(
    #[string] hash: String,
    #[buffer] secret: &[u8],
    #[buffer] salt: &[u8],
    iterations: u32,
    bits: u32,
) -> Vec<u8> {
    let alg = match hash.to_ascii_uppercase().replace('_', "-").as_str() {
        "SHA-1" | "SHA1" => ring::pbkdf2::PBKDF2_HMAC_SHA1,
        "SHA-256" | "SHA256" => ring::pbkdf2::PBKDF2_HMAC_SHA256,
        "SHA-384" | "SHA384" => ring::pbkdf2::PBKDF2_HMAC_SHA384,
        "SHA-512" | "SHA512" => ring::pbkdf2::PBKDF2_HMAC_SHA512,
        _ => return vec![0],
    };
    let (Some(iters), true) = (std::num::NonZeroU32::new(iterations), bits % 8 == 0 && bits > 0) else {
        return vec![0];
    };
    let mut out = vec![0u8; (bits / 8) as usize];
    ring::pbkdf2::derive(alg, iters, salt, secret, &mut out);
    ok(out)
}

struct HkdfLen(usize);
impl ring::hkdf::KeyType for HkdfLen {
    fn len(&self) -> usize {
        self.0
    }
}

#[op2]
#[buffer]
pub fn op_crypto_hkdf(
    #[string] hash: String,
    #[buffer] ikm: &[u8],
    #[buffer] salt: &[u8],
    #[buffer] info: &[u8],
    bits: u32,
) -> Vec<u8> {
    let alg = match hash.to_ascii_uppercase().replace('_', "-").as_str() {
        "SHA-1" | "SHA1" => ring::hkdf::HKDF_SHA1_FOR_LEGACY_USE_ONLY,
        "SHA-256" | "SHA256" => ring::hkdf::HKDF_SHA256,
        "SHA-384" | "SHA384" => ring::hkdf::HKDF_SHA384,
        "SHA-512" | "SHA512" => ring::hkdf::HKDF_SHA512,
        _ => return vec![0],
    };
    if bits % 8 != 0 || bits == 0 {
        return vec![0];
    }
    let len = (bits / 8) as usize;
    let prk = ring::hkdf::Salt::new(alg, salt).extract(ikm);
    let info_parts = [info];
    let Ok(okm) = prk.expand(&info_parts, HkdfLen(len)) else { return vec![0] };
    let mut out = vec![0u8; len];
    match okm.fill(&mut out) {
        Ok(()) => ok(out),
        Err(_) => vec![0],
    }
}


/// Compression Streams backend. `decompress`=false compresses. Formats:
/// "gzip", "deflate" (zlib) and "deflate-raw". Result: status byte
/// (0 = ok, 1 = error) followed by the output bytes.
#[op2]
#[buffer]
pub fn op_compression_run(#[string] format: String, decompress: bool, #[buffer] data: &[u8]) -> Vec<u8> {
    use std::io::{Read, Write};
    let mut out = vec![0u8];
    let res: std::io::Result<()> = (|| {
        match (format.as_str(), decompress) {
            ("gzip", false) => {
                let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                e.write_all(data)?;
                out.extend(e.finish()?);
            }
            ("deflate", false) => {
                let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
                e.write_all(data)?;
                out.extend(e.finish()?);
            }
            ("deflate-raw", false) => {
                let mut e = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
                e.write_all(data)?;
                out.extend(e.finish()?);
            }
            ("gzip", true) => { flate2::read::MultiGzDecoder::new(data).read_to_end(&mut out)?; }
            ("deflate", true) => { flate2::read::ZlibDecoder::new(data).read_to_end(&mut out)?; }
            ("deflate-raw", true) => { flate2::read::DeflateDecoder::new(data).read_to_end(&mut out)?; }
            _ => return Err(std::io::Error::other("format")),
        }
        Ok(())
    })();
    if res.is_err() {
        return vec![1];
    }
    out
}

deno_core::extension!(
    crypto_extension,
    ops = [
        op_compression_run,
        op_crypto_digest,
        op_crypto_random_fill,
        op_crypto_aes_gcm,
        op_crypto_hmac,
        op_crypto_pbkdf2,
        op_crypto_hkdf,
    ],
);
