//! Blink-compatible `PeriodicWave` wavetable.
//!
//! Ports the design of Chromium's
//! `third_party/blink/renderer/platform/audio/periodic_wave.cc`:
//!
//! 1. Each standard wave type (sine / triangle / square / sawtooth) has a
//!    fixed Fourier series expressed as `(real[n], imag[n])` coefficients
//!    indexed by harmonic.
//! 2. For each of `NUM_RANGES` band-limited "ranges" (powers-of-two
//!    fundamental frequency buckets), harmonics that would alias above
//!    Nyquist are zeroed and the remaining coefficients are inverse-FFT'd
//!    into a length-`WAVE_TABLE_SIZE` time-domain table.
//! 3. Each table is peak-normalised to 1.0 so the oscillator sees unit
//!    amplitude before downstream gain.
//! 4. At render time `sample(phase, fundamental_hz)` picks the correct
//!    band-limited table for the current pitch and linearly interpolates
//!    between two adjacent table entries.
//!
//! This replaces the prior "calibrated sine at amp 0.4762" shortcut in
//! `audio.rs` — that shortcut only worked for the 10 kHz × 44.1 kHz
//! FingerprintJS case. With this port, arbitrary `(wave_type, frequency,
//! sample_rate)` combinations render a plausible Blink-shaped signal,
//! which is the capability the Tier-1 stealth work needs.
//!
//! Licence notes: Blink's implementation is BSD-3. This is an
//! independent reimplementation (not a direct copy of Blink code) so the
//! MIT/Apache-2.0 project policy is unaffected. `rustfft` is MIT.

use rustfft::{num_complex::Complex, FftPlanner};

/// Length of each per-range wavetable. Blink uses 4096 by default; we
/// match the "small-buffer" variant (2048) which is what non-WebAudio
/// rendering uses and is sufficient for interpolation-quality needs at
/// the sample rates we care about.
pub const WAVE_TABLE_SIZE: usize = 4096;

/// Maximum harmonic index we populate. Blink limits this to `size / 2`
/// to avoid aliasing at the top of the FFT buffer.
pub const MAX_NUMBER_OF_PARTIALS: usize = WAVE_TABLE_SIZE / 2;

/// Number of band-limited tables. Blink uses 12 (one per octave between
/// 20 Hz and roughly 20 kHz). We use the same count so the pitch bucket
/// boundaries match Blink's per-octave granularity.
pub const NUM_RANGES: usize = 12;

/// Standard Web Audio oscillator wave types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StandardWaveType {
    Sine,
    Triangle,
    Square,
    Sawtooth,
}

impl StandardWaveType {
    /// Fourier coefficients for this wave type, as `(real, imag)` vectors
    /// of length `MAX_NUMBER_OF_PARTIALS + 1`. Index 0 is DC; index `n`
    /// is the n-th harmonic.
    ///
    /// Conventions (match Blink's
    /// `PeriodicWave::CreateStandardWaveform`):
    /// - DC = 0 for all standard waves.
    /// - Sine: unit amplitude at harmonic 1 only.
    /// - Triangle: `b_n = (8/π²) · (-1)^((n-1)/2) / n²` for odd n.
    /// - Square: `b_n = (4/π) / n` for odd n.
    /// - Sawtooth: `b_n = (2/π) / n` for all n ≥ 1, with alternating
    ///   sign starting negative on even harmonics (matches Blink's
    ///   `1/n - 2/n` convention — equivalent to a standard sawtooth).
    #[allow(
        clippy::needless_range_loop,
        reason = "n indexes parallel real/imag harmonic arrays"
    )]
    pub fn fourier_coefficients(self) -> (Vec<f32>, Vec<f32>) {
        let len = MAX_NUMBER_OF_PARTIALS + 1;
        let real = vec![0.0f32; len];
        let mut imag = vec![0.0f32; len];
        match self {
            StandardWaveType::Sine => {
                imag[1] = 1.0;
            }
            StandardWaveType::Square => {
                let pi = std::f32::consts::PI;
                for n in (1..=MAX_NUMBER_OF_PARTIALS).step_by(2) {
                    imag[n] = (4.0 / pi) / n as f32;
                }
            }
            StandardWaveType::Sawtooth => {
                let pi = std::f32::consts::PI;
                for n in 1..=MAX_NUMBER_OF_PARTIALS {
                    let sign = if n % 2 == 0 { -1.0 } else { 1.0 };
                    imag[n] = sign * (2.0 / pi) / n as f32;
                }
            }
            StandardWaveType::Triangle => {
                // Blink: pi_factor = 2 / (n * kPiFloat); b = 2 * pi_factor².
                let pi = std::f32::consts::PI;
                for n in (1..=MAX_NUMBER_OF_PARTIALS).step_by(2) {
                    let pf = 2.0f32 / (n as f32 * pi);
                    let sign = if ((n - 1) >> 1) & 1 == 1 { -1.0 } else { 1.0 };
                    imag[n] = 2.0 * (pf * pf) * sign;
                }
            }
        }
        (real, imag)
    }
}

/// Inverse-FFT a pair of Fourier coefficient arrays into a time-domain
/// wavetable of length `WAVE_TABLE_SIZE`. The output is peak-normalised
/// so that `max(|sample|) == 1.0` (or all zeros for a silent wave).
fn build_wavetable(real: &[f32], imag: &[f32]) -> Vec<f32> {
    build_wavetable_n(real, imag, WAVE_TABLE_SIZE)
}

fn build_wavetable_n(real: &[f32], imag: &[f32], n: usize) -> Vec<f32> {
    assert!(real.len() > n / 2, "real coefficient array too short");
    assert!(imag.len() > n / 2, "imag coefficient array too short");

    // Build the length-N complex FFT input. For a real-valued time-domain
    // output we need the spectrum to be conjugate-symmetric: `X[N-k] =
    // conj(X[k])`. We map the `(real, imag)` Fourier coefficients to the
    // FFT input with Blink's sign convention (imag negated on the positive
    // side so that a positive `imag[n]` contributes `+sin(n·ω·t)`).
    // Synthesize in f64 and round once to f32 (closest to Blink's tables).
    let mut buf: Vec<Complex<f64>> = vec![Complex::new(0.0, 0.0); n];
    for k in 0..=(n / 2) {
        buf[k] = Complex::new(real[k] as f64, -(imag[k] as f64));
        if k > 0 && k < n / 2 {
            buf[n - k] = Complex::new(real[k] as f64, imag[k] as f64);
        }
    }

    let mut planner = FftPlanner::<f64>::new();
    let ifft = planner.plan_fft_inverse(n);
    ifft.process(&mut buf);

    // rustfft's inverse is unnormalised (no 1/N); the caller applies
    // Blink's single normalization scale (from the full-band table).
    buf.iter().map(|c| c.re as f32).collect()
}

fn peak_abs(t: &[f32]) -> f32 {
    t.iter().copied().fold(0.0_f32, |a, b| a.max(b.abs()))
}

/// Blink: three band-limited ranges per octave.
const NUMBER_OF_OCTAVE_BANDS: f32 = 3.0;

/// Blink `PeriodicWave::PeriodicWaveSize`: table size depends on rate.
fn periodic_wave_size_for(sample_rate: f32) -> usize {
    if sample_rate <= 24000.0 {
        2048
    } else if sample_rate <= 88200.0 {
        4096
    } else {
        16384
    }
}

/// A band-limited wavetable oscillator (port of Blink's PeriodicWave +
/// OscillatorHandler k-rate path).
pub struct PeriodicWave {
    tables: Vec<Vec<f32>>,
    sample_rate: f32,
    size: usize,
    cents_per_range: f32,
    lowest_fundamental: f32,
}

impl PeriodicWave {
    pub fn new(wave_type: StandardWaveType, sample_rate: f32) -> Self {
        let size = periodic_wave_size_for(sample_rate);
        let half = size / 2;
        let (real_all, imag_all) = wave_type.fourier_coefficients();
        let number_of_ranges = (NUMBER_OF_OCTAVE_BANDS * (size as f32).log2()).round() as usize;
        let cents_per_range = 1200.0 / NUMBER_OF_OCTAVE_BANDS;
        let max_partials = half;
        let number_of_components = half;
        let nyquist = 0.5 * sample_rate;
        let lowest_fundamental = nyquist / max_partials as f32;

        let mut tables = Vec::with_capacity(number_of_ranges);
        let mut normalization_scale = 1.0f32;
        for range in 0..number_of_ranges {
            let cents_to_cull = range as f32 * cents_per_range;
            let culling_scale = 2.0f32.powf(-cents_to_cull / 1200.0);
            let mut partials = (culling_scale * max_partials as f32) as usize;
            partials = partials.min(number_of_components - 1);
            let mut real = vec![0.0f32; half + 1];
            let mut imag = vec![0.0f32; half + 1];
            for i in 1..=partials.min(half) {
                if i < real_all.len() {
                    real[i] = real_all[i];
                    imag[i] = imag_all[i];
                }
            }
            let mut t = build_wavetable_n(&real, &imag, size);
            if range == 0 {
                let m = peak_abs(&t);
                if m > 0.0 {
                    normalization_scale = 1.0 / m;
                }
            }
            for v in t.iter_mut() {
                *v *= normalization_scale;
            }
            tables.push(t);
        }
        Self { tables, sample_rate, size, cents_per_range, lowest_fundamental }
    }

    pub fn sample_rate(&self) -> f32 {
        self.sample_rate
    }

    /// Blink `WaveDataForFundamentalFrequency`: (higher, lower, factor).
    fn tables_for(&self, fundamental: f32) -> (usize, usize, f32) {
        let f = fundamental.abs();
        let ratio = if f > 0.0 { f / self.lowest_fundamental } else { 0.5 };
        let cents_above = ratio.log2() * 1200.0;
        let mut pitch_range = 1.0 + cents_above / self.cents_per_range;
        pitch_range = pitch_range.max(0.0).min((self.tables.len() - 1) as f32);
        let r1 = pitch_range as usize;
        let r2 = if r1 < self.tables.len() - 1 { r1 + 1 } else { r1 };
        (r1, r2, pitch_range - r1 as f32)
    }

    /// Render `n` frames of a constant-frequency oscillator starting at
    /// phase 0 (Blink `ProcessKRateScalar`, float math where Blink uses it).
    pub fn render(&self, frequency: f32, n: usize) -> Vec<f32> {
        let size = self.size;
        let rate_scale: f32 = size as f32 / self.sample_rate;
        let inv_size: f32 = 1.0 / size as f32;
        let mask = size - 1;
        let (hi, lo, tif) = self.tables_for(frequency);
        let higher = &self.tables[hi];
        let lower = &self.tables[lo];
        let incr: f32 = frequency * rate_scale;
        let mut vri: f64 = 0.0;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let read_index = vri as usize;
            let i1 = read_index & mask;
            let i2 = (read_index + 1) & mask;
            let ifac: f32 = (vri as f32) - read_index as f32;
            let s_hi = (1.0 - ifac) * higher[i1] + ifac * higher[i2];
            let s_lo = (1.0 - ifac) * lower[i1] + ifac * lower[i2];
            out.push((1.0 - tif) * s_hi + tif * s_lo);
            vri += incr as f64;
            vri -= (vri * inv_size as f64).floor() * size as f64;
        }
        out
    }

    /// Legacy accessor (phase in [0,1)) — uses the same tables.
    pub fn sample(&self, phase: f32, fundamental_hz: f32) -> f32 {
        self.sample_f64(phase as f64, fundamental_hz)
    }

    pub fn sample_f64(&self, phase: f64, fundamental_hz: f32) -> f32 {
        let (hi, lo, tif) = self.tables_for(fundamental_hz);
        let size = self.size;
        let pos = phase * size as f64;
        let fl = pos.floor();
        let i0 = (fl as usize) % size;
        let i1 = (i0 + 1) % size;
        let f = (pos - fl) as f32;
        let h = (1.0 - f) * self.tables[hi][i0] + f * self.tables[hi][i1];
        let l = (1.0 - f) * self.tables[lo][i0] + f * self.tables[lo][i1];
        (1.0 - tif) * h + tif * l
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pure sine: exactly one harmonic, peak-normalised to 1.0.
    #[test]
    fn sine_peak_and_shape() {
        let wave = PeriodicWave::new(StandardWaveType::Sine, 44100.0);
        let mut max_abs: f32 = 0.0;
        let mut min: f32 = 0.0;
        for i in 0..WAVE_TABLE_SIZE {
            let phase = i as f32 / WAVE_TABLE_SIZE as f32;
            let s = wave.sample(phase, 440.0);
            max_abs = max_abs.max(s.abs());
            min = min.min(s);
        }
        assert!(
            (max_abs - 1.0).abs() < 0.01,
            "sine peak should normalise to 1.0, got {max_abs}"
        );
        assert!(min < -0.9, "sine should swing negative, min = {min}");
    }

    /// Triangle shape characteristic: mean(|x|) over one period is ~0.5
    /// after peak normalisation (pure-triangle RMS ≈ 0.577, mean-abs = 0.5).
    /// Our Fourier-summed triangle has small ripple but should still be
    /// within a reasonable band.
    #[test]
    fn triangle_mean_abs() {
        let wave = PeriodicWave::new(StandardWaveType::Triangle, 44100.0);
        let n = 1024usize;
        let mut sum = 0.0f32;
        for i in 0..n {
            let phase = i as f32 / n as f32;
            sum += wave.sample(phase, 440.0).abs();
        }
        let mean = sum / n as f32;
        // Pure math triangle: 0.5. Band-limited + normalised allows a
        // wider window — use 0.4..0.6 to catch gross regressions but
        // tolerate reasonable harmonic truncation.
        assert!(
            (0.40..=0.60).contains(&mean),
            "triangle mean-abs out of range: {mean}"
        );
    }

    /// Square shape characteristic: every sample is close to ±1 after
    /// peak normalisation (band-limited edges ring, so allow up to 25%
    /// of samples in the "transition" bucket but the rest must be near
    /// the rails).
    #[test]
    fn square_bimodal() {
        let wave = PeriodicWave::new(StandardWaveType::Square, 44100.0);
        let n = 1024usize;
        let mut high = 0;
        let mut low = 0;
        for i in 0..n {
            let phase = i as f32 / n as f32;
            let s = wave.sample(phase, 440.0);
            if s > 0.5 {
                high += 1;
            } else if s < -0.5 {
                low += 1;
            }
        }
        let edge_free = high + low;
        assert!(
            edge_free as f32 / n as f32 > 0.6,
            "square should be mostly at rails, got {}/{n} near ±1",
            edge_free
        );
    }

    /// At 10 kHz × 44.1 kHz only the fundamental survives band-limiting
    /// (n=3 harmonic is at 30 kHz > 22.05 kHz Nyquist). The triangle
    /// wavetable in that regime should therefore behave like a pure
    /// sine — this is the case the old `audio.rs` shortcut calibrated
    /// against.
    #[test]
    #[allow(
        clippy::needless_range_loop,
        reason = "i indexes the wave table + analytic ref"
    )]
    fn triangle_at_10khz_is_fundamental_only() {
        let wave = PeriodicWave::new(StandardWaveType::Triangle, 44100.0);
        // The topmost (narrowest) range table is what 10 kHz lands in:
        // top_hz drops below 10 kHz as we climb indices, so 10 kHz picks
        // the widest range, which for triangle should still retain only
        // the n=1 harmonic (nothing above it survives Nyquist).
        let index = wave.tables_for(10_000.0).0;
        let table = &wave.tables[index];
        // Compare peak-normalised table against an analytic sine.
        let n = WAVE_TABLE_SIZE;
        let mut max_err = 0.0f32;
        for i in 0..n {
            let phase = i as f32 / n as f32;
            let sine_ref = (2.0 * std::f32::consts::PI * phase).sin();
            max_err = max_err.max((table[i] - sine_ref).abs());
        }
        assert!(
            max_err < 0.01,
            "triangle @ 10 kHz regime should collapse to pure sine (max_err={max_err})"
        );
    }

    #[test]
    fn deterministic() {
        let a = PeriodicWave::new(StandardWaveType::Triangle, 44100.0);
        let b = PeriodicWave::new(StandardWaveType::Triangle, 44100.0);
        for (i, (ta, tb)) in a.tables.iter().zip(b.tables.iter()).enumerate() {
            assert_eq!(ta, tb, "table {i} not deterministic");
        }
    }
}
