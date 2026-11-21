// Shopping Sessions pipeline. Implements rules.md exactly; see README.md for the design.
//
// Phases
//   A  links union-find (one thread)  ||  parse+validate events and clicks (all other threads)
//   B  dedup events per event_id hash bucket, resolve each kept event's person, scatter by person
//   C  per person bucket: sort, sessionize, credit + assists, user-day and campaign-day aggregates
//   D  write Parquet
//
// Every input file is memory-mapped and leaked, so field slices are &'static [u8]. After parsing,
// IDs travel as 16-byte inline `Key`s so later phases never touch the mapped files at random.
use rayon::prelude::*;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

type Bytes = &'static [u8];

const SEC: i64 = 1000;
const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const LATE: i64 = DAY;
const NBITS: u32 = 7;
const NB: usize = 1 << NBITS;
const SHIFT: u32 = 64 - NBITS;
const CHUNK: usize = 8 << 20;
const PD: usize = 24; // prefetch distance

// ---------- packed slice refs (all data lives in leaked mmaps) ----------
#[inline]
fn pack(s: Bytes) -> u64 {
    ((s.as_ptr() as u64) << 16) | (s.len().min(0xFFFF) as u64)
}
#[inline]
fn unpack(p: u64) -> Bytes {
    unsafe { std::slice::from_raw_parts((p >> 16) as *const u8, (p & 0xFFFF) as usize) }
}

// ---------- inline keys ----------
/// Up to 15 bytes inline (length in the top byte of `b`); longer keys hold a packed slice ref.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
struct Key {
    a: u64,
    b: u64,
}
const LONG: u64 = 0xFF << 56;
const EMPTY: u64 = 0xFE << 56;
impl Key {
    #[inline]
    fn new(s: Bytes) -> Key {
        let n = s.len();
        if n <= 15 {
            // overlapping in-bounds loads instead of a variable-length copy
            let p = s.as_ptr();
            let rd8 = |i: usize| unsafe { (p.add(i) as *const u64).read_unaligned() };
            let rd4 = |i: usize| unsafe { (p.add(i) as *const u32).read_unaligned() as u64 };
            let (a, b) = if n >= 8 {
                let b = if n == 8 { 0 } else { rd8(n - 8) >> (8 * (16 - n)) };
                (rd8(0), b)
            } else if n >= 4 {
                (rd4(0) | (rd4(n - 4) << (8 * (n - 4))), 0)
            } else {
                let mut a = 0u64;
                for (i, &c) in s.iter().enumerate() {
                    a |= (c as u64) << (8 * i);
                }
                (a, 0)
            };
            Key { a, b: b | ((n as u64) << 56) }
        } else {
            Key { a: pack(s), b: LONG }
        }
    }
    #[inline]
    fn same(&self, o: &Key) -> bool {
        (self.a == o.a && self.b == o.b) || (self.b == LONG && o.b == LONG && unpack(self.a) == unpack(o.a))
    }
    /// Hash of the key's bytes (inline keys hash their two words, long keys their bytes).
    #[inline]
    fn h(&self) -> u64 {
        if self.b == LONG {
            hash(unpack(self.a))
        } else {
            fmix(self.a.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ self.b.wrapping_mul(0xC2B2_AE3D_27D4_EB4F).rotate_left(29))
        }
    }
    fn append_to(&self, out: &mut Vec<u8>) {
        if self.b == LONG {
            out.extend_from_slice(unpack(self.a));
        } else {
            let mut buf = [0u8; 16];
            buf[..8].copy_from_slice(&self.a.to_le_bytes());
            buf[8..].copy_from_slice(&self.b.to_le_bytes());
            out.extend_from_slice(&buf[..buf[15] as usize]);
        }
    }
    #[cfg_attr(not(test), allow(dead_code))]
    fn bytes(&self) -> Vec<u8> {
        if self.b == LONG {
            unpack(self.a).to_vec()
        } else {
            let mut buf = [0u8; 16];
            buf[..8].copy_from_slice(&self.a.to_le_bytes());
            buf[8..].copy_from_slice(&self.b.to_le_bytes());
            buf[..buf[15] as usize].to_vec()
        }
    }
}

// ---------- hashing ----------
#[inline]
fn fmix(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^ (h >> 33)
}
#[inline]
fn hash(s: &[u8]) -> u64 {
    let mut h: u64 = 0x9E37_79B9_7F4A_7C15 ^ (s.len() as u64).wrapping_mul(0x2545F4914F6CDD1D);
    let mut c = s.chunks_exact(8);
    for w in &mut c {
        let w = u64::from_le_bytes(w.try_into().unwrap());
        h = (h ^ w).wrapping_mul(0xbf58476d1ce4e5b9);
        h ^= h >> 31;
    }
    let r = c.remainder();
    if !r.is_empty() {
        let mut b = [0u8; 8];
        b[..r.len()].copy_from_slice(r);
        h = (h ^ u64::from_le_bytes(b)).wrapping_mul(0x94d049bb133111eb);
        h ^= h >> 29;
    }
    fmix(h)
}

// ---------- memory ----------
/// An empty Vec with room for cap items, backed by transparent huge pages when large.
fn huge_vec<T>(cap: usize) -> Vec<T> {
    let sz = std::mem::size_of::<T>().max(1);
    if cap * sz < (4 << 20) {
        return Vec::with_capacity(cap);
    }
    let v: Vec<T> = Vec::with_capacity(cap + (2 << 20) / sz);
    unsafe {
        let p = v.as_ptr() as usize;
        let a = (p + (2 << 20) - 1) & !((2 << 20) - 1);
        let len = (p + v.capacity() * sz - a) & !((2 << 20) - 1);
        libc::madvise(a as *mut libc::c_void, len, libc::MADV_HUGEPAGE);
    }
    v
}

// ---------- open addressing table keyed by Key ----------
#[derive(Clone, Copy)]
struct Slot<V: Copy> {
    h: u64,
    k: Key,
    v: V,
}
struct Table<V: Copy> {
    mask: usize,
    slots: Vec<Slot<V>>,
}
impl<V: Copy + Default> Table<V> {
    fn new(n: usize) -> Self {
        let cap = (n + n / 2 + 16).next_power_of_two();
        let mut slots = huge_vec::<Slot<V>>(cap);
        slots.resize(cap, Slot { h: 0, k: Key { a: 0, b: EMPTY }, v: V::default() });
        Table { mask: cap - 1, slots }
    }
    #[inline]
    fn prefetch(&self, h: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            let p = self.slots.as_ptr().add((h as usize) & self.mask) as *const i8;
            _mm_prefetch(p, _MM_HINT_T0);
            _mm_prefetch(p.add(std::mem::size_of::<Slot<V>>() - 1), _MM_HINT_T0);
        }
    }
    #[inline]
    fn get(&self, k: &Key, h: u64) -> Option<V> {
        let mut i = (h as usize) & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s.k.b == EMPTY {
                return None;
            }
            if s.h == h && s.k.same(k) {
                return Some(s.v);
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    fn get_b(&self, s: Bytes) -> Option<V> {
        let k = Key::new(s);
        self.get(&k, k.h())
    }
    /// Returns (value, inserted).
    fn get_or_insert(&mut self, k: Key, h: u64, v: V) -> (V, bool) {
        let mut i = (h as usize) & self.mask;
        loop {
            let s = &self.slots[i];
            if s.k.b == EMPTY {
                self.slots[i] = Slot { h, k, v };
                return (v, true);
            }
            if s.h == h && s.k.same(&k) {
                return (s.v, false);
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ---------- CSV dialect ----------
/// Bit masks of (',' | '"' | '\n', '"', '\n') for the 64 bytes at base (zero past the end).
#[inline(always)]
fn masks(d: &[u8], base: usize) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    if base + 64 <= d.len() {
        unsafe {
            use std::arch::x86_64::*;
            let p = d.as_ptr().add(base) as *const __m256i;
            let a = _mm256_loadu_si256(p);
            let b = _mm256_loadu_si256(p.add(1));
            let mm = |v: __m256i, c: i8| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(c))) as u32 as u64;
            let mc = mm(a, b',' as i8) | (mm(b, b',' as i8) << 32);
            let mq = mm(a, b'"' as i8) | (mm(b, b'"' as i8) << 32);
            let mn = mm(a, b'\n' as i8) | (mm(b, b'\n' as i8) << 32);
            return (mc, mq, mn);
        }
    }
    let (mut mc, mut mq, mut mn) = (0u64, 0u64, 0u64);
    let end = (base + 64).min(d.len());
    for (i, &c) in d[base..end].iter().enumerate() {
        mc |= ((c == b',') as u64) << i;
        mq |= ((c == b'"') as u64) << i;
        mn |= ((c == b'\n') as u64) << i;
    }
    (mc, mq, mn)
}

/// Calls f with the N fields of every well-formed line in the chunk (rules.md CSV dialect):
/// only '\n' ends a line, every ',' separates fields, a field may be wrapped in quotes as a
/// whole and must hold no other quote, and a line with any other field count is dropped.
///
/// Only commas and newlines are visited. Quotes are counted once per line with popcount: a line
/// is valid when every field that starts with '"' also ends with one (length >= 2) and the line
/// holds exactly two quotes per such field, so no quote can sit anywhere else.
#[inline]
fn scan<const N: usize>(chunk: Bytes, mut f: impl FnMut(&[Bytes; N])) {
    let mut ends = [0usize; N];
    let mut k = 0usize; // separators seen on this line
    let mut ls = 0usize; // line start
    let mut qls = 0u64; // quotes before the line start
    let mut qbase = 0u64; // quotes before this block
    let n = chunk.len();
    #[inline(always)]
    fn line<const N: usize>(chunk: Bytes, ls: usize, ends: &[usize; N], quotes: u64, out: &mut [Bytes; N]) -> bool {
        let mut a = ls;
        let mut qf = 0u64;
        let mut ok = true;
        for i in 0..N {
            let b = ends[i];
            if quotes == 0 {
                out[i] = unsafe { chunk.get_unchecked(a..b) };
            } else {
                let q = a < b && unsafe { *chunk.get_unchecked(a) } == b'"';
                if q {
                    ok &= b >= a + 2 && unsafe { *chunk.get_unchecked(b - 1) } == b'"';
                    out[i] = unsafe { chunk.get_unchecked(a + 1..(b - 1).max(a + 1)) };
                    qf += 1;
                } else {
                    out[i] = unsafe { chunk.get_unchecked(a..b) };
                }
            }
            a = b + 1;
        }
        ok && quotes == 2 * qf
    }
    let mut out: [Bytes; N] = [b""; N];
    let mut base = 0usize;
    while base < n {
        let (mc, mq, mn) = masks(chunk, base);
        let mut m = mc | mn;
        while m != 0 {
            let bit = m.trailing_zeros();
            m &= m - 1;
            let p = base + bit as usize;
            if k < N {
                ends[k] = p;
            }
            k += 1;
            if (mn >> bit) & 1 != 0 {
                let qp = qbase + (mq & ((1u64 << bit) - 1)).count_ones() as u64;
                if k == N && line::<N>(chunk, ls, &ends, qp - qls, &mut out) {
                    f(&out);
                }
                k = 0;
                ls = p + 1;
                qls = qp;
            }
        }
        qbase += mq.count_ones() as u64;
        base += 64;
    }
    if ls < n {
        if k < N {
            ends[k] = n;
        }
        k += 1;
        if k == N && line::<N>(chunk, ls, &ends, qbase - qls, &mut out) {
            f(&out);
        }
    }
}

// ---------- timestamps ----------
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}
#[inline]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a > 9 || b > 9 {
        return None;
    }
    Some((a * 10 + b) as i64)
}

/// Parses F1/F2/F3 to epoch ms (None = bad), with a cache of validated calendar dates.
struct TsParser {
    keys: Box<[(u64, u16); DC]>,
    days: Box<[i64; DC]>,
}
const DC: usize = 256;
impl Default for TsParser {
    fn default() -> Self {
        TsParser { keys: Box::new([(0, 0); DC]), days: Box::new([i64::MIN; DC]) }
    }
}
impl TsParser {
    #[inline]
    fn parse(&mut self, s: &[u8]) -> Option<i64> {
        let n = s.len();
        if (n != 20 && n != 25) || s[4] != b'-' {
            // F3: ASCII digits only, any length, must fit in i64
            if n == 0 {
                return None;
            }
            if (9..=16).contains(&n) {
                // two 8-digit words: s[n-8..n] and s[..n-8] left-padded with '0'
                let rd = |i: usize| u64::from_le_bytes(s[i..i + 8].try_into().unwrap());
                let lo = rd(n - 8);
                let l = n - 8;
                let hraw = rd(0);
                let hi = if l == 8 { hraw } else { (hraw << (8 * (8 - l))) | (0x3030_3030_3030_3030u64 >> (8 * l)) };
                if !all_digits8(lo) || !all_digits8(hi) {
                    return None;
                }
                return Some((digits8(hi) * 100_000_000 + digits8(lo)) as i64);
            }
            let mut v: u64 = 0;
            for &c in s {
                let d = c.wrapping_sub(b'0');
                if d > 9 {
                    return None;
                }
                v = v.checked_mul(10)?.checked_add(d as u64)?;
            }
            if v > i64::MAX as u64 {
                return None;
            }
            return Some(v as i64);
        }
        let k = (u64::from_le_bytes(s[0..8].try_into().unwrap()), u16::from_le_bytes([s[8], s[9]]));
        let slot = ((k.0 ^ (k.1 as u64).wrapping_mul(0x9E37_79B9)).wrapping_mul(0x2545F4914F6CDD1D) >> 56) as usize;
        let days = if self.keys[slot] == k && self.days[slot] != i64::MIN {
            self.days[slot]
        } else {
            if s[7] != b'-' {
                return None;
            }
            let y = d2(s, 0)? * 100 + d2(s, 2)?;
            let mo = d2(s, 5)?;
            let d = d2(s, 8)?;
            if !(1..=12).contains(&mo) || d < 1 {
                return None;
            }
            let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
            let dim = match mo {
                2 => {
                    if leap {
                        29
                    } else {
                        28
                    }
                }
                4 | 6 | 9 | 11 => 30,
                _ => 31,
            };
            if d > dim {
                return None;
            }
            let dd = days_from_civil(y, mo, d);
            self.keys[slot] = k;
            self.days[slot] = dd;
            dd
        };
        if s[10] != b'T' {
            return None;
        }
        // "HH:MM:SS" at 11..19, checked eight bytes at once
        let x = u64::from_le_bytes(s[11..19].try_into().unwrap()) ^ 0x3030_3030_3030_3030;
        const COLONS: u64 = 0x0000_0A00_000A_0000; // bytes 2 and 5
        const CMASK: u64 = 0x0000_FF00_00FF_0000;
        if x & CMASK != COLONS {
            return None;
        }
        let y = x & !CMASK;
        if ((y.wrapping_add(0x7676_7676_7676_7676)) | y) & 0x8080_8080_8080_8080 != 0 {
            return None;
        }
        let b = y.to_le_bytes();
        let hh = (b[0] * 10 + b[1]) as i64;
        let mi = (b[3] * 10 + b[4]) as i64;
        let ss = (b[6] * 10 + b[7]) as i64;
        if hh > 23 || mi > 59 || ss > 59 {
            return None;
        }
        let base = days * DAY + hh * 3_600_000 + mi * 60_000 + ss * SEC;
        if n == 20 {
            if s[19] != b'Z' {
                return None;
            }
            Some(base)
        } else {
            let sign = match s[19] {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            if s[22] != b':' {
                return None;
            }
            let oh = d2(s, 20)?;
            let om = d2(s, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            Some(base - sign * (oh * 60 + om) * 60_000)
        }
    }
}

/// True if all eight bytes are ASCII digits.
#[inline(always)]
fn all_digits8(x: u64) -> bool {
    (x & 0xF0F0_F0F0_F0F0_F0F0) == 0x3030_3030_3030_3030
        && ((x.wrapping_add(0x0606_0606_0606_0606)) & 0xF0F0_F0F0_F0F0_F0F0) == 0x3030_3030_3030_3030
}
/// Value of eight ASCII digits loaded little-endian (first byte most significant).
#[inline(always)]
fn digits8(x: u64) -> u64 {
    let v = x - 0x3030_3030_3030_3030;
    let v = (v * 10) + (v >> 8);
    (((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32)))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32))))
        >> 32
}

fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v >= 1 { Some(v as u32) } else { None }
}

// ---------- files ----------
fn leak_mmap(p: &Path) -> Bytes {
    let f = File::open(p).expect("open");
    let len = f.metadata().unwrap().len();
    if len == 0 {
        return b"";
    }
    let m = unsafe { memmap2::Mmap::map(&f).expect("mmap") };
    // start reading the whole file now (no-op when it is already cached)
    let _ = m.advise(memmap2::Advice::WillNeed);
    let m: &'static memmap2::Mmap = Box::leak(Box::new(m));
    &m[..]
}
fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}
#[inline]
fn find_nl(d: &[u8], from: usize) -> Option<usize> {
    memchr::memchr(b'\n', &d[from..]).map(|i| i + from)
}
/// Byte ranges of data (the header line skipped) cut at line boundaries.
fn chunks(files: &[Bytes]) -> Vec<Bytes> {
    let mut out = vec![];
    for &d in files {
        let start = match find_nl(d, 0) {
            Some(i) => i + 1,
            None => continue,
        };
        let mut s = start;
        while s < d.len() {
            let mut e = (s + CHUNK).min(d.len());
            if e < d.len() {
                e = match find_nl(d, e) {
                    Some(i) => i + 1,
                    None => d.len(),
                };
            }
            out.push(&d[s..e]);
            s = e;
        }
    }
    out
}
fn read_small<const N: usize>(p: &Path) -> Vec<[Bytes; N]> {
    let d = leak_mmap(p);
    let mut v = vec![];
    for c in chunks(&[d]) {
        scan::<N>(c, |f| v.push(*f));
    }
    v
}

/// A slot that exactly one task empties; lets per-bucket inputs be freed as they are consumed.
struct Take<T>(std::cell::UnsafeCell<T>);
unsafe impl<T: Send> Sync for Take<T> {}
impl<T: Default> Take<T> {
    fn new(t: T) -> Self {
        Take(std::cell::UnsafeCell::new(t))
    }
    /// Safety contract: each bucket index is processed by exactly one task.
    fn get(&self) -> &T {
        unsafe { &*self.0.get() }
    }
    fn take(&self) -> T {
        unsafe { std::mem::take(&mut *self.0.get()) }
    }
}

// ---------- records ----------
#[derive(Clone, Copy)]
struct Ev {
    eh: u64,
    eid: u64, // packed ref, compared only when hashes match
    uk: Key,
    ingest: u32, // seconds since INGEST_BASE (ingest_ts is always whole-second F1)
    ts: u32,     // ms since the click window start
    price: u32,
    cat: u16,
    flags: u8, // 1 = valid, 2 = purchase
}
#[derive(Clone, Copy)]
struct Kept {
    pk: Key, // person name; its hash is pk.h()
    ts: u32,
    price: u32,
    cat: u16,
    purchase: bool,
}
#[derive(Clone, Copy)]
struct RawCk {
    uh: u64,
    uk: Key,
    ts: u32,
    cid: u64,
    camp: u16,
}
#[derive(Clone, Copy)]
struct Ck {
    pk: Key,
    ts: u32,
    cid: u64,
    camp: u16,
}
#[derive(Clone, Copy)]
struct URow {
    name: Key,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: i64,
    top: i32, // -1 = null
}

#[derive(Clone, Copy, Default)]
struct LinkV {
    pk: Key,
    ph: u64,
}

#[inline]
fn resolve(links: &Table<LinkV>, uk: &Key, h: u64) -> (u64, Key) {
    match links.get(uk, h) {
        Some(v) => (v.ph, v.pk),
        None => (h, *uk),
    }
}

fn log(t0: &Instant, msg: &str) {
    if std::env::var_os("QUIET").is_none() {
        eprintln!("[{:7.3}s] {}", t0.elapsed().as_secs_f64(), msg);
    }
}

/// Union-find over links/: every ID maps to its person's name (the byte-wise smallest ID).
fn build_links(lk_files: &[Bytes]) -> Table<LinkV> {
    let mut pairs: Vec<(Bytes, Bytes)> = vec![];
    for c in chunks(lk_files) {
        scan::<2>(c, |&[a, b]| {
            if !a.is_empty() && !b.is_empty() {
                pairs.push((a, b));
            }
        });
    }
    let npairs = pairs.len();
    let mut ids: Table<u32> = Table::new(npairs * 2);
    let mut names: Vec<(Bytes, u64, Key)> = Vec::with_capacity(npairs * 2);
    let mut parent: Vec<u32> = Vec::with_capacity(npairs * 2);
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        while p[x as usize] != x {
            let g = p[p[x as usize] as usize];
            p[x as usize] = g;
            x = g;
        }
        x
    }
    for (k, &(a, b)) in pairs.iter().enumerate() {
        if let Some(&(na, _)) = pairs.get(k + 8) {
            ids.prefetch(Key::new(na).h());
        }
        let mut idx = |s: Bytes| {
            let key = Key::new(s);
            let h = key.h();
            let (i, ins) = ids.get_or_insert(key, h, names.len() as u32);
            if ins {
                names.push((s, h, key));
                parent.push(i);
            }
            i
        };
        let ia = idx(a);
        let ib = idx(b);
        let ra = find(&mut parent, ia);
        let rb = find(&mut parent, ib);
        if ra != rb {
            // the root is always the byte-wise smallest ID of its set
            if names[ra as usize].0 <= names[rb as usize].0 {
                parent[rb as usize] = ra;
            } else {
                parent[ra as usize] = rb;
            }
        }
    }
    drop(ids);
    let mut links: Table<LinkV> = Table::new(names.len());
    for i in 0..names.len() {
        let r = find(&mut parent, i as u32) as usize;
        let (_, h, key) = names[i];
        let (_, rh, rk) = names[r];
        links.get_or_insert(key, h, LinkV { pk: rk, ph: rh });
    }
    links
}

fn main() {
    let t0 = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let outd = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let ncpu: usize = std::env::var("NCPU").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    if ncpu > 0 && std::env::var_os("RAYON_NUM_THREADS").is_none() {
        rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().unwrap();
    }

    // manifest
    let man = std::fs::read_to_string(data.join("manifest.json")).expect("manifest");
    let field = |k: &str| -> i64 {
        let i = man.find(&format!("\"{k}\"")).expect("key") + k.len() + 2;
        let rest = &man[i..];
        let a = rest.find('"').unwrap() + 1;
        let b = a + rest[a..].find('"').unwrap();
        TsParser::default().parse(rest[a..b].as_bytes()).expect("manifest ts")
    };
    let ws = field("window_start");
    let we = field("window_end");
    let ck_lo = ws - WEEK;
    // ingest_ts only orders copies of one event: seconds from well before any window
    let ibase = ws - 1000 * DAY;

    // products & campaigns
    let mut cat_names: Vec<Bytes> = vec![];
    let mut cat_tab: Table<u16> = Table::new(64);
    let plines = read_small::<2>(&data.join("products.csv"));
    let mut products: Table<u16> = Table::new(plines.len());
    for [pid, cat] in plines {
        let (ci, ins) = cat_tab.get_or_insert(Key::new(cat), Key::new(cat).h(), cat_names.len() as u16);
        if ins {
            cat_names.push(cat);
        }
        products.get_or_insert(Key::new(pid), Key::new(pid).h(), ci);
    }
    let unknown_cat = cat_names.len() as u16;
    cat_names.push(b"unknown");
    let mut order: Vec<usize> = (0..cat_names.len()).collect();
    order.sort_by(|&a, &b| cat_names[a].cmp(cat_names[b]));
    let mut cat_rank = vec![0u32; cat_names.len()];
    for (r, &i) in order.iter().enumerate() {
        cat_rank[i] = r as u32;
    }
    let mut campaigns: Table<u16> = Table::new(4096);
    let mut camp_names: Vec<(Bytes, Bytes)> = vec![];
    for [cid, ch] in read_small::<2>(&data.join("campaigns.csv")) {
        let (_, ins) = campaigns.get_or_insert(Key::new(cid), Key::new(cid).h(), camp_names.len() as u16);
        if ins {
            camp_names.push((cid, ch));
        }
    }
    let ncamp = camp_names.len();

    let ev_paths = list_csv(&data.join("events"));
    let ck_paths = list_csv(&data.join("clicks"));
    let lk_paths = list_csv(&data.join("links"));
    log(&t0, &format!("files: {} events, {} clicks, {} links", ev_paths.len(), ck_paths.len(), lk_paths.len()));

    // ---------- phase A ----------
    let (links, (parts, ck_raw)) = rayon::join(
        || {
            let lk_files: Vec<Bytes> = lk_paths.iter().map(|p| leak_mmap(p)).collect();
            let l = build_links(&lk_files);
            log(&t0, "links built");
            l
        },
        || {
            let ev_files: Vec<Bytes> = ev_paths.par_iter().map(|p| leak_mmap(p)).collect();
            let ck_files: Vec<Bytes> = ck_paths.par_iter().map(|p| leak_mmap(p)).collect();
            let ev_chunks = chunks(&ev_files);
            let parts: Vec<Vec<Vec<Ev>>> = ev_chunks
                .par_iter()
                .map(|&c| {
                    let mut b: Vec<Vec<Ev>> = (0..NB).map(|_| Vec::with_capacity(c.len() / 60 / NB + 32)).collect();
                    let mut tp = TsParser::default();
                    scan::<7>(c, |&[eid, uid, ts, ing, ty, pid, pr]| {
                        if eid.is_empty() {
                            return;
                        }
                        let eh = hash(eid);
                        let ingest = tp.parse(ing).unwrap_or(i64::MAX);
                        let mut e = Ev {
                            eh,
                            eid: pack(eid),
                            uk: Key::default(),
                            ingest: (ingest.saturating_sub(ibase) / SEC).clamp(0, u32::MAX as i64) as u32,
                            ts: 0,
                            price: 0,
                            cat: 0,
                            flags: 0,
                        };
                        'v: {
                            if uid.is_empty() || pid.is_empty() {
                                break 'v;
                            }
                            let Some(t) = tp.parse(ts) else { break 'v };
                            if t < ws || t >= we {
                                break 'v;
                            }
                            let purchase = match ty {
                                b"view" | b"cart" => false,
                                b"purchase" => true,
                                _ => break 'v,
                            };
                            if purchase {
                                match parse_price(pr) {
                                    Some(x) => e.price = x,
                                    None => break 'v,
                                }
                            }
                            if ingest.saturating_sub(t) > LATE {
                                break 'v;
                            }
                            e.ts = (t - ck_lo) as u32;
                            e.uk = Key::new(uid);
                            e.cat = products.get_b(pid).unwrap_or(unknown_cat);
                            e.flags = 1 | if purchase { 2 } else { 0 };
                        }
                        unsafe { b.get_unchecked_mut((eh >> SHIFT) as usize) }.push(e);
                    });
                    b
                })
                .collect();
            log(&t0, &format!("events parsed: {} chunks", ev_chunks.len()));
            let ck_raw: Vec<Vec<RawCk>> = chunks(&ck_files)
                .par_iter()
                .map(|&c| {
                    let mut v: Vec<RawCk> = Vec::with_capacity(c.len() / 45);
                    let mut tp = TsParser::default();
                    scan::<4>(c, |&[cid, uid, camp, ts]| {
                        if cid.is_empty() || uid.is_empty() {
                            return;
                        }
                        let Some(t) = tp.parse(ts) else { return };
                        if t < ck_lo || t >= we {
                            return;
                        }
                        let Some(ci) = campaigns.get_b(camp) else { return };
                        let uk = Key::new(uid);
                        v.push(RawCk { uh: uk.h(), uk, ts: (t - ck_lo) as u32, cid: pack(cid), camp: ci });
                    });
                    v
                })
                .collect();
            log(&t0, "clicks parsed");
            (parts, ck_raw)
        },
    );

    // ---------- phase B: dedup on event_id, resolve persons, scatter by person ----------
    let links = &links;
    let parts: Vec<Vec<Take<Vec<Ev>>>> = parts.into_iter().map(|p| p.into_iter().map(Take::new).collect()).collect();
    let kept_parts: Vec<Vec<Vec<Kept>>> = (0..NB)
        .into_par_iter()
        .map(|bi| {
            // this bucket's slices, read in place
            let sl: Vec<&[Ev]> = parts.iter().map(|p| p[bi].get().as_slice()).collect();
            let mut offs: Vec<u32> = Vec::with_capacity(sl.len() + 1);
            let mut n = 0u32;
            for x in &sl {
                offs.push(n);
                n += x.len() as u32;
            }
            offs.push(n);
            let at = |i: u32| -> &Ev {
                let p = offs.partition_point(|&o| o <= i) - 1;
                &sl[p][(i - offs[p]) as usize]
            };
            let n = n as usize;
            let cap = (n * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            // slot: (tag = low 32 bits of hash, index of the earliest-ingested copy so far)
            let mut tab: Vec<(u32, u32)> = vec![(0, u32::MAX); cap];
            let mut win = vec![true; n];
            let mut i = 0u32;
            for x in &sl {
                for e in x.iter() {
                    let tag = e.eh as u32;
                    let mut s = ((e.eh >> 20) as usize) & mask;
                    loop {
                        let (tg, t) = tab[s];
                        if t == u32::MAX {
                            tab[s] = (tag, i);
                            break;
                        }
                        if tg == tag {
                            let o = at(t);
                            if o.eh == e.eh && unpack(o.eid) == unpack(e.eid) {
                                if e.ingest < o.ingest {
                                    win[t as usize] = false;
                                    tab[s].1 = i;
                                } else {
                                    win[i as usize] = false;
                                }
                                break;
                            }
                        }
                        s = (s + 1) & mask;
                    }
                    i += 1;
                }
            }
            drop(tab);
            let mut keep: Vec<&Ev> = Vec::with_capacity(n);
            let mut i = 0usize;
            for x in &sl {
                for e in x.iter() {
                    if win[i] && e.flags & 1 != 0 {
                        keep.push(e);
                    }
                    i += 1;
                }
            }
            let hs: Vec<u64> = keep.iter().map(|e| e.uk.h()).collect();
            let mut out: Vec<Vec<Kept>> = (0..NB).map(|_| Vec::with_capacity(keep.len() * 5 / 4 / NB + 32)).collect();
            for &h in hs.iter().take(PD) {
                links.prefetch(h);
            }
            for (k, e) in keep.iter().enumerate() {
                if let Some(&nh) = hs.get(k + PD) {
                    links.prefetch(nh);
                }
                let (ph, pk) = resolve(links, &e.uk, hs[k]);
                unsafe { out.get_unchecked_mut((ph >> SHIFT) as usize) }.push(Kept {
                    pk,
                    ts: e.ts,
                    price: e.price,
                    cat: e.cat,
                    purchase: e.flags & 2 != 0,
                });
            }
            drop(keep);
            for p in &parts {
                drop(p[bi].take());
            }
            out
        })
        .collect();
    drop(parts);
    log(&t0, "events deduped");

    let ck_parts: Vec<Vec<Vec<Ck>>> = ck_raw
        .par_iter()
        .map(|v| {
            let mut b: Vec<Vec<Ck>> = (0..NB).map(|_| Vec::with_capacity(v.len() / NB + 16)).collect();
            for (k, c) in v.iter().enumerate() {
                if let Some(nx) = v.get(k + PD) {
                    links.prefetch(nx.uh);
                }
                let (ph, pk) = resolve(links, &c.uk, c.uh);
                unsafe { b.get_unchecked_mut((ph >> SHIFT) as usize) }.push(Ck { pk, ts: c.ts, cid: c.cid, camp: c.camp });
            }
            b
        })
        .collect();
    drop(ck_raw);
    log(&t0, "clicks resolved");

    // ---------- phase C: sessions per person bucket ----------
    let day_lo = ws.div_euclid(DAY);
    let day_hi = (we - 1).div_euclid(DAY);
    let ndays = (day_hi - day_lo + 1) as usize;
    let ncs = ncamp + 1; // + organic
    let ncat = cat_names.len();
    let g = Glob { base: ck_lo, ndays, day_lo, ncamp, cat_rank: &cat_rank };

    struct Acc {
        users: Vec<URow>,
        camp: Vec<[i64; 4]>, // sessions, purchasing, revenue, assisted
    }
    let kept_parts: Vec<Vec<Take<Vec<Kept>>>> =
        kept_parts.into_iter().map(|p| p.into_iter().map(Take::new).collect()).collect();
    let accs: Vec<Acc> = (0..NB)
        .into_par_iter()
        .fold(
            || Acc { users: vec![], camp: vec![[0; 4]; ncs * ndays] },
            |mut acc, bi| {
                let mine: Vec<Vec<Kept>> = kept_parts.iter().map(|p| p[bi].take()).collect();
                let n: usize = mine.iter().map(|p| p.len()).sum();
                // group events by person (exact: hash and key) with counting sort
                let mut pt = PTab::new(n / 4 + 64);
                let mut cnt: Vec<u32> = vec![];
                let mut src: Vec<Er> = Vec::with_capacity(n);
                for p in &mine {
                    for e in p {
                        let i = pt.get_or_insert(e.pk.h(), &e.pk);
                        if i as usize == cnt.len() {
                            cnt.push(0);
                        }
                        cnt[i as usize] += 1;
                        src.push(Er { ts: e.ts, pid: i, price: e.price, cat: e.cat, purchase: e.purchase });
                    }
                }
                drop(mine);
                // LSD radix sort by ts, then a stable scatter by person: every person's slice
                // comes out in time order with no per-person sort
                radix_ts(&mut src);
                let np = cnt.len();
                let mut off: Vec<u32> = Vec::with_capacity(np + 1);
                let mut s = 0u32;
                for &c in &cnt {
                    off.push(s);
                    s += c;
                }
                off.push(s);
                let mut fill: Vec<u32> = off[..np].to_vec();
                let mut evs: Vec<Ec> = vec![Ec::default(); n];
                for e in &src {
                    let i = e.pid as usize;
                    evs[fill[i] as usize] = Ec { ts: e.ts, price: e.price, cat: e.cat, purchase: e.purchase };
                    fill[i] += 1;
                }
                drop(src);
                // clicks of the same people
                let mut cpid: Vec<u32> = vec![];
                let mut ccnt: Vec<u32> = vec![0; np];
                for p in &ck_parts {
                    for c in &p[bi] {
                        let i = pt.get(c.pk.h(), &c.pk);
                        if i != u32::MAX {
                            ccnt[i as usize] += 1;
                        }
                        cpid.push(i);
                    }
                }
                let mut coff: Vec<u32> = Vec::with_capacity(np + 1);
                let mut s = 0u32;
                for &c in &ccnt {
                    coff.push(s);
                    s += c;
                }
                coff.push(s);
                let mut cfill: Vec<u32> = coff[..np].to_vec();
                let mut cks: Vec<Cc> = vec![Cc::default(); s as usize];
                let mut k = 0;
                for p in &ck_parts {
                    for c in &p[bi] {
                        let i = cpid[k];
                        k += 1;
                        if i != u32::MAX {
                            cks[cfill[i as usize] as usize] = Cc { ts: c.ts, cid: c.cid, camp: c.camp };
                            cfill[i as usize] += 1;
                        }
                    }
                }
                drop(cpid);
                let mut st = State {
                    cnt: vec![0u32; ncamp],
                    pos: vec![0u32; ncamp],
                    active: vec![],
                    catsum: vec![0i64; ncat],
                    touched: vec![],
                };
                for i in 0..np {
                    let es = &evs[off[i] as usize..off[i + 1] as usize];
                    let cs = &mut cks[coff[i] as usize..coff[i + 1] as usize];
                    if cs.len() > 1 {
                        cs.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| unpack(a.cid).cmp(unpack(b.cid))));
                    }
                    person(pt.names[i], es, cs, &mut st, &mut acc.users, &mut acc.camp, &g);
                }
                acc
            },
        )
        .collect();
    drop(kept_parts);
    log(&t0, "sessions done");

    let mut camp = vec![[0i64; 4]; ncs * ndays];
    for a in &accs {
        for (x, y) in camp.iter_mut().zip(a.camp.iter()) {
            for k in 0..4 {
                x[k] += y[k];
            }
        }
    }
    let mut crow: Vec<(Bytes, Bytes, i32, [i64; 4])> = vec![];
    for c in 0..ncs {
        for d in 0..ndays {
            let s = camp[c * ndays + d];
            if s[0] > 0 || s[3] > 0 {
                let (id, ch): (Bytes, Bytes) = if c == ncamp { (b"organic", b"organic") } else { camp_names[c] };
                crow.push((id, ch, (day_lo + d as i64) as i32, s));
            }
        }
    }

    // ---------- phase D: write ----------
    let ud = outd.join("user_daily");
    let cd = outd.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();
    let cat_names = Arc::new(cat_names);
    rayon::join(
        || {
            accs.par_iter().enumerate().for_each(|(k, a)| {
                if !a.users.is_empty() || k == 0 {
                    write_users(&ud.join(format!("part-{k:05}.parquet")), &a.users, &cat_names);
                }
            })
        },
        || write_camps(&cd.join("part-00000.parquet"), &crow),
    );
    log(
        &t0,
        &format!("done: {} user rows, {} campaign rows", accs.iter().map(|a| a.users.len()).sum::<usize>(), crow.len()),
    );
}

/// Event with its person index, for the radix sort in phase C.
#[derive(Clone, Copy, Default)]
struct Er {
    ts: u32,
    pid: u32,
    price: u32,
    cat: u16,
    purchase: bool,
}

/// Stable LSD radix sort by ts (three passes of 11 bits).
fn radix_ts(v: &mut Vec<Er>) {
    let n = v.len();
    if n < 64 {
        v.sort_by_key(|e| e.ts);
        return;
    }
    let mut h = vec![[0u32; 2048]; 3];
    for e in v.iter() {
        h[0][(e.ts & 2047) as usize] += 1;
        h[1][((e.ts >> 11) & 2047) as usize] += 1;
        h[2][(e.ts >> 22) as usize] += 1;
    }
    let mut tmp: Vec<Er> = vec![Er::default(); n];
    for pass in 0..3 {
        let sh = 11 * pass as u32;
        // skip a pass whose digit is the same for every element
        if h[pass].iter().any(|&c| c as usize == n) {
            continue;
        }
        let mut pos = [0u32; 2048];
        let mut acc = 0u32;
        for d in 0..2048 {
            pos[d] = acc;
            acc += h[pass][d];
        }
        for e in v.iter() {
            let d = ((e.ts >> sh) & 2047) as usize;
            unsafe { *tmp.get_unchecked_mut(pos[d] as usize) = *e };
            pos[d] += 1;
        }
        std::mem::swap(v, &mut tmp);
    }
}

/// Compact per-event and per-click records for phase C.
#[derive(Clone, Copy, Default)]
struct Ec {
    ts: u32,
    price: u32,
    cat: u16,
    purchase: bool,
}
#[derive(Clone, Copy, Default)]
struct Cc {
    ts: u32,
    cid: u64,
    camp: u16,
}

/// Person table for one bucket: (hash, Key) -> dense index. Grows at half load.
struct PTab {
    mask: usize,
    slots: Vec<(u64, u32)>,
    names: Vec<Key>,
}
impl PTab {
    fn new(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(64);
        PTab { mask: cap - 1, slots: vec![(0, u32::MAX); cap], names: vec![] }
    }
    #[inline]
    fn get(&self, h: u64, k: &Key) -> u32 {
        let mut i = (h as usize) & self.mask;
        loop {
            let (sh, si) = self.slots[i];
            if si == u32::MAX {
                return u32::MAX;
            }
            if sh == h && self.names[si as usize].same(k) {
                return si;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    fn get_or_insert(&mut self, h: u64, k: &Key) -> u32 {
        let mut i = (h as usize) & self.mask;
        loop {
            let (sh, si) = self.slots[i];
            if si == u32::MAX {
                let id = self.names.len() as u32;
                self.slots[i] = (h, id);
                self.names.push(*k);
                if self.names.len() * 2 > self.slots.len() {
                    self.grow();
                }
                return id;
            }
            if sh == h && self.names[si as usize].same(k) {
                return si;
            }
            i = (i + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let cap = self.slots.len() * 2;
        let old = std::mem::replace(&mut self.slots, vec![(0, u32::MAX); cap]);
        self.mask = cap - 1;
        for (h, id) in old {
            if id != u32::MAX {
                let mut i = (h as usize) & self.mask;
                while self.slots[i].1 != u32::MAX {
                    i = (i + 1) & self.mask;
                }
                self.slots[i] = (h, id);
            }
        }
    }
}

struct Glob<'a> {
    base: i64, // epoch ms of relative ts 0
    ndays: usize,
    day_lo: i64,
    ncamp: usize,
    cat_rank: &'a [u32],
}

struct State {
    cnt: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u16>,
    catsum: Vec<i64>,
    touched: Vec<u16>,
}

#[inline]
fn flush_day(st: &mut State, row: &mut URow, users: &mut Vec<URow>, cat_rank: &[u32]) {
    if row.sessions == 0 {
        return;
    }
    let mut best: i32 = -1;
    let mut bs = 0i64;
    for &c in &st.touched {
        let s = st.catsum[c as usize];
        if best < 0 || s > bs || (s == bs && cat_rank[c as usize] < cat_rank[best as usize]) {
            best = c as i32;
            bs = s;
        }
        st.catsum[c as usize] = 0;
    }
    st.touched.clear();
    row.top = best;
    users.push(*row);
}

/// One person: events sorted by ts, clicks sorted by (ts, click_id).
fn person(name: Key, es: &[Ec], cs: &[Cc], st: &mut State, users: &mut Vec<URow>, camp: &mut [[i64; 4]], g: &Glob) {
    let mut row = URow { name, day: i32::MIN, sessions: 0, events: 0, revenue: 0, top: -1 };
    let (mut lo, mut hi) = (0usize, 0usize);
    let mut i = 0;
    let n = es.len();
    while i < n {
        let start = es[i].ts as i64;
        let mut j = i + 1;
        while j < n && (es[j].ts - es[j - 1].ts) as i64 <= GAP {
            j += 1;
        }
        let day = (g.base + start).div_euclid(DAY);
        if day as i32 != row.day {
            flush_day(st, &mut row, users, g.cat_rank);
            row = URow { name, day: day as i32, sessions: 0, events: 0, revenue: 0, top: -1 };
        }
        let mut rev = 0i64;
        let mut purch = false;
        for e in &es[i..j] {
            if e.purchase {
                purch = true;
                rev += e.price as i64;
                let c = e.cat as usize;
                if st.catsum[c] == 0 {
                    st.touched.push(e.cat);
                }
                st.catsum[c] += e.price as i64;
            }
        }
        row.sessions += 1;
        row.events += (j - i) as u32;
        row.revenue += rev;

        // clicks in [start - 7d, start]: a sliding window with per-campaign counts
        while hi < cs.len() && cs[hi].ts as i64 <= start {
            let c = cs[hi].camp as usize;
            if st.cnt[c] == 0 {
                st.pos[c] = st.active.len() as u32;
                st.active.push(c as u16);
            }
            st.cnt[c] += 1;
            hi += 1;
        }
        while lo < hi && (cs[lo].ts as i64) < start - WEEK {
            let c = cs[lo].camp as usize;
            st.cnt[c] -= 1;
            if st.cnt[c] == 0 {
                let p = st.pos[c] as usize;
                let last = *st.active.last().unwrap();
                st.active[p] = last;
                st.pos[last as usize] = p as u32;
                st.active.pop();
            }
            lo += 1;
        }
        // credit: latest ts, then smallest click_id (clicks are sorted that way)
        let credited = if hi > lo {
            let t = cs[hi - 1].ts;
            let mut k = hi - 1;
            while k > lo && cs[k - 1].ts == t {
                k -= 1;
            }
            cs[k].camp as usize
        } else {
            g.ncamp
        };
        let di = (day - g.day_lo) as usize;
        debug_assert!(di < g.ndays);
        let s = &mut camp[credited * g.ndays + di];
        s[0] += 1;
        s[1] += purch as i64;
        s[2] += rev;
        for &a in &st.active {
            camp[a as usize * g.ndays + di][3] += 1;
        }
        i = j;
    }
    flush_day(st, &mut row, users, g.cat_rank);
    for &a in &st.active {
        st.cnt[a as usize] = 0;
    }
    st.active.clear();
}

// ---------- parquet ----------
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .build(),
    )
}

fn write_users(path: &Path, rows: &[URow], cats: &[Bytes]) {
    let schema = Arc::new(
        parse_message_type(
            "message schema {
              required binary user_id (STRING);
              required int32 day (DATE);
              required int64 sessions;
              required int64 events;
              required int64 revenue_cents;
              optional binary top_category (STRING);
            }",
        )
        .unwrap(),
    );
    let f = File::create(path).unwrap();
    let mut w = SerializedFileWriter::new(f, schema, props()).unwrap();
    let mut rg = w.next_row_group().unwrap();
    let mut idx = 0;
    while let Some(mut col) = rg.next_column().unwrap() {
        match idx {
            0 => {
                // one shared buffer for all names: no allocation per row
                let mut buf: Vec<u8> = Vec::with_capacity(rows.len() * 12);
                let mut ends: Vec<usize> = Vec::with_capacity(rows.len());
                for r in rows {
                    r.name.append_to(&mut buf);
                    ends.push(buf.len());
                }
                let big = bytes::Bytes::from(buf);
                let mut st = 0;
                let v: Vec<ByteArray> = ends
                    .iter()
                    .map(|&e| {
                        let b = ByteArray::from(big.slice(st..e));
                        st = e;
                        b
                    })
                    .collect();
                col.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
            }
            1 => {
                let v: Vec<i32> = rows.iter().map(|r| r.day).collect();
                col.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
            }
            2..=4 => {
                let v: Vec<i64> = rows
                    .iter()
                    .map(|r| match idx {
                        2 => r.sessions as i64,
                        3 => r.events as i64,
                        _ => r.revenue,
                    })
                    .collect();
                col.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
            }
            _ => {
                let v: Vec<ByteArray> =
                    rows.iter().filter(|r| r.top >= 0).map(|r| ByteArray::from(bytes::Bytes::from_static(cats[r.top as usize]))).collect();
                let d: Vec<i16> = rows.iter().map(|r| (r.top >= 0) as i16).collect();
                col.typed::<ByteArrayType>().write_batch(&v, Some(&d), None).unwrap();
            }
        }
        col.close().unwrap();
        idx += 1;
    }
    rg.close().unwrap();
    w.close().unwrap();
}

fn write_camps(path: &Path, rows: &[(Bytes, Bytes, i32, [i64; 4])]) {
    let schema = Arc::new(
        parse_message_type(
            "message schema {
              required binary campaign_id (STRING);
              required binary channel (STRING);
              required int32 day (DATE);
              required int64 sessions;
              required int64 purchasing_sessions;
              required int64 revenue_cents;
              required int64 assisted_sessions;
            }",
        )
        .unwrap(),
    );
    let f = File::create(path).unwrap();
    let mut w = SerializedFileWriter::new(f, schema, props()).unwrap();
    let mut rg = w.next_row_group().unwrap();
    let mut idx = 0;
    while let Some(mut col) = rg.next_column().unwrap() {
        match idx {
            0 | 1 => {
                let v: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(bytes::Bytes::from_static(if idx == 0 { r.0 } else { r.1 }))).collect();
                col.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
            }
            2 => {
                let v: Vec<i32> = rows.iter().map(|r| r.2).collect();
                col.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
            }
            k => {
                let v: Vec<i64> = rows.iter().map(|r| r.3[k - 3]).collect();
                col.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
            }
        }
        col.close().unwrap();
        idx += 1;
    }
    rg.close().unwrap();
    w.close().unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leak(v: Vec<u8>) -> Bytes {
        Box::leak(v.into_boxed_slice())
    }

    #[test]
    fn keys_roundtrip_and_compare() {
        let mut x = 12345u64;
        let mut all = vec![];
        for n in 0..24 {
            for _ in 0..50 {
                let v: Vec<u8> = (0..n)
                    .map(|_| {
                        x = fmix(x + 1);
                        [b'u', b'U', b' ', 0, b'\r', 0xFF, b'9'][(x % 7) as usize]
                    })
                    .collect();
                let s = leak(v);
                let k = Key::new(s);
                assert_eq!(k.bytes(), s, "len {n}");
                all.push((s, k));
            }
        }
        for (s1, k1) in &all {
            for (s2, k2) in all.iter().take(300) {
                assert_eq!(k1.same(k2), s1 == s2);
            }
        }
    }

    #[test]
    fn timestamps() {
        let mut p = TsParser::default();
        let ok = |p: &mut TsParser, s: &str| p.parse(s.as_bytes());
        assert_eq!(ok(&mut p, "2026-10-03T23:58:10Z"), ok(&mut p, "2026-10-03T19:58:10-04:00"));
        assert_eq!(ok(&mut p, "2026-10-03T23:58:10Z"), Some(1791071890000));
        assert_eq!(ok(&mut p, "0001791071890000"), Some(1791071890000));
        assert_eq!(ok(&mut p, "2026-10-03T23:58:10+00:00"), ok(&mut p, "2026-10-03T23:58:10-00:00"));
        for bad in [
            "", "2026-10-03T23:58:10.000Z", "2026-10-03T23:58:10z", "2026-10-03t23:58:10Z", "2026-10-03 23:58:10Z",
            "2026-10-03T23:58:10", "1791071890000.0", "1.79e12", "-5", "2026-13-03T23:58:10Z", "2026-02-30T23:58:10Z",
            "2027-02-29T10:00:00Z", "2026-10-03T24:00:00Z", "2026-10-03T23:60:00Z", "2026-10-03T23:58:60Z",
            "2026-10-03T23:58:10+24:00", "2026-10-03T23:58:10+05:60", "2026-10-03T23:58:10+0200",
            "99999999999999999999999", "2026-10-0３T23:58:10Z", "2026-10-03T2３:58:10Z", "2026/10/03T23:58:10Z",
            "2026-10-03T23-58:10Z", "2026-10-03T23:58-10Z",
        ] {
            assert_eq!(ok(&mut p, bad), None, "{bad:?}");
        }
        assert!(ok(&mut p, "2028-02-29T10:00:00Z").is_some());
        assert!(ok(&mut p, "2000-02-29T10:00:00Z").is_some());
        assert!(ok(&mut p, "1900-02-29T10:00:00Z").is_none());
        assert_eq!(ok(&mut p, "86400000"), Some(86_400_000));
        for v in [1u64, 9, 10, 99, 123456789, 1791071890000, 1791071890001, 9999999999999999, 100000000, 1000000000000000] {
            for pad in 0..4 {
                let t = format!("{}{}", "0".repeat(pad), v);
                assert_eq!(ok(&mut p, &t), if t.len() <= 19 { Some(v as i64) } else { None }, "{t}");
            }
        }
        for bad in ["17910718900:0", "179107189000/", "1791071890000 ", " 1791071890000", "12345678a", "a12345678"] {
            assert_eq!(ok(&mut p, bad), None, "{bad:?}");
        }
    }

    #[test]
    fn csv_dialect() {
        let text: Bytes = b"a,b\n\"x\",\"\"\n\"x,y\"\nx\"y,z\n,\n\n\"q\"\"\",z\nlast,\"r\r\"";
        let mut got = vec![];
        scan::<2>(text, |f| got.push((f[0].to_vec(), f[1].to_vec())));
        let exp: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (b"a".to_vec(), b"b".to_vec()),
            (b"x".to_vec(), b"".to_vec()),
            (b"".to_vec(), b"".to_vec()),
            (b"last".to_vec(), b"r\r".to_vec()),
        ];
        assert_eq!(got, exp);
    }
}
