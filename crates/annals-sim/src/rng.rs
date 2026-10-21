//! Per-entity random streams, and seeds derived from seeds.
//!
//! **Never one shared stream.** With a shared stream, draw order depends on
//! iteration order, and adding one entity to a simulation perturbs every other
//! entity's rolls. A stream per owner keeps an entity's dice identical
//! whatever else is on the field.
//!
//! **Rolls are re-derived, not remembered.** A roll's stream is a pure
//! function of `(seed, owner, use index)` — [`Rng::for_use`] — so nothing has
//! to hold a generator's position between commands. The use index is a count
//! the domain already keeps in its folded state (how many times this card has
//! been played, this unit has fired). A domain's `annals::Domain::Ctx` therefore
//! needs no RNG position, and a stream replayed from its log and cut anywhere
//! between commands carries on with exactly the rolls the original made.
//!
//! Anything a roll spawns that needs dice of its own takes a seed derived
//! with [`derive`] (or [`mix`] over more parts), never the parent's stream.

/// SplitMix64: small, fast, dependency-free, and identical on every
/// platform.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Rng {
    state: u64,
}

impl Rng {
    /// The stream `owner` draws from under `seed`. The owner is mixed in so
    /// streams stay well separated even for small seeds and ids.
    pub fn new(seed: u64, owner: u64) -> Rng {
        let s = seed
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(owner.wrapping_mul(0xBF58_476D_1CE4_E5B9));
        Rng { state: s }
    }

    /// The stream for one use of something owned: a pure function of
    /// `(seed, owner, use_index)`, so it can be rebuilt at any time from data
    /// the folded state already holds.
    pub fn for_use(seed: u64, owner: u64, use_index: u64) -> Rng {
        Rng::new(mix(&[seed, owner, use_index]), 0)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, n)`; 0 when `n` is 0 or 1. Rejection-sampled, so it is
    /// unbiased and integer.
    pub fn below(&mut self, n: u32) -> u32 {
        if n <= 1 {
            return 0;
        }
        let n64 = n as u64;
        let limit = u64::MAX - (u64::MAX % n64);
        loop {
            let v = self.next_u64();
            if v < limit {
                return (v % n64) as u32;
            }
        }
    }
}

/// Fold values into a seed. Order matters, and every input moves the output.
pub fn mix(parts: &[u64]) -> u64 {
    let mut acc = 0x5EED_5EED_5EED_5EEDu64;
    for p in parts {
        let mut s = Rng {
            state: acc ^ p.wrapping_mul(0x9E37_79B9_7F4A_7C15),
        };
        acc = s.next_u64();
    }
    acc
}

/// A child seed: `seed` with `salt` folded in. For something a roll spawns
/// that needs its own dice — distinct salts give independent children.
pub fn derive(seed: u64, salt: u64) -> u64 {
    mix(&[seed, salt])
}

/// FNV-1a over bytes. Stable across platforms and releases, which `std`'s
/// hasher is not — for deriving seeds from names.
pub fn fnv1a(bytes: impl AsRef<[u8]>) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes.as_ref() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[cfg(test)]
#[path = "rng.test.rs"]
mod tests;
