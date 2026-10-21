use super::*;

#[test]
fn streams_are_independent_of_each_other() {
    let mut a1 = Rng::new(7, 1);
    let mut a2 = Rng::new(7, 1);
    let mut b = Rng::new(7, 2);
    let first: Vec<u64> = (0..5).map(|_| a1.next_u64()).collect();
    let _ = b.next_u64();
    let second: Vec<u64> = (0..5).map(|_| a2.next_u64()).collect();
    assert_eq!(first, second);
}

#[test]
fn below_stays_in_range_and_covers_it() {
    let mut r = Rng::new(3, 3);
    let mut seen = [0u32; 6];
    for _ in 0..60_000 {
        seen[r.below(6) as usize] += 1;
    }
    for n in seen {
        assert!((9_000..11_000).contains(&n), "{seen:?}");
    }
    assert_eq!(r.below(0), 0);
    assert_eq!(r.below(1), 0);
}

#[test]
fn a_use_rebuilds_the_same_stream() {
    let a: Vec<u64> = {
        let mut r = Rng::for_use(9, 4, 2);
        (0..3).map(|_| r.next_u64()).collect()
    };
    let mut r = Rng::for_use(9, 4, 2);
    assert_eq!(a, (0..3).map(|_| r.next_u64()).collect::<Vec<_>>());
    assert_ne!(Rng::for_use(9, 4, 2), Rng::for_use(9, 4, 3));
}

#[test]
fn mixing_is_order_sensitive_and_stable() {
    assert_ne!(mix(&[1, 2]), mix(&[2, 1]));
    assert_eq!(mix(&[1, 2]), mix(&[1, 2]));
    assert_eq!(derive(5, 1), mix(&[5, 1]));
    assert_ne!(derive(5, 1), derive(5, 2));
    assert_eq!(fnv1a("abc"), fnv1a(b"abc"));
    // Pinned: these values are part of every stored log's meaning.
    assert_eq!(fnv1a(""), 0xcbf2_9ce4_8422_2325);
    assert_eq!(fnv1a("a"), 0xaf63_dc4c_8601_ec8c);
    assert_eq!(Rng::new(0, 0).next_u64(), 0xe220_a839_7b1d_cdaf);
}

/// The generator whose next draw is `v`: SplitMix64's output step run
/// backwards, so a test can hand [`Rng::below`] an exact value.
fn about_to_draw(v: u64) -> Rng {
    // x ^ (x >> k), undone a k-bit band at a time from the top.
    fn unshift(x: u64, k: u32) -> u64 {
        let mut r = x;
        for _ in 0..=64 / k {
            r = x ^ (r >> k);
        }
        r
    }
    // The inverse of an odd multiplier mod 2^64, by Newton's iteration.
    fn inverse(a: u64) -> u64 {
        let mut x = a;
        for _ in 0..6 {
            x = x.wrapping_mul(2u64.wrapping_sub(a.wrapping_mul(x)));
        }
        x
    }
    let mut z = unshift(v, 31);
    z = unshift(z.wrapping_mul(inverse(0x94D0_49BB_1331_11EB)), 27);
    z = unshift(z.wrapping_mul(inverse(0xBF58_476D_1CE4_E5B9)), 30);
    let r = Rng {
        state: z.wrapping_sub(0x9E37_79B9_7F4A_7C15),
    };
    assert_eq!(drawn(r, 0).next_u64(), v, "the inversion is exact");
    r
}

/// `r` after `n` draws.
fn drawn(mut r: Rng, n: usize) -> Rng {
    for _ in 0..n {
        r.next_u64();
    }
    r
}

#[test]
fn below_keeps_the_last_whole_run_and_rejects_the_remainder() {
    // 2^64 - 4 is the largest multiple of 6 that fits: draws below it are
    // used, and the four above it would favour 0..4, so they are redrawn.
    let last_kept = 0xFFFF_FFFF_FFFF_FFFB;
    let first_rejected = 0xFFFF_FFFF_FFFF_FFFC;

    let mut r = about_to_draw(last_kept);
    assert_eq!(r.below(6), (last_kept % 6) as u32);
    assert_eq!(r, drawn(about_to_draw(last_kept), 1), "used in one draw");

    let start = about_to_draw(first_rejected);
    let mut r = start;
    let redrawn = drawn(start, 1).next_u64();
    assert!(redrawn < first_rejected, "the redraw is kept");
    assert_eq!(r.below(6), (redrawn % 6) as u32);
    assert_eq!(r, drawn(start, 2), "rejected, then redrawn once");
}

#[test]
fn seeds_and_streams_are_pinned() {
    // Pinned: these values are part of every stored log's meaning.
    assert_eq!(mix(&[]), 0x5EED_5EED_5EED_5EED);
    assert_eq!(mix(&[1, 2]), 0x749e_15d4_53b0_915c);
    assert_eq!(derive(5, 1), 0xc584_1682_0294_f1c8);
    assert_eq!(Rng::for_use(9, 4, 2).next_u64(), 0xa49d_9127_adff_5dc5);
    let mut r = Rng::new(7, 1);
    assert_eq!([r.below(6), r.below(6), r.below(6)], [5, 5, 1]);
}
