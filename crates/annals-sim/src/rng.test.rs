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
