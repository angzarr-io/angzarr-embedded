use super::parallel;

#[test]
fn keeps_the_order() {
    let items: Vec<u64> = (0..1000).collect();
    let out = parallel(&items, |x| x * x);
    assert_eq!(out, items.iter().map(|x| x * x).collect::<Vec<_>>());
}

#[test]
fn handles_nothing() {
    assert!(parallel(&[] as &[u8], |x| *x).is_empty());
}
