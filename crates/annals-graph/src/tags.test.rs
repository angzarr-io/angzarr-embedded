use super::*;

#[test]
fn tags_read_as_written() {
    let mut r = TagRegistry::new();
    let (roof, v) = r.parse("terrain:rooftop").unwrap();
    assert_eq!(v, TagValue::Flag);
    let (smoke, v) = r.parse("hazard:smoke=3").unwrap();
    assert_eq!(v, TagValue::Int(3));
    let (_, v) = r.parse("cover:grade=-1").unwrap();
    assert_eq!(v, TagValue::Int(-1));
    let (owner, v) = r.parse("faction:owner=crimson").unwrap();
    assert_eq!(r.show(owner, v), "faction:owner=crimson");
    assert_eq!(r.show(smoke, TagValue::Int(3)), "hazard:smoke=3");
    assert_eq!(r.name(roof), "terrain:rooftop");
}

#[test]
fn interning_is_stable_and_remembers_namespaces() {
    let mut r = TagRegistry::new();
    let (a, _) = r.parse("terrain:rooftop").unwrap();
    let (b, _) = r.parse("terrain:doorway").unwrap();
    let (c, _) = r.parse("hazard:smoke=1").unwrap();
    assert_eq!(r.parse("terrain:rooftop").unwrap().0, a);
    assert_eq!(r.get("terrain:doorway"), Some(b));
    assert_eq!(r.get("terrain:sewer"), None);
    assert_eq!(r.namespace_of(a), r.namespace_of(b));
    assert_ne!(r.namespace_of(a), r.namespace_of(c));
    // Same name, different namespace: different keys.
    let (d, _) = r.parse("hazard:rooftop").unwrap();
    assert_ne!(a, d);
}

#[test]
fn malformed_tags_are_refused_with_the_text() {
    let mut r = TagRegistry::new();
    for (bad, problem) in [
        (":rooftop", TagProblem::EmptyNamespace),
        ("terrain:", TagProblem::BadName),
        ("Terrain:roof", TagProblem::BadNamespace),
        ("terrain:roof top", TagProblem::BadName),
        ("faction:owner=Crimson", TagProblem::BadSymbol),
        ("", TagProblem::BadName),
    ] {
        let e = r.parse(bad).unwrap_err();
        assert_eq!(
            e,
            BadTag {
                text: bad.to_string(),
                problem
            },
            "{bad:?}"
        );
    }
    assert_eq!(
        r.parse("Terrain:roof").unwrap_err().to_string(),
        "bad tag \"Terrain:roof\": a namespace is lowercase letters, digits, _ and -"
    );
}

#[test]
fn the_namespace_is_optional_but_listed() {
    let mut r = TagRegistry::new();
    let (bare, _) = r.parse("rooftop").unwrap();
    let (spaced, _) = r.parse("terrain:rooftop").unwrap();
    assert_ne!(bare, spaced);
    assert_eq!(r.name(bare), "rooftop");
    assert_eq!(r.get("rooftop"), Some(bare));
    assert_eq!(r.unnamespaced(), vec![bare]);
}

#[test]
fn elevation_is_just_a_number() {
    let mut r = TagRegistry::new();
    let (elev, v) = r.parse("terrain:elevation=600").unwrap();
    let roof = TagSet::new().with(elev, v);
    assert!(roof.value(elev).and_then(TagValue::int).unwrap() >= 300);
}

#[test]
fn a_set_stays_sorted_and_changes() {
    let a = TagSet::new()
        .with(TagId(5), TagValue::Flag)
        .with(TagId(1), TagValue::Int(2));
    let b = TagSet::new()
        .with(TagId(1), TagValue::Int(2))
        .with(TagId(5), TagValue::Flag);
    assert_eq!(a, b);
    let mut s = TagSet::new().with(TagId(3), TagValue::Int(1));
    s.set(TagId(3), TagValue::Int(4)); // the smoke thickens
    assert_eq!(s.value(TagId(3)).and_then(TagValue::int), Some(4));
    assert!(s.remove(TagId(3))); // and clears
    assert!(!s.has(TagId(3)));
}
