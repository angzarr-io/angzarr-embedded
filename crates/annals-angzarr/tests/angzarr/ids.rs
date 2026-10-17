//! annals' ids are angzarr's: the kernel's UUID is the wire's 16 bytes,
//! derived ids come from `angzarr_proto::ids`, and text is for display and
//! input only.

use annals::next::{Correlation, DeriveIds, Root, Uuid};
use annals_angzarr::ids::{self, ANGZARR_IDS};
use annals_angzarr::pb;
use annals_angzarr::BookError;

#[test]
fn the_kernels_uuid_is_the_wire_uuid() {
    let bytes = [
        0x01, 0x8f, 0x3c, 0x2a, 0x7b, 0x10, 0x7c, 0xde, 0x8a, 0x00, 0x00, 0x11, 0x22, 0x33, 0x44,
        0xff,
    ];
    let id = Uuid(bytes);
    assert_eq!(ids::to_wire(id), pb::Uuid::from_bytes(bytes));
    assert_eq!(ids::from_wire(&ids::to_wire(id)), Ok(id));
    assert_eq!(
        id.to_string(),
        pb::Uuid::from_bytes(bytes).to_string(),
        "one canonical text in both"
    );
    assert_eq!(
        ids::from_wire(&pb::Uuid { value: vec![0; 15] }),
        Err(BookError::BadId { len: 15 })
    );
}

#[test]
fn text_is_parsed_only_at_the_boundary_as_angzarr_parses_it() {
    let text = "018f3c2a-7b10-7cde-8a00-0011223344ff";
    let id = ids::parse(text).unwrap();
    assert_eq!(id.to_string(), text);
    assert_eq!(ids::parse(&text.to_uppercase()), Ok(id), "either case");
    assert_eq!(
        ids::parse("018f3c2a7b107cde8a000011223344ff"),
        Err(BookError::BadIdText),
        "the hyphenated form only, as every angzarr language"
    );
}

#[test]
fn derived_ids_are_angzarrs_uuidv5s() {
    let root = ids::compute_root("player", "alice@example.com").unwrap();
    assert_eq!(
        Uuid(root.to_bytes()).to_string(),
        "cfe936c0-2912-5ca9-96e6-f37c0b441759",
        "types.proto's worked example"
    );
    assert_eq!(Uuid(root.to_bytes()).version(), 5);
    assert_eq!(
        ids::compute_root("a:b", "c"),
        Err(BookError::DomainContainsColon {
            domain: "a:b".to_string()
        })
    );
    assert_ne!(
        root,
        Root::from_bytes(
            ids::fact_correlation("player:alice@example.com")
                .0
                .to_bytes()
        ),
        "two namespaces: a correlation never equals a root by accident"
    );

    let correlation = ids::fact_correlation("pi_1234");
    assert_eq!(
        correlation.0.to_string(),
        "917f26ae-ea2e-58af-86de-77013a69617e"
    );
    assert_eq!(
        ANGZARR_IDS.fact_correlation("pi_1234"),
        correlation,
        "what the kernel is handed"
    );
    assert_eq!(
        correlation,
        Correlation(ids::from_wire(&angzarr_proto::ids::fact_correlation("pi_1234")).unwrap())
    );
}
