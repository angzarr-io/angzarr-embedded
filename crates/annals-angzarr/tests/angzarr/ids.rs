//! annals' ids are angzarr's: the kernel's UUID is the wire's 16 bytes,
//! derived ids come from `angzarr_proto::ids`, and text is for display and
//! input only.

use annals::next::{Correlation, DeriveIds, Fingerprint, Root, Uuid};
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

#[test]
fn the_fingerprint_is_angzarrs_uuidv5_over_the_any_as_received() {
    let any = |type_url: &str, value: &[u8]| prost_types::Any {
        type_url: type_url.to_string(),
        value: value.to_vec(),
    };
    let deposit =
        annals_angzarr::fingerprint(&any("type.googleapis.com/examples.Deposit", &[0x08, 0x2a]));
    assert_eq!(
        deposit.0.to_string(),
        "6d42fa9e-0dca-5047-94b1-0ae371532630",
        "types.proto's worked example (derived_ids region)"
    );
    assert_eq!(deposit.0.version(), 5);
    assert_eq!(
        annals_angzarr::fingerprint(&any("type.googleapis.com/examples.Deposit", &[]))
            .0
            .to_string(),
        "838a6be3-ad9d-5945-be34-61ec4c3d6432",
        "an empty value"
    );
    assert_eq!(
        annals_angzarr::fingerprint(&any("", &[])).0.to_string(),
        "d0be6173-febd-50a7-a4fe-d083d14497ca",
        "an empty type URL and value"
    );
    assert_ne!(
        annals_angzarr::fingerprint(&any("a.B", b"Cx")),
        annals_angzarr::fingerprint(&any("a.BC", b"x")),
        "the type URL is length-prefixed, so it never runs into the value"
    );
    assert_eq!(
        deposit,
        Fingerprint(
            ids::from_wire(&angzarr_proto::ids::fingerprint(
                "type.googleapis.com/examples.Deposit",
                &[0x08, 0x2a]
            ))
            .unwrap()
        ),
        "angzarr's own function, not a second implementation"
    );
}
