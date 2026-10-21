//! Conflict resolution: all three strategies, COMMUTATIVE the default where
//! the state declares conflict units, STRICT otherwise.

use crate::next::v1::ConflictResolution;
use crate::next::{
    Failure, Handled, InMemoryStore, KernelError, Key, Payload, Recovery, UnitPath, Units,
};

use crate::next::toys::{
    at, command, cover, handle, opened, payloads, registry, set, shelf, Account, AccountCmd, Edit,
    Shelf, ShelfCmd, ShelfEvent,
};

/// A shelf (stream 3) with items a = 1 then b = 2: sequences 0 and 1.
fn shelf_a_b(store: &mut InMemoryStore) {
    let registry = registry();
    handle::<Shelf>(
        store,
        &registry,
        command(cover("shelf", 3), set("a", 1)).id(1),
        &at(1),
    )
    .unwrap();
    handle::<Shelf>(
        store,
        &registry,
        command(cover("shelf", 3), set("b", 2)).id(2),
        &at(2),
    )
    .unwrap();
}

fn shelf_events(handled: &Handled) -> Vec<ShelfEvent> {
    payloads(&handled.books())
}

fn stale(expected: u64, actual: u64) -> Failure<std::convert::Infallible> {
    Failure::Kernel(KernelError::Stale {
        stream: cover("shelf", 3),
        expected,
        actual,
    })
}

#[test]
fn commutative_is_the_default_for_a_state_with_conflict_units() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    shelf_a_b(&mut store);

    // Issued when only a existed (expected 1), no resolution asked for: the
    // window changed items[b], the command changes items[a]. Disjoint.
    let disjoint = command(cover("shelf", 3), set("a", 5))
        .id(3)
        .expecting(1, ConflictResolution::Unspecified);
    let handled = handle::<Shelf>(&mut store, &registry, disjoint, &at(3)).unwrap();
    assert_eq!(
        shelf_events(&handled),
        [ShelfEvent::WasSet(shelf::WasSet {
            item: "a".to_string(),
            count: 5,
            window: None
        })],
        "kept; the kernel resolved, so decide was never handed the window"
    );

    // Issued at the same point, changing items[b], which the window changed.
    let overlapping = command(cover("shelf", 3), set("b", 7))
        .id(4)
        .expecting(1, ConflictResolution::Unspecified);
    let failure = handle::<Shelf>(&mut store, &registry, overlapping, &at(4))
        .err()
        .unwrap();
    assert_eq!(failure, stale(1, 3));
    assert_eq!(failure.recovery(), Recovery::Retry);
}

#[test]
fn strict_is_the_default_for_a_state_without_conflict_units() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 100);

    let behind = command(cover("account", 1), AccountCmd::Deposit(1).to_any())
        .id(3)
        .expecting(1, ConflictResolution::Unspecified);
    assert_eq!(
        handle::<Account>(&mut store, &registry, behind, &at(20))
            .err()
            .unwrap(),
        Failure::Kernel(KernelError::Stale {
            stream: cover("account", 1),
            expected: 1,
            actual: 2
        }),
        "refused before decide"
    );
}

#[test]
fn a_command_type_configured_strict_is_strict() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    shelf_a_b(&mut store);
    let note = command(
        cover("shelf", 3),
        ShelfCmd::Note("restock".to_string()).to_any(),
    )
    .id(3)
    .expecting(1, ConflictResolution::Unspecified);
    assert_eq!(
        handle::<Shelf>(&mut store, &registry, note, &at(3))
            .err()
            .unwrap(),
        stale(1, 2),
        "a note is never merged, though the shelf is COMMUTATIVE"
    );
}

#[test]
fn an_unmarked_field_conflicts_with_everything() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    shelf_a_b(&mut store);
    let note = command(
        cover("shelf", 3),
        ShelfCmd::Note("restock".to_string()).to_any(),
    )
    .id(3);
    handle::<Shelf>(&mut store, &registry, note, &at(3)).unwrap();

    // Behind the note: the window changed `notes`, which carries no marker.
    let behind = command(cover("shelf", 3), set("c", 1))
        .id(4)
        .expecting(2, ConflictResolution::Commutative);
    assert_eq!(
        handle::<Shelf>(&mut store, &registry, behind, &at(4))
            .err()
            .unwrap(),
        stale(2, 3),
        "never a blind merge"
    );
}

#[test]
fn aggregate_handles_demands_the_nonce_back() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    shelf_a_b(&mut store);

    let handled = handle::<Shelf>(
        &mut store,
        &registry,
        command(cover("shelf", 3), set("b", 7))
            .id(3)
            .expecting(1, ConflictResolution::AggregateHandles),
        &at(3),
    )
    .unwrap();
    assert_eq!(
        shelf_events(&handled),
        [ShelfEvent::WasSet(shelf::WasSet {
            item: "b".to_string(),
            count: 7,
            window: Some(1)
        })],
        "decide saw the one event it missed, and acknowledged it"
    );

    let unresolved = shelf::Set {
        item: "a".to_string(),
        count: 9,
        resolve: false,
    };
    let failure = handle::<Shelf>(
        &mut store,
        &registry,
        command(cover("shelf", 3), crate::next::pack(&unresolved))
            .id(4)
            .expecting(1, ConflictResolution::AggregateHandles),
        &at(4),
    )
    .err()
    .unwrap();
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::Unresolved {
            stream: cover("shelf", 3),
            expected: 1,
            actual: 3
        }),
        "a decide that never acknowledged the window fails as stale"
    );
    assert_eq!(failure.recovery(), Recovery::Retry);
}

#[test]
fn an_expected_sequence_past_the_head_is_stale() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    shelf_a_b(&mut store);
    for (n, resolution) in [
        ConflictResolution::Strict,
        ConflictResolution::AggregateHandles,
        ConflictResolution::Commutative,
    ]
    .into_iter()
    .enumerate()
    {
        let ahead = command(cover("shelf", 3), set("z", 1))
            .id(10 + n as u128)
            .expecting(5, resolution);
        assert_eq!(
            handle::<Shelf>(&mut store, &registry, ahead, &at(5))
                .err()
                .unwrap(),
            stale(5, 2),
            "{resolution:?}"
        );
    }
}

/// Units from paths written as text (no bytes keys).
fn units(paths: &[&str]) -> Units {
    paths
        .iter()
        .map(|p| match p.strip_prefix('*') {
            Some(rest) => UnitPath::field(rest).wildcard(),
            None => UnitPath::field(p),
        })
        .collect()
}

#[test]
fn unit_paths_follow_angzarrs_syntax() {
    let items = UnitPath::field("items");
    let path = |p: &UnitPath| p.to_string();
    assert_eq!(
        path(&items.key(Key::Uint(42)).dot("weight")),
        "items[42].weight"
    );
    assert_eq!(path(&items.key(Key::Int(-1))), "items[-1]");
    assert_eq!(path(&items.key(Key::Bool(true))), "items[true]");
    assert_eq!(path(&UnitPath::field("columns").order()), "columns[#order]");
    assert_eq!(
        path(&items.key(Key::Str("#order"))),
        "items[\\#order]",
        "a key never starts with a bare #, so it never collides with [#order]"
    );
    assert_eq!(path(&items.key(Key::Str("a]b\\c"))), "items[a\\]b\\\\c]");
    assert_eq!(
        items.key(Key::Bytes(&[0xff, b']'])).as_bytes(),
        b"items[\xff\\]]",
        "bytes keys are raw"
    );
    assert_eq!(path(&UnitPath::field("notes").wildcard()), "*notes");
    assert!(UnitPath::unknown().is_wildcard());
}

#[test]
fn units_overlap_by_angzarrs_rule() {
    let overlaps = |a: &[&str], b: &[&str]| units(a).overlaps(&units(b));
    let path = |p: &str| UnitPath::field(p);
    assert!(path("items[42]").overlaps(&path("items[42].weight")));
    assert!(path("items[42].weight").overlaps(&path("items[42]")));
    assert!(!path("items[4]").overlaps(&path("items[42]")));
    assert!(!path("item").overlaps(&path("items")));
    assert!(path("settings").overlaps(&path("settings.cooldown_days")));

    assert!(overlaps(&["items[a]"], &["items[a]", "columns"]));
    assert!(!overlaps(&["items[a]"], &["items[b]", "columns"]));
    assert!(overlaps(&["*notes"], &["items[b]"]), "unmarked: fail-safe");
    assert!(overlaps(&["items[b]"], &["*notes"]));
    assert!(!overlaps(&["*notes"], &[]), "nothing to overlap");
    assert!(!overlaps(&[], &["items[a]"]), "an empty window commutes");
    assert!(
        Units::everything().overlaps(&units(&["columns"])),
        "a state without units changes everything"
    );
}

#[test]
fn a_state_without_conflict_units_changes_everything() {
    use crate::next::State;
    let state = crate::next::toys::account::AccountState::default();
    assert_eq!(
        <crate::next::toys::account::AccountState as State>::changed(&state, &state),
        Units::everything(),
        "the fail-safe default: never a blind merge"
    );
}

#[test]
fn a_fact_keeps_its_sent_correlation() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    let payment = crate::next::toys::fact(
        cover("account", 1),
        crate::next::toys::AccountFact::PaymentReceived(5).to_any(),
        "pi_c",
    )
    .correlated(4);
    let handled = handle::<Account>(&mut store, &registry, payment, &at(1)).unwrap();
    assert_eq!(
        handled.books()[0].pages[0]
            .meta
            .as_ref()
            .unwrap()
            .correlation_id,
        Some(crate::next::toys::correlation(4))
    );
}

/// The old note command: upcast to `toy.shelf.cmd.v1.Note`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct OldNote {
    #[prost(string, tag = "1")]
    pub text: String,
}

impl prost::Name for OldNote {
    const NAME: &'static str = "Note";
    const PACKAGE: &'static str = "toy.shelf.cmd.v0";
}

#[test]
fn an_old_command_type_has_its_current_types_resolution() {
    use crate::next::{Registry, Rulebook, Upcaster};
    let mut registry = Registry::new();
    registry
        .register::<Shelf>(
            Rulebook::new("v1", ()),
            Upcaster::new("shelf-upcaster", "shelf")
                .rule::<OldNote, shelf::Note>(|old| Ok(shelf::Note { note: old.text })),
        )
        .unwrap();
    let mut store = InMemoryStore::new();
    for (n, item) in [(1, "a"), (2, "b")] {
        handle::<Shelf>(
            &mut store,
            &registry,
            command(cover("shelf", 3), set(item, 1)).id(n),
            &at(n as i64),
        )
        .unwrap();
    }
    let old = || {
        crate::next::pack(&OldNote {
            text: "restock".to_string(),
        })
    };
    assert_eq!(
        handle::<Shelf>(
            &mut store,
            &registry,
            command(cover("shelf", 3), old())
                .id(3)
                .expecting(1, ConflictResolution::Unspecified),
            &at(3),
        )
        .err()
        .unwrap(),
        stale(1, 2),
        "configured STRICT by its current type, though sent as v0"
    );
    let handled = handle::<Shelf>(
        &mut store,
        &registry,
        command(cover("shelf", 3), old()).id(4),
        &at(4),
    )
    .unwrap();
    assert_eq!(
        shelf_events(&handled),
        [ShelfEvent::Noted("restock".to_string())]
    );

    // An upcast that fails is looked up as sent, and decoding reports it.
    let mut failing = Registry::new();
    failing
        .register::<Shelf>(
            Rulebook::new("v1", ()),
            Upcaster::new("shelf-upcaster", "shelf").rule::<OldNote, shelf::Note>(|_| {
                Err(annals_diagnostic::Report::new("toy-no", "no"))
            }),
        )
        .unwrap();
    assert!(matches!(
        handle::<Shelf>(
            &mut store,
            &failing,
            command(cover("shelf", 3), old()).id(5),
            &at(5)
        ),
        Err(Failure::Kernel(KernelError::Upcast { .. }))
    ));
}
