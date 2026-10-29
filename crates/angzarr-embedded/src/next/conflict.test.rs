//! Conflict resolution: all three strategies, COMMUTATIVE the default where
//! the state declares conflict units, STRICT otherwise.
use crate::next::toys::prelude::*;
use crate::next::v1::ConflictResolution;
use crate::next::{Failure, Handled, KernelError, Key, Payload, Recovery, UnitPath, Units};

use crate::next::toys::{
    at, command, cover, handle, opened, payloads, registry, set, shelf, Account, AccountCmd, Edit,
    Shelf, ShelfCmd, ShelfEvent,
};

/// A shelf (stream 3) with items a = 1 then b = 2: sequences 0 and 1.
fn shelf_a_b(store: &MemoryStore) {
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

fn stale(expected: u64, actual: u64) -> Failure<crate::next::Rejected> {
    Failure::Kernel(KernelError::Stale {
        stream: cover("shelf", 3),
        expected,
        actual,
    })
}

#[test]
fn commutative_is_the_default_for_a_state_with_conflict_units() {
    let registry = registry();
    let store = MemoryStore::new();
    shelf_a_b(&store);

    // Issued when only a existed (expected 1), no resolution asked for: the
    // window changed items[b], the command changes items[a]. Disjoint.
    let disjoint = command(cover("shelf", 3), set("a", 5))
        .id(3)
        .expecting(1, ConflictResolution::Unspecified);
    let handled = handle::<Shelf>(&store, &registry, disjoint, &at(3)).unwrap();
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
    let failure = handle::<Shelf>(&store, &registry, overlapping, &at(4))
        .err()
        .unwrap();
    assert_eq!(failure, stale(1, 3));
    assert_eq!(failure.recovery(), Recovery::Retry);
}

#[test]
fn strict_is_the_default_for_a_state_without_conflict_units() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 100);

    let behind = command(cover("account", 1), AccountCmd::Deposit(1).to_any())
        .id(3)
        .expecting(1, ConflictResolution::Unspecified);
    assert_eq!(
        handle::<Account>(&store, &registry, behind, &at(20))
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
    let store = MemoryStore::new();
    shelf_a_b(&store);
    let note = command(
        cover("shelf", 3),
        ShelfCmd::Note("restock".to_string()).to_any(),
    )
    .id(3)
    .expecting(1, ConflictResolution::Unspecified);
    assert_eq!(
        handle::<Shelf>(&store, &registry, note, &at(3))
            .err()
            .unwrap(),
        stale(1, 2),
        "a note is never merged, though the shelf is COMMUTATIVE"
    );
}

#[test]
fn an_unmarked_field_conflicts_with_everything() {
    let registry = registry();
    let store = MemoryStore::new();
    shelf_a_b(&store);
    let note = command(
        cover("shelf", 3),
        ShelfCmd::Note("restock".to_string()).to_any(),
    )
    .id(3);
    handle::<Shelf>(&store, &registry, note, &at(3)).unwrap();

    // Behind the note: the window changed `notes`, which carries no marker.
    let behind = command(cover("shelf", 3), set("c", 1))
        .id(4)
        .expecting(2, ConflictResolution::Commutative);
    assert_eq!(
        handle::<Shelf>(&store, &registry, behind, &at(4))
            .err()
            .unwrap(),
        stale(2, 3),
        "never a blind merge"
    );
}

#[test]
fn aggregate_handles_demands_the_nonce_back() {
    let registry = registry();
    let store = MemoryStore::new();
    shelf_a_b(&store);

    let handled = handle::<Shelf>(
        &store,
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
        &store,
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
    let store = MemoryStore::new();
    shelf_a_b(&store);
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
            handle::<Shelf>(&store, &registry, ahead, &at(5))
                .err()
                .unwrap(),
            stale(5, 2),
            "{resolution:?}"
        );
    }
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
    let store = MemoryStore::new();
    let payment = crate::next::toys::fact(
        cover("account", 1),
        crate::next::toys::AccountFact::PaymentReceived(5).to_any(),
        "pi_c",
    )
    .correlated(4);
    let handled = handle::<Account>(&store, &registry, payment, &at(1)).unwrap();
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
            crate::next::toys::handlers::<crate::next::toys::Shelf>((), "v1"),
        )
        .unwrap();
    let store = MemoryStore::new();
    for (n, item) in [(1, "a"), (2, "b")] {
        handle::<Shelf>(
            &store,
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
            &store,
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
        &store,
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
                Err(angzarr_embedded_diagnostic::Report::new("toy-no"))
            }),
            crate::next::toys::handlers::<crate::next::toys::Shelf>((), "v1"),
        )
        .unwrap();
    assert!(matches!(
        handle::<Shelf>(
            &store,
            &failing,
            command(cover("shelf", 3), old()).id(5),
            &at(5)
        ),
        Err(Failure::Kernel(KernelError::Upcast { .. }))
    ));
}

/// A state that lists COMMUTATIVE yet keeps [`State::changed`]'s default, so
/// every change it reports is "everything": only a command with no window
/// between its expected sequence and the head commits.
mod tally {
    use std::convert::Infallible;

    use crate::next::angzarr_router::domain::DomainDispatch;
    use crate::next::v1::{ConflictResolution, StateDescriptor, StateOptions};
    use crate::next::{pack, Decoder, Domain, Payload, PayloadError, State};

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Tally {
        #[prost(uint32, tag = "1")]
        pub count: u32,
    }

    impl prost::Name for Tally {
        const NAME: &'static str = "Tally";
        const PACKAGE: &'static str = "toy.tally.v1";
    }

    impl State for Tally {
        type Snapshot = Self;

        fn to_snapshot(&self) -> Self {
            self.clone()
        }

        fn from_snapshot(snapshot: Self) -> Result<Self, angzarr_embedded_diagnostic::Report> {
            Ok(snapshot)
        }

        fn descriptor() -> StateDescriptor {
            StateDescriptor {
                type_url: "/toy.tally.v1.Tally".to_string(),
                options: Some(StateOptions {
                    conflict_resolutions: vec![
                        ConflictResolution::Commutative as i32,
                        ConflictResolution::Strict as i32,
                        ConflictResolution::AggregateHandles as i32,
                    ],
                }),
            }
        }
    }

    /// The command and the event it becomes.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Count {}

    impl prost::Name for Count {
        const NAME: &'static str = "Count";
        const PACKAGE: &'static str = "toy.tally.v1";
    }

    impl Payload for Count {
        fn to_any(&self) -> crate::next::Any {
            pack(self)
        }

        fn from_any(any: &crate::next::Any) -> Result<Self, PayloadError> {
            Decoder::new().on::<Count>(|c| c).decode(any)
        }

        fn type_urls() -> Vec<String> {
            Vec::new()
        }
    }

    pub struct Counter;

    impl Domain for Counter {
        type State = Tally;
        type Command = Count;
        type Fact = Infallible;
        type Event = Count;
        type Rejection = Infallible;
        type Rules = ();

        const NAME: &'static str = "tally";

        fn apply(state: &mut Tally, _: &Count, (): &()) {
            state.count += 1;
        }
    }

    /// The counter's one handler, on the router's table: every count
    /// counts.
    pub fn handlers(table: DomainDispatch<Tally>) -> DomainDispatch<Tally> {
        table.on_command("toy.tally.v1.Count", |_, _, _| {
            Ok(Some(crate::next::v1::EventBook {
                pages: vec![crate::next::v1::EventPage {
                    payload: Some(pack(&Count {})),
                    ..Default::default()
                }],
                ..Default::default()
            }))
        })
    }
}

#[test]
fn a_commutative_command_at_the_head_has_no_window_whatever_its_units() {
    use crate::next::{Registry, Rulebook, Upcaster};
    let mut registry = Registry::new();
    registry
        .register::<tally::Counter>(
            Rulebook::new("v1", ()),
            Upcaster::new("tally-upcaster", "tally"),
            tally::handlers,
        )
        .unwrap();
    let store = MemoryStore::new();
    let count = |n: u128| command(cover("tally", 1), crate::next::pack(&tally::Count {})).id(n);
    for n in 1..=2 {
        assert!(
            handle::<tally::Counter>(&store, &registry, count(n), &at(n as i64)).is_ok(),
            "stamped at the head: nothing between, nothing to overlap"
        );
    }
    assert!(handle::<tally::Counter>(
        &store,
        &registry,
        count(3).expecting(2, ConflictResolution::Commutative),
        &at(3),
    )
    .is_ok());
    assert_eq!(
        handle::<tally::Counter>(
            &store,
            &registry,
            count(4).expecting(1, ConflictResolution::Commutative),
            &at(4),
        )
        .err(),
        Some(Failure::Kernel(KernelError::Stale {
            stream: cover("tally", 1),
            expected: 1,
            actual: 3,
        })),
        "a window, and every change overlaps it"
    );
}
