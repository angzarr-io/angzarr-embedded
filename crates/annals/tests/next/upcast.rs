//! Upcasting by type URL, for events and (discouraged) commands, with the
//! router's semantics and strict decoding.

use annals::next::v1::{EventMeta, EventPage, UpcastRequest};
use annals::next::{
    pack, ChainProblem, EventStore, Failure, InMemoryStore, KernelError, Registry, StoreError,
    Stream, UpcastChainError, Upcaster,
};

use crate::toys::{
    account, account_upcaster, at, command, command_id, cover, fees, handle, instant, opened,
    payloads, registry, registry_with_upcasters, Account, AccountEvent, Edit,
};

fn page(sequence: u64, payload: annals::next::Any) -> EventPage {
    EventPage {
        payload: Some(payload),
        meta: Some(EventMeta {
            causation: Some(annals::next::v1::event_meta::Causation::CommandId(
                command_id(sequence as u128),
            )),
            correlation_id: Some(command_id(sequence as u128)),
            recorded_at: Some(instant(sequence as i64)),
            rules_version: "v2".to_string(),
            ..EventMeta::default()
        }),
        sequence,
    }
}

#[test]
fn upcasting_by_type_url_runs_the_chain_as_the_router_does() {
    let upcaster = account_upcaster();
    assert_eq!(
        upcaster.source_types(),
        ["toy.account.cmd.v0.Credit", "toy.account.v0.Credited"]
    );
    let current = pack(&account::Deposited { cents: 200 });
    assert_eq!(
        upcaster.upcast_event(&pack(&account::v0::Credited { euros: 2 })),
        Ok(current.clone())
    );
    assert_eq!(upcaster.upcast_event(&current), Ok(current.clone()));
    let unknown = pack(&account::v9::Bonus { cents: 1 });
    assert_eq!(
        upcaster.upcast_event(&unknown),
        Ok(unknown.clone()),
        "the router passes other types through; strictness is at decode"
    );

    let old = page(
        u64::from(u32::MAX) + 1,
        pack(&account::v0::Credited { euros: 2 }),
    );
    let upcast = upcaster.upcast_page(&old).unwrap();
    assert_eq!(upcast.sequence, old.sequence, "sequence kept");
    assert_eq!(upcast.meta, old.meta, "meta kept");
    assert_eq!(upcast.payload, Some(current));

    let elsewhere = UpcastRequest {
        domain: "ledger".to_string(),
        events: vec![old.clone()],
    };
    assert_eq!(upcaster.dispatch(&elsewhere).unwrap().events, [old]);
}

#[test]
#[ignore = "proposal: old events are upcast on every read and never rewritten"]
fn old_events_replay_through_the_upcaster() {
    let registry = registry_with_upcasters();
    let log = [
        page(0, pack(&account::Opened {})),
        page(1, pack(&account::v0::Credited { euros: 3 })),
    ];
    let stream = Stream::<Account>::replay(cover("account", 1), &log, &registry).unwrap();
    assert_eq!(stream.state().cents, 300);
    assert_eq!(stream.events()[1].event(), &AccountEvent::Deposited(300));

    let mut store = InMemoryStore::new();
    store.append(&cover("account", 1), 0, &log).unwrap();
    assert_eq!(
        store.read(&cover("account", 1)).unwrap().unwrap().pages,
        log,
        "the store keeps what was written"
    );
}

#[test]
#[ignore = "proposal: an event type nothing upcasts or decodes fails replay, naming it"]
fn an_unknown_event_type_fails_replay_naming_it() {
    let log = [page(0, pack(&account::v9::Bonus { cents: 1 }))];
    let error = Stream::<Account>::replay(cover("account", 1), &log, &registry())
        .err()
        .unwrap();
    let KernelError::Store(StoreError::Unreadable {
        stream,
        sequence,
        reason,
    }) = error
    else {
        panic!("never skipped: {error:?}");
    };
    assert_eq!((stream, sequence), (cover("account", 1), 0));
    assert!(format!("{reason:?}").contains("toy.account.v9.Bonus"));
}

#[test]
#[ignore = "proposal: commands may be upcast, through the same chain, before decode"]
fn an_old_command_is_upcast_then_decided() {
    let registry = registry_with_upcasters();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);

    let old = command(
        cover("account", 1),
        pack(&account::cmd::v0::Credit { euros: 2 }),
    )
    .id(5);
    let handled = handle::<Account>(&mut store, &registry, old, &at(20)).unwrap();
    assert_eq!(
        payloads::<AccountEvent>(&handled.books()),
        [AccountEvent::Deposited(200)]
    );

    let without = crate::toys::registry();
    let old = command(
        cover("account", 1),
        pack(&account::cmd::v0::Credit { euros: 2 }),
    )
    .id(6);
    assert_eq!(
        handle::<Account>(&mut store, &without, old, &at(21))
            .err()
            .unwrap(),
        Failure::Kernel(KernelError::UnknownType {
            stream: cover("account", 1),
            type_url: "/toy.account.cmd.v0.Credit".to_string()
        }),
        "no upcaster path: refused, naming the type URL"
    );
}

#[test]
#[ignore = "proposal: an upcaster that cannot finish is refused before anything is read"]
fn an_upcaster_is_checked_whole() {
    let problems = |upcaster: Upcaster| -> Vec<ChainProblem> {
        let mut registry = Registry::new();
        registry.register::<Account>(fees(), upcaster).unwrap();
        registry
            .check()
            .err()
            .unwrap_or_default()
            .into_iter()
            .map(|UpcastChainError { problem, .. }| problem)
            .collect()
    };

    assert_eq!(problems(account_upcaster()), []);
    let dead_end =
        Upcaster::new("u", "account").rule::<account::v0::Credited, account::v9::Bonus>(|c| {
            Ok(account::v9::Bonus { cents: c.euros })
        });
    assert_eq!(
        problems(dead_end),
        [ChainProblem::DeadEnd {
            source: "toy.account.v0.Credited".to_string(),
            ends_at: "toy.account.v9.Bonus".to_string()
        }],
        "v9 is not a type the account decodes"
    );
    let twice = account_upcaster().rule::<account::v0::Credited, account::Deposited>(|c| {
        Ok(account::Deposited {
            cents: c.euros * 100,
        })
    });
    assert_eq!(
        problems(twice),
        [ChainProblem::Duplicate {
            source: "toy.account.v0.Credited".to_string()
        }]
    );
}
