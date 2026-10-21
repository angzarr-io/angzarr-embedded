//! Step 3: deduplication by `command_id` on the events, with its accepted
//! costs. No claims, no fingerprints, no commands table.

use annals::next::v1::DeliveryOutcome;
use annals::next::{Failure, Handled, InMemoryStore, Payload};

use crate::toys::{
    at, command, cover, fact, handle, opened, payloads, registry, AccountCmd, AccountEvent,
    AccountFact, Edit,
};

fn deposit(n: u128, cents: i64) -> annals::next::v1::CommandBook {
    command(cover("account", 1), AccountCmd::Deposit(cents).to_any()).id(n)
}

#[test]
fn a_retried_command_is_a_duplicate_answered_with_its_events() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 100);

    let first =
        handle::<crate::toys::Account>(&mut store, &registry, deposit(3, 25), &at(20)).unwrap();
    assert_eq!(first.outcome().delivery, DeliveryOutcome::Applied as i32);
    let retry =
        handle::<crate::toys::Account>(&mut store, &registry, deposit(3, 25), &at(21)).unwrap();
    assert_eq!(retry.outcome().delivery, DeliveryOutcome::Duplicate as i32);
    assert!(matches!(retry, Handled::Duplicate(_)));
    assert_eq!(
        retry.books(),
        first.books(),
        "the first delivery's events, re-read"
    );
    assert_eq!(
        payloads::<AccountEvent>(&retry.books()),
        [AccountEvent::Deposited(25)]
    );
    let page = &retry.books()[0].pages[0];
    assert_eq!(
        page.meta.as_ref().unwrap().recorded_at.unwrap().seconds,
        20,
        "nothing re-stamped"
    );
}

#[test]
fn a_zero_event_decision_is_decided_again() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);

    // A sweep of an empty account withdraws nothing: applied, no events.
    let sweep = || command(cover("account", 1), AccountCmd::Sweep.to_any()).id(7);
    let first = handle::<crate::toys::Account>(&mut store, &registry, sweep(), &at(20)).unwrap();
    assert_eq!(first.outcome().delivery, DeliveryOutcome::Applied as i32);
    assert!(payloads::<AccountEvent>(&first.books()).is_empty());

    // Money arrives; the same sweep is redelivered.
    let payment = fact(
        cover("account", 1),
        AccountFact::PaymentReceived(50).to_any(),
        "pi_1",
    );
    handle::<crate::toys::Account>(&mut store, &registry, payment, &at(21)).unwrap();
    let again = handle::<crate::toys::Account>(&mut store, &registry, sweep(), &at(22)).unwrap();
    assert_eq!(
        again.outcome().delivery,
        DeliveryOutcome::Applied as i32,
        "the accepted cost: re-decided against the stream as it is now"
    );
    assert_eq!(
        payloads::<AccountEvent>(&again.books()),
        [AccountEvent::Withdrew(49)],
        "50 less the v2 fee"
    );
}

#[test]
fn a_reused_command_id_with_another_payload_is_answered_with_the_first() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 100);

    handle::<crate::toys::Account>(&mut store, &registry, deposit(3, 25), &at(20)).unwrap();
    let reused =
        handle::<crate::toys::Account>(&mut store, &registry, deposit(3, 999), &at(21)).unwrap();
    assert_eq!(
        reused.outcome().delivery,
        DeliveryOutcome::Duplicate as i32,
        "the accepted cost: payloads are not fingerprinted"
    );
    assert_eq!(
        payloads::<AccountEvent>(&reused.books()),
        [AccountEvent::Deposited(25)]
    );
}

#[test]
fn the_same_command_id_to_another_stream_is_another_command() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 100);
    opened(&mut store, &registry, 2, 100);

    handle::<crate::toys::Account>(&mut store, &registry, deposit(3, 25), &at(20)).unwrap();
    let other = command(cover("account", 2), AccountCmd::Deposit(25).to_any()).id(3);
    let applied = handle::<crate::toys::Account>(&mut store, &registry, other, &at(21)).unwrap();
    assert_eq!(applied.outcome().delivery, DeliveryOutcome::Applied as i32);
}

#[test]
fn a_redelivered_fact_is_a_duplicate() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);
    let payment = || {
        fact(
            cover("account", 1),
            AccountFact::PaymentReceived(50).to_any(),
            "pi_1234",
        )
    };

    handle::<crate::toys::Account>(&mut store, &registry, payment(), &at(20)).unwrap();
    let again = handle::<crate::toys::Account>(&mut store, &registry, payment(), &at(21)).unwrap();
    assert_eq!(again.outcome().delivery, DeliveryOutcome::Duplicate as i32);
    let meta = again.books()[0].pages[0].meta.clone().unwrap();
    assert_eq!(
        meta.causation,
        Some(annals::next::v1::event_meta::Causation::CommandId(
            annals::next::ids::fact_command_id("pi_1234")
        )),
        "the id is on the event, where the lookup finds it"
    );
}

#[test]
fn a_rejection_leaves_nothing_and_a_retry_runs_again() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    let early = || command(cover("account", 1), AccountCmd::Deposit(5).to_any()).id(4);

    let refused = handle::<crate::toys::Account>(&mut store, &registry, early(), &at(5));
    assert!(matches!(
        refused,
        Err(Failure::Rejected(crate::toys::Refused::NotOpen))
    ));
    assert!(store.streams().is_empty());

    opened(&mut store, &registry, 1, 0);
    let retried = handle::<crate::toys::Account>(&mut store, &registry, early(), &at(20)).unwrap();
    assert_eq!(retried.outcome().delivery, DeliveryOutcome::Applied as i32);
}

/// A unit of work that plays READ COMMITTED with a concurrent commit: the
/// store already holds the original delivery's events, but the first read
/// this unit makes (whichever it is) sees the stream as it was before them;
/// every later read, and every append, sees the store as it is.
struct CommitLandsAfterFirstRead<'s> {
    store: &'s mut InMemoryStore,
    /// The stream's length before the original commit.
    before: u64,
    seen: bool,
}

impl CommitLandsAfterFirstRead<'_> {
    fn first_read(&mut self) -> bool {
        !std::mem::replace(&mut self.seen, true)
    }
}

impl annals::next::EventStore for CommitLandsAfterFirstRead<'_> {
    fn append(
        &mut self,
        stream: &annals::next::v1::Cover,
        expected: u64,
        pages: &[annals::next::v1::EventPage],
    ) -> Result<u64, annals::next::StoreError> {
        self.store.append(stream, expected, pages)
    }

    fn read_from(
        &mut self,
        stream: &annals::next::v1::Cover,
        from: u64,
    ) -> Result<Option<annals::next::Slice>, annals::next::StoreError> {
        let early = self.first_read();
        let mut slice = self.store.read_from(stream, from)?;
        if early {
            if let Some(slice) = slice.as_mut() {
                slice.pages.retain(|p| p.sequence < self.before);
                slice.next_sequence = self.before;
            }
        }
        Ok(slice)
    }

    fn find_command(
        &mut self,
        stream: &annals::next::v1::Cover,
        command_id: &annals::next::v1::Uuid,
    ) -> Result<Vec<annals::next::v1::EventPage>, annals::next::StoreError> {
        let early = self.first_read();
        let mut pages = self.store.find_command(stream, command_id)?;
        if early {
            pages.retain(|p| p.sequence < self.before);
        }
        Ok(pages)
    }

    fn covers(
        &mut self,
        domain: &str,
    ) -> Result<Vec<annals::next::v1::Cover>, annals::next::StoreError> {
        self.store.covers(domain)
    }
}

impl annals::next::UnitOfWork for CommitLandsAfterFirstRead<'_> {
    fn commit(self) -> Result<(), annals::next::StoreError> {
        Ok(())
    }
}

#[test]
fn a_retry_racing_its_original_commit_is_never_applied_twice() {
    // The stream is loaded before the command is looked up, so an original
    // that commits between the retry's two reads is seen by the lookup.
    // (Looked up first, the retry would find nothing, then load the head
    // after the original's events and append again.)
    use annals::next::EventStore;
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 100);
    let before = store
        .read(&cover("account", 1))
        .unwrap()
        .unwrap()
        .next_sequence;
    handle::<crate::toys::Account>(&mut store, &registry, deposit(3, 25), &at(20)).unwrap();

    let log = annals::next::MemoryDeadLetters::new();
    let kernel = annals::next::Kernel::new(&registry, &log);
    let racing = CommitLandsAfterFirstRead {
        store: &mut store,
        before,
        seen: false,
    };
    let retry = kernel
        .handle::<crate::toys::Account, _, _>(
            racing,
            &annals::next::Routes::new(),
            deposit(3, 25),
            &at(21),
            &mut (),
        )
        .unwrap();
    assert!(
        matches!(retry, Handled::Duplicate(_)),
        "found, not re-applied"
    );
    let deposits = store
        .read(&cover("account", 1))
        .unwrap()
        .unwrap()
        .pages
        .len() as u64;
    assert_eq!(deposits, before + 1, "the deposit is on the stream once");
}
