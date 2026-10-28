#[allow(unused_imports)]
use crate::next::toys::prelude::*;
mod dedup {
    //! Step 3: deduplication by `command_id` on the events, with its accepted
    //! costs. No claims, no fingerprints, no commands table.
    use crate::next::toys::prelude::*;
    use crate::next::v1::DeliveryOutcome;
    use crate::next::{Failure, Handled, Payload};

    use crate::next::toys::{
        at, command, cover, fact, handle, opened, payloads, registry, AccountCmd, AccountEvent,
        AccountFact, Edit,
    };

    fn deposit(n: u128, cents: i64) -> crate::next::v1::CommandBook {
        command(cover("account", 1), AccountCmd::Deposit(cents).to_any()).id(n)
    }

    #[test]
    fn a_retried_command_is_a_duplicate_answered_with_its_events() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);

        let first =
            handle::<crate::next::toys::Account>(&store, &registry, deposit(3, 25), &at(20))
                .unwrap();
        assert_eq!(first.outcome().delivery, DeliveryOutcome::Applied as i32);
        let retry =
            handle::<crate::next::toys::Account>(&store, &registry, deposit(3, 25), &at(21))
                .unwrap();
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
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 0);

        // A sweep of an empty account withdraws nothing: applied, no events.
        let sweep = || command(cover("account", 1), AccountCmd::Sweep.to_any()).id(7);
        let first =
            handle::<crate::next::toys::Account>(&store, &registry, sweep(), &at(20)).unwrap();
        assert_eq!(first.outcome().delivery, DeliveryOutcome::Applied as i32);
        assert!(payloads::<AccountEvent>(&first.books()).is_empty());

        // Money arrives; the same sweep is redelivered.
        let payment = fact(
            cover("account", 1),
            AccountFact::PaymentReceived(50).to_any(),
            "pi_1",
        );
        handle::<crate::next::toys::Account>(&store, &registry, payment, &at(21)).unwrap();
        let again =
            handle::<crate::next::toys::Account>(&store, &registry, sweep(), &at(22)).unwrap();
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
    fn a_creating_command_redelivered_after_a_purge_creates_its_stream_again() {
        let registry = registry();
        let store = MemoryStore::new();
        let stream = cover("account", 1);
        let open = || command(stream.clone(), AccountCmd::Open.to_any()).id(5);
        handle::<crate::next::toys::Account>(&store, &registry, open(), &at(20)).unwrap();
        let retry =
            handle::<crate::next::toys::Account>(&store, &registry, open(), &at(21)).unwrap();
        assert_eq!(retry.outcome().delivery, DeliveryOutcome::Duplicate as i32);

        assert_eq!(store.purge(sref(&stream)).ready(), Ok(1));
        let again =
            handle::<crate::next::toys::Account>(&store, &registry, open(), &at(22)).unwrap();
        assert_eq!(
            again.outcome().delivery,
            DeliveryOutcome::Applied as i32,
            "the accepted cost: nothing left to find, so decided again"
        );
        assert_eq!(
            payloads::<AccountEvent>(&again.books()),
            [AccountEvent::Opened]
        );
        let slice = slice(&store, &stream);
        assert_eq!(slice.next_sequence, 1, "a new stream, from sequence 0");
        assert_eq!(slice.last_recorded_at.unwrap().seconds, 22);
    }

    #[test]
    fn a_reused_command_id_with_another_payload_is_answered_with_the_first() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);

        handle::<crate::next::toys::Account>(&store, &registry, deposit(3, 25), &at(20)).unwrap();
        let reused =
            handle::<crate::next::toys::Account>(&store, &registry, deposit(3, 999), &at(21))
                .unwrap();
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
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        opened(&store, &registry, 2, 100);

        handle::<crate::next::toys::Account>(&store, &registry, deposit(3, 25), &at(20)).unwrap();
        let other = command(cover("account", 2), AccountCmd::Deposit(25).to_any()).id(3);
        let applied =
            handle::<crate::next::toys::Account>(&store, &registry, other, &at(21)).unwrap();
        assert_eq!(applied.outcome().delivery, DeliveryOutcome::Applied as i32);
    }

    #[test]
    fn a_redelivered_fact_is_a_duplicate() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 0);
        let payment = || {
            fact(
                cover("account", 1),
                AccountFact::PaymentReceived(50).to_any(),
                "pi_1234",
            )
        };

        handle::<crate::next::toys::Account>(&store, &registry, payment(), &at(20)).unwrap();
        let again =
            handle::<crate::next::toys::Account>(&store, &registry, payment(), &at(21)).unwrap();
        assert_eq!(again.outcome().delivery, DeliveryOutcome::Duplicate as i32);
        let meta = again.books()[0].pages[0].meta.clone().unwrap();
        assert_eq!(
            meta.causation,
            Some(crate::next::v1::event_meta::Causation::CommandId(
                crate::next::ids::fact_command_id("pi_1234")
            )),
            "the id is on the event, where the lookup finds it"
        );
    }

    #[test]
    fn a_rejection_leaves_nothing_and_a_retry_runs_again() {
        let registry = registry();
        let store = MemoryStore::new();
        let early = || command(cover("account", 1), AccountCmd::Deposit(5).to_any()).id(4);

        let refused = handle::<crate::next::toys::Account>(&store, &registry, early(), &at(5));
        assert!(matches!(
            refused,
            Err(Failure::Rejected(crate::next::toys::Refused::NotOpen))
        ));
        assert!(streams(&store).is_empty());

        opened(&store, &registry, 1, 0);
        let retried =
            handle::<crate::next::toys::Account>(&store, &registry, early(), &at(20)).unwrap();
        assert_eq!(retried.outcome().delivery, DeliveryOutcome::Applied as i32);
    }

    /// A store that plays READ COMMITTED with a concurrent commit: it
    /// already holds the original delivery's events, but the first read
    /// made of it (whichever it is) sees the stream as it was before them;
    /// every later read, and every write, sees the store as it is.
    struct CommitLandsAfterFirstRead {
        store: MemoryStore,
        /// The stream's length before the original commit.
        before: u64,
        seen: std::sync::atomic::AtomicBool,
    }

    impl CommitLandsAfterFirstRead {
        fn first_read(&self) -> bool {
            !self.seen.swap(true, std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl crate::next::EventStore for CommitLandsAfterFirstRead {
        async fn write(
            &self,
            write: crate::next::Write<'_>,
        ) -> Result<crate::next::Written, crate::next::StoreError> {
            self.store.write(write).await
        }

        async fn read(
            &self,
            stream: crate::next::StreamRef<'_>,
            from: u64,
            to: Option<u64>,
        ) -> Result<crate::next::Slice, crate::next::StoreError> {
            let early = self.first_read();
            let mut slice = self.store.read(stream, from, to).await?;
            if early {
                slice.pages.retain(|p| p.sequence < self.before);
                slice.next_sequence = self.before;
            }
            Ok(slice)
        }

        async fn find_command(
            &self,
            stream: crate::next::StreamRef<'_>,
            command_id: angzarr_ports::Uuid,
        ) -> Result<Vec<crate::next::v1::EventPage>, crate::next::StoreError> {
            let early = self.first_read();
            let mut pages = self.store.find_command(stream, command_id).await?;
            if early {
                pages.retain(|p| p.sequence < self.before);
            }
            Ok(pages)
        }

        async fn covers(
            &self,
            domain: &str,
        ) -> Result<Vec<crate::next::v1::Cover>, crate::next::StoreError> {
            self.store.covers(domain).await
        }

        async fn commit_position(&self) -> Result<u64, crate::next::StoreError> {
            self.store.commit_position().await
        }
    }

    impl crate::next::SnapshotStore for CommitLandsAfterFirstRead {
        async fn read_snapshot(
            &self,
            stream: crate::next::StreamRef<'_>,
        ) -> Result<Option<crate::next::v1::Snapshot>, crate::next::StoreError> {
            self.store.read_snapshot(stream).await
        }

        async fn write_snapshot(
            &self,
            stream: crate::next::StreamRef<'_>,
            snapshot: &crate::next::v1::Snapshot,
        ) -> Result<(), crate::next::StoreError> {
            self.store.write_snapshot(stream, snapshot).await
        }
    }

    impl crate::next::PositionStore for CommitLandsAfterFirstRead {
        async fn position(
            &self,
            key: crate::next::PositionKey<'_>,
        ) -> Result<u64, crate::next::StoreError> {
            self.store.position(key).await
        }
    }

    #[test]
    fn a_retry_racing_its_original_commit_is_never_applied_twice() {
        // The stream is loaded before the command is looked up, so an original
        // that commits between the retry's two reads is seen by the lookup.
        // (Looked up first, the retry would find nothing, then load the head
        // after the original's events and append again.)
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        let before = pages(&store, &cover("account", 1)).len() as u64;
        handle::<crate::next::toys::Account>(&store, &registry, deposit(3, 25), &at(20)).unwrap();

        let log = crate::next::MemoryDeadLetters::new();
        let kernel = crate::next::Kernel::new(&registry, &log);
        let racing = CommitLandsAfterFirstRead {
            store: store.clone(),
            before,
            seen: std::sync::atomic::AtomicBool::new(false),
        };
        let retry = kernel
            .handle::<crate::next::toys::Account, _, _>(&racing, deposit(3, 25), &at(21), &mut ())
            .ready()
            .unwrap();
        assert!(
            matches!(retry, Handled::Duplicate(_)),
            "found, not re-applied"
        );
        let deposits = pages(&store, &cover("account", 1)).len() as u64;
        assert_eq!(deposits, before + 1, "the deposit is on the stream once");
    }
}

mod pipeline {
    //! The normative order: validate → identify → load, then deduplicate →
    //! resolve → decode (and upcast) → decide or ingest. Each test pins one
    //! boundary by sending something that two adjacent steps would answer
    //! differently.
    use crate::next::toys::prelude::*;

    use super::super::{steps::Coordinator, Deduplicated};
    use crate::next::v1::{ConflictResolution, DeliveryOutcome, EventRef};
    use crate::next::{check, pack, Failure, KernelError, Malformed, Payload, Recovery};

    use crate::next::toys::{
        account, at, command, cover, handle, opened, registry, Account, AccountCmd, Edit,
    };

    fn deposit(n: u128) -> crate::next::v1::CommandBook {
        command(cover("account", 1), AccountCmd::Deposit(5).to_any()).id(n)
    }

    #[test]
    fn an_applied_strict_retry_is_a_duplicate_never_stale() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);

        let at_head = || deposit(3).expecting(2, ConflictResolution::Strict);
        handle::<Account>(&store, &registry, at_head(), &at(20)).unwrap();
        handle::<Account>(&store, &registry, deposit(4), &at(21)).unwrap();

        // The stream is at 4 now; the retry still expects 2.
        let retry = handle::<Account>(&store, &registry, at_head(), &at(22)).unwrap();
        assert_eq!(
            retry.outcome().delivery,
            DeliveryOutcome::Duplicate as i32,
            "the retry's events are found before its sequence is checked"
        );

        // A new command expecting 2 is stale, as STRICT says.
        let stale = handle::<Account>(
            &store,
            &registry,
            deposit(5).expecting(2, ConflictResolution::Strict),
            &at(23),
        )
        .err()
        .unwrap();
        assert_eq!(
            stale,
            Failure::Kernel(KernelError::Stale {
                stream: cover("account", 1),
                expected: 2,
                actual: 4
            })
        );
        assert_eq!(stale.recovery(), Recovery::Retry);
    }

    #[test]
    fn a_malformed_retry_of_an_applied_command_is_still_malformed() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        handle::<Account>(&store, &registry, deposit(3), &at(20)).unwrap();

        let forged = deposit(3).caused_by(EventRef {
            stream: Some(cover("account", 1)),
            sequence: 0,
        });
        let refused = handle::<Account>(&store, &registry, forged, &at(21))
            .err()
            .unwrap();
        assert_eq!(
            refused,
            Failure::Kernel(KernelError::Malformed(Malformed::CausationFromOutside)),
            "refused before anything is looked up"
        );
        assert_eq!(refused.recovery(), Recovery::Invalid);
    }

    #[test]
    fn an_unknown_command_type_is_refused_at_decode() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        let unknown = || command(cover("account", 1), pack(&account::v9::Bonus { cents: 1 }));

        let refused = handle::<Account>(&store, &registry, unknown().id(8), &at(20))
            .err()
            .unwrap();
        assert_eq!(
            refused,
            Failure::Kernel(KernelError::UnknownType {
                stream: cover("account", 1),
                type_url: "/toy.account.v9.Bonus".to_string()
            }),
            "named by its type URL"
        );
        assert_eq!(refused.recovery(), Recovery::Invalid);

        // Stale and undecodable: STRICT's check comes first.
        let stale = handle::<Account>(
            &store,
            &registry,
            unknown().id(9).expecting(1, ConflictResolution::Strict),
            &at(21),
        )
        .err()
        .unwrap();
        assert!(matches!(stale, Failure::Kernel(KernelError::Stale { .. })));

        // Applied under that id already: the duplicate is found first, the
        // payload never decoded (and never compared: a reused id goes
        // undetected).
        let dup = handle::<Account>(
            &store,
            &registry,
            command(cover("account", 1), pack(&account::v9::Bonus { cents: 1 })).id(2),
            &at(22),
        )
        .unwrap();
        assert_eq!(dup.outcome().delivery, DeliveryOutcome::Duplicate as i32);
    }

    #[test]
    fn the_coordinator_runs_dedup_then_load_then_decode() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        let mut coordinator = Coordinator::new(&store, &registry);

        // An applied command: step 3 answers it.
        let applied = check(deposit(2), &registry).unwrap().identify(&at(30));
        assert!(matches!(
            coordinator.deduplicate(applied).unwrap(),
            Deduplicated::Duplicate { .. }
        ));

        // A new one goes on: stamped at the head, STRICT, no window, no nonce.
        let novel = check(deposit(3), &registry).unwrap().identify(&at(30));
        let Deduplicated::Novel(novel) = coordinator.deduplicate(novel).unwrap() else {
            panic!("never applied");
        };
        assert_eq!(novel.head(), 2, "the head as step 3 loaded it");
        let contextual = coordinator.load(novel).unwrap();
        assert_eq!(contextual.conflict_resolution(), ConflictResolution::Strict);
        assert_eq!(contextual.expected_sequence(), 2, "stamped with the head");
        assert_eq!(contextual.head(), 2);
        assert!(contextual.window().is_empty());
        assert!(!contextual.has_nonce());

        let prepared = contextual.decode::<Account>(&registry).unwrap();
        assert_eq!(prepared.stream().state().cents, 100);
    }

    #[test]
    fn the_coordinator_hands_a_windowed_command_its_window_and_a_nonce() {
        use crate::next::toys::{set, Shelf};
        let registry = registry();
        let store = MemoryStore::new();
        for (n, item) in ["a", "b", "c"].into_iter().enumerate() {
            handle::<Shelf>(
                &store,
                &registry,
                command(cover("shelf", 3), set(item, 1)).id(n as u128 + 1),
                &at(n as i64),
            )
            .unwrap();
        }
        let mut coordinator = Coordinator::new(&store, &registry);
        let book = command(cover("shelf", 3), set("d", 1))
            .id(9)
            .expecting(1, ConflictResolution::AggregateHandles);
        let identified = check(book, &registry).unwrap().identify(&at(30));
        let Deduplicated::Novel(novel) = coordinator.deduplicate(identified).unwrap() else {
            panic!("never applied");
        };
        assert_eq!(
            novel.command().command_id(),
            &crate::next::toys::command_id(9)
        );
        assert_eq!(novel.head(), 3);
        let contextual = coordinator.load(novel).unwrap();
        assert_eq!(
            contextual.conflict_resolution(),
            ConflictResolution::AggregateHandles
        );
        assert_eq!(contextual.expected_sequence(), 1);
        assert_eq!(contextual.head(), 3);
        assert_eq!(contextual.pages().len(), 3);
        assert_eq!(
            contextual
                .window()
                .iter()
                .map(|p| p.sequence)
                .collect::<Vec<_>>(),
            [1, 2],
            "from the expected sequence to the head"
        );
        assert!(contextual.has_nonce());
        assert!(contextual.snapshot().is_none());
        assert_eq!(
            contextual.command().command_id(),
            &crate::next::toys::command_id(9)
        );
    }

    #[test]
    fn the_coordinators_decision_commits_to_its_stream() {
        let registry = registry();
        let log = crate::next::MemoryDeadLetters::new();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        let mut coordinator = Coordinator::new(&store, &registry);
        let identified = check(deposit(3), &registry).unwrap().identify(&at(30));
        let Deduplicated::Novel(novel) = coordinator.deduplicate(identified).unwrap() else {
            panic!("never applied");
        };
        let mut prepared = coordinator
            .load(novel)
            .unwrap()
            .decode::<Account>(&registry)
            .unwrap();
        assert_eq!(
            prepared.command().command_id(),
            &crate::next::toys::command_id(3)
        );
        let decision = prepared.decide(&at(30), &registry, &log).unwrap();
        assert_eq!(decision.stream(), &cover("account", 1));
        assert_eq!(decision.head(), 2);
        assert_eq!(decision.command_id(), &crate::next::toys::command_id(3));
        assert_eq!(decision.events().len(), 1);
        assert_eq!(decision.events()[0].sequence(), 2);
        assert!(!decision.is_empty());
        let mut stream = prepared.into_stream();
        assert_eq!(stream.commit(decision, &registry, &mut []), Ok(3));
        assert_eq!(stream.state().cents, 105);

        // A decision against the old head no longer commits.
        let mut again = Coordinator::new(&store, &registry);
        let identified = check(deposit(4), &registry).unwrap().identify(&at(31));
        let Deduplicated::Novel(novel) = again.deduplicate(identified).unwrap() else {
            panic!("never applied");
        };
        let mut prepared = again
            .load(novel)
            .unwrap()
            .decode::<Account>(&registry)
            .unwrap();
        let decision = prepared.decide(&at(31), &registry, &log).unwrap();
        assert_eq!(
            stream.commit(decision, &registry, &mut []),
            Err(KernelError::Stale {
                stream: cover("account", 1),
                expected: 2,
                actual: 3
            })
        );

        opened(&store, &registry, 2, 0);
        let sweep = check(
            command(cover("account", 2), AccountCmd::Sweep.to_any()).id(5),
            &registry,
        )
        .unwrap()
        .identify(&at(32));
        let mut empty = Coordinator::new(&store, &registry);
        let Deduplicated::Novel(novel) = empty.deduplicate(sweep).unwrap() else {
            panic!("never applied");
        };
        let mut prepared = empty
            .load(novel)
            .unwrap()
            .decode::<Account>(&registry)
            .unwrap();
        let decision = prepared.decide(&at(32), &registry, &log).unwrap();
        assert!(decision.is_empty(), "a sweep of nothing withdraws nothing");

        // A cover of another domain than the one decoded for.
        let mut wrong = Coordinator::new(&store, &registry);
        let identified = check(deposit(6), &registry).unwrap().identify(&at(33));
        let Deduplicated::Novel(novel) = wrong.deduplicate(identified).unwrap() else {
            panic!("never applied");
        };
        assert_eq!(
            wrong
                .load(novel)
                .unwrap()
                .decode::<crate::next::toys::Ledger>(&registry)
                .err(),
            Some(KernelError::WrongDomain {
                stream: cover("account", 1),
                expected: "ledger"
            })
        );
    }
}

mod snapshot {
    //! Snapshots: a framework optimisation, never business data. Used only
    //! under the current rules version and state type, and never one that
    //! hides part of a conflict window.
    use crate::next::toys::prelude::*;

    use super::super::{steps::Coordinator, Deduplicated};
    use crate::next::v1::{ConflictResolution, Snapshot};
    use crate::next::{check, pack, Payload, Stream};

    use crate::next::toys::{
        account, at, command, cover, opened, registry, root, set, shelf, Account, AccountCmd, Edit,
        Shelf,
    };

    /// A snapshot of an account at sequence 1 claiming `cents`: a poisoned one
    /// shows when it is used.
    fn poisoned(rules: &str, cents: i64) -> Snapshot {
        Snapshot {
            sequence: 1,
            state: Some(pack(&account::AccountState {
                open: true,
                cents,
                ..Default::default()
            })),
            rules_version: rules.to_string(),
        }
    }

    /// Load (steps 2 to 5) a deposit for account 1, expecting `expected` under
    /// `resolution` if given.
    fn load(
        store: &MemoryStore,
        expected: Option<(u64, ConflictResolution)>,
    ) -> (Option<u64>, i64) {
        let registry = registry();
        let mut book = command(cover("account", 1), AccountCmd::Deposit(1).to_any()).id(9);
        if let Some((sequence, resolution)) = expected {
            book = book.expecting(sequence, resolution);
        }
        let mut coordinator = Coordinator::new(store, &registry);
        let identified = check(book, &registry).unwrap().identify(&at(30));
        let Deduplicated::Novel(novel) = coordinator.deduplicate(identified).unwrap() else {
            panic!("never applied");
        };
        let contextual = coordinator.load(novel).unwrap();
        let used = contextual.snapshot().map(|s| s.sequence);
        let prepared = contextual.decode::<Account>(&registry).unwrap();
        (used, prepared.stream().state().cents)
    }

    #[test]
    fn a_snapshot_under_another_rules_version_is_discarded() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);

        store
            .write_snapshot(sref(&cover("account", 1)), &poisoned("v1", 999))
            .ready()
            .unwrap();
        assert_eq!(
            load(&store, None),
            (None, 100),
            "folded under v1, the current rules are v2: replayed from 0"
        );

        store
            .write_snapshot(sref(&cover("account", 1)), &poisoned("v2", 999))
            .ready()
            .unwrap();
        assert_eq!(
            load(&store, None),
            (Some(1), 999),
            "a current one is used (and here shows its poison)"
        );
    }

    #[test]
    fn a_snapshot_of_another_state_type_is_discarded() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        let foreign = Snapshot {
            sequence: 1,
            state: Some(pack(&shelf::ShelfState::default())),
            rules_version: "v2".to_string(),
        };
        store
            .write_snapshot(sref(&cover("account", 1)), &foreign)
            .ready()
            .unwrap();
        assert_eq!(load(&store, None), (None, 100));
    }

    #[test]
    fn a_snapshot_never_hides_part_of_the_window() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 100);
        store
            .write_snapshot(sref(&cover("account", 1)), &poisoned("v2", 999))
            .ready()
            .unwrap();

        assert_eq!(
            load(&store, Some((1, ConflictResolution::AggregateHandles))),
            (None, 100),
            "the snapshot (at 1) would fold the window (from 1): not used"
        );
        assert_eq!(
            load(&store, Some((2, ConflictResolution::AggregateHandles))),
            (Some(1), 999),
            "at or below expected-1, it is"
        );
    }

    /// A snapshot at or past the head claims events the stream does not
    /// hold (one a purge left, or one written ahead of its events): not
    /// used, and the stream is read from its events. Used, the stream
    /// would resume past its head, its next events would not run on from
    /// it, and every later command on it would fail.
    #[test]
    fn a_snapshot_at_or_past_the_head_is_not_used() {
        use crate::next::toys::handle;
        let registry = registry();
        for sequence in [2, 5] {
            let store = MemoryStore::new();
            opened(&store, &registry, 1, 100);
            let ahead = Snapshot {
                sequence,
                ..poisoned("v2", 999)
            };
            store
                .write_snapshot(sref(&cover("account", 1)), &ahead)
                .ready()
                .unwrap();
            assert_eq!(
                load(&store, None),
                (None, 100),
                "a snapshot at {sequence}, the head at 2: read from the events"
            );
            for (n, head) in [(3, 3), (4, 4)] {
                let deposit = command(cover("account", 1), AccountCmd::Deposit(1).to_any()).id(n);
                let handled = handle::<Account>(&store, &registry, deposit, &at(20))
                    .unwrap_or_else(|e| panic!("a snapshot at {sequence}, command {n}: {e}"));
                assert_eq!(handled.books()[0].next_sequence, head);
            }
        }
    }

    #[test]
    fn a_stream_snapshots_under_the_current_rules_version() {
        let registry = registry();
        let mut stream = Stream::<Shelf>::new(root(3), None).unwrap();
        let log = crate::next::MemoryDeadLetters::new();
        stream
            .execute(
                command(cover("shelf", 3), set("a", 1)),
                &registry,
                &at(1),
                &log,
                &mut [],
            )
            .unwrap();
        let snapshot = stream.snapshot(&registry).unwrap();
        assert_eq!(snapshot.sequence, 0);
        assert_eq!(snapshot.rules_version, "v1");
        assert_eq!(
            snapshot.state.as_ref().unwrap().type_url,
            "/toy.shelf.v1.ShelfState"
        );
        assert!(registry.snapshot_usable("shelf", &snapshot));
        assert!(Stream::<Shelf>::new(root(4), None)
            .unwrap()
            .snapshot(&registry)
            .is_none());
    }

    /// Ledger 1 after recording `cents`, executed in memory.
    fn recorded(cents: i64) -> Stream<crate::next::toys::Ledger> {
        use crate::next::MemoryDeadLetters;
        let registry = registry();
        let mut stream = Stream::<crate::next::toys::Ledger>::new(root(1), None).unwrap();
        let book = command(
            cover("ledger", 1),
            crate::next::toys::ledger::Record {
                source_sequence: 0,
                cents,
            }
            .to_any(),
        );
        stream
            .execute(
                book,
                &registry,
                &at(10),
                &MemoryDeadLetters::default(),
                &mut [],
            )
            .unwrap();
        stream
    }

    #[test]
    fn a_domain_typed_state_snapshots_as_its_wire_form_and_resumes_from_it() {
        let registry = registry();
        let stream = recorded(40);

        let snapshot = stream.snapshot(&registry).unwrap();
        assert_eq!(
            snapshot.state.as_ref().unwrap().type_url,
            "/toy.ledger.v1.LedgerSnapshot"
        );
        let resumed = Stream::<crate::next::toys::Ledger>::resume(
            cover("ledger", 1),
            &snapshot,
            None,
            &[],
            &registry,
        )
        .unwrap();
        assert_eq!(resumed.state(), stream.state());
        assert_eq!(resumed.state().total, 40);
    }

    #[test]
    fn a_snapshot_whose_wire_form_is_not_a_valid_state_is_unreadable() {
        use crate::next::KernelError;
        use angzarr_embedded_diagnostic::Diagnostic;
        let snapshot = Snapshot {
            sequence: 0,
            state: Some(pack(&crate::next::toys::ledger::LedgerSnapshot {
                next_source: 1,
                total: -5,
            })),
            rules_version: "v1".to_string(),
        };
        let error = Stream::<crate::next::toys::Ledger>::resume(
            cover("ledger", 1),
            &snapshot,
            None,
            &[],
            &registry(),
        )
        .err()
        .expect("a negative total is not a ledger");
        let KernelError::Unreadable { reason, .. } = error else {
            panic!("expected an unreadable snapshot, got {error:?}");
        };
        assert_eq!(reason.code(), "angzarr-embedded-next-payload-invalid");
    }
}
