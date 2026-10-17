//! Snapshots: used only under the rules version they were folded with;
//! otherwise discarded and the stream replayed (angzarr's D8).

use annals::next::{
    load, EventStore, InMemoryStore, Seq, Slice, Snapshot, SnapshotStore, StoreError, StreamKey,
};

use crate::toys::{at, book, fees, stream, Account, AccountCmd, AccountEvent, Balance, Fees};

/// One stream's events and its newest snapshot.
#[derive(Default)]
struct Snapshotted {
    events: InMemoryStore<AccountEvent>,
    snapshot: Option<Snapshot<Balance>>,
}

impl EventStore<AccountEvent> for Snapshotted {
    fn append(
        &mut self,
        key: &StreamKey,
        expected: Seq,
        events: &[annals::next::Envelope<AccountEvent>],
    ) -> Result<Seq, StoreError> {
        self.events.append(key, expected, events)
    }

    fn read_from(
        &mut self,
        key: &StreamKey,
        from: Seq,
    ) -> Result<Option<Slice<AccountEvent>>, StoreError> {
        self.events.read_from(key, from)
    }
}

impl SnapshotStore<Balance> for Snapshotted {
    fn read_snapshot(&mut self, _: &StreamKey) -> Result<Option<Snapshot<Balance>>, StoreError> {
        Ok(self.snapshot.clone())
    }

    fn write_snapshot(
        &mut self,
        _: &StreamKey,
        snapshot: &Snapshot<Balance>,
    ) -> Result<(), StoreError> {
        self.snapshot = Some(snapshot.clone());
        Ok(())
    }
}

#[test]
#[ignore = "proposal: a snapshot under another rules version is discarded, and the stream replayed"]
fn a_snapshot_under_another_rules_version_is_discarded() {
    let fees_1 = fees("fees-1", 0);
    let mut s = stream::<Account>(1);
    for (i, cmd) in [
        AccountCmd::Open,
        AccountCmd::Deposit(40),
        AccountCmd::Deposit(2),
    ]
    .into_iter()
    .enumerate()
    {
        let b = book(&s, cmd, i as u128);
        s.execute(&b, &at(i as i64), &fees_1, &mut []).unwrap();
    }
    let mut store = Snapshotted::default();
    store.append(s.key(), 0, s.events()).unwrap();

    // A snapshot after seq 1, poisoned, so using it shows in the state.
    let mut snapshot = Snapshot {
        sequence: 1,
        state: Balance {
            open: true,
            cents: 999_000,
        },
        rules_version: "fees-1".into(),
        recorded_at: s.events()[1].meta.recorded_at,
    };
    assert!(snapshot.usable(&fees_1));
    store.write_snapshot(s.key(), &snapshot).unwrap();

    let loaded = load::<Account, _>(&mut store, s.key(), &fees_1)
        .unwrap()
        .unwrap();
    assert_eq!(loaded.from_snapshot, Some(1), "same version: used");
    assert_eq!(
        loaded.stream.state().cents,
        999_002,
        "only seq 2 was folded"
    );
    assert_eq!(loaded.stream.first(), 2);
    assert_eq!(loaded.stream.len(), 3);

    let fees_2 = fees("fees-2", 5).with("fees-1", Fees { withdrawal: 0 });
    assert!(!snapshot.usable(&fees_2));
    let loaded = load::<Account, _>(&mut store, s.key(), &fees_2)
        .unwrap()
        .unwrap();
    assert_eq!(loaded.from_snapshot, None, "another version: discarded");
    assert_eq!(*loaded.stream.state(), *s.state(), "replayed from seq 0");
    assert_eq!(loaded.stream.first(), 0);

    snapshot.rules_version = "fees-2".into();
    assert!(snapshot.usable(&fees_2), "a snapshot taken under fees-2 is");
}

#[test]
#[ignore = "proposal: a stream snapshots under the current rules version"]
fn a_stream_snapshots_under_the_current_rules_version() {
    let fees_2 = fees("fees-2", 5).with("fees-1", Fees { withdrawal: 0 });
    let mut s = stream::<Account>(1);
    assert_eq!(s.snapshot(&fees_2), None, "nothing to snapshot");
    let b = book(&s, AccountCmd::Open, 1);
    s.execute(&b, &at(7), &fees_2, &mut []).unwrap();
    assert_eq!(
        s.snapshot(&fees_2),
        Some(Snapshot {
            sequence: 0,
            state: s.state().clone(),
            rules_version: "fees-2".into(),
            recorded_at: annals::next::Timestamp(7),
        })
    );
}
