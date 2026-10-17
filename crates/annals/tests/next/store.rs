//! The store ports: append's checks, `read_from`, conflict apart from
//! failure, the unit of work, and the async port.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use annals::next::{
    AsyncEventStore, AsyncUnitOfWork, Claim, Envelope, EventStore, InMemoryStore, Meta, Origin,
    Ready, Root, RulesVersion, Seq, StoreError, StreamKey, Timestamp, UnitOfWork,
};

use crate::toys::{correlation, edition, fingerprint, key, request};

fn stamped(seq: Seq, event: &str) -> Envelope<String> {
    Envelope {
        seq,
        meta: Meta {
            actor: None,
            correlation: correlation(1),
            origin: Origin::request(request(1)),
            recorded_at: Timestamp(seq as i64),
            base_sequence: seq,
            rules_version: RulesVersion::from("none"),
        },
        event: event.to_string(),
    }
}

fn log(range: std::ops::Range<Seq>) -> Vec<Envelope<String>> {
    range.map(|seq| stamped(seq, &format!("e{seq}"))).collect()
}

#[test]
#[ignore = "proposal: append returns the new length (B6)"]
fn append_returns_the_new_length_and_refuses_a_stale_expected() {
    let mut store = InMemoryStore::new();
    let h = key(1, "toy");
    assert_eq!(store.append(&h, 0, &log(0..2)), Ok(2));
    assert_eq!(store.append(&h, 2, &log(2..3)), Ok(3));
    assert_eq!(
        store.append(&h, 2, &log(2..3)),
        Err(StoreError::Conflict {
            stream: key(1, "toy"),
            expected: 2,
            actual: 3
        })
    );
    assert_eq!(store.read(&h).unwrap().unwrap().len, 3, "nothing appended");
}

#[test]
#[ignore = "proposal: append checks seq continuity (B6)"]
fn append_refuses_events_that_do_not_continue_the_stream() {
    let mut store = InMemoryStore::new();
    let h = key(1, "toy");
    store.append(&h, 0, &log(0..2)).unwrap();
    assert_eq!(
        store.append(&h, 2, &log(3..4)),
        Err(StoreError::Discontinuous {
            stream: key(1, "toy"),
            expected: 2,
            found: 3
        })
    );
    let mut repeated = log(2..4);
    repeated[1].seq = 2;
    assert_eq!(
        store.append(&h, 2, &repeated),
        Err(StoreError::Discontinuous {
            stream: key(1, "toy"),
            expected: 3,
            found: 2
        }),
        "every event, not just the first"
    );
    assert_eq!(store.read(&h).unwrap().unwrap().len, 2);
}

#[test]
#[ignore = "proposal: a store keys streams by (domain, edition, root)"]
fn the_same_root_in_another_domain_or_edition_is_another_stream() {
    let mut store = InMemoryStore::new();
    let root = Root::from(1);
    let main = StreamKey::main("toy", root);
    store.append(&main, 0, &log(0..3)).unwrap();
    for other in [
        StreamKey::main("other", root),
        StreamKey::new("toy", edition("v2"), root),
    ] {
        assert_eq!(store.read(&other), Ok(None), "{other:?}");
        assert_eq!(store.append(&other, 0, &log(0..1)), Ok(1));
    }
    assert_eq!(
        store
            .read(&StreamKey::new("toy", None, root))
            .unwrap()
            .unwrap()
            .len,
        3,
        "main is the stream with no edition"
    );
    assert_eq!(store.keys().len(), 3);
}

#[test]
#[ignore = "proposal: read_from reads a tail; a missing stream is not an error (B6)"]
fn read_from_returns_the_tail_and_a_missing_stream_is_none() {
    let mut store = InMemoryStore::new();
    let h = key(1, "toy");
    store.append(&h, 0, &log(0..5)).unwrap();
    let tail = store.read_from(&h, 3).unwrap().unwrap();
    assert_eq!(tail.key, h);
    assert_eq!(tail.len, 5);
    assert_eq!(tail.events, log(3..5));
    let past = store.read_from(&h, 9).unwrap().unwrap();
    assert!(past.events.is_empty());
    assert_eq!(past.len, 5);
    assert_eq!(store.read(&key(99, "toy")), Ok(None));
}

/// A store whose database is down.
struct Down;

impl EventStore<String> for Down {
    fn append(&mut self, _: &StreamKey, _: Seq, _: &[Envelope<String>]) -> Result<Seq, StoreError> {
        Err(StoreError::Unavailable(annals_diagnostic::Report::new(
            "toy-db-down",
            "the database is down",
        )))
    }

    fn read_from(
        &mut self,
        _: &StreamKey,
        _: Seq,
    ) -> Result<Option<annals::next::Slice<String>>, StoreError> {
        Err(StoreError::Unavailable(annals_diagnostic::Report::new(
            "toy-db-down",
            "the database is down",
        )))
    }
}

#[test]
fn an_unavailable_store_is_not_a_missing_stream_nor_a_conflict() {
    let mut down = Down;
    let read = down.read(&key(1, "toy"));
    assert!(
        matches!(read, Err(StoreError::Unavailable(_))),
        "the port's own default read keeps the failure"
    );
}

#[test]
#[ignore = "proposal: a unit of work lands all or nothing"]
fn a_unit_of_work_lands_at_commit_or_not_at_all() {
    let mut store = InMemoryStore::new();
    let (a, b) = (key(1, "toy"), key(2, "toy"));
    {
        let mut unit = store.begin();
        unit.append(&a, 0, &log(0..2)).unwrap();
        unit.append(&b, 0, &log(0..1)).unwrap();
        assert_eq!(unit.read(&a).unwrap().unwrap().len, 2, "it reads its own");
    }
    assert_eq!(store.read(&a), Ok(None), "dropped: rolled back");

    let mut unit = store.begin();
    unit.append(&a, 0, &log(0..2)).unwrap();
    unit.append(&b, 0, &log(0..1)).unwrap();
    unit.commit().unwrap();
    assert_eq!(store.read(&a).unwrap().unwrap().len, 2);
    assert_eq!(store.read(&b).unwrap().unwrap().len, 1);
}

/// Poll a future to completion with a waker that does nothing: enough for
/// futures that are ready at once.
fn ready<F: Future>(f: F) -> F::Output {
    fn noop(_: *const ()) {}
    fn clone(p: *const ()) -> RawWaker {
        RawWaker::new(p, &VTABLE)
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    // SAFETY: every vtable function ignores the data pointer.
    let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
    let mut f = pin!(f);
    match f.as_mut().poll(&mut Context::from_waker(&waker)) {
        Poll::Ready(out) => out,
        Poll::Pending => panic!("not ready at once"),
    }
}

#[test]
#[ignore = "proposal: the async port serves the same store"]
fn the_async_port_has_the_same_contract() {
    let mut store = InMemoryStore::new();
    let h = key(1, "toy");
    {
        let mut port = Ready(store.begin());
        assert_eq!(ready(port.append(&h, 0, &log(0..3))), Ok(3));
        assert_eq!(
            ready(port.append(&h, 0, &log(0..1))),
            Err(StoreError::Conflict {
                stream: key(1, "toy"),
                expected: 0,
                actual: 3
            })
        );
        assert_eq!(
            ready(port.read_from(&h, 2)).unwrap().unwrap().events,
            log(2..3)
        );
        ready(port.commit()).unwrap();
    }
    assert_eq!(store.read(&h).unwrap().unwrap().len, 3);
}

/// A claim of `origin` with payload `print`, applied as `from..to`.
fn claim(origin: &Origin, print: u8, from: Seq, to: Seq) -> Claim {
    Claim {
        origin: origin.clone(),
        fingerprint: fingerprint(print),
        from,
        to,
    }
}

#[test]
#[ignore = "proposal: a unit of work records a keyed origin once per stream, at commit"]
fn a_unit_of_work_records_a_keyed_origin_once_per_stream() {
    let mut store = InMemoryStore::<String>::new();
    let (a, b) = (key(1, "toy"), key(2, "toy"));
    let payment = Origin::fact("pi_1", "Stripe webhook");
    {
        let mut unit = store.begin();
        unit.append(&a, 0, &log(0..1)).unwrap();
        unit.record_command(&a, &claim(&payment, 1, 0, 1)).unwrap();
        assert_eq!(
            unit.record_command(&a, &claim(&payment, 1, 1, 1)),
            Err(StoreError::Duplicate {
                stream: a.clone(),
                origin: payment.clone(),
                from: 0,
                to: 1
            }),
            "within the unit"
        );
        // Dropped: rolled back, the key with it.
    }

    let mut unit = store.begin();
    unit.append(&a, 0, &log(0..1)).unwrap();
    unit.record_command(&a, &claim(&payment, 1, 0, 1)).unwrap();
    unit.record_command(&b, &claim(&payment, 1, 0, 0))
        .expect("another stream: another claim");
    unit.commit().unwrap();

    let mut unit = store.begin();
    assert_eq!(
        unit.record_command(&a, &claim(&payment, 1, 1, 2)),
        Err(StoreError::Duplicate {
            stream: a.clone(),
            origin: payment.clone(),
            from: 0,
            to: 1
        }),
        "after commit"
    );
    assert_eq!(
        unit.record_command(&a, &claim(&Origin::fact("pi_1", "Stripe, again"), 1, 1, 2)),
        Err(StoreError::Duplicate {
            stream: a,
            origin: payment,
            from: 0,
            to: 1
        }),
        "a fact's key is its external id: the description is not part of it"
    );
}

#[test]
#[ignore = "proposal: a claim records the payload's fingerprint; any key reused is KeyReused"]
fn a_key_reused_with_another_fingerprint_is_refused_whatever_its_kind() {
    let mut store = InMemoryStore::<String>::new();
    let a = key(1, "toy");
    let request = Origin::request(request(1));
    let payment = Origin::fact("pi_1", "Stripe webhook");
    let mut unit = store.begin();
    unit.record_command(&a, &claim(&request, 1, 0, 1)).unwrap();
    unit.record_command(&a, &claim(&payment, 1, 1, 2)).unwrap();
    unit.commit().unwrap();

    let mut unit = store.begin();
    assert_eq!(
        unit.record_command(&a, &claim(&request, 2, 2, 3)),
        Err(StoreError::KeyReused {
            stream: a.clone(),
            origin: request.clone(),
            from: 0,
            to: 1
        }),
        "angzarr's FAILED_PRECONDITION, never DUPLICATE"
    );
    assert_eq!(
        unit.record_command(&a, &claim(&request, 1, 2, 3)),
        Err(StoreError::Duplicate {
            stream: a.clone(),
            origin: request,
            from: 0,
            to: 1
        }),
        "the same payload: DUPLICATE"
    );
    assert_eq!(
        unit.record_command(&a, &claim(&payment, 2, 2, 3)),
        Err(StoreError::KeyReused {
            stream: a.clone(),
            origin: payment.clone(),
            from: 1,
            to: 2
        }),
        "a fact too: the store reports the mismatch with the claimed range, \
         and the kernel decides (Domain::fact_mismatch)"
    );
    assert_eq!(
        unit.record_command(&a, &claim(&payment, 1, 2, 3)),
        Err(StoreError::Duplicate {
            stream: a.clone(),
            origin: payment,
            from: 1,
            to: 2
        }),
        "the source redelivered the same bytes: DUPLICATE"
    );
    assert_eq!(
        StoreError::KeyReused {
            stream: a,
            origin: Origin::request(crate::toys::request(1)),
            from: 0,
            to: 1
        }
        .recovery(),
        annals::next::Recovery::Bug
    );
}

#[test]
#[ignore = "proposal: a causation key reused for another payload is a bug, refused (angzarr KS-12)"]
fn a_causation_key_reused_for_another_payload_is_a_bug() {
    // The kernel never records a causation (Origin::is_keyed): a chain's
    // reactions commit with it, so nothing redelivers them. A store handed
    // one anyway keys it like any other, and a reuse is refused as a bug;
    // with no dead-letter queue, the chain fails back to its caller.
    let mut store = InMemoryStore::<String>::new();
    let a = key(1, "toy");
    let caused = Origin::Causation(annals::next::Causation {
        event: annals::next::EventId {
            stream: key(9, "ledger"),
            seq: 3,
        },
        component: "books".to_string(),
        index: 0,
    });
    assert!(!caused.is_keyed(), "the kernel never offers it");
    let mut unit = store.begin();
    unit.record_command(&a, &claim(&caused, 1, 0, 1)).unwrap();
    let reused = unit.record_command(&a, &claim(&caused, 2, 1, 2));
    assert_eq!(
        reused,
        Err(StoreError::KeyReused {
            stream: a,
            origin: caused,
            from: 0,
            to: 1
        })
    );
    assert_eq!(
        reused.unwrap_err().recovery(),
        annals::next::Recovery::Bug,
        "a framework bug: refused, never answered DUPLICATE"
    );
}
