//! The store ports: append's checks, `read_from`, conflict apart from
//! failure, the unit of work, and the async port.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use annals::next::{
    AsyncEventStore, AsyncUnitOfWork, CommandId, Envelope, EventStore, Header, InMemoryStore, Meta,
    Ready, RequestId, RulesVersion, Seq, StoreError, StreamId, Timestamp, UnitOfWork,
};

use crate::toys::header;

fn stamped(seq: Seq, event: &str) -> Envelope<String> {
    Envelope {
        seq,
        meta: Meta {
            recorded_at: Timestamp(seq as i64),
            actor: None,
            command: CommandId::Origin {
                request: RequestId(1),
                index: 0,
            },
            correlation: RequestId(1).into(),
            cause: None,
            schema: 1,
            rules: RulesVersion::from("none"),
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
    let h = header(1, "toy");
    assert_eq!(store.append(&h, 0, &log(0..2)), Ok(2));
    assert_eq!(store.append(&h, 2, &log(2..3)), Ok(3));
    assert_eq!(
        store.append(&h, 2, &log(2..3)),
        Err(StoreError::Conflict {
            stream: StreamId::from(1),
            expected: 2,
            actual: 3
        })
    );
    assert_eq!(
        store.read(h.id).unwrap().unwrap().len,
        3,
        "nothing appended"
    );
}

#[test]
#[ignore = "proposal: append checks seq continuity (B6)"]
fn append_refuses_events_that_do_not_continue_the_stream() {
    let mut store = InMemoryStore::new();
    let h = header(1, "toy");
    store.append(&h, 0, &log(0..2)).unwrap();
    assert_eq!(
        store.append(&h, 2, &log(3..4)),
        Err(StoreError::Discontinuous {
            stream: StreamId::from(1),
            expected: 2,
            found: 3
        })
    );
    let mut repeated = log(2..4);
    repeated[1].seq = 2;
    assert_eq!(
        store.append(&h, 2, &repeated),
        Err(StoreError::Discontinuous {
            stream: StreamId::from(1),
            expected: 3,
            found: 2
        }),
        "every event, not just the first"
    );
    assert_eq!(store.read(h.id).unwrap().unwrap().len, 2);
}

#[test]
#[ignore = "proposal: append checks the header is unchanged (B6)"]
fn append_refuses_a_changed_header() {
    let mut store = InMemoryStore::new();
    store.append(&header(1, "toy"), 0, &log(0..1)).unwrap();
    assert_eq!(
        store.append(&header(1, "other"), 1, &log(1..2)),
        Err(StoreError::HeaderChanged {
            stream: StreamId::from(1)
        })
    );
}

#[test]
#[ignore = "proposal: read_from reads a tail; a missing stream is not an error (B6)"]
fn read_from_returns_the_tail_and_a_missing_stream_is_none() {
    let mut store = InMemoryStore::new();
    let h = header(1, "toy");
    store.append(&h, 0, &log(0..5)).unwrap();
    let tail = store.read_from(h.id, 3).unwrap().unwrap();
    assert_eq!(tail.header, h);
    assert_eq!(tail.len, 5);
    assert_eq!(tail.events, log(3..5));
    let past = store.read_from(h.id, 9).unwrap().unwrap();
    assert!(past.events.is_empty());
    assert_eq!(past.len, 5);
    assert_eq!(store.read(StreamId::from(99)), Ok(None));
}

/// A store whose database is down.
struct Down;

impl EventStore<String> for Down {
    fn append(&mut self, _: &Header, _: Seq, _: &[Envelope<String>]) -> Result<Seq, StoreError> {
        Err(StoreError::Unavailable(annals_diagnostic::Report::new(
            "toy-db-down",
            "the database is down",
        )))
    }

    fn read_from(
        &mut self,
        _: StreamId,
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
    let read = down.read(StreamId::from(1));
    assert!(
        matches!(read, Err(StoreError::Unavailable(_))),
        "the port's own default read keeps the failure"
    );
}

#[test]
#[ignore = "proposal: a unit of work lands all or nothing"]
fn a_unit_of_work_lands_at_commit_or_not_at_all() {
    let mut store = InMemoryStore::new();
    let (a, b) = (header(1, "toy"), header(2, "toy"));
    {
        let mut unit = store.begin();
        unit.append(&a, 0, &log(0..2)).unwrap();
        unit.append(&b, 0, &log(0..1)).unwrap();
        assert_eq!(unit.read(a.id).unwrap().unwrap().len, 2, "it reads its own");
    }
    assert_eq!(store.read(a.id), Ok(None), "dropped: rolled back");

    let mut unit = store.begin();
    unit.append(&a, 0, &log(0..2)).unwrap();
    unit.append(&b, 0, &log(0..1)).unwrap();
    unit.commit().unwrap();
    assert_eq!(store.read(a.id).unwrap().unwrap().len, 2);
    assert_eq!(store.read(b.id).unwrap().unwrap().len, 1);
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
    let h = header(1, "toy");
    {
        let mut port = Ready(store.begin());
        assert_eq!(ready(port.append(&h, 0, &log(0..3))), Ok(3));
        assert_eq!(
            ready(port.append(&h, 0, &log(0..1))),
            Err(StoreError::Conflict {
                stream: StreamId::from(1),
                expected: 0,
                actual: 3
            })
        );
        assert_eq!(
            ready(port.read_from(h.id, 2)).unwrap().unwrap().events,
            log(2..3)
        );
        ready(port.commit()).unwrap();
    }
    assert_eq!(store.read(h.id).unwrap().unwrap().len, 3);
}
