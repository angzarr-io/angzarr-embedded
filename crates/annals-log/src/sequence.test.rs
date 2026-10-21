//! What a reader checks as it streams — seqs, causes and, for a ticked
//! domain, ticks — and seeking by seq and by tick.

use std::io::Cursor;

use crate::testing::assert_tick_is_field_1;
use crate::{event_tick, Envelope, Error, Reader, WireProblem, Writer};
use annals::{Header, StreamId};
use prost::Message;

use crate::toy::TimedEvent;

fn header() -> Header {
    Header {
        id: StreamId(1),
        domain: "timed".into(),
        schema: 1,
        rules: "toy".into(),
    }
}

/// A log of raw envelopes: (seq, cause, tick).
fn log(rows: &[(u64, Option<u64>, u64)]) -> Vec<u8> {
    let mut w = Writer::new(Vec::new(), &header()).unwrap();
    for &(seq, cause, tick) in rows {
        let event = TimedEvent {
            tick,
            what: format!("event {seq}"),
        };
        w.append_raw(&Envelope {
            seq,
            cause,
            event: event.encode_to_vec(),
        })
        .unwrap();
    }
    w.into_inner()
}

/// Read every envelope; the seqs read before the first error, and the error.
fn read(bytes: &[u8], ticked: bool) -> (Vec<u64>, Option<Error>) {
    let mut r = Reader::new(bytes).unwrap();
    if ticked {
        r = r.ticked();
    }
    let mut seqs = Vec::new();
    loop {
        match r.next_raw() {
            Ok(Some(e)) => seqs.push(e.seq),
            Ok(None) => return (seqs, None),
            Err(e) => return (seqs, Some(e)),
        }
    }
}

fn out_of_sequence(err: Option<Error>) -> (usize, u64) {
    match err {
        Some(Error::Sequence(annals::ReplayError::OutOfSequence { index, found })) => {
            (index, found)
        }
        other => panic!("expected out of sequence, got {other:?}"),
    }
}

// ---------------------------------------------------------------- seq and cause

#[test]
fn a_well_formed_log_reads_through() {
    let bytes = log(&[(0, None, 1), (1, Some(0), 1), (2, Some(0), 4)]);
    let (seqs, err) = read(&bytes, true);
    assert_eq!(seqs, [0, 1, 2]);
    assert!(err.is_none(), "{err:?}");
}

#[test]
fn a_gap_is_refused_at_the_envelope_after_it() {
    let (seqs, err) = read(&log(&[(0, None, 0), (1, None, 0), (3, None, 0)]), false);
    assert_eq!(seqs, [0, 1]);
    assert_eq!(
        err.as_ref().map(ToString::to_string).as_deref(),
        Some("the log is out of sequence: event 2 claims sequence number 3, not 2")
    );
    assert_eq!(out_of_sequence(err), (2, 3));
}

#[test]
fn a_duplicate_is_refused() {
    let (seqs, err) = read(&log(&[(0, None, 0), (1, None, 0), (1, None, 0)]), false);
    assert_eq!(seqs, [0, 1]);
    assert_eq!(out_of_sequence(err), (2, 1));
}

#[test]
fn an_out_of_order_seq_is_refused() {
    let (seqs, err) = read(&log(&[(0, None, 0), (2, None, 0), (1, None, 0)]), false);
    assert_eq!(seqs, [0]);
    assert_eq!(out_of_sequence(err), (1, 2));
}

#[test]
fn a_log_must_start_at_zero() {
    let (seqs, err) = read(&log(&[(1, None, 0)]), false);
    assert!(seqs.is_empty());
    assert_eq!(out_of_sequence(err), (0, 1));
}

#[test]
fn a_forward_or_self_cause_is_refused() {
    for cause in [2, 3] {
        let (seqs, err) = read(
            &log(&[(0, None, 0), (1, Some(0), 0), (2, Some(cause), 0)]),
            false,
        );
        assert_eq!(seqs, [0, 1]);
        match err {
            Some(Error::Sequence(annals::ReplayError::CauseNotEarlier { seq: 2, cause: c })) => {
                assert_eq!(c, cause)
            }
            other => panic!("cause {cause}: {other:?}"),
        }
    }
}

// ---------------------------------------------------------------- ticks

#[test]
fn the_field_1_reader_agrees_with_prost() {
    for tick in [0, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
        let e = TimedEvent {
            tick,
            what: "x".repeat(tick as usize % 200),
        };
        let bytes = e.encode_to_vec();
        assert_eq!(
            event_tick(&bytes).unwrap(),
            TimedEvent::decode(bytes.as_slice()).unwrap().tick
        );
    }
    assert_tick_is_field_1(
        &TimedEvent {
            tick: 77,
            what: "sample".into(),
        },
        77,
    );
}

#[test]
#[should_panic(expected = "field 1 of the event is not a tick")]
fn the_helper_catches_a_message_without_the_tick_first() {
    // The tick is there, but as field 2; field 1 is a string.
    #[derive(Clone, PartialEq, Message)]
    struct Wrong {
        #[prost(string, tag = "1")]
        name: String,
        #[prost(uint64, tag = "2")]
        tick: u64,
    }
    assert_tick_is_field_1(
        &Wrong {
            name: "a".into(),
            tick: 5,
        },
        5,
    );
}

#[test]
fn a_decreasing_tick_is_refused_when_ticked() {
    let bytes = log(&[(0, None, 5), (1, None, 5), (2, None, 9), (3, None, 8)]);
    let (seqs, err) = read(&bytes, true);
    assert_eq!(seqs, [0, 1, 2]);
    match err {
        Some(Error::TickWentBack {
            seq: 3,
            tick: 8,
            previous: 9,
        }) => {}
        other => panic!("{other:?}"),
    }
    // A reader that is not ticked does not look.
    assert_eq!(read(&bytes, false).0, [0, 1, 2, 3]);
}

#[test]
fn a_ticked_reader_refuses_a_field_1_that_is_not_a_varint() {
    let mut w = Writer::new(Vec::new(), &header()).unwrap();
    w.append_raw(&Envelope {
        seq: 0,
        cause: None,
        // Field 1 as a length-delimited string.
        event: vec![0x0a, 0x01, b'x'],
    })
    .unwrap();
    let (_, err) = read(&w.into_inner(), true);
    assert!(
        matches!(err, Some(Error::Wire(WireProblem::TickNotVarint))),
        "{err:?}"
    );
    assert_eq!(
        err.unwrap().to_string(),
        "bad wire format: tick (field 1) is not a varint"
    );
}

/// Ticks 2, 2, 2, 5, 5, 9 — several events at a tick, and empty ticks
/// between.
const TICKS: [u64; 6] = [2, 2, 2, 5, 5, 9];

fn ticked_log() -> Vec<u8> {
    let rows: Vec<_> = TICKS
        .iter()
        .enumerate()
        .map(|(i, &t)| (i as u64, None, t))
        .collect();
    log(&rows)
}

#[test]
fn an_index_maps_a_tick_to_the_first_seq_at_or_after_it() {
    let bytes = ticked_log();
    let index = Reader::new(bytes.as_slice())
        .unwrap()
        .ticked()
        .index()
        .unwrap();
    assert_eq!(index.end_seq(), 6);
    let cases = [
        (0, 0), // before everything
        (2, 0), // the first of three at tick 2
        (3, 3), // an empty tick: the next tick's first
        (4, 3),
        (5, 3),
        (6, 5),
        (9, 5),
        (10, 6), // after everything: one past the last
    ];
    for (tick, seq) in cases {
        assert_eq!(index.seq_at_or_after(tick), Some(seq), "tick {tick}");
    }
    for (seq, &t) in TICKS.iter().enumerate() {
        assert_eq!(index.tick(seq as u64), Some(t));
    }
    // Built without ticks, it cannot answer.
    let plain = Reader::new(bytes.as_slice()).unwrap().index().unwrap();
    assert_eq!(plain.seq_at_or_after(2), None);
}

#[test]
fn seeking_reads_on_from_the_seq_and_never_decodes_before_it() {
    let bytes = ticked_log();
    let index = Reader::new(bytes.as_slice())
        .unwrap()
        .ticked()
        .index()
        .unwrap();
    for (tick, want) in [(0u64, 0u64), (3, 3), (6, 5), (10, 6)] {
        let mut r = Reader::new(Cursor::new(bytes.clone())).unwrap().ticked();
        assert_eq!(r.seek_tick(tick, &index).unwrap(), want);
        assert_eq!(r.position(), index.offset(want).unwrap());
        let mut rest = Vec::new();
        while let Some(e) = r.next_event::<TimedEvent>().unwrap() {
            rest.push((e.seq, e.event.tick));
        }
        let expected: Vec<_> = (want..6).map(|s| (s, TICKS[s as usize])).collect();
        assert_eq!(rest, expected, "tick {tick}");
    }
    // By seq, with or without seeking.
    let mut r = Reader::new(Cursor::new(bytes.clone())).unwrap();
    r.seek_to(4, &index).unwrap();
    assert_eq!(r.next_raw().unwrap().unwrap().seq, 4);
    assert!(matches!(r.seek_to(7, &index), Err(Error::NotInLog(7))));
    let mut r = Reader::new(bytes.as_slice()).unwrap();
    r.skip_to(4).unwrap();
    assert_eq!(r.next_raw().unwrap().unwrap().seq, 4);
    assert!(matches!(
        r.skip_to(2),
        Err(Error::Behind { wanted: 2, next: 5 })
    ));
    assert!(matches!(r.skip_to(9), Err(Error::NotInLog(9))));
}

#[test]
fn a_seek_keeps_checking_ticks_across_the_jump() {
    // Seek to seq 3 (tick 5), then the log says tick 4: refused, because the
    // index remembers seq 2's tick.
    let bytes = log(&[(0, None, 1), (1, None, 3), (2, None, 5), (3, None, 4)]);
    let index = Reader::new(bytes.as_slice()).unwrap().index().unwrap();
    let ticked_index = {
        // Built from a log that is fine up to seq 2.
        let good = log(&[(0, None, 1), (1, None, 3), (2, None, 5), (3, None, 6)]);
        Reader::new(good.as_slice())
            .unwrap()
            .ticked()
            .index()
            .unwrap()
    };
    assert_eq!(index.offset(3), ticked_index.offset(3));
    let mut r = Reader::new(Cursor::new(bytes)).unwrap().ticked();
    r.seek_to(3, &ticked_index).unwrap();
    assert!(matches!(
        r.next_raw(),
        Err(Error::TickWentBack {
            seq: 3,
            tick: 4,
            previous: 5
        })
    ));
}
