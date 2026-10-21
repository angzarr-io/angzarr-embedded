//! The log format, proven on a toy counter domain.

use std::fmt;
use std::io::{self, Read};

use crate::{read_log, write_log, Envelope, Error, LogHeader, Reader, Writer, MAX_MESSAGE_LEN};
use annals::{Domain, Header, Stream, StreamId, Tx};
use annals_diagnostic::{Arg, Diagnostic};
use prost::Message;

use crate::toy::counter_event::Kind;
use crate::toy::{CounterEnvelope, CounterEvent};

// ---------------------------------------------------------------- the domain

struct Counter;

#[derive(Debug)]
struct NothingToAdd;

impl fmt::Display for NothingToAdd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("nothing to add")
    }
}

impl Diagnostic for NothingToAdd {
    fn code(&self) -> &'static str {
        "toy-counter-nothing-to-add"
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![]
    }
}

impl Domain for Counter {
    type State = i64;
    type Command = i64;
    type Event = CounterEvent;
    type Rejection = NothingToAdd;
    type Rules = ();
    type Ctx = ();

    fn decide(add: &i64, tx: &mut Tx<Self>) -> Result<(), NothingToAdd> {
        if *add == 0 {
            return Err(NothingToAdd);
        }
        if *tx.state() + add < 0 {
            tx.emit(CounterEvent {
                kind: Some(Kind::Reset("would go negative".into())),
            });
            return Ok(());
        }
        tx.emit(CounterEvent {
            kind: Some(Kind::Added(*add)),
        });
        Ok(())
    }

    fn apply(total: &mut i64, e: &CounterEvent, _: &()) {
        match &e.kind {
            Some(Kind::Added(n)) => *total += n,
            Some(Kind::Reset(_)) => *total = 0,
            None => {}
        }
    }
}

fn header() -> Header {
    Header {
        id: StreamId(42),
        domain: "counter".into(),
        schema: 3,
        rules: "toy 1.0".into(),
    }
}

fn counted() -> Stream<Counter> {
    let mut s = Stream::<Counter>::new(header());
    for add in [5, -2, 7, -20, 4] {
        s.execute_caused(&add, s.len().checked_sub(1), &(), &mut ())
            .expect("decides");
    }
    s
}

fn log_bytes() -> Vec<u8> {
    let s = counted();
    write_log(Vec::new(), s.header(), s.events()).expect("writes")
}

// ---------------------------------------------------------------- round trips

#[test]
fn a_stream_round_trips_and_replays() {
    let s = counted();
    let bytes = log_bytes();
    let (h, events) = read_log::<CounterEvent, _>(bytes.as_slice()).expect("reads");
    assert_eq!(&h, s.header());
    assert_eq!(events, s.events());
    let replayed = Stream::<Counter>::replay(h, events, &()).expect("replays");
    assert_eq!(replayed.state(), s.state());
    assert_eq!(*replayed.state(), 4);
}

#[test]
fn the_header_carries_everything_es_holds() {
    let h = header();
    let pb = LogHeader::from(&h);
    assert_eq!(pb.stream_id, 42);
    assert_eq!(pb.schema_version, 3);
    assert_eq!(Header::from(pb), h);
}

#[test]
fn reading_streams_one_byte_at_a_time() {
    struct Trickle<'a>(&'a [u8]);
    impl Read for Trickle<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.0.is_empty() || buf.is_empty() {
                return Ok(0);
            }
            buf[0] = self.0[0];
            self.0 = &self.0[1..];
            Ok(1)
        }
    }
    let bytes = log_bytes();
    let mut r = Reader::new(Trickle(&bytes)).expect("header");
    assert_eq!(r.header(), &header());
    let mut n = 0;
    while let Some(e) = r.next_event::<CounterEvent>().expect("event") {
        assert_eq!(e.seq, n);
        n += 1;
    }
    assert_eq!(n, counted().len());
}

// ---------------------------------------------------------------- the wire trick

fn sample_event() -> CounterEvent {
    CounterEvent {
        kind: Some(Kind::Reset("because".into())),
    }
}

#[test]
fn a_typed_envelope_is_byte_for_byte_the_generic_one() {
    let event = sample_event();
    let typed = CounterEnvelope {
        seq: 9,
        cause: Some(4),
        event: Some(event.clone()),
    };
    let generic = Envelope {
        seq: 9,
        cause: Some(4),
        event: event.encode_to_vec(),
    };
    assert_eq!(typed.encode_to_vec(), generic.encode_to_vec());
    assert_eq!(
        Envelope::decode(typed.encode_to_vec().as_slice()).unwrap(),
        generic
    );
    assert_eq!(
        CounterEnvelope::decode(generic.encode_to_vec().as_slice()).unwrap(),
        typed
    );
}

#[test]
fn a_log_written_generic_reads_typed_and_back() {
    // Written through annals::Envelope<E> (the generic envelope underneath)...
    let bytes = log_bytes();
    let mut r = Reader::new(bytes.as_slice()).unwrap();
    let mut typed = Vec::new();
    while let Some(t) = r.next_message::<CounterEnvelope>().unwrap() {
        typed.push(t);
    }
    let s = counted();
    assert_eq!(typed.len(), s.events().len());
    for (t, e) in typed.iter().zip(s.events()) {
        assert_eq!(
            (t.seq, t.cause, t.event.as_ref()),
            (e.seq, e.cause, Some(&e.event))
        );
    }
    // ...and written typed, it reads back generic and through annals.
    let mut w = Writer::new(Vec::new(), &header()).unwrap();
    for t in &typed {
        w.write_message(t).unwrap();
    }
    let rewritten = w.into_inner();
    assert_eq!(rewritten, bytes);
}

#[test]
fn an_empty_event_reads_back_as_the_default() {
    let e = annals::Envelope {
        seq: 0,
        cause: None,
        event: CounterEvent { kind: None },
    };
    let bytes = write_log(Vec::new(), &header(), std::slice::from_ref(&e)).unwrap();
    let (_, events) = read_log::<CounterEvent, _>(bytes.as_slice()).unwrap();
    assert_eq!(events, [e]);
    let mut r = Reader::new(bytes.as_slice()).unwrap();
    assert_eq!(
        r.next_message::<CounterEnvelope>().unwrap().unwrap().event,
        None
    );
}

// ---------------------------------------------------------------- bad input

#[test]
fn empty_input_has_no_header() {
    assert!(matches!(Reader::new(&[][..]), Err(Error::Empty)));
}

#[test]
fn every_truncation_is_an_error_or_a_shorter_log() {
    let bytes = log_bytes();
    let whole = read_log::<CounterEvent, _>(bytes.as_slice()).unwrap().1;
    let mut clean_cuts = 0;
    for cut in 0..bytes.len() {
        match read_log::<CounterEvent, _>(&bytes[..cut]) {
            Ok((_, events)) => {
                clean_cuts += 1;
                assert!(events.len() < whole.len());
                assert_eq!(events, whole[..events.len()], "cut at {cut}");
            }
            Err(Error::Truncated | Error::Empty | Error::Decode(_)) => {}
            Err(e) => panic!("cut at {cut}: unexpected {e}"),
        }
    }
    // One clean cut after the header and after each event but the last.
    assert_eq!(clean_cuts, whole.len());
}

#[test]
fn garbage_gives_errors_not_panics() {
    let mut x: u64 = 0x2545_F491_4F6C_DD1D;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    for len in 0..300 {
        for _ in 0..20 {
            let bytes: Vec<u8> = (0..len).map(|_| next() as u8).collect();
            let _ = read_log::<CounterEvent, _>(bytes.as_slice());
            let _ = Reader::new(bytes.as_slice()).map(|r| r.events::<Envelope>().count());
        }
    }
    // Garbage spliced into a good log, too.
    let good = log_bytes();
    for at in 0..good.len() {
        let mut bad = good.clone();
        bad[at] ^= next() as u8 | 1;
        let _ = read_log::<CounterEvent, _>(bad.as_slice());
    }
}

#[test]
fn a_huge_length_is_refused_before_allocating() {
    let mut bytes = log_bytes();
    // A length prefix of u64::MAX.
    bytes.extend_from_slice(&[0xff; 9]);
    bytes.push(0x01);
    match read_log::<CounterEvent, _>(bytes.as_slice()) {
        Err(Error::TooLong(n)) => assert!(n > MAX_MESSAGE_LEN),
        other => panic!("{other:?}"),
    }
}

#[test]
fn an_overlong_varint_is_refused() {
    let mut bytes = log_bytes();
    bytes.extend_from_slice(&[0x80; 11]);
    assert!(matches!(
        read_log::<CounterEvent, _>(bytes.as_slice()),
        Err(Error::BadLength)
    ));
}

#[test]
fn a_length_of_exactly_the_limit_is_allowed() {
    let mut bytes = log_bytes();
    // MAX_MESSAGE_LEN as a varint, then nothing: allowed, so truncated.
    let mut n = MAX_MESSAGE_LEN;
    while n >= 0x80 {
        bytes.push(n as u8 | 0x80);
        n >>= 7;
    }
    bytes.push(n as u8);
    assert!(matches!(
        read_log::<CounterEvent, _>(bytes.as_slice()),
        Err(Error::Truncated)
    ));
}

#[test]
fn a_length_whose_tenth_byte_is_0_or_1_is_a_varint() {
    // Ten bytes, the tenth 0: a long way to write 0, the header's length.
    let mut zero = vec![0x80; 9];
    zero.push(0x00);
    let r = Reader::new(zero.as_slice()).expect("an empty header");
    assert_eq!(r.position(), 10);
    // The tenth byte 2 would set a bit past 64.
    let mut over = vec![0xff; 9];
    over.push(0x02);
    assert!(matches!(
        Reader::new(over.as_slice()),
        Err(Error::BadLength)
    ));
}

#[test]
fn input_that_ends_inside_a_length_is_truncated() {
    let mut bytes = log_bytes();
    bytes.push(0x80);
    assert!(matches!(
        read_log::<CounterEvent, _>(bytes.as_slice()),
        Err(Error::Truncated)
    ));
}

/// Reads `bytes`, but first fails once with `kind`.
struct FailsOnce<'a> {
    kind: Option<io::ErrorKind>,
    bytes: &'a [u8],
}

impl Read for FailsOnce<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if let Some(kind) = self.kind.take() {
            return Err(io::Error::from(kind));
        }
        self.bytes.read(buf)
    }
}

#[test]
fn an_interrupted_read_is_retried() {
    let bytes = log_bytes();
    let input = FailsOnce {
        kind: Some(io::ErrorKind::Interrupted),
        bytes: &bytes,
    };
    assert_eq!(Reader::new(input).expect("header").header(), &header());
}

#[test]
fn any_other_failed_read_is_an_error() {
    let bytes = log_bytes();
    let input = FailsOnce {
        kind: Some(io::ErrorKind::BrokenPipe),
        bytes: &bytes,
    };
    match Reader::new(input) {
        Err(Error::Io(e)) => assert_eq!(e.kind(), io::ErrorKind::BrokenPipe),
        Err(e) => panic!("{e}"),
        Ok(_) => panic!("read past a failure"),
    }
}

#[test]
fn an_error_s_source_is_the_error_it_wraps() {
    use std::error::Error as _;
    let io = Error::Io(io::Error::other("disk"));
    assert_eq!(io.source().unwrap().to_string(), "disk");
    let decode_error = Envelope::decode(&[0xff][..]).unwrap_err();
    let shown = decode_error.to_string();
    let decode = Error::Decode(decode_error);
    assert_eq!(decode.source().unwrap().to_string(), shown);
    let replay = annals::ReplayError::OutOfSequence { index: 1, found: 4 };
    let shown = replay.to_string();
    let sequence = Error::Sequence(replay);
    assert_eq!(sequence.source().unwrap().to_string(), shown);
    assert!(Error::Empty.source().is_none());
}

#[test]
fn write_log_flushes_what_it_wrote() {
    #[derive(Default)]
    struct Flushed {
        bytes: Vec<u8>,
        flushed: usize,
    }
    impl io::Write for Flushed {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.bytes.write(buf)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.flushed = self.bytes.len();
            Ok(())
        }
    }
    let s = counted();
    let out = write_log(Flushed::default(), s.header(), s.events()).expect("writes");
    assert_eq!(out.flushed, out.bytes.len());
    assert_eq!(out.bytes, log_bytes());
}
