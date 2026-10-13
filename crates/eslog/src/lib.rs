//! A binary event-log format for [`es`] streams.
//!
//! A log is a stream of length-delimited protobuf messages — each a varint
//! byte length, then the message: one [`LogHeader`], then one [`Envelope`]
//! per event, in sequence order. The schema is
//! `proto/annals/log/v1/log.proto`, package `annals.log.v1`.
//!
//! # One envelope for every domain
//!
//! [`Envelope`] carries its event as `bytes event = 3`. On the wire a `bytes`
//! field and an embedded message are the same thing — wire type 2, a length,
//! the encoded message — so a domain may declare its own typed envelope,
//!
//! ```proto
//! message MyEnvelope {
//!   uint64 seq = 1;
//!   optional uint64 cause = 2;
//!   MyEvent event = 3;
//! }
//! ```
//!
//! and it reads and writes exactly the bytes the generic one does. Tools that
//! know no domain read any log through [`Envelope`]; a domain reads it typed,
//! with [`Reader::next_message`], or through [`es::Envelope<E>`] for any
//! `E: prost::Message` with [`Reader::next_event`]. An event that encodes to
//! no bytes is omitted from the wire either way, and reads back as
//! `E::default()` (a typed envelope sees `None`).
//!
//! # Checked as it streams
//!
//! A [`Reader`] checks every envelope's seq and cause before handing it out:
//! seqs run 0, 1, 2… with no gap, duplicate or reordering, and every cause is
//! an earlier seq — what `es::Stream::replay` checks. A violation is
//! [`Error::Sequence`] at the offending envelope.
//!
//! # Ticked domains
//!
//! A domain whose events happen on a clock puts the tick first in its event
//! message: `uint64 tick = 1`. A reader switched to [`Reader::ticked`] then
//! also checks that ticks never decrease along seq ([`Error::TickWentBack`]),
//! and an [`Index`] built by such a reader maps a tick to the first seq at or
//! after it. Both read field 1 straight from the event's bytes
//! ([`event_tick`]) and never decode a domain event. A domain checks that its
//! message keeps the convention with [`testing::assert_tick_is_field_1`].
//!
//! # Reading untrusted input
//!
//! Truncated or garbage input gives an [`Error`], never a panic. A length
//! prefix over [`MAX_MESSAGE_LEN`] is refused before anything is allocated
//! for it. Reading pulls a byte at a time for length prefixes, so wrap a file
//! in a `BufReader` (which also seeks, for [`Reader::seek_to`]).
//!
//! # The wire types
//!
//! [`LogHeader`] and [`Envelope`] (in [`pb`]) are prost messages written by
//! hand, field for field the `.proto`, which stays the schema of record that
//! `buf lint` and `buf breaking` guard. The crate builds with plain `cargo`
//! and no codegen step. A test behind the `codegen-check` feature (on in
//! `just test`) encodes varied values with these types and with the ones
//! `buf generate` makes from the `.proto`, and checks the bytes match both
//! ways.

use std::fmt;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;

use prost::Message;

/// The `annals.log.v1` messages, written by hand to match
/// `proto/annals/log/v1/log.proto` field for field.
pub mod pb {
    /// What a stream is, stamped once at the head of its log.
    #[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
    pub struct LogHeader {
        /// The stream's id.
        #[prost(uint64, tag = "1")]
        pub stream_id: u64,
        /// Which domain the events belong to.
        #[prost(string, tag = "2")]
        pub domain: ::prost::alloc::string::String,
        /// The event schema version: what an upcaster reads.
        #[prost(uint32, tag = "3")]
        pub schema_version: u32,
        /// The rules and content the events were decided under.
        #[prost(string, tag = "4")]
        pub rules_version: ::prost::alloc::string::String,
    }

    /// One stored event.
    ///
    /// A reader checks, as it streams, that `seq` runs 0, 1, 2… with no gap
    /// and that every `cause` is an earlier seq: what `es::Stream::replay`
    /// checks.
    #[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
    pub struct Envelope {
        /// The event's position in its stream, from 0.
        #[prost(uint64, tag = "1")]
        pub seq: u64,
        /// The earlier event this one follows from, if any.
        #[prost(uint64, optional, tag = "2")]
        pub cause: ::core::option::Option<u64>,
        /// The domain's event message, encoded.
        #[prost(bytes = "vec", tag = "3")]
        pub event: ::prost::alloc::vec::Vec<u8>,
    }
}

pub use pb::{Envelope, LogHeader};

/// The largest message a reader accepts: 64 MiB.
pub const MAX_MESSAGE_LEN: u64 = 64 << 20;

/// Why a log could not be read, or a position in it not found.
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    /// The input held nothing, not even a header.
    Empty,
    /// The input ended inside a message or its length prefix.
    Truncated,
    /// A length prefix ran past ten bytes.
    BadLength,
    /// A length prefix claims more than [`MAX_MESSAGE_LEN`] bytes.
    TooLong(u64),
    /// A message's bytes are not that message.
    Decode(prost::DecodeError),
    /// A message's bytes are not valid protobuf wire format, or a field the
    /// format fixes has the wrong wire type.
    Wire(&'static str),
    /// An envelope's seq is out of order, or its cause is not earlier.
    Sequence(es::ReplayError),
    /// A ticked log's event at `seq` is at `tick`, before the `previous`
    /// event's.
    TickWentBack {
        seq: u64,
        tick: u64,
        previous: u64,
    },
    /// No envelope with this seq, nor is it one past the last.
    NotInLog(u64),
    /// The reader is already past `wanted`; it reads `next` next.
    Behind {
        wanted: u64,
        next: u64,
    },
    /// A tick was asked of an index built without ticks.
    NotTicked,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "reading the log: {e}"),
            Error::Empty => write!(f, "the log is empty: no header"),
            Error::Truncated => write!(f, "the log ends inside a message"),
            Error::BadLength => write!(f, "a length prefix is not a valid varint"),
            Error::TooLong(n) => write!(
                f,
                "a message claims {n} bytes, over the {MAX_MESSAGE_LEN} limit"
            ),
            Error::Decode(e) => write!(f, "a message does not decode: {e}"),
            Error::Wire(why) => write!(f, "bad wire format: {why}"),
            Error::Sequence(e) => write!(f, "the log is out of sequence: {e:?}"),
            Error::TickWentBack {
                seq,
                tick,
                previous,
            } => write!(
                f,
                "event {seq} is at tick {tick}, before the previous event's {previous}"
            ),
            Error::NotInLog(seq) => write!(f, "no event {seq} in the log"),
            Error::Behind { wanted, next } => {
                write!(f, "cannot go back to event {wanted}: the next is {next}")
            }
            Error::NotTicked => write!(f, "the index was built without ticks"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Decode(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::Io(e)
    }
}

impl From<prost::DecodeError> for Error {
    fn from(e: prost::DecodeError) -> Error {
        Error::Decode(e)
    }
}

// ---------------------------------------------------------------- es <-> pb

impl From<&es::Header> for LogHeader {
    fn from(h: &es::Header) -> LogHeader {
        LogHeader {
            stream_id: h.id.0,
            domain: h.domain.clone(),
            schema_version: h.schema,
            rules_version: h.rules.clone(),
        }
    }
}

impl From<LogHeader> for es::Header {
    fn from(h: LogHeader) -> es::Header {
        es::Header {
            id: es::StreamId(h.stream_id),
            domain: h.domain,
            schema: h.schema_version,
            rules: h.rules_version,
        }
    }
}

/// An event as stored: its message encoded into the generic envelope.
pub fn encode_envelope<E: Message>(e: &es::Envelope<E>) -> Envelope {
    Envelope {
        seq: e.seq,
        cause: e.cause,
        event: e.event.encode_to_vec(),
    }
}

/// A stored event, its message decoded.
pub fn decode_envelope<E: Message + Default>(
    raw: &Envelope,
) -> Result<es::Envelope<E>, prost::DecodeError> {
    Ok(es::Envelope {
        seq: raw.seq,
        cause: raw.cause,
        event: E::decode(raw.event.as_slice())?,
    })
}

// ---------------------------------------------------------------- wire

/// One field's value, as the wire carries it.
enum Wire<'a> {
    Varint(u64),
    Bytes(&'a [u8]),
    Fixed,
}

/// Visit every field of an encoded message, in wire order, without a schema.
fn walk<'a>(
    mut buf: &'a [u8],
    mut visit: impl FnMut(u64, Wire<'a>) -> Result<(), Error>,
) -> Result<(), Error> {
    while !buf.is_empty() {
        let key = slice_varint(&mut buf)?;
        let field = key >> 3;
        if field == 0 {
            return Err(Error::Wire("field number 0"));
        }
        let value = match key & 7 {
            0 => Wire::Varint(slice_varint(&mut buf)?),
            1 => {
                take(&mut buf, 8)?;
                Wire::Fixed
            }
            2 => {
                let len = slice_varint(&mut buf)?;
                let len = usize::try_from(len).map_err(|_| Error::Wire("length too large"))?;
                Wire::Bytes(take(&mut buf, len)?)
            }
            5 => {
                take(&mut buf, 4)?;
                Wire::Fixed
            }
            _ => return Err(Error::Wire("unsupported wire type")),
        };
        visit(field, value)?;
    }
    Ok(())
}

fn take<'a>(buf: &mut &'a [u8], n: usize) -> Result<&'a [u8], Error> {
    if buf.len() < n {
        return Err(Error::Wire("field runs past the message"));
    }
    let (head, rest) = buf.split_at(n);
    *buf = rest;
    Ok(head)
}

fn slice_varint(buf: &mut &[u8]) -> Result<u64, Error> {
    let mut value: u64 = 0;
    for i in 0..10 {
        let Some((&byte, rest)) = buf.split_first() else {
            return Err(Error::Wire("varint runs past the message"));
        };
        *buf = rest;
        if i == 9 && byte > 1 {
            return Err(Error::Wire("varint over 64 bits"));
        }
        value |= u64::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(Error::Wire("varint over ten bytes"))
}

/// An envelope's seq, cause and event bytes, read without copying the event.
struct Head<'a> {
    seq: u64,
    cause: Option<u64>,
    event: &'a [u8],
}

fn head(envelope: &[u8]) -> Result<Head<'_>, Error> {
    let mut h = Head {
        seq: 0,
        cause: None,
        event: &[],
    };
    walk(envelope, |field, value| {
        match (field, value) {
            (1, Wire::Varint(v)) => h.seq = v,
            (2, Wire::Varint(v)) => h.cause = Some(v),
            (3, Wire::Bytes(b)) => h.event = b,
            (1..=3, _) => return Err(Error::Wire("envelope field has the wrong wire type")),
            _ => {}
        }
        Ok(())
    })?;
    Ok(h)
}

/// A ticked event's tick: field 1 of its encoded message, read without the
/// domain's schema. 0 when the field is absent, as proto3 omits a zero.
pub fn event_tick(event: &[u8]) -> Result<u64, Error> {
    let mut tick = 0;
    walk(event, |field, value| {
        match (field, value) {
            (1, Wire::Varint(v)) => tick = v,
            (1, _) => return Err(Error::Wire("tick (field 1) is not a varint")),
            _ => {}
        }
        Ok(())
    })?;
    Ok(tick)
}

// ---------------------------------------------------------------- writing

/// Writes a log: the header on creation, then envelopes.
pub struct Writer<W: Write> {
    out: W,
    buf: Vec<u8>,
}

impl<W: Write> Writer<W> {
    /// Start a log by writing its header.
    pub fn new(out: W, header: &es::Header) -> io::Result<Writer<W>> {
        let mut w = Writer {
            out,
            buf: Vec::new(),
        };
        w.write_message(&LogHeader::from(header))?;
        Ok(w)
    }

    /// Append one event.
    pub fn append<E: Message>(&mut self, e: &es::Envelope<E>) -> io::Result<()> {
        self.write_message(&encode_envelope(e))
    }

    /// Append one event already in the generic envelope.
    pub fn append_raw(&mut self, e: &Envelope) -> io::Result<()> {
        self.write_message(e)
    }

    /// Append any message, length-delimited — a domain's typed envelope.
    pub fn write_message<M: Message>(&mut self, m: &M) -> io::Result<()> {
        self.buf.clear();
        m.encode_length_delimited(&mut self.buf)
            .expect("a Vec grows to fit");
        self.out.write_all(&self.buf)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }

    pub fn into_inner(self) -> W {
        self.out
    }
}

/// Write a whole log.
pub fn write_log<E: Message, W: Write>(
    out: W,
    header: &es::Header,
    events: &[es::Envelope<E>],
) -> io::Result<W> {
    let mut w = Writer::new(out, header)?;
    for e in events {
        w.append(e)?;
    }
    w.flush()?;
    Ok(w.into_inner())
}

// ---------------------------------------------------------------- reading

/// Reads a log as a stream: the header on creation, then one envelope at a
/// time, each checked before it is handed out.
pub struct Reader<R: Read> {
    input: R,
    header: es::Header,
    buf: Vec<u8>,
    /// Bytes consumed from the start of the log.
    pos: u64,
    /// The seq the next envelope must carry.
    next_seq: u64,
    /// Whether events carry a tick in field 1, to be checked.
    ticked: bool,
    /// The last envelope's tick, once one has been read.
    last_tick: Option<u64>,
}

impl<R: Read> Reader<R> {
    /// Start reading a log: read and decode its header.
    pub fn new(mut input: R) -> Result<Reader<R>, Error> {
        let mut buf = Vec::new();
        let Some(pos) = read_frame(&mut input, &mut buf)? else {
            return Err(Error::Empty);
        };
        let header = LogHeader::decode(buf.as_slice())?.into();
        Ok(Reader {
            input,
            header,
            buf,
            pos,
            next_seq: 0,
            ticked: false,
            last_tick: None,
        })
    }

    /// Read the log as a ticked domain's: check that each event's tick
    /// (field 1) is no earlier than the one before, and record ticks in an
    /// [`Index`].
    pub fn ticked(mut self) -> Reader<R> {
        self.ticked = true;
        self
    }

    pub fn header(&self) -> &es::Header {
        &self.header
    }

    /// The seq the next envelope will carry.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Bytes consumed from the start of the log: where the next envelope
    /// begins.
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// The tick of the last envelope read, in a ticked reader.
    pub fn last_tick(&self) -> Option<u64> {
        self.last_tick
    }

    /// Read the next envelope's frame and check it, without decoding its
    /// event. `false` at a clean end of input.
    fn next_frame(&mut self) -> Result<bool, Error> {
        let Some(n) = read_frame(&mut self.input, &mut self.buf)? else {
            return Ok(false);
        };
        self.pos += n;
        let h = head(&self.buf)?;
        if h.seq != self.next_seq {
            return Err(Error::Sequence(es::ReplayError::OutOfSequence {
                index: self.next_seq as usize,
                found: h.seq,
            }));
        }
        if let Some(cause) = h.cause {
            if cause >= h.seq {
                return Err(Error::Sequence(es::ReplayError::CauseNotEarlier {
                    seq: h.seq,
                    cause,
                }));
            }
        }
        if self.ticked {
            let tick = event_tick(h.event)?;
            if let Some(previous) = self.last_tick {
                if tick < previous {
                    return Err(Error::TickWentBack {
                        seq: h.seq,
                        tick,
                        previous,
                    });
                }
            }
            self.last_tick = Some(tick);
        }
        self.next_seq += 1;
        Ok(true)
    }

    /// The next message, as `M` — a domain's typed envelope. `None` at a
    /// clean end of input.
    pub fn next_message<M: Message + Default>(&mut self) -> Result<Option<M>, Error> {
        if !self.next_frame()? {
            return Ok(None);
        }
        Ok(Some(M::decode(self.buf.as_slice())?))
    }

    /// The next event in the generic envelope.
    pub fn next_raw(&mut self) -> Result<Option<Envelope>, Error> {
        self.next_message()
    }

    /// The next event, its message decoded.
    pub fn next_event<E: Message + Default>(&mut self) -> Result<Option<es::Envelope<E>>, Error> {
        match self.next_raw()? {
            Some(raw) => Ok(Some(decode_envelope(&raw)?)),
            None => Ok(None),
        }
    }

    /// Read forward, checking but not decoding events, until the next
    /// envelope is `seq`. For input that cannot seek; [`Reader::seek_to`]
    /// jumps there on input that can.
    pub fn skip_to(&mut self, seq: u64) -> Result<(), Error> {
        if self.next_seq > seq {
            return Err(Error::Behind {
                wanted: seq,
                next: self.next_seq,
            });
        }
        while self.next_seq < seq {
            if !self.next_frame()? {
                return Err(Error::NotInLog(seq));
            }
        }
        Ok(())
    }

    /// The remaining events, decoded. Stops after the first error.
    pub fn events<E: Message + Default>(self) -> Events<R, E> {
        Events {
            reader: Some(self),
            event: PhantomData,
        }
    }

    /// Scan the rest of the log, checking but not decoding events: where
    /// each envelope begins and, in a ticked reader, its tick.
    pub fn index(mut self) -> Result<Index, Error> {
        let mut index = Index {
            first: self.next_seq,
            offsets: Vec::new(),
            ticks: self.ticked.then(Vec::new),
            end: self.pos,
        };
        loop {
            let at = self.pos;
            if !self.next_frame()? {
                index.end = at;
                return Ok(index);
            }
            index.offsets.push(at);
            if let (Some(ticks), Some(t)) = (index.ticks.as_mut(), self.last_tick) {
                ticks.push(t);
            }
        }
    }
}

impl<R: Read + Seek> Reader<R> {
    /// Jump straight to envelope `seq` by its byte offset, and carry on
    /// reading and checking from there. `seq` may be one past the last
    /// envelope: the end of the log.
    pub fn seek_to(&mut self, seq: u64, index: &Index) -> Result<(), Error> {
        let at = index.offset(seq).ok_or(Error::NotInLog(seq))?;
        self.input.seek(SeekFrom::Start(at))?;
        self.pos = at;
        self.next_seq = seq;
        self.last_tick = seq.checked_sub(1).and_then(|s| index.tick(s));
        Ok(())
    }

    /// Jump to the first envelope at or after `tick`, and return its seq —
    /// one past the last if every event is earlier.
    pub fn seek_tick(&mut self, tick: u64, index: &Index) -> Result<u64, Error> {
        let seq = index.seq_at_or_after(tick).ok_or(Error::NotTicked)?;
        self.seek_to(seq, index)?;
        Ok(seq)
    }
}

/// Where each envelope of a log begins, by seq, and in a ticked log its
/// tick: built once by [`Reader::index`], then used by [`Reader::seek_to`]
/// and [`Reader::seek_tick`]. Eight bytes per event, sixteen ticked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Index {
    first: u64,
    offsets: Vec<u64>,
    ticks: Option<Vec<u64>>,
    end: u64,
}

impl Index {
    /// The byte offset envelope `seq` begins at; the end of the log for one
    /// past the last.
    pub fn offset(&self, seq: u64) -> Option<u64> {
        let i = usize::try_from(seq.checked_sub(self.first)?).ok()?;
        match i.cmp(&self.offsets.len()) {
            std::cmp::Ordering::Less => Some(self.offsets[i]),
            std::cmp::Ordering::Equal => Some(self.end),
            std::cmp::Ordering::Greater => None,
        }
    }

    /// The seq one past the last envelope: the log's length.
    pub fn end_seq(&self) -> u64 {
        self.first + self.offsets.len() as u64
    }

    /// Envelope `seq`'s tick, in a ticked index.
    pub fn tick(&self, seq: u64) -> Option<u64> {
        let i = usize::try_from(seq.checked_sub(self.first)?).ok()?;
        self.ticks.as_ref()?.get(i).copied()
    }

    /// The first seq whose tick is `tick` or later — [`Index::end_seq`] if
    /// none is. A binary search: ticks never decrease along seq. `None` for
    /// an index built without ticks.
    pub fn seq_at_or_after(&self, tick: u64) -> Option<u64> {
        let ticks = self.ticks.as_ref()?;
        Some(self.first + ticks.partition_point(|&t| t < tick) as u64)
    }
}

/// The events of a [`Reader`], one at a time.
pub struct Events<R: Read, E> {
    reader: Option<Reader<R>>,
    event: PhantomData<fn() -> E>,
}

impl<R: Read, E: Message + Default> Iterator for Events<R, E> {
    type Item = Result<es::Envelope<E>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.reader.as_mut()?.next_event().transpose();
        if !matches!(result, Some(Ok(_))) {
            self.reader = None;
        }
        result
    }
}

/// Read a whole log.
pub fn read_log<E: Message + Default, R: Read>(
    input: R,
) -> Result<(es::Header, Vec<es::Envelope<E>>), Error> {
    let reader = Reader::new(input)?;
    let header = reader.header().clone();
    let events = reader.events().collect::<Result<_, _>>()?;
    Ok((header, events))
}

/// Read one length-delimited frame into `buf`, returning the bytes it took,
/// prefix included. `None` at a clean end of input, before any byte of a
/// frame.
fn read_frame<R: Read>(input: &mut R, buf: &mut Vec<u8>) -> Result<Option<u64>, Error> {
    let (len, prefix) = match read_varint(input)? {
        None => return Ok(None),
        Some((n, _)) if n > MAX_MESSAGE_LEN => return Err(Error::TooLong(n)),
        Some(v) => v,
    };
    buf.clear();
    let got = input.by_ref().take(len).read_to_end(buf)?;
    if (got as u64) < len {
        return Err(Error::Truncated);
    }
    Ok(Some(prefix + len))
}

/// A varint and how many bytes it took, or `None` if the input ends before
/// its first byte.
fn read_varint<R: Read>(input: &mut R) -> Result<Option<(u64, u64)>, Error> {
    let mut value: u64 = 0;
    for i in 0..10 {
        let byte = match read_byte(input)? {
            Some(b) => b,
            None if i == 0 => return Ok(None),
            None => return Err(Error::Truncated),
        };
        if i == 9 && byte > 1 {
            return Err(Error::BadLength);
        }
        value |= u64::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            return Ok(Some((value, i + 1)));
        }
    }
    Err(Error::BadLength)
}

fn read_byte<R: Read>(input: &mut R) -> Result<Option<u8>, Error> {
    let mut b = [0u8];
    loop {
        match input.read(&mut b) {
            Ok(0) => return Ok(None),
            Ok(_) => return Ok(Some(b[0])),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

/// Checks a domain runs in its own tests.
pub mod testing {
    use prost::Message;

    /// Assert that a ticked domain's event message keeps the convention:
    /// its tick is field 1, a varint. Encode a `sample` whose tick is
    /// `tick` — not 0, which proto3 leaves off the wire — and read field 1
    /// back without the schema.
    pub fn assert_tick_is_field_1<E: Message>(sample: &E, tick: u64) {
        assert_ne!(tick, 0, "use a sample with a non-zero tick: proto3 omits 0");
        match super::event_tick(&sample.encode_to_vec()) {
            Ok(t) => assert_eq!(
                t, tick,
                "field 1 of the event reads {t}, not the tick {tick}: the tick must be `uint64 tick = 1`"
            ),
            Err(e) => panic!("field 1 of the event is not a tick: {e}"),
        }
    }
}
