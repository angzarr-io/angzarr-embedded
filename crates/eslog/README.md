# eslog

A binary event-log format for [`es`](../es) streams: length-delimited
protobuf, written and read as a stream.

## The format

A log is a sequence of length-delimited messages (a varint byte length, then
the message), defined in `proto/gamekit/eslog/v1/eslog.proto`, package
`gamekit.eslog.v1`:

1. one `LogHeader`: `stream_id`, `domain`, `schema_version`, `rules_version`
   — everything `es::Header` holds;
2. one `Envelope` per event, in order: `seq`, optional `cause`, and
   `bytes event = 3`.

### One envelope for every domain

On the wire, a `bytes` field and an embedded message are the same thing:
wire type 2, a length, the encoded message. So a domain can declare its own
typed envelope with the same field numbers,

```proto
message MyEnvelope {
  uint64 seq = 1;
  optional uint64 cause = 2;
  MyEvent event = 3;
}
```

and it is byte-for-byte the generic `Envelope` with `event` set to
`MyEvent`'s encoding. Generic tools read any log; a domain reads its own
typed. `tests/format.rs` proves it with the toy counter in
`proto/gamekit/eslog/toy/v1/toy.proto`. An event that encodes to no bytes is
left off the wire either way and reads back as the event type's default.

## Checked as it streams

A `Reader` checks each envelope before handing it out: seqs run 0, 1, 2…
with no gap, duplicate or reordering, and every cause is an earlier seq —
what `es::Stream::replay` checks. A violation is `Error::Sequence` at the
offending envelope. The check reads the envelope's own fields only; it never
decodes the event.

## Ticked domains

A domain whose events happen on a clock (simkit's `Ticked<K>`) puts the tick
first in its event message:

```proto
message MyEvent {
  uint64 tick = 1;
  ...
}
```

The envelope stays `seq = 1, cause = 2, event = 3`. Because the tick is
always field 1 of the event, eslog reads it straight from the event's bytes
with a wire-format varint parse (`event_tick`), knowing nothing of the
domain's schema:

- `Reader::ticked()` also checks that ticks never decrease along seq:
  `Error::TickWentBack { seq, tick, previous }`.
- `Reader::index()` scans a log once into an `Index`: each envelope's byte
  offset and, when ticked, its tick. `Index::seq_at_or_after(tick)` binary
  searches for the first seq at or after a tick; `Reader::seek_to(seq, &index)`
  and `Reader::seek_tick(tick, &index)` jump there on seekable input and carry
  on reading, still checking. `Reader::skip_to(seq)` reads forward to a seq on
  input that cannot seek. None of these decode a domain event.
- `testing::assert_tick_is_field_1(&sample, tick)` is for a domain's own
  tests: it encodes a sample event with a non-zero tick and reads field 1
  back, failing if the message breaks the convention.

A domain that is not ticked uses field 1 as it likes and never calls these.

## API

| Item | What it is |
|---|---|
| `Writer::new(out, &es::Header)` | writes the header; then `append(&es::Envelope<E>)`, `append_raw(&Envelope)`, `write_message(&M)` |
| `Reader::new(input)` | reads the header (`header()`); then `next_event::<E>()`, `next_raw()`, `next_message::<M>()`, `events::<E>()`; `ticked()`, `skip_to`, `seek_to`, `seek_tick`, `index()`, `position()` |
| `Index` | `offset(seq)`, `tick(seq)`, `seq_at_or_after(tick)`, `end_seq()` |
| `write_log`, `read_log` | a whole log at once |
| `encode_envelope`, `decode_envelope` | `es::Envelope<E>` to and from the generic `Envelope`, for any `E: prost::Message` |
| `event_tick` | field 1 of an encoded event, without its schema |
| `From` impls | `es::Header` to and from `LogHeader` |
| `Error` | `Io`, `Empty`, `Truncated`, `BadLength`, `TooLong`, `Decode`, `Wire`, `Sequence`, `TickWentBack`, `NotInLog`, `Behind`, `NotTicked` |

Truncated or garbage input gives an `Error`, never a panic. A length over
`MAX_MESSAGE_LEN` (64 MiB) is refused before anything is allocated for it.
Length prefixes are read a byte at a time: wrap files in a `BufReader`, which
also seeks.

## The wire types and the schema

`LogHeader` and `Envelope` are prost messages written by hand in `src/lib.rs`
(`#[derive(prost::Message)]`), field for field the `.proto`. The crate needs
no codegen: it builds with plain `cargo`, including as a git dependency.

The `.proto` is the schema of record. Change it first, then the Rust to
match. `just lint` runs `buf lint` on it and `just breaking` checks it
against main for wire-breaking changes.

Codegen exists only to check the hand-written types. `just gen` runs `buf
generate` into `gen/` (gitignored), and `tests/codegen.rs`, compiled only
with the `codegen-check` feature (which `just test` turns on), encodes varied
values with both the hand-written and the generated types — absent and
present `cause`, `u64::MAX`, empty and large payloads — and checks the bytes
are equal and that each side decodes the other's. Plain `cargo test` skips
it. The toy test domains in `proto/gamekit/eslog/toy/v1/toy.proto` are
hand-written the same way, in `tests/toy/mod.rs`, and checked by the same
test.

The check needs:

- `protoc-gen-prost` 0.5.0, installed with
  `cargo install protoc-gen-prost --version 0.5.0 --locked`;
- the `prost` runtime 0.14, which that plugin generates for (it is built on
  prost-build 0.14.1) and which the hand-written types derive from. prost
  0.14 needs Rust 1.85.

On the `windows-gnu` toolchain the plugin's dependencies need `dlltool` with
an assembler beside it; the toolchain's bundled `dlltool.exe` has none. Point
`-Cdlltool=` at a copy of `dlltool.exe` sitting next to a MinGW `as.exe`.
