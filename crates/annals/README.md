# annals

A small, deterministic event-sourcing engine. No I/O, no clock, and nothing
specific to any game, so any game can build on it. Its dependencies are
annals-diagnostic (for its errors) and angzarr's wire types (angzarr-proto,
with prost and prost-types), which the proposed kernel, `annals::next`,
operates on directly; angzarr-proto is a git dependency whose build runs
`protoc`.

## The model

- A **domain** declares its state, commands, events, rejections, rules and
  context, plus two functions: **decide** and **apply**. A rejection is an
  `annals_diagnostic::Diagnostic`, a stable code and named args, so the game
  client can show it in the player's language (`Infallible` for a domain
  that never rejects).
- A **stream** is a domain's append-only event log, plus the state folded
  from it. **No state is ever stored.** Every state is a fold of a log.
- A **projection** is a read model: another fold over one or more streams.
- A **policy** is the write-side counterpart: it reacts to a source stream's
  events by commanding target streams of another domain. It stores
  nothing. Its live targets are a fold of the source; each target's
  checkpoint is in the target's own stream, as the source seqs of the
  events injected into it. Delivery is idempotent: a seq the target holds
  emits nothing, and a skipped seq is refused as a gap.
- An **event store** keeps streams and nothing else. You implement it;
  `InMemoryStore` is included for tests.

```rust
use annals::{Domain, Stream, Tx};

struct Counter;

#[derive(Debug)]
struct NothingToAdd; // with Display, and Diagnostic code "counter-nothing-to-add"

impl Domain for Counter {
    type State = i64;          // folded, never stored
    type Command = i64;        // decided, never stored
    type Event = i64;          // the only thing stored
    type Rejection = NothingToAdd; // a Diagnostic
    type Rules = ();           // immutable reference data, read by both halves
    type Ctx = ();             // what decide may use beyond state: dice, a clock

    fn decide(add: &i64, tx: &mut Tx<Self>) -> Result<(), NothingToAdd> {
        if *add == 0 {
            return Err(NothingToAdd);
        }
        tx.emit(*add);
        Ok(())
    }

    fn apply(total: &mut i64, added: &i64, _: &()) {
        *total += added;
    }
}
```

## The rules it keeps

- **Decide reads, apply writes.** A decide reads state through `Tx` and
  emits events. `emit` applies each event immediately, so a decide sees the
  effects of its own events. Apply is the only thing that changes state.
- **Validate, then emit.** A decide may reject only before its first emit.
  Rejecting afterwards is a bug: it panics in debug builds. In release builds
  the emitted events stand, so that the log always matches the state.
- **Apply never sees the context.** Dice, clocks and other inputs live in
  `Ctx`, and apply is never given it, so apply cannot roll a die. Outcomes
  are decided once and recorded in the events. Replaying a log never
  re-decides anything, so changing a random-number generator, fixing a dice
  bug or retuning a table never changes an old log.
- **Deterministic.** Order is the log's order. Nothing iterates a hash map.

## API

| Item | What it is |
|---|---|
| `Domain` | the trait a domain implements |
| `Tx` | a decide in progress: `state`, `rules`, `ctx`, `emit`, `emit_caused` |
| `Stream<D>` | `new`, `execute`, `execute_caused`, `replay`, `state_at`, `visit_states`, `events`, `state` |
| `Header` | stream id, domain name, schema version, rules version |
| `Envelope<E>` | `seq`, `cause` (the earlier event it follows from), `event` |
| `ReplayError` | why a log does not replay: a `Diagnostic`, with messages in `locales/en-US/annals.ftl` |
| `diagnostic_samples` | one value of every error, for catalog tests |

## Not yet

- **The audited kernel** (`annals::next`) operates on
  angzarr's proto types directly: a stream is a `Cover` (domain, edition,
  root; main is the unset edition), a command a `CommandBook`, an event an
  `EventPage` with its `EventMeta` (causation, the producing command's id;
  fact source, correlation, recorded-at, rules version; no actor and no
  reaction trigger), every id a 16-byte
  `UUID`. One domain-blind parse (`check`) checks a book and yields a
  `Checked` with non-optional accessors; `identify` assigns its command id
  and correlation; the in-process coordinator loads the snapshot and
  events, then deduplicates by command id on the stream's events (load
  first, so a concurrent commit of the same command is never missed), and
  resolves the conflict resolution (COMMUTATIVE by default where the state declares
  conflict units, else STRICT); then the command or fact is decoded
  (upcast by type URL) and decided, or ingested with a read-only context.
  Policies run in the same atomic chain, targeting from the event (so a
  policy may forward only some events), and the dead-letter queue is a
  logger. It is implemented and tested; hatpick moves onto it before
  it replaces the root API. See `docs/design/audit.md`.

- **Upcasting.** `Header::schema` is reserved and never read. Upcasting
  arrives with `annals::next`, by payload type URL, for events and
  (discouraged) commands.
- **Snapshots** are deliberately absent from the root API. Folding is cheap
  at the sizes this is built for; `annals::next` specifies them.
