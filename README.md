# es

A small, deterministic event-sourcing engine. Zero dependencies, no I/O, no
clock, and nothing specific to any game. It is meant to be reused: it lives in
this workspace until it is stable, then moves to its own repository.

## The model

- A **domain** declares its state, commands, events, rejections, rules and
  context, plus two functions: **decide** and **apply**.
- A **stream** is a domain's append-only event log, plus the state folded
  from it. **No state is ever stored.** Every state is a fold of a log.
- A **projection** is a read model: another fold over one or more streams.
- An **event store** keeps streams and nothing else. You implement it;
  `InMemoryStore` is included for tests.

```rust
use es::{Domain, Stream, Tx};

struct Counter;

impl Domain for Counter {
    type State = i64;          // folded, never stored
    type Command = i64;        // decided, never stored
    type Event = i64;          // the only thing stored
    type Rejection = &'static str;
    type Rules = ();           // immutable reference data, read by both halves
    type Ctx = ();             // what decide may use beyond state: dice, a clock

    fn decide(add: &i64, tx: &mut Tx<Self>) -> Result<(), &'static str> {
        if *add == 0 {
            return Err("nothing to add");
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
- **Deterministic.** Order is the log's order. Streams in a store are kept
  in id order. Nothing iterates a hash map.

## API

| Item | What it is |
|---|---|
| `Domain` | the trait a domain implements |
| `Tx` | a decide in progress: `state`, `rules`, `ctx`, `emit`, `emit_caused` |
| `Stream<D>` | `new`, `execute`, `execute_caused`, `replay`, `state_at`, `events`, `state` |
| `Header` | stream id, domain name, schema version, rules version |
| `Envelope<E>` | `seq`, `cause` (the earlier event it follows from), `event` |
| `Projection<E>`, `project` | read models |
| `EventStore<E>`, `InMemoryStore<E>` | the storage port; appends use an expected length for optimistic concurrency |

## Not yet

- **Upcasting.** `Header::schema` is reserved. The upcast hook works on the
  stored form, so it arrives with serialisation, behind a `serde` feature.
- **Snapshots** are deliberately absent. Folding is cheap at the sizes this
  is built for.
