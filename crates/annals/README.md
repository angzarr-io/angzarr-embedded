# annals

A small, deterministic event-sourcing engine. No third-party dependencies
(only annals-diagnostic, for its errors), no I/O, no clock, and nothing
specific to any game, so any game can build on it.

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
- **Deterministic.** Order is the log's order. Streams in a store are kept
  in id order. Nothing iterates a hash map.

## API

| Item | What it is |
|---|---|
| `Domain` | the trait a domain implements |
| `Tx` | a decide in progress: `state`, `rules`, `ctx`, `emit`, `emit_caused` |
| `Stream<D>` | `new`, `execute`, `execute_caused`, `replay`, `state_at`, `visit_states`, `events`, `state` |
| `Header` | stream id, domain name, schema version, rules version |
| `Envelope<E>` | `seq`, `cause` (the earlier event it follows from), `event` |
| `Projection<E>`, `project` | read models |
| `policy::Policy`, `policy::backlog` | a stateless policy from a source domain to a target domain; the commands a target has not yet received, from its checkpoint |
| `policy::Injected<E>`, `policy::admit`, `policy::Gap` | a source event carried into a target with its source seq; placing it against the target's checkpoint (next, redelivery, or a gap) |
| `EventStore<E>`, `InMemoryStore<E>` | the storage port; appends use an expected length for optimistic concurrency |
| `ReplayError`, `Conflict`, `policy::Gap` | why a log does not replay; why an append was refused; why a delivery was refused. All are `Diagnostic`s, with messages in `locales/en-US/annals.ftl` |
| `diagnostic_samples` | one value of every error, for catalog tests |

## Not yet

- **The audited kernel** (`annals::next`, a proposal): streams keyed by
  (domain, edition, root) with main as no edition, event metadata with an
  always-set correlation and request id (16-byte UUIDs), pure decide
  beside an infallible ingest for facts, conflict resolution (COMMUTATIVE
  by generated conflict units), atomic command chains of policies, store
  ports with a unit of work and fingerprinted claims, and snapshots
  discarded on a rules-version mismatch. Its wire form is angzarr's, mapped by `annals-angzarr`. See
  `docs/design/audit.md`.

- **Upcasting.** `Header::schema` is reserved and never read. Upcasting
  arrives with `annals::next`, by payload type URL, in `annals-angzarr`.
- **Snapshots** are deliberately absent from the root API. Folding is cheap
  at the sizes this is built for; `annals::next` specifies them.
