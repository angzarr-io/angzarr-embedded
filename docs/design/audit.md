# The audited kernel on angzarr's proto types: design

Status: **implemented** in `crates/annals/src/next/`, pinned by the tests in
`crates/annals/tests/next/`. It does not yet replace the crate root's
kernel: hatpick moves onto it first.

The wire contract is angzarr's protos at angzarr-project `feat/audit-meta`
**5850057** (pass 13: `EventMeta.causation` is the event's direct cause,
`oneof { command_id, triggering_event }`, with no command-causation on
events; PM process events deduplicate on their trigger; reactor re-runs
always deduplicate. On top of pass 12: one `command_id` per command,
deduplication on the events, no actor, `StateOptions`/`StateDescriptor`,
commands may be upcast),
and the record in angzarr's `docs/design/audit-meta/` (`decision-log.md`,
`idempotency.md`, `annals-divergence.md`) and
`docs/guide/state-and-conflicts.md`.

**Causation is the direct cause** (angzarr decisions 13.1 and 13.3). An
event's `EventMeta.causation` names what produced it: its command
(`command_id`, every command's event, a policy's included), or, for a
process manager's own process event, the event it reacted to
(`triggering_event`). A policy's command still names its trigger in
`CommandPage.causation_id`, the command's own causation (transport and
trace metadata, and the input its derived id hashes), but that is never
copied onto its events: the trigger-to-command edge belongs in tracing and
logs. There is no re-run switch: a reaction's id never names a rules
version, so a re-run deduplicates, and repairs are new commands (decision
13.4; see [Reactor re-runs](#reactor-re-runs)).

## Why

People choose event sourcing for different reasons; here the main reason is
**auditability**. Every change should answer when it was made, in which
workflow, why (which command, and what caused it), and under which rules.
Replay should reproduce exactly what was seen at the time. Who made it is a
security question, answered by a separate security log, not by the events.
An architectural review of the root kernel found gaps in exactly those
places:

| ID | Finding | Resolution here |
|---|---|---|
| A1 | `Envelope` holds only seq, cause and event: no when, no why. Consumers put metadata in the payload (hatpick's `Stamp`, annals-sim's `Ticked`). | Every event carries angzarr's `EventMeta`, stamped by the kernel. Business code emits payloads only. [Metadata](#metadata) |
| A2 | `cause` is only within a stream; there is no correlation id. | Every event names its direct cause (its command, or a process event's trigger, an `EventRef` into any stream); a reaction command names its trigger as its own `causation_id`; correlation is always set and kept through every reaction. [Identification](#step-2-identification), [Metadata](#metadata) |
| A3 | `Header.rules` is a free string replay never checks. | The rules version is registered, stamped per event, resolved by a `Rulebook` at replay, and invalidates snapshots. [Rules versions](#rules-versions) |
| A5 | `Header.schema` is unused; there is no upcasting. | A payload's type URL is its version. Old types are upcast by type URL, strictly, events at read and (discouraged) commands before decode. [Decode](#step-5-decode-then-decide-or-ingest) |
| A6 | Events emitted before a rejection stay in the `Stream`. | Decide is pure and stages its events; a rejection leaves nothing. |
| B1 | The "saga" is a stateless policy; the target-contiguity invariant is undocumented; `backlog` replays the source once per target. | Policies and projections share one base; the invariant is documented and checked; the backlog is one pass. [Reactions](#reactions-and-the-chain) |
| B2 | `Projection::on` gets no stream identity or position. | Every reaction gets a `Committed`: the cover, the sequence, the metadata, the event, the state just after. |
| B6 | The store port is synchronous, whole-stream, `u64`-keyed; `read -> Option` hides I/O failure. | Sync and async ports over opaque pages keyed by `Cover`, a unit of work, `read_from`, `find_command`, continuity checks; `StoreError` separates conflict, caller bug, unreadable and unavailable. [The store port](#the-store-port) |
| B8 | Kernel faults are folded into business rejections. | `Failure` keeps the rejection apart from malformed input, kernel faults and failures down the chain, each classified for recovery. [Errors](#errors-and-recovery) |

## Layering

```mermaid
flowchart TB
  subgraph angzarr-project
    P["io.angzarr.v1 protos<br/>(Cover, CommandBook, Handling, EventPage,<br/>EventMeta, EventRef, Snapshot, StateDescriptor, Outcome)"]
    AP["angzarr-proto<br/>(prost types, prost::Name, ids)"]
    P --> AP
  end
  subgraph annals
    K["annals::next<br/>(kernel: parse, coordinator, chain, upcaster, registry)"]
  end
  AP --> K
  subgraph hatpick
    E["hatpick-events<br/>(domains; prost payloads, versioned packages)"]
    S["hatpick-server<br/>(Postgres pages via diesel-async, Kernel::handle_async)"]
    E --> S
  end
  K --> E
  K --> S
```

- **angzarr owns the wire contract.** annals defines no wire schema of its
  own for the kernel's events: its types are angzarr-proto's.
- **annals operates on the proto types directly.** The kernel depends on
  angzarr-proto and handles `v1::Cover`, `CommandBook`, `EventPage`,
  `EventMeta`, `EventRef`, `Snapshot`, `Uuid`, `ConflictResolution`,
  `StateDescriptor` and `AngzarrDeadLetter` as generated. There is no
  parallel stream key, event id, book, metadata, provenance, origin or UUID
  type, and so no mapping layer. Dependency-freeness was given up for this,
  deliberately: a mapping layer is a second home for every value, and every
  second home is a place for the two to disagree.
- **annals-angzarr is folded into the kernel.** What it held that was more
  than mapping lives on in `annals::next`: the `Payload` trait, `pack`,
  `type_name` and `Decoder`; the `Upcaster` (a mirror of angzarr-router's
  `UpcasterDispatch`), now also for commands; the `Registry`, now also the
  mirror of `ComponentDescriptor.state`; and the golden replay fixtures. The
  opaque-row store adapter (`Opaque`, `Stored`) is gone: the store port
  itself is opaque now. The id helpers are angzarr-proto's own
  (`angzarr_proto::ids`, re-exported as `annals::next::ids`). One crate, not
  two, because with no mapping left the adapter would be a crate of
  re-exports.
- **annals is the in-process runtime**, embedded in hatpick's server. A
  chain's staged events across streams go into one database transaction.
  hatpick calls annals directly, in process: there is no HTTP or gRPC layer
  into annals, so no caller outside the process can deliver a reaction.
- **annals is not angzarr.** They share the protos, some code and their
  intents. Where the execution models differ, annals' choice is deliberate
  and listed under [Divergences](#divergences-from-angzarr).
- **Storage is opaque**: the encoded page as written and the metadata as
  typed columns. Typed views are projections; in hatpick they are replayed
  on read.

## Design constraints

- **annals does no I/O**, keeps no infrastructure and never retries. Its
  failures say whether to retry (`Recovery`), and a retry is always safe
  (it starts at the deduplication lookup).
- **Pure and deterministic.** No clock, no randomness, no I/O and no
  hash-map iteration in the kernel (maps keyed by `Cover` are lookup only,
  since prost types have no ordering). The application samples the instant
  and mints the UUIDv7 a client's command without an id is given
  (`Sampled { now, command_id }`). UUIDv5 derivations are angzarr-proto's.
  The conflict nonce is a counter.
- **The kernel stays synchronous.** The only async parts are the async store
  port and the handler over it, with native `impl Future` in traits.
- **MSRV 1.85** (prost 0.14). **annals builds for
  `wasm32-unknown-unknown`**, angzarr-proto included; `just wasm` checks it.
- **Must not preclude hash chaining** (deferred). Stored pages are never
  rewritten; upcasting happens on read; metadata is deterministic; no
  personal data is in an event.

## The pipeline

One command, end to end, in angzarr's normative order (`types.proto`, the
`handling` region). Each step's output type is the next step's only input,
so the order cannot be skipped or reordered:

```text
CommandBook ─check──▶ Checked ─identify──▶ Identified ─Coordinator::deduplicate──▶ Deduplicated
                                                         ├─ Duplicate { events }            (answer: DUPLICATE)
                                                         └─ Novel ─Coordinator::load──▶ Contextual
                                                                     ─decode::<D>──▶ Prepared<D> ─decide──▶ Decision<D>
```

1. **Validate** (`check`): domain-blind, everything at once.
2. **Identify** (`Checked::identify`): command id, correlation, causation.
3. **Deduplicate** (`Coordinator::deduplicate`): the stream already holds
   events with this command id? They are the answer.
4. **Load and resolve** (`Coordinator::load`): the conflict resolution, the
   snapshot and events, the expected sequence stamped or checked (STRICT's
   stale refusal is here), the window and nonce.
5. **Decode, then decide or ingest** (`Contextual::decode`,
   `Prepared::decide`): typed, upcast, strict; the body arm picks decide or
   ingest; the kernel stamps the events.

**Deduplication comes before the sequence check**, so a retry of an applied
command whose expected sequence is now stale is answered DUPLICATE, never
stale. **Validation comes before deduplication**, so a malformed retry of an
applied command is still malformed. **Decoding comes after both**, so an
unknown command type is refused only for a command that is new and current.

## Step 1: one check

```rust
pub fn check(book: CommandBook, registry: &Registry) -> Result<Checked, Malformed>;

impl Checked {
    pub fn cover(&self) -> &Cover;            pub fn domain(&self) -> &str;
    pub fn root(&self) -> &Uuid;              // present, 16 bytes
    pub fn edition(&self) -> Option<&str>;    // None: main; never Some("")
    pub fn body(&self) -> &Body;              // Command { payload } | Fact { payload, source }
    pub fn payload(&self) -> &Any;
    pub fn command_id(&self) -> Option<&Uuid>;       // the sender's, 16 bytes if set
    pub fn correlation_id(&self) -> Option<&Uuid>;
    pub fn expected_sequence(&self) -> Option<u64>;
    pub fn conflict_resolution(&self) -> Option<ConflictResolution>;   // supported if set
    pub fn is_reaction(&self) -> bool;
    pub fn identify(self, sampled: &Sampled) -> Identified;
}
```

`check` is **the one place a `CommandBook` is checked**. It takes the book
apart once; what it returns holds the required parts outside their prost
`Option`s, so nothing downstream unwraps one. The accessors that stay
`Option` are the sender's genuine choices. `Body` attaches `fact_origin` to
the fact arm, so a fact's source is never optional.

**Domain-blind.** `check` knows no domain's types. It asks the `Registry`
only what angzarr's coordinator reads off a `StateDescriptor`: is
`cover.domain` registered, and which resolutions does its state support.

**Malformed**, in the order tried (the first that applies is the answer):
no cover; an unregistered domain; no root; a root that is not 16 bytes; a
set empty edition; no page; no body; no payload; a set command or
correlation id that is not 16 bytes; a causation from a client
(`CausationFromOutside`); for a policy's book, ids of its own
(`IdOnReaction`), a fact (`FactFromReaction`) or handling
(`HandlingOnReaction`), or a trigger that cannot name an id (`BadTrigger`);
a fact without `fact_origin`; an empty `external_id`; a command with a
`fact_origin`; a fact whose `command_id` is not its derivation; a fact with
**any** handling field (`HandlingOnFact`); a `conflict_resolution` value the
build does not know; a resolution the domain's state does not support
(COMMUTATIVE on a state without conflict units, above all). **An empty
`Handling` is unset**: the same as none.

**No causation from outside.** `check` is the client entry. The kernel's
chain builds its policies' books itself and checks them through a
crate-private entry (`check_reaction`) that applies the same checks plus
the reaction rules and derives the command id from the trigger. annals
needs no authentication for this: no one outside the process can call it.

## Step 2: identification

```rust
pub struct Sampled { pub now: Timestamp, pub command_id: Uuid }   // the application's v7

impl Identified {
    pub fn command_id(&self) -> &Uuid;            // never absent
    pub fn correlation_id(&self) -> &Uuid;        // never absent
    pub fn causation_id(&self) -> Option<&EventRef>; // the command's (CommandPage), never its events'
    pub fn meta(&self, recorded_at: Timestamp, rules_version: &str) -> EventMeta;
    pub fn to_book(&self) -> CommandBook;         // ids filled: for a dead letter or an audit line
    // and the Checked accessors
}
```

| Kind | `command_id` | `correlation_id` | `causation_id` |
|---|---|---|---|
| a client's command | as sent, else `Sampled::command_id` (a UUIDv7 the application minted) | as sent, else the command id | none |
| a fact | `ids::fact_command_id(external_id)` (UUIDv5; a sent one must equal it) | as sent, else the same value (`ids::fact_correlation`) | none |
| a policy's command | `ids::reaction_command_id(trigger, component, index)` (UUIDv5) | the trigger's | the trigger's `EventRef` |

`component` is `Reaction::NAME`; `index` is the order's place among those
the trigger set off for that policy. The `causation_id` column is the
command's own (`CommandPage.causation_id`); `Identified::meta` stamps the
events with `causation.command_id` only. Identification is infallible: the
check refused everything that could make it fail. Text forms of ids exist
only at display and input (`Display`/`FromStr` on `v1::Uuid`).

## Step 3: deduplication

**By `command_id`, on the events.** There is no key store, no claim, no
fingerprint and no commands table. "Already applied?" means "the addressed
stream holds events whose `EventMeta.command_id` is this one":
`EventStore::find_command(stream, command_id)`, over a non-unique index on
(stream, command_id). On a hit the answer is `Handled::Duplicate` with those
events, in sequence order (angzarr's DUPLICATE), and nothing below runs.
Because the id is written with the events, the record of having applied a
command is atomic with its effects, with nothing else to keep consistent.
(KurrentDB deduplicates appends by event id the same way, and Equinox keeps
processed ids in the stream.)

**Accepted costs** (angzarr's decision 12.2, the same in both runtimes):

- **A decision that produced no events leaves nothing to find**, so a
  redelivery of it is decided again, against the stream as it is then
  (Equinox-style), and answered APPLIED. Pinned by
  `a_zero_event_decision_is_decided_again`.
- **A command id reused for a different command goes undetected**: it is
  answered DUPLICATE with the first command's events. Payloads are not
  fingerprinted; a client mints a new id for every new command.
- **Deduplication is forever**: events are permanent, so there is no
  expiry, purge or retention window.

Rejections leave nothing, so a rejected command's retry runs again. Within
a chain, each policy command takes step 3 too (cheap, and uniform);
the chain's staged events count, so a chain never applies one command
twice.

**Races.** Two deliveries of one command race on the append's sequence
check: one commits; the other's commit fails `StoreError::Conflict`
(`Recovery::Retry`). Run again with the same command id in a fresh unit of
work, it starts at step 3 and finds the winner's events.

## Step 4: load and conflict resolution

```rust
pub struct Coordinator<'u, U> { /* the unit of work, the registry, the nonce counter */ }
impl<'u, U: EventStore> Coordinator<'u, U> {
    pub fn new(unit: &'u mut U, registry: &'u Registry) -> Self;
    pub fn deduplicate(&mut self, command: Identified) -> Result<Deduplicated, StoreError>;
    pub fn load(&mut self, command: Novel) -> Result<Contextual, KernelError>;
}
impl Contextual {                       // angzarr's ContextualCommand, in process
    pub fn conflict_resolution(&self) -> ConflictResolution;   // never UNSPECIFIED
    pub fn expected_sequence(&self) -> u64;                    // the book's, or the head
    pub fn head(&self) -> u64;
    pub fn snapshot(&self) -> Option<&Snapshot>;
    pub fn pages(&self) -> &[EventPage];
    pub fn window(&self) -> &[EventPage];                      // expected .. head
    pub fn has_nonce(&self) -> bool;
    pub fn decode<D: Domain>(self, registry: &Registry) -> Result<Prepared<D>, KernelError>;
}
```

The **in-process coordinator** is a struct over the application's unit of
work, domain-blind like angzarr's: it sees covers, pages and the registry.
There is no service and nothing is sent.

**The resolution** (`Registry::resolve`, angzarr's precedence, highest
first): the book's `Handling.conflict_resolution`; the domain's
configuration for the command's type (`Domain::CONFLICT_RESOLUTION_BY_COMMAND`,
by full type name); the domain's own (`Domain::CONFLICT_RESOLUTION`); the
built-in default, **COMMUTATIVE where the state declares conflict units,
otherwise STRICT**. Configuration a state does not support is refused at
registration (`Misconfigured::UnsupportedResolution`); a book's request, at
step 1.

**The load.** The newest snapshot, if usable (its state's type URL is the
registered state's and its rules version the current one; otherwise it is
discarded and the stream replayed from 0, never migrated), then the events
after it to the head. Under AGGREGATE_HANDLES and COMMUTATIVE the snapshot
used is **at or below expected−1**, so no part of the window is hidden in
snapshot state; a newer one is not used.

**The expected sequence.** Absent: stamped with the head, and the command
can never be stale. Present:

- **STRICT**: it must equal the head, else `KernelError::Stale` (angzarr's
  ABORTED), before anything is decoded.
- **AGGREGATE_HANDLES**: the events from it to the head are the window;
  decide receives them with a `Nonce` and must give the nonce back
  (`Tx::resolve_conflicts`), or the command fails as stale
  (`KernelError::Unresolved`).
- **COMMUTATIVE**: the same window and nonce; the kernel itself resolves
  (below).
- Past the head: stale under every resolution.

**COMMUTATIVE.** decide runs on the head, so every invariant it checks
holds against the window's effects. Then `window = changed(state at
expected, head)` and `mine = changed(head, head with the command's
events)`, both by the state's generated `State::changed`; the events are
kept iff the two do not overlap (`Units::overlaps`: equal units or a prefix
at a `.`/`[` boundary; a wildcard overlaps any non-empty set; an empty set
nothing), else the command is stale. Unmarked fields conflict with
everything (the fail-safe wildcard), so an unmarked change only makes
COMMUTATIVE stricter.

**The `State` trait** is what angzarr's generator emits for a state message
from its `(io.angzarr.v1.state)` option and `(io.angzarr.v1.conflict)` field
markers:

```rust
pub trait State: prost::Message + prost::Name + Default + Clone + Send + 'static {
    fn descriptor() -> StateDescriptor;     // type URL + the full resolved list of resolutions
    fn changed(before: &Self, after: &Self) -> Units { Units::everything() }
}
```

`Registry::register::<D>` fills its entry from `D::State::descriptor()`, as
angzarr's `registry.register::<D>()` fills `ComponentDescriptor.state`.
Until angzarr's generator exists (backlog CR-1), a hand-written impl stands
in, in test fixtures only (`tests/next/toys.rs`).

**The risk of COMMUTATIVE.** No structural diff sees a value derived from,
or meaningful only together with, another unit: a total over items, two
fields that must change together. A stale caller's intent can then merge
when it should not. That is a modelling failure: put dependent data in one
unit (mark the containing message without `descend`), derive the value in
decide or a projection rather than storing it, or configure that command
type STRICT. angzarr's `docs/guide/state-and-conflicts.md` shows the
failures and their fixes; it applies to annals unchanged.

## Step 5: decode, then decide or ingest

```rust
pub trait Domain: Sized + 'static {
    type State: State;
    type Command: Payload + Send + 'static;
    type Fact: Payload + Send + 'static;           // Infallible: none
    type Event: Payload + Clone + Send + Sync + 'static;
    type Rejection: Diagnostic + Send + 'static;
    type Rules: Send + Sync + 'static;
    const NAME: &'static str;                      // the Cover.domain; the registry key
    const CONFLICT_RESOLUTION: Option<ConflictResolution> = None;
    const CONFLICT_RESOLUTION_BY_COMMAND: &'static [(&'static str, ConflictResolution)] = &[];
    fn decide(cmd: &Self::Command, tx: &mut Tx<'_, Self>) -> Result<(), Self::Rejection>;
    fn ingest(state: &Self::State, fact: &Self::Fact, ctx: &IngestCtx<'_, Self>) -> Vec<Self::Event>;
    fn apply(state: &mut Self::State, event: &Self::Event, rules: &Self::Rules);
}
```

**Decode.** The snapshot's state, every event and the command's payload go
through the domain's `Upcaster` by type URL, then through `Payload::from_any`.
Strict: an event type nothing upcasts or decodes is
`StoreError::Unreadable` naming its type URL (never skipped); a command or
fact type is `KernelError::UnknownType` naming its type URL (angzarr's
INVALID_ARGUMENT). Golden replay fixtures (`ReplayFixture`,
`assert_replay_fixtures`) and `Registry::check` catch a missing upcaster in
CI first.

**Commands may be upcast**, through the same chain (`Upcaster::upcast_command`).
**Discouraged**: it hides stale clients (they keep working, so nobody
upgrades them) and is machinery for data that is never stored. Prefer an
additive change, or decoding both versions in the domain for a transition;
upcast a command only as a migration aid with a planned removal. Event
upcasters are permanent.

**Decide** is pure, in a staging `Tx`: it reads `state()`, `rules()`,
`rules_version()`, `stream()`, `recorded_at()`, `command_id()`,
`correlation_id()`, `causation_id()`, `conflicts()` (AGGREGATE_HANDLES
only), emits through `emit`, and its events are kept only if it returns
`Ok`. It may reject at any point; nothing staged is kept. No `Ctx`: random
values are sampled by the caller into the command.

**Ingest** is infallible by type: a fact already happened, so an awkward
one is recorded (a payment for an unknown order becomes an event that says
so), never refused. It reads a **read-only context**:

```rust
impl IngestCtx<'_, D> {
    pub fn stream(&self) -> &Cover;            pub fn recorded_at(&self) -> Timestamp;
    pub fn command_id(&self) -> &Uuid;         pub fn correlation_id(&self) -> &Uuid;
    pub fn source(&self) -> &FactOrigin;       // external_id, description
    pub fn rules(&self) -> &D::Rules;          pub fn rules_version(&self) -> &str;
}
```

A fact is never checked against a sequence nor conflict-resolved: it is
always stamped at the head. A panicking ingest is a bug: the kernel catches
it, fails the chain (`KernelError::IngestPanicked`, `Recovery::Bug`),
writes nothing and logs a [dead letter](#dead-letters-a-logger). On a
`panic = "abort"` target the process aborts instead; nothing is written
either way.

**Business output is payloads only.** The kernel builds the pages: their
sequences and their `EventMeta` (`Identified::meta`).

## Metadata

Every event carries angzarr's `EventMeta`, the same in both runtimes:

| Field | annals stamps |
|---|---|
| `causation` | the event's direct cause, exactly one of: `command_id`, the producing command's (step 2: a client's, a fact's or a policy's), its deduplication key; or `triggering_event`, for a process event a process manager wrote in reaction to another event with no command between (see [Process managers](#process-managers)) |
| `fact` | the fact's `FactOrigin`, for a fact; unset otherwise |
| `correlation_id` | the producing command's; never unset |
| `recorded_at` | `max(Sampled::now, the stream's last recorded_at)`: non-decreasing per stream, one instant per chain, so as-of-time reads are prefixes |
| `rules_version` | the registered rulebook's current version |

**No trigger on a reaction's events**: a policy's command names its trigger
(`CommandPage.causation_id`), its events name the command. The edge from
trigger to command goes to tracing and logs, through the application's
[`TriggerLog`](#dead-letters-a-logger). Readers find a reaction's events by
correlation and group them by `command_id`.

**No actor**, anywhere: who did it belongs in the deployment's separate
security log, written by the application with the command id, so erasing
or rotating personal data never touches an event. **No base sequence**: the
events of one command are grouped by `command_id`, and the version decided
against is the first one's sequence. Business time (a deadline, a draw's
instant) belongs in the payload, where `apply` sees it; decide copies
`tx.recorded_at()` there when the domain needs it.

A decoded event is an `Envelope<E>`: a checked `EventPage`, its metadata's
required fields outside their `Option`s (`causation()`, the generated
`v1::event_meta::Causation`; `correlation_id()`, `recorded_at()`,
`rules_version()`; `command_id()` and `triggering_event()` are the two arms
of `causation()`), its payload decoded. A stored page
whose meta lacks one is `ReplayError::IncompleteMeta`.

## Process managers

annals has no process manager today: its reactions are stateless policies
whose only output is commands. The metadata is ready for one, and this is
how one would fit the chain:

- A process manager is a domain with its own stream (per correlation, or a
  root it derives), reacting to source events. For each trigger it **writes
  its own events** (its state: what it has seen and decided) and may
  **issue commands**.
- Its own events are produced by no command: they carry
  `causation.triggering_event` = the trigger's `EventRef` and the
  **trigger's correlation**, never a `command_id`. They are deduplicated
  on the trigger within the process manager's stream
  (`EventStore::find_triggered`, over an index on (stream, triggering
  event)), so a redelivered trigger writes nothing twice.
- Its commands are reaction commands like a policy's: checked through the
  reaction entry, identified from the trigger (`reaction_command_id` with
  the process manager's name), with the command's `causation_id` = the
  trigger; their events name the command.
- In the chain it is one more route kind: staged and committed with
  everything else, its rejection-free event write followed by its commands
  taking steps 3 to 5 in their targets, depth first, under the same depth
  limit.

Nothing of this is implemented beyond `Envelope`'s `causation()`
accessors and the defaulted `find_triggered` (which finds nothing: no
process manager writes yet).

## Rules versions

A rules version is a short label the domain's author chooses (`"v3"`) and
bumps whenever decide, ingest or apply semantics change. The domain
registers a `Rulebook` (the current version and every older one its logs
name); the current version is stamped on every event. In annals it is
**load-bearing**: replay folds each event with the rules its version names
(`ReplayError::UnknownRules` if the rulebook lacks it), and a snapshot
folded under another version is discarded. A rules change therefore never
needs a snapshot migration.

## Reactions and the chain

```rust
pub trait Reaction {
    type Source: Domain; type Target: Domain; type TargetId: Ord + Clone;
    const NAME: &'static str;           // the component its command ids derive from
    fn targets(source: &<Self::Source as Domain>::State) -> Vec<Self::TargetId>;
    fn root(target: &Self::TargetId) -> Uuid;   // covered in the source's edition
    fn position(target: &<Self::Target as Domain>::State) -> Position;
}
pub trait Policy: Reaction {
    fn command(target: &Self::TargetId, c: &Committed<'_, Self::Source>) -> <Self::Target as Domain>::Command;
}
pub trait Projection<D: Domain> { fn project(&mut self, c: &Committed<'_, D>); }

let routes = Routes::new().policy::<ListToDrawSystems>();   // built once, no store type
let kernel = Kernel::new(&registry, &dead_letters);          // .limit(n), default 8; .triggers(&log)
kernel.handle::<List, _, _>(unit, &routes, book, &sampled, &mut projections)?;   // -> Handled
```

- **Reactions emit commands only.** Facts come from outside the system, so
  there is no reaction-injected fact and no `Translation`. A policy's
  command is a `CommandBook` the kernel builds (the target's cover, the
  command packed as `Any`, no ids, no handling), checked and identified
  from its trigger, then taken through steps 3 to 5 in its target.
- **The atomic chain.** Every policy routed for a source domain runs on the
  staged events, depth first; a later step sees the staged state (and
  command ids) of any stream an earlier step touched. Nothing is appended
  until the whole chain has succeeded; then every staged stream is
  appended, each against the length it was loaded at, and the unit of work
  committed: one transaction. **A reaction's rejection fails the origin**
  (`Failure::Downstream { path, error }`, the path a list of
  `Step { stream, cause }`). Projections run on committed events only;
  their failures are reported (`Recorded::projector_failures`), never
  raised.
- **`Routes` names no store type**: each entry owes and checks a policy's
  orders, purely, and the chain reaches the target's typed step through the
  registry by the target's domain name. So routes are built once and serve
  the sync and the async handler alike.
- **Depth limit** (`Kernel::limit`, default 8): `KernelError::ChainTooDeep`,
  the cycle check for a chain that may legitimately revisit a stream.
- **The contiguity invariant**: once a target appears in `targets()`, it
  stays for every source event up to the one that retires it, and never
  comes back; `orders_owed` (one fold of the source for all targets, for a
  catch-up outside a chain, delivered by `Kernel::deliver`) checks it as
  `KernelError::TargetReturned`.
- **The position contract**: `Position` (the last sequence handled) places
  a delivery as next, redelivery or gap; a target's position is a fold of
  its own stream (the source sequence travels in the command's payload,
  because apply never sees metadata). `Tracked<H>` keeps one per cover.
  Within a chain neither a redelivery nor a gap can happen.
- **Reactor re-runs always deduplicate** (see
  [below](#reactor-re-runs)).
- **Propagating one aggregate into another** (hatpick's list into its draw
  systems) is a policy whose target records the change in its own events.
  A command to a retired or never-created target means the contiguity
  invariant was broken: decide refuses it, and the list edit fails whole,
  loudly. So retiring a draw system goes through the list.
- **Read models are replayed on read** (`Stream::dispatch` from 0); none is
  persisted.

### Reactor re-runs

There is no switch (angzarr decision 13.4). A reaction's command id is
UUIDv5(trigger, `Reaction::NAME`, index) and names nothing else: not a
rules version, not the policy's code. So:

- **A re-run deduplicates.** Re-delivering a trigger (a catch-up after a
  reset, `orders_owed` then `Kernel::deliver`) re-derives the same ids, and
  every target answers DUPLICATE with the events the first run wrote, even
  if the policy would now decide differently (the "reused id with another
  payload" cost, applied to reactions).
- **Repairs are new commands.** To fix what a buggy policy did, issue new
  commands, with new ids, that make the corrections; their events append
  after the buggy-era ones. **History is never rewritten.** angzarr plans
  standard re-drive commands (its backlog RD-1); annals would adopt them
  as ordinary commands.
- **Inside a chain** a reaction runs exactly once per trigger, atomically
  with it, so the question does not arise there.

## The store port

```rust
pub trait EventStore {
    fn append(&mut self, stream: &Cover, expected: u64, pages: &[EventPage]) -> Result<u64, StoreError>;
    fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError>;
    fn read(&mut self, stream: &Cover) -> Result<Option<Slice>, StoreError>;      // from 0
    fn find_command(&mut self, stream: &Cover, command_id: &Uuid) -> Result<Vec<EventPage>, StoreError>;
    fn find_triggered(&mut self, stream: &Cover, trigger: &EventRef) -> Result<Vec<EventPage>, StoreError>; // PMs; none by default
    fn read_snapshot(&mut self, stream: &Cover) -> Result<Option<Snapshot>, StoreError>;  // none by default
    fn write_snapshot(&mut self, stream: &Cover, snapshot: &Snapshot) -> Result<(), StoreError>;
}
pub trait UnitOfWork: EventStore { fn commit(self) -> Result<(), StoreError>; }
pub struct Slice { pub stream: Cover, pub next_sequence: u64, pub last_recorded_at: Option<Timestamp>, pub pages: Vec<EventPage> }
// AsyncEventStore / AsyncUnitOfWork: the same, Send futures; Ready<S> serves a sync store async.
```

**Opaque.** A store holds angzarr's pages as written and never decodes a
payload; upcasting is the kernel's, on every read. The Postgres shape:

| Column | From |
|---|---|
| `domain`, `edition`, `root` | the cover; `edition` NULL for main; `root` a `uuid` |
| `sequence` | `EventPage.sequence` (`bigint`; u64 in range) |
| `type_url` | the payload's type URL as written, before any upcast |
| `page` | the `EventPage` bytes as written |
| `command_id` | `EventMeta.causation.command_id`, `uuid`; NULL for a process manager's process event |
| `trigger_domain`, `trigger_edition`, `trigger_root`, `trigger_sequence` | `EventMeta.causation.triggering_event`; NULL unless a process manager wrote it |
| `correlation_id` | `EventMeta`'s, `uuid` |
| `external_id` | `EventMeta.fact.external_id`; NULL unless a fact produced it |
| `recorded_at`, `rules_version` | the rest of `EventMeta` |

Unique on (`domain`, `edition`, `root`, `sequence`); a **non-unique index on
(`domain`, `edition`, `root`, `command_id`)** for `find_command`, and one
on the stream and the triggering event for `find_triggered`. The columns
and the page's `EventMeta` hold the same metadata; the page is what an
angzarr reader is served. A store answers the stream, `None` (no such
stream), a refusal it is sure of (`Conflict`, `Discontinuous`) or a failure
(`Unavailable`, `Unreadable`); an I/O failure is never "no such stream".
Snapshots live beside the events, the newest per stream. The async port's
futures are `Send`; a single-threaded adapter implements the sync port.

## Snapshots

A snapshot is angzarr's `Snapshot { sequence, state: Any, rules_version }`,
a framework optimisation, never business data. `Stream::snapshot` makes one
under the current rules version; the application writes it
(`write_snapshot`) when it likes. On load the coordinator uses it only if
`Registry::snapshot_usable` (the state's type URL and the rules version
match the registration) and, under AGGREGATE_HANDLES and COMMUTATIVE, its
sequence is at or below expected−1. Otherwise it is discarded and the
stream replayed from 0. The floor for the next `recorded_at` comes from the
store (`Slice::last_recorded_at`), not the snapshot.

## Dead letters: a logger

angzarr dead-letters what it cannot deliver and cannot refuse to a caller.
annals is in process and atomic: there is no queue, no store and no replay.
Whatever angzarr would dead-letter fails the chain back to its caller with
nothing written, and the kernel writes one line to the application's
`DeadLetterLog`:

```rust
pub const DEAD_LETTER_PREFIX: &str =
    "DEAD LETTER (almost certainly a bug; nothing was written and nothing will retry):";
pub trait DeadLetterLog: Send + Sync { fn write(&self, line: &str); }
pub fn dead_letter_line(letter: &AngzarrDeadLetter) -> String;   // prefix, component, reason, command
```

Cases: a panicking ingest; a policy's book the kernel built malformed; a
decide under AGGREGATE_HANDLES that never acknowledged its window. **Every
line starts with the prefix**: anything arriving there almost certainly
indicates a problem, so an operator alerts on it. The line is built from
angzarr's own `AngzarrDeadLetter`, so it says what an angzarr DLQ entry
would. annals never answers PARKED.

**Trigger edges go beside it.** No event records the edge from a trigger
to the reaction command it set off (angzarr decision 13.3; angzarr sends it
to OpenTelemetry, its backlog PS-6). annals has no tracing dependency, so
the chain hands the application each reaction command's (`causation_id`,
`command_id`, target cover) as it dispatches it, and the application logs
or spans it:

```rust
pub trait TriggerLog: Send + Sync {
    fn dispatched(&self, causation_id: &EventRef, command_id: &Uuid, target: &Cover) {}  // no-op
}
let kernel = Kernel::new(&registry, &dead_letters).triggers(&log);   // default: NoTriggerLog
```

It is called before the command is deduplicated or decided, so a chain
that fails later has still reported the edges it dispatched.

## Errors and recovery

| Error | Meaning | `Recovery` |
|---|---|---|
| `Failure::Rejected(R)` | the domain refused the command | `Business` |
| `Failure::Downstream { path, error }` | a step down the chain failed; nothing appended anywhere | by `StepError`: a rejection `Business`, a malformed policy book `Bug`, else by `KernelError` |
| `KernelError::Malformed` | step 1 refused the book | `Invalid` |
| `KernelError::UnknownType` | a command or fact type the domain does not decode, after upcasting | `Invalid` |
| `KernelError::Stale`, `Unresolved`, `Gap` | re-read and re-issue; catch up | `Retry` |
| `StoreError::Conflict`, `Unavailable` | run again with the same command id (step 3 makes it safe) | `Retry` |
| `IngestPanicked`, `Upcast`, `WrongDomain`, `ChainTooDeep`, `TargetReturned`, `BeforeSnapshot`, `Replay(..)`, `StoreError::Discontinuous`, `Unreadable` | a bug or a misconfiguration | `Bug` |

A duplicate is not an error: it is `Handled::Duplicate`. Each error becomes
a `Diagnostic` with the implementation (codes under `annals-next-*`,
messages in `locales/en-US/annals.ftl`).

## Mapping: identity

There is no mapping left. What annals means by each concept is the proto
type itself:

| Concept | Type |
|---|---|
| stream identity | `v1::Cover { domain, root: Uuid, optional edition }`; main is the unset edition |
| an event's identity; a reaction command's causation; a process event's trigger | `v1::EventRef { stream: Cover, sequence }` |
| a command or a fact | `v1::CommandBook { cover, page: CommandPage { body: Command \| Fact, command_id, correlation_id, causation_id, fact_origin }, handling }` |
| how to handle it | `v1::Handling { optional expected_sequence, conflict_resolution }` |
| a conflict resolution | `v1::ConflictResolution` |
| a state's capabilities | `v1::StateDescriptor { type_url, StateOptions { conflict_resolutions } }` |
| an event, stored | `v1::EventPage { sequence, meta: EventMeta, payload: Any }` |
| an event's metadata | `v1::EventMeta { oneof causation { command_id, triggering_event }, fact, correlation_id, recorded_at, rules_version }` |
| an event's direct cause | `v1::event_meta::Causation::{CommandId(Uuid), TriggeringEvent(EventRef)}` (`Envelope::causation()`) |
| every id | `v1::Uuid` (16 bytes); text through `ids` at the boundary |
| derived ids | `ids::{compute_root, reaction_command_id, fact_command_id, fact_correlation}` |
| a snapshot | `v1::Snapshot { sequence, state: Any, rules_version }` |
| the answer | `Handled::outcome() -> v1::Outcome { delivery: APPLIED \| DUPLICATE, projector_failures }`; `Handled::books() -> Vec<EventBook>` |
| a dead letter | `v1::AngzarrDeadLetter`, as a log line |
| a payload's version | its type URL (`pkg.v1.Foo` then `pkg.v2.Foo`) |

annals' own types are the ones angzarr has no message for, because they
are execution, not wire: `Checked`, `Identified`, `Contextual`, `Prepared`,
`Envelope` (a checked, decoded page), `Stream`, `Tx`, `IngestCtx`, `Nonce`,
`Rulebook`, `Registry`, `Routes`, `Chain`, `Kernel`.

## Key signatures

```rust
// step 1-2
pub fn check(book: CommandBook, registry: &Registry) -> Result<Checked, Malformed>;
impl Checked { pub fn identify(self, sampled: &Sampled) -> Identified; }
// step 3-5
impl<U: EventStore> Coordinator<'_, U> {
    pub fn deduplicate(&mut self, command: Identified) -> Result<Deduplicated, StoreError>;
    pub fn load(&mut self, command: Novel) -> Result<Contextual, KernelError>;
}
impl Contextual { pub fn decode<D: Domain>(self, registry: &Registry) -> Result<Prepared<D>, KernelError>; }
impl<D: Domain> Prepared<D> {
    pub fn decide(&mut self, sampled: &Sampled, registry: &Registry, dead_letters: &dyn DeadLetterLog)
        -> Result<Decision<D>, Failure<D::Rejection>>;
}
// the whole loop
impl Kernel<'_> {
    pub fn handle<D: Domain, U: UnitOfWork, Ps: Project>(&self, unit: U, routes: &Routes,
        book: CommandBook, sampled: &Sampled, projections: &mut Ps) -> Result<Handled, Failure<D::Rejection>>;
    pub async fn handle_async<D: Domain, U: AsyncUnitOfWork, Ps: Project>(/* the same */);
    pub fn deliver<P: Policy, U: UnitOfWork, Ps: Project>(&self, unit: U, routes: &Routes,
        order: &Order<P>, sampled: &Sampled, projections: &mut Ps) -> Result<Handled, Failure<..>>;
}
// in memory, no store
impl<D: Domain> Stream<D> {
    pub fn execute(&mut self, book: CommandBook, registry: &Registry, sampled: &Sampled,
        dead_letters: &dyn DeadLetterLog, handlers: &mut [&mut dyn Handler<D>])
        -> Result<Executed<'_, D::Event>, Failure<D::Rejection>>;
    pub fn replay(cover: Cover, pages: &[EventPage], registry: &Registry) -> Result<Stream<D>, KernelError>;
}
// registration
impl Registry {
    pub fn register<D: Domain>(&mut self, rules: Rulebook<D::Rules>, upcaster: Upcaster) -> Result<(), Misconfigured>;
    pub fn resolve(&self, domain: &str, command_type_url: &str, requested: Option<ConflictResolution>)
        -> Option<ConflictResolution>;
    pub fn snapshot_usable(&self, domain: &str, snapshot: &Snapshot) -> bool;
}
```

## Divergences from angzarr

annals is **not** angzarr; the wire meaning is shared, the execution model
is not. This list matches angzarr's `docs/design/audit-meta/annals-divergence.md`.

1. **The atomic in-process chain (intentional).** A command, its events and
   every policy reaction are staged across streams and committed in one
   transaction; **a reaction's rejection fails the origin**. angzarr appends
   one stream per write and runs reactions asynchronously. annals keeps the
   chain because it makes in-process Rust easy: one transaction, no bus, no
   outbox, no retry loop. (Multi-stream units of work are standard
   elsewhere: KurrentDB `MultiStreamAppend`, Marten, Wolverine.)
2. **No transient outbox.** Reactions commit with their trigger; nothing is
   re-sent. Their command ids are still the derived ones, so the audit
   record is identical.
3. **Causation is never accepted from outside**, because nothing outside
   the process can call annals; angzarr needs mTLS-identified framework
   components for the same guarantee. (This is the command's
   `causation_id`; it is never persisted on events, in either runtime.)
4. **The dead-letter queue is a logger**, every line prefixed with its
   warning; there is no PARKED and no replay.
5. **`rules_version` is load-bearing**: replay resolves rules per event, and
   snapshot validity is checked by the domain-blind coordinator through the
   registry (in angzarr, by the client library).
6. **COMMUTATIVE and AGGREGATE_HANDLES resolve in the kernel**, which is
   both coordinator and client library; the nonce is an in-process token,
   not bytes on a `ContextualCommand`.
7. **Configuration lives on the domain** (`CONFLICT_RESOLUTION`,
   `CONFLICT_RESOLUTION_BY_COMMAND`), checked at registration; there is no
   configuration that forbids a `Handling` override.
8. **No edition registry or branching.** Any non-empty edition name is
   accepted without `CreateEdition`; its streams start empty, with no
   read-through to main.
9. **`recorded_at` is sampled by the application** once per chain, then
   raised per stream; same guarantee as angzarr's stamp at persist.

Not divergences (pass 13): `EventMeta` is the same in both runtimes
(`causation`, the event's direct cause; no trigger on a reaction's events,
no actor), and reactor re-runs deduplicate in both, with no switch.

## hatpick changes

**hatpick-events** (pinned to an annals rev; nothing breaks until it moves):

- Events, commands and facts are prost messages with `prost::Name` under
  versioned packages (`hatpick.list.v1`, `hatpick.draw_system.v1`), one
  message per kind; each domain's enums implement `Payload` with a
  `Decoder`. Each domain registers an `Upcaster` (empty to start) and a
  `Rulebook::new("v1", ())` in one `Registry`.
- Each domain gains `type Fact` (`Infallible` today) and `ingest`.
- **`Stamp` goes, and with it every actor.** Who did what is written by the
  server to its security log, keyed by the command id; events record no
  person. `Stamp.at` becomes `EventMeta.recorded_at`, copied into a payload
  (`tx.recorded_at()`) only where a fold needs business time.
- **`ListState` becomes a prost message** with `option (io.angzarr.v1.state)
  = { conflict_resolutions: [CONFLICT_RESOLUTION_COMMUTATIVE] }` and
  `(io.angzarr.v1.conflict)` markers on every field (items per key, columns
  whole or keyed and ordered, settings whole), so content edits default to
  COMMUTATIVE. Until angzarr's generator exists, its `State` impl is written
  by hand, against the same markers, with a test that names it a stand-in.
  Draws are STRICT, via `CONFLICT_RESOLUTION_BY_COMMAND` (or the draw-system
  state lists no COMMUTATIVE). Every map field is a `btree_map`, so encoding
  is deterministic.
- `saga.rs`'s `ListToDrawSystems` becomes a **`Policy`**; `Instantiate` and
  `Inject` become the commands `Instantiate { source_seq, list }` and
  `ApplyListChange { source_seq, change }`, recorded by the draw system as
  its own events; `DrawSystemRejection::Gap` disappears (the kernel's `Gap`);
  `Retired` and `NotInstantiated` stay, as bugs. Draw systems are created
  and retired through list events, keeping the contiguity invariant.
- **The draw lifecycle is events in the draw-system stream**: `Draw {
  random }` (sampled by the API handler) → `Drawn`; `Accepted`, `Skipped`,
  `Expired`. History and stats replay on read.
- Roots keyed by a business key use `ids::compute_root(domain, key)`.

**hatpick-server**:

- One Postgres events table in the [store shape](#the-store-port) with its
  (stream, command_id) index, behind a diesel-async adapter implementing
  `AsyncEventStore` and `AsyncUnitOfWork` over one transaction. **No
  commands table.**
- Per request: build the `CommandBook` (the cover, the command packed with
  `Payload::to_any`, the client's `command_id` if it sent one, parsed at the
  boundary with `Uuid::from_str` and refused 400 otherwise, its correlation
  if any, `Handling` from `If-Match`-style headers if any); sample the
  instant and mint a UUIDv7 (`uuid` with `v7`, in the server only) into
  `Sampled { now, command_id }`; call `kernel.handle_async::<D, _, _>`.
  `Handled::Duplicate` is answered as the original response. A client
  sending several commands gives each its own command id and one shared
  correlation.
- `Malformed` and `UnknownType` (`Recovery::Invalid`) are 400s; `Stale` and
  `Unresolved` 409/412; `Rejected` 422; `Retry` store failures are retried
  once with the same book; `Bug` is a 500 and is already in the
  dead-letter log, which the server routes to `tracing` at error level and
  alerts on by its prefix.
- `Routes`, the `Registry` and the `Kernel` are built once at start-up.

**annals-sim.** `schedule::advance(tx: &mut Tx<D>, ..)` and `Clock::run`
move to `next::Tx` unchanged in shape. Its test domain's state becomes a
prost message with a `State` impl, and gains `type Fact = Infallible`.

**annals-log.** Keeps `annals.log.v1` for the root API; the kernel's wire
form is angzarr's.

## Open questions, each with a recommendation

1. **Re-drive commands.** Repairs are new commands; angzarr may ship
   standard ones (its backlog RD-1, undecided). I recommend annals adopt
   whatever angzarr ships as ordinary commands, and hand-build any it needs
   before then.
2. **Snapshot validity in the coordinator**, not the typed step. The
   registry holds the state's type URL and the rules version, so the
   domain-blind coordinator can discard early and load from 0 in one read.
   I recommend keeping it there (divergence 5).
3. **Dedup for policy commands inside a chain.** Uniform with the origin,
   cheap with the index, and it makes a chain that revisits a stream
   idempotent by construction. I recommend keeping it.
4. **Configuration on the domain as consts** (by command full name),
   checked at registration, rather than registration options. The compiler
   keeps it beside the types it names. I recommend consts; revisit if
   hatpick needs per-deployment configuration.
5. **`Envelope` holds the checked metadata fields**, not the `EventMeta`,
   so its accessors need no unwrap (`meta()` rebuilds the message). I
   recommend it: it is the read-side twin of `Checked`.
6. **A panicking ingest on `panic = "abort"` targets** (wasm32 by default)
   aborts the process instead of failing the chain. Nothing is written
   either way. I recommend documenting it and not adding unwind
   requirements.
7. **`Recovery::Invalid`** is new: a malformed book or an unknown command
   type is the caller's to fix, not a bug. I recommend keeping it distinct
   from `Bug` so the server can answer 400 without alerting.
8. **`Stream::execute`** runs the whole pipeline in memory with no store.
   I recommend keeping it, for tests and in-browser use.
9. **The upcaster: a mirror or the router's.** I recommend the mirror, and
    switching when angzarr remaps the router onto angzarr-proto.
10. **Old `annals.log.v1` logs.** I recommend leaving them: hatpick stores
    none.
11. **The default depth limit of 8.** I recommend 8; hatpick's deepest
    chain is two levels.

Decided: every annals user builds angzarr-proto (no feature gate; the
workspace `CLAUDE.md` says so); there is no re-run switch; trigger edges go
to the application's `TriggerLog`, beside the dead-letter log.

## Not now

| Deferred | Why | What revives it |
|---|---|---|
| Hash chaining | Not asked for. Kept possible: immutable pages, upcast on read, deterministic meta, no personal data. | A tamper-evidence requirement. |
| Generated `State` impls, and angzarr's golden conflict-unit parity suite run here | The generator is angzarr's backlog CR-1; until then a hand-written fixture stands in. | CR-1 landing: annals generates from the same markers and runs every fixture pair through `Units::overlaps`. |
| Snapshot frequency and retention configuration | `write_snapshot` is the application's; the load and discard rules are specified. | Replay latency that matters. |
| Persisted projections | Replay on read. | Read latency; `Tracked` is ready. |
| Edition registry, branching, read-through | hatpick uses only main. | A what-if feature in hatpick. |
| Process managers (stateful reactors writing their own events) | hatpick needs none; the metadata (`causation.triggering_event`) and the store lookup (`find_triggered`) are ready. | A workflow that must remember across triggers. |
| Configuration that forbids a `Handling` override | No hatpick command needs it. | A command type whose resolution a client must not choose. |
| A `!Send` async store port | No browser store exists. | A wasm adapter over IndexedDB. |
| Reading angzarr `EventBook`s into annals | annals writes books; nothing feeds it angzarr's. | An angzarr runtime delivering to annals. |

## The tests

`crates/annals/tests/next/`, one file per contract, over the toy domains
in `toys.rs`. Unit tests in `src/next/check.rs` pin what only the kernel
can reach: a policy's book is checked through the reaction entry (no ids,
no fact, no handling, a trigger that names an id) and identified from its
trigger, and a reaction's events carry `causation.command_id` and nothing
of its trigger, while its book keeps the trigger as `causation_id`.

- **parse.rs**: a checked command's accessors are non-optional where
  the book had to say something; a fact carries its source in its body; the
  sender's ids and handling are kept; an empty handling is unset; a fact
  may name its derived id and nothing else; COMMUTATIVE is accepted where
  the state declares units, AGGREGATE_HANDLES everywhere; **every malformed
  case is refused, the first malformation the answer** (no cover, unknown
  domain, no root, a 15-byte root, an empty edition, no page, body or
  payload, short ids, a causation from outside, a fact without its origin
  or with an empty external id or a non-derived id, a command with a fact
  origin, a fact with any handling, an unknown resolution, COMMUTATIVE on a
  state without conflict units).
- **identify.rs**: a client's command without ids gets the minted
  v7 as both ids; sent ids are kept, an absent correlation is the command
  id; a fact's ids are the UUIDv5 of its external id (types.proto's worked
  example); every event's `EventMeta` carries its command as causation and
  no actor; an
  identified book re-checks and re-identifies the same; `recorded_at` is
  raised, never lowered.
- **registry.rs**: registration fills the state descriptor;
  **COMMUTATIVE is the default where the state declares units, else STRICT**,
  under the precedence; misconfiguration is refused at registration; a
  configured resolution the state lacks is refused; a snapshot is usable
  only under the current rules and state type.
- **payload.rs**: payloads are `Any` packed by `prost::Name`; the
  type URL is the version; an old type is refused, never decoded as
  current.
- **dead_letter.rs**: a dead-letter line starts with the warning prefix
  and names the command; **a panicking ingest aborts the chain,
  writes nothing and logs a prefixed line**.
- **dedup.rs**: a retried command is DUPLICATE with its events, nothing
  re-stamped; **a zero-event decision is decided again**; a reused id with
  another payload goes undetected; dedup is per stream; a redelivered fact
  is found by its derived id; a rejection leaves nothing and its retry
  runs.
- **pipeline.rs**: **an applied STRICT retry is DUPLICATE, never stale**,
  while a new stale command is stale; a malformed retry of an applied
  command is still malformed; an unknown command type is refused at decode,
  after the sequence check and after dedup; the coordinator's steps take
  only the previous step's output, and a windowed command gets its window
  and a nonce.
- **conflict.rs**: COMMUTATIVE by default for a state with units (disjoint
  kept, overlapping stale); STRICT by default without units; a command type
  configured STRICT on a COMMUTATIVE state; an unmarked field conflicts with
  everything; AGGREGATE_HANDLES demands the nonce back; an expected sequence
  past the head is stale everywhere; unit-path syntax; the overlap rule.
- **snapshot.rs**: **a snapshot under another rules version is discarded**
  (a poisoned one shows when used); one of another state type is
  discarded; a snapshot never hides part of the window; a stream snapshots
  under the current version.
- **upcast.rs**: upcasting runs the chain as the router does; old events
  replay through the upcaster and are never rewritten; an unknown event
  type fails replay naming it; **an old command is upcast then decided**,
  and refused naming its type URL without an upcaster; an upcaster is
  checked whole (duplicates, rules out of order, dead ends; untyped rules
  not followed); a rule's own failure and unreadable bytes are reported.
- **ingest.rs**: **ingest reads a read-only context** (recorded instant,
  ids, source, rules version); a fact is never refused; a fact is stamped
  at the head.
- **chain.rs**: a policy's events carry the derived reaction id as their
  causation, the trigger's correlation, and no trigger; **a rejection downstream fails
  the whole chain, the origin included, with the path**, and its retry
  runs again; a chain deeper than its limit is refused; a chain in an
  edition stays in it; a book for another domain is refused; each reaction
  command's trigger edge goes to the `TriggerLog`; a chain sees its own
  staged events and streams; projections run after commit, a duplicate
  runs none; the async handler has the same contract and a `Send` future;
  a catch-up delivers each owed order once, in order, refusing a gap;
  a target that comes back breaks the backlog; a policy book for an
  unregistered target is a dead-lettered bug.
- **handlers.rs**: handlers receive the cover, sequence, meta and state
  after; a position places next, redelivery and gap; a tracked handler
  drops redeliveries and keeps the first gap until it advances.
- **store.rs**: append checks the expected length and continuity; a cover
  is (domain, edition, root); **`find_command` answers the dedup lookup from
  the events**, per stream; a unit of work lands at commit or not at all;
  the async port has the same contract; a unit of work checks and reads
  its pending appends; an empty append creates nothing; a store keeps the
  newest snapshot per stream.
- **replay.rs**: replay folds each event with the rules it names, and
  refuses a log that does not hold together (sequence, metadata, instant,
  rules, domain, a process event's trigger); a stream resumes from a
  snapshot and dispatches only after it; a decision commits only to its
  own stream and head; golden fixtures replay through the upcaster and
  name every failure; every failure says how to recover; errors render
  their English, and wrappers their cause.
