# simkit

Deterministic simulation parts on top of [`es`](../es). No dependencies
beyond `es`, no clock, and nothing specific to any game.

## Dice: `rng`

- `Rng` is SplitMix64: small, identical on every platform.
- **One stream per owner, never one shared stream.** With a shared stream,
  adding an entity shifts every other entity's rolls. `Rng::new(seed, owner)`
  keeps an entity's dice the same whatever else is on the field.
- **Rolls are re-derived, not remembered.** `Rng::for_use(seed, owner, use)`
  is a pure function of data the folded state already holds (how many times
  this owner has acted). So a domain's `Ctx` holds no generator position, and
  a stream cut anywhere between commands resumes with the same rolls.
- `derive(seed, salt)` gives a child seed for anything a roll spawns;
  `mix(&[u64])` folds any number of parts; `fnv1a` turns names into seeds.

## The clock: `schedule`

`Scheduler<Due>` lives **in the domain's folded state** and only `apply`
changes it: a "scheduled" event pushes, a "taken" event pops. Its sequence
counter is folded too, so the queue after any prefix of a log is exactly the
queue the live run had there — which is what lets a run resume from any cut.

Order is `Key { tick, phase, side, seq }`. The domain says what phases and
sides mean (impacts before reveals; which team wins a genuine tie); `seq`
keeps the rest first-scheduled, first-run.

A domain implements `Clock` (where its scheduler is, what its "taken" event
is, how to run a due thing) and calls `advance(tx, limit)` from its decide.
`advance` jumps straight to the next scheduled tick — empty ticks are never
visited — and runs everything due at it, including work scheduled for the
same tick while it runs, then returns `Ran { tick, taken }`, `Idle` or
`Beyond(tick)` so the domain can check its end condition once per instant.

`Ticked<K> { tick, kind }` wraps a domain's events — `type Event =
Ticked<FightEvent>` — so `apply` sees when each happened. Stored with eslog,
a ticked event message puts `uint64 tick = 1` first (see eslog's README).

## Jobs: `job` and `record`

Scheduled work is data: a `JobRecord { kind, data: Value }` inside an event.
Each mechanic implements `Job<K>` and registers it:
`Registry::new().register::<Swing>().register::<Land>()`. A domain picks what
jobs run against with `JobContext` — usually `type Cx<'a> = es::Tx<'a, D>` —
plus an `At` (where and as whom) kept beside the job on the clock.

An unknown kind or undecodable data panics, naming the kind.
`failing_round_trips()` lists every kind whose `sample()` does not survive
encode then decode; a test asserts it is empty.

`Value` is small, self-describing and ordered (maps are `Vec`s). `Rec` writes
a type as a `Value` and reads it back; `rec_enum!` and `rec_newtype!` cover
fieldless enums and id newtypes.

## Also

- `parallel(items, f)`: `f` over `items` on every core, order kept.
- `gherkin`: a hand-rolled runner for `harness = false` test targets —
  `parse_feature`, `quoted`, `numbers`, and `main(dir, new_world, step)`,
  which runs each scenario in a fresh world under `catch_unwind` and exits
  non-zero on failure. See `tests/gherkin.rs`.
