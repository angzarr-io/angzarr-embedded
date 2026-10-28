# annals

A small, deterministic event-sourcing engine on angzarr's wire types:
angzarr's local runtime. No clock and nothing specific to any application.
Its dependencies are annals-diagnostic (for its errors) and angzarr's wire
types (angzarr-proto, with prost and prost-types), which the kernel,
`annals::next`, operates on directly; angzarr-proto is a git dependency
whose build runs `protoc`.

## The model

- A **domain** declares its state, commands, facts, events, rejections and
  rules: **decide** turns a command into events (or a rejection, a
  `Diagnostic` with a stable code and named args), **ingest** turns a fact
  into events, and **apply** folds an event into the state. No state is
  ever stored: every state is a fold of a stream, resumed from a snapshot
  at most. A process manager is a domain too.
- A stream is angzarr's: a `Cover` (domain, edition, root), its events
  `EventPage`s with angzarr's `EventMeta`, which the kernel stamps.
- A **policy** carries one domain's events into another's commands.
- A **projection** is a read model: a fold over committed events.
- Markup (annals-macros) generates a domain's, policy's or projector's glue
  from a hand-written impl.

One command, end to end, in angzarr's normative order: validate
(`check`), identify, load then deduplicate (by command id, on the
stream's events), resolve (the expected sequence, under the resolved
conflict resolution), decode (upcast by type URL) and decide or ingest.
See `docs/design/audit.md`.
