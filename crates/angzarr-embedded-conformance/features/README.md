# Tier: coordinator-contract

Cucumber specs for behaviour the **coordinator** (core) owns: what happens
to commands, events and snapshots after a client hands them over. Client
libraries only set proto fields that select this behaviour; they never
implement it.

## Files

| Feature | Covers |
|---|---|
| `conflict_resolution.feature` | The default resolution from the registered state, unsupported resolutions refused, STRICT's ABORTED COORDINATOR_REASON_EXPECTED_SEQUENCE_STALE for a sequence behind the head (C-0717), a sequence ahead of the head malformed under every resolution, INVALID_ARGUMENT COORDINATOR_REASON_EXPECTED_SEQUENCE_AHEAD (C-0727; 14.47), a retry of an applied command answered DUPLICATE whatever its expected sequence (C-0728), an asynchronous command refused ahead dead-lettered with the key and alerted (C-0729), AGGREGATE_HANDLES and COMMUTATIVE delivered with the window and a fresh nonce; a moved stream outside STRICT decided by the resolution's own rule, never the stale key (C-0721); a same-stream race at the write: a STRICT command with an expected sequence stale, one without re-run on the new head, another stream's write no conflict (C-0718 to C-0720), AGGREGATE_HANDLES and COMMUTATIVE re-delivered the widened window for their rule to decide again (C-0722, C-0723; 14.45); dead-lettered at the retry cap (C-0724) or, a stamped re-run, at the race re-run cap (C-0725), the waiting caller answered INTERNAL COORDINATOR_REASON_RETRY_CAP_EXHAUSTED, a fact at the race re-run cap parked instead (C-0726; 14.46); a race re-run cap of 0 the opt-out for a consumer that owns its retries: a lost race handed back as the store's retryable conflict, UNAVAILABLE COORDINATOR_REASON_STORE_CONFLICT (14.51), never re-run, parked or dead-lettered, for a command (C-0740), a fact (C-0741) and a policy step, whose position holds at its trigger (C-0742; 14.46, amended); reaction commands are never stale; the coordinator judges the router's conflict report: the AGGREGATE_HANDLES nonce (C-0683) and COMMUTATIVE unit overlap (C-0684; 14.42) |
| `component_discovery.feature` | The coordinator asks its business process to describe its one component on connect and on every reconnect; nothing is delivered before, a waiting caller answered UNAVAILABLE COORDINATOR_REASON_COMPONENT_NOT_REGISTERED (C-0562; 14.51); readiness; a versionless description is refused; failed requests logged, alerted after the grace period; the generated descriptor validated by the coordinator (C-0687; 14.42) |
| `process_manager_delivery.feature` | A process manager is a domain behind its own coordinator (14.31): its own delivery, state and stream; which inputs start an instance (B1: a non-starting command refused, a fact skipped); commands it may refuse, facts it never does; dedup before the handler; other domains' events arriving only through policies, and decisions leaving through policies |
| `upcasting.feature` | The owning coordinator upcasts, through the component's upcaster sidecar (14.39), on load, on query and wherever stored events reach a subscriber (live or stored history), and every incoming command and fact payload before it judges or delivers it (the unknown-type refusal naming both types, a PM's start rule on the upcast type); a sidecar that is down stalls what needs it, a waiting caller answered UNAVAILABLE COORDINATOR_REASON_UPCASTER_UNAVAILABLE (C-0681; 14.51); the store is never rewritten |
| `query_server.feature` | A standalone query server: queries with no component behind it, no commands (UNIMPLEMENTED COORDINATOR_REASON_COMMANDS_NOT_SERVED, C-0588; 14.51), never a snapshot |
| `rules_version.feature` | The reported build version stamped in full on events and snapshots; the state returned with every answer stored per policy and offered unchanged; which snapshot is offered (registered state type, the default major.minor rule, exact match for non-semantic versions, a domain's accept and reject patterns); an answer reporting another version refused, INTERNAL COORDINATOR_REASON_RULES_VERSION_MISMATCH (C-0748; 14.56) |
| `fact_flow.feature` | Fact injection: 0-based sequencing, `external_deferred.external_id` idempotency, failure handling; a fact carrying a causation_id accepted only by its trigger's domain |
| `state_building.feature` | Aggregate state reconstruction from snapshot + pages, the coordinator delivering only the pages after the snapshot (14.40), `next_sequence` arithmetic; the delivery's `recorded_at`, stamped when the input is accepted (14.37) |
| `edition_propagation.feature` | Source/trigger edition stamped onto every policy emission, a refusal fact included; a PM's events in its input's edition; main-timeline aliases (`""`, unset, `"angzarr"`) |
| `edition_forks.feature` | Editions as per-stream forks (14.32): the fork event at the sequence it states, at or below main's head (14.36), the per-stream read rule, the fork travelling with the data, outside inputs refused until propagated (EDITION_NOT_FORKED), main reads for unforked streams, the unknown edition, listing with no registry, deletion sequenced after the work in flight, removing every row of the edition (a later arrival re-forks; a failed delete is dead-lettered), main byte-for-byte unchanged, PMs |
| `temporal_query.feature` | `TemporalQuery` as_of_sequence / as_of_time and when a snapshot may start the result |
| `policy_addressing.feature` | Who a policy's command addresses (14.36, as amended): the root the policy names, for every target alike (none is a policy bug, COORDINATOR_REASON_POLICY_COMMAND_NO_ROOT); the trigger's correlation carried unchanged (another is COORDINATOR_REASON_POLICY_COMMAND_OTHER_CORRELATION); one PM instance taking several correlations; one event fanning out to an aggregate and a PM; every check on a policy's commands is the coordinator's, a command outside the one destination included (C-0583; 14.42); a reaction setting its own identity, a Fact or a fact origin, each keyed (C-0734, C-0735; 14.48, amended); a reaction for a domain its coordinator cannot route, COORDINATOR_REASON_POLICY_COMMAND_UNKNOWN_DOMAIN (C-0736), and a chain deeper than the coordinator's depth limit, COORDINATOR_REASON_CHAIN_TOO_DEEP (C-0737; 14.49) |
| `policy_refusal.feature` | A refused policy command: the target records nothing and may return a typed refusal object; the policy builds the one refusal fact, passing CommandRefused (the object packed) through to its source's on_refused handler or translating it into a domestic fact; the refused command's id is the fact's id; one outbox transaction; no path (a run-time dead letter), a failing callback, a fact for another domain; an untyped refusal as its code and args (14.37); the coordinator builds the pass-through fact when the policy answers no translation, and refuses a translated fact that is more than a payload (C-0685; 14.42) |
| `sync_modes.feature` | ASYNC, DECISION, SIMPLE (CASCADE and ISOLATED retired with C-0430 to C-0433, RM-1) and the per-command `PageHeader.sync_mode` (caller's mode is a floor); a rejection's and a projector failure's key and args (14.37) |
| `dead_letter_queue.feature` | `angzarr.dlq.{domain}` routing and `AngzarrDeadLetter` contents per failure source; its reason and failures a code and args, never text (14.37); exactly one rejection_details variant, chosen by the reason (14.48): sequence mismatch details for an asynchronous command ahead (C-0730) or STRICT-stale (C-0731), processing failed details at a retry cap (C-0732), a typed refusal's object kept in command refused details (C-0733); an unreachable business process dead-lettered at the retry cap with COORDINATOR_REASON_HANDLER_UNAVAILABLE (C-0738), a decision past the router's step limit with ROUTER_REASON_TOO_MANY_STEPS (C-0739; 14.49); a retryable failure at the retry cap with its own key: an event store that keeps failing, COORDINATOR_REASON_STORE_UNAVAILABLE (C-0744), an unretrievable claim-check payload, COORDINATOR_REASON_PAYLOAD_UNAVAILABLE (C-0447; 14.51); compensation sources retired with C-0448 (RM-2) |

## Who runs these

- **core** is the only implementer. Each scenario is a requirement on the
  coordinator; core binds them either through a coordinator cucumber runner
  or through unit tests that cite the scenario ID (edition propagation is
  covered that way today — see the header of `edition_propagation.feature`).
- **Client repos do not run this tier.** A client-side step definition can
  only simulate the coordinator, which tests nothing; client-rust's
  simulation worlds for `fact_flow`, `merge_strategy` (now `conflict_resolution`) and `state_building`
  are slated for removal for that reason.

## Domain vocabulary

Generic only — `order`, `inventory`, `payment`, `shipping`. No example (blackjack) types
(same rule as `../client/`, STEP_VOCABULARY.md §12, §17).

## Scenario IDs

`@C-NNNN`, shared with `../client/` and `../../parity/`; every scenario is
tagged and `just check-feature-ids` enforces presence, format and
uniqueness. Allocate the next with:

```bash
git grep -hoE '@C-[0-9]{4}' -- features parity | sort -u | tail -1
```
