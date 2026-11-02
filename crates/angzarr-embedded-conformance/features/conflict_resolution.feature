Feature: Conflict resolution - how the coordinator treats a stale command
  A command may say which stream head it was decided against (its expected
  sequence). When the stream has moved on since, the command is stale, and
  its conflict resolution decides what happens. An expected sequence ahead
  of the head is not stale but malformed, under every resolution: refused
  INVALID_ARGUMENT with reason COORDINATOR_REASON_EXPECTED_SEQUENCE_AHEAD
  before business code runs (decision 14.47). Three resolutions exist:

  - STRICT: a stale command is rejected as ABORTED, with reason
    COORDINATOR_REASON_EXPECTED_SEQUENCE_STALE, before business code runs;
    the caller re-reads and re-issues.
  - AGGREGATE_HANDLES: business code receives the events the command did
    not see (the window) with a nonce, and applies its own conflict rule.
    The router reports the nonce business code resolved with, and the
    coordinator keeps the events only if it is the one delivered.
  - COMMUTATIVE: delivered exactly like AGGREGATE_HANDLES; the router
    reports the conflict units the window and the command changed
    (handler-contract, handling.feature), and the coordinator keeps the
    events only if they do not overlap (decision 14.42).

  Which resolutions a stream supports comes from its registered state. Every
  state supports STRICT and AGGREGATE_HANDLES; COMMUTATIVE only where the
  state declares conflict units. With no resolution given, the default is
  COMMUTATIVE where supported and STRICT otherwise.

  The stream may also move between a command's load and its write, when
  another write to the same stream lands first (decision 14.45; ordering is
  per stream, so no other stream's writes matter). Under STRICT a command
  with an expected sequence then fails ABORTED with reason
  COORDINATOR_REASON_EXPECTED_SEQUENCE_STALE; that key is STRICT's alone
  (decision 14.45). Under AGGREGATE_HANDLES and COMMUTATIVE a moved stream
  is never answered that key: the resolution's own rule decides. If the
  stream moves after the window is delivered and before the write, the
  coordinator re-delivers the widened window, with a fresh nonce, and the
  rule decides again on it; re-deliveries stop at the retry cap. A
  command without one is decided against the head, so it is never stale:
  the coordinator re-runs it on the new head, up to the race re-run cap.
  Re-runs back off with jitter between them. At either cap the command is
  dead-lettered and alerted like any exhausted retry, with reason
  COORDINATOR_REASON_RETRY_CAP_EXHAUSTED, and a caller waiting for it is
  answered INTERNAL with that reason; a fact, never refused, is answered
  PARKED instead (decision 14.46). A race re-run cap of 0 opts out, for a
  consumer that owns its retries: a lost race is never re-run, parked or
  dead-lettered by the coordinator, but goes back to its caller as the
  store's retryable conflict, UNAVAILABLE COORDINATOR_REASON_STORE_CONFLICT
  (decision 14.51), and a policy step's position holds at its
  trigger (C-0740 to C-0742). Reaction commands (from policies; a process manager sends
  none, decision 14.31) never carry an expected sequence or a resolution.

  Retired with decision 14.47: C-0162, which answered a STRICT command
  ahead of the stream ABORTED; C-0727 now refuses it, under every
  resolution, as malformed. Deduplication comes first, so a retry of an
  applied command answers DUPLICATE whatever its expected sequence
  (C-0728); an asynchronous command refused ahead is dead-lettered with
  the key as its reason and alerted (C-0729).

  Background:
    Given an "order" aggregate whose registered state supports COMMUTATIVE
    And the aggregate has events:
      | sequence | type           |
      | 0        | OrderCreated   |
      | 1        | ItemAdded      |
      | 2        | OrderNoteAdded |
    # next_sequence is 3

  # ===========================================================================
  # The default resolution
  # ===========================================================================

  @conflict_resolution
  @C-0155
  Scenario Outline: A command that names no resolution gets the state's default
    Given an "order" aggregate whose registered state <supports>
    And an AddItem command with no conflict resolution targeting sequence 1
    When the coordinator processes the command
    Then the command is handled under <resolution>

    Examples:
      | supports                       | resolution  |
      | supports COMMUTATIVE           | COMMUTATIVE |
      | does not support COMMUTATIVE   | STRICT      |

  @conflict_resolution
  @C-0531
  Scenario: A command naming a resolution its stream does not support is refused
    Given an "order" aggregate whose registered state does not support COMMUTATIVE
    And an AddItem command with conflict resolution COMMUTATIVE targeting sequence 3
    When the coordinator processes the command
    Then the command fails with INVALID_ARGUMENT status
    And business code is not invoked
    And no events are persisted

  # ===========================================================================
  # STRICT - optimistic concurrency
  # ===========================================================================

  @strict
  @C-0160
  Scenario: Strict - a command at the head succeeds without a nonce
    Given an AddItem command with conflict resolution STRICT targeting sequence 3
    When the coordinator processes the command
    Then the command succeeds
    And business code received no conflict nonce
    And an ItemAdded event is persisted at sequence 3

  @strict
  @C-0161
  Scenario: Strict - a stale command is rejected before business code
    Given a ChangeShippingAddress command with conflict resolution STRICT targeting sequence 1
    When the coordinator processes the command
    Then the command fails with ABORTED status
    And business code is not invoked
    And no events are persisted

  @strict
  @C-0717
  Scenario Outline: Strict - a stale command names its expected sequence and the head
    Given an AddItem command with conflict resolution STRICT targeting sequence <expected>
    When the coordinator processes the command
    Then the command fails ABORTED with reason COORDINATOR_REASON_EXPECTED_SEQUENCE_STALE, expected "<expected>" and actual "3", and nothing is persisted
    And business code is not invoked

    Examples:
      | expected |
      | 1        |
      | 2        |

  @conflict_resolution
  @C-0727
  Scenario Outline: A command ahead of the stream is malformed under every resolution
    Given an AddItem command with conflict resolution <resolution> targeting sequence <expected>
    When the coordinator processes the command
    Then the command fails INVALID_ARGUMENT with reason COORDINATOR_REASON_EXPECTED_SEQUENCE_AHEAD, expected "<expected>" and head "3", and nothing is persisted
    And business code is not invoked
    And the command is never answered COORDINATOR_REASON_EXPECTED_SEQUENCE_STALE

    Examples:
      | resolution        | expected |
      | STRICT            | 4        |
      | STRICT            | 7        |
      | AGGREGATE_HANDLES | 4        |
      | AGGREGATE_HANDLES | 7        |
      | COMMUTATIVE       | 4        |
      | COMMUTATIVE       | 7        |

  @conflict_resolution
  @C-0728
  Scenario Outline: A retry of an applied command answers DUPLICATE even with an expected sequence ahead
    # Decision 14.47: deduplication (pipeline step 3) runs before the ahead
    # check (step 4), so the retry is answered as its first delivery was.
    Given an AddItem command with command_id "0192f3a4-5b6c-7d8e-9f01-23456789ac28" was applied, its ItemAdded event persisted at sequence 3
    And a retry of that AddItem command, with the same command_id, conflict resolution <resolution> targeting sequence 9
    When the coordinator processes the command
    Then the caller is answered DUPLICATE with the ItemAdded event at sequence 3
    And business code is not invoked
    And no events are persisted
    And the command is never answered COORDINATOR_REASON_EXPECTED_SEQUENCE_AHEAD

    Examples:
      | resolution        |
      | STRICT            |
      | AGGREGATE_HANDLES |
      | COMMUTATIVE       |

  @conflict_resolution
  @C-0729
  Scenario Outline: An asynchronous command ahead of the stream is dead-lettered with the key and alerted
    # Decision 14.47: HandleAsync validates before enqueueing, and the
    # ahead check needs the head, so the refusal comes when the command is
    # processed, with no caller to answer.
    When an AddItem command with conflict resolution <resolution> targeting sequence 7 is sent asynchronously
    Then HandleAsync answers OK
    And business code is not invoked
    And no events are persisted
    And the command is dead-lettered with reason COORDINATOR_REASON_EXPECTED_SEQUENCE_AHEAD, expected "7" and head "3", and an alert is raised
    And the command is not retried

    Examples:
      | resolution        |
      | STRICT            |
      | AGGREGATE_HANDLES |
      | COMMUTATIVE       |

  # ===========================================================================
  # AGGREGATE_HANDLES - business code owns the conflict rule
  # ===========================================================================

  @aggregate_handles
  @C-0164
  Scenario: AggregateHandles - a stale command reaches business code with the window and a nonce
    Given an AddItem command with conflict resolution AGGREGATE_HANDLES targeting sequence 1
    When the coordinator processes the command
    Then business code is invoked
    And business code receives the events at sequences 1 and 2 as the window
    And business code receives a conflict nonce

  @aggregate_handles
  @C-0532
  Scenario: AggregateHandles - a snapshot inside the window is not used
    Given the "order" aggregate has a snapshot at sequence 2
    And an AddItem command with conflict resolution AGGREGATE_HANDLES targeting sequence 1
    When the coordinator processes the command
    Then business code receives no snapshot
    And business code receives the events at sequences 1 and 2 as the window

  @aggregate_handles
  @C-0165
  Scenario: AggregateHandles - a business rejection is returned unchanged
    Given an AddItem command with conflict resolution AGGREGATE_HANDLES targeting sequence 1
    And business code rejects the command with code ORDER_CLOSED
    When the coordinator processes the command
    Then the command fails with the business rejection reason ORDER_CLOSED
    And no events are persisted

  @aggregate_handles
  @C-0166
  Scenario: AggregateHandles - two stale commands that business code accepts both apply
    Given business code accepts every window it examines
    And two AddItem commands with conflict resolution AGGREGATE_HANDLES both targeting sequence 1
    When the coordinator processes both commands
    Then both commands succeed
    And ItemAdded events are persisted at sequences 3 and 4

  @aggregate_handles
  @C-0551
  Scenario: Each delivery carries a fresh nonce
    Given an AddItem command with conflict resolution AGGREGATE_HANDLES targeting sequence 1
    And business code rejects the first delivery with code TRY_AGAIN
    When the caller sends the same command twice
    Then business code is invoked twice
    And the two deliveries carry different conflict nonces

  @aggregate_handles
  @C-0683
  Scenario Outline: AggregateHandles - the coordinator judges the nonce the router reports
    Given an AddItem command with conflict resolution AGGREGATE_HANDLES targeting sequence 1
    And the coordinator delivers it with conflict nonce "n-1"
    And the business process answers an ItemAdded event, reporting <reported>
    When the coordinator processes the command
    Then <outcome>

    Examples:
      | reported                 | outcome                                                                                               |
      | resolved nonce "n-1"     | an ItemAdded event is persisted at sequence 3                                                         |
      | no resolved nonce        | the command fails ABORTED with reason COORDINATOR_REASON_CONFLICT_UNRESOLVED and nothing is persisted |
      | resolved nonce "made-up" | the command fails ABORTED with reason COORDINATOR_REASON_CONFLICT_UNRESOLVED and nothing is persisted |

  # ===========================================================================
  # COMMUTATIVE - delivered like AGGREGATE_HANDLES
  # ===========================================================================

  @commutative
  @C-0684
  Scenario Outline: Commutative - the coordinator judges the conflict units the router reports
    Given an AddItem command with conflict resolution COMMUTATIVE targeting sequence 1
    And the business process answers an ItemAdded event, reporting window units <window> and command units <mine>
    When the coordinator processes the command
    Then <outcome>

    Examples:
      | window        | mine               | outcome                                                                                                                                                         |
      | "items[pear]" | "items[apple]"     | an ItemAdded event is persisted at sequence 3                                                                                                                   |
      | "items[pear]" | "items[pear]"      | the command fails ABORTED with reason COORDINATOR_REASON_CONFLICT_OVERLAP, window_units "items[pear]" and command_units "items[pear]", and nothing is persisted |
      | "items[42]"   | "items[42].weight" | the command fails ABORTED with reason COORDINATOR_REASON_CONFLICT_OVERLAP and nothing is persisted                                                              |
      | "items[4]"    | "items[42]"        | an ItemAdded event is persisted at sequence 3                                                                                                                   |
      | "*notes"      | "items[apple]"     | the command fails ABORTED with reason COORDINATOR_REASON_CONFLICT_OVERLAP and nothing is persisted                                                              |
      | none          | "items[apple]"     | an ItemAdded event is persisted at sequence 3                                                                                                                   |

  @commutative
  @C-0149
  Scenario: Commutative - a command at the head is delivered with an empty window and a nonce
    Given an AddItem command with conflict resolution COMMUTATIVE targeting sequence 3
    When the coordinator processes the command
    Then business code receives an empty window
    And business code receives a conflict nonce
    And an ItemAdded event is persisted at sequence 3

  @conflict_resolution
  @C-0721
  Scenario Outline: A moved stream is never stale outside STRICT - the resolution's own rule decides
    Given an AddItem command with conflict resolution <resolution> targeting sequence 1
    And the business process answers an ItemAdded event, reporting <reported>
    When the coordinator processes the command
    Then business code receives the events at sequences 1 and 2 as the window
    And <outcome>

    Examples:
      | resolution        | reported                                                     | outcome                                                                                               |
      | AGGREGATE_HANDLES | the conflict nonce it was delivered as its resolved nonce    | an ItemAdded event is persisted at sequence 3                                                         |
      | AGGREGATE_HANDLES | no resolved nonce                                            | the command fails ABORTED with reason COORDINATOR_REASON_CONFLICT_UNRESOLVED and nothing is persisted |
      | COMMUTATIVE       | window units "items[pear]" and command units "items[apple]"  | an ItemAdded event is persisted at sequence 3                                                         |
      | COMMUTATIVE       | window units "items[pear]" and command units "items[pear]"   | the command fails ABORTED with reason COORDINATOR_REASON_CONFLICT_OVERLAP and nothing is persisted    |

  @conflict_resolution
  @C-0172
  Scenario Outline: The resolution decides how a stale command is delivered
    Given an AddItem command with conflict resolution <resolution> targeting sequence 1
    When the coordinator processes the command
    Then the outcome is <outcome>

    Examples:
      | resolution        | outcome                                                   |
      | STRICT            | rejected with ABORTED before business code                |
      | AGGREGATE_HANDLES | delivered to business code with the window and a nonce    |
      | COMMUTATIVE       | delivered to business code with the window and a nonce    |

  # ===========================================================================
  # A same-stream race at the write (decision 14.45)
  # ===========================================================================

  @write_race @strict
  @C-0718
  Scenario: Strict - a command with an expected sequence that loses a race at its write is stale
    Given an AddItem command with conflict resolution STRICT targeting sequence 3
    And another write to the "order" stream lands at sequence 3 after the command is loaded and before it is written
    When the coordinator processes the command
    Then the command fails ABORTED with reason COORDINATOR_REASON_EXPECTED_SEQUENCE_STALE, expected "3" and actual "4", and nothing is persisted
    And the coordinator does not re-run the command

  @write_race @aggregate_handles
  @C-0722
  Scenario Outline: AggregateHandles - a race lost at the write re-delivers the widened window, and the nonce decides on it
    Given an AddItem command with conflict resolution AGGREGATE_HANDLES targeting sequence 1
    And the business process answers the first delivery with an ItemAdded event, reporting the conflict nonce it was delivered as its resolved nonce
    And another write to the "order" stream lands at sequence 3 after the window is delivered and before the command is written
    And the business process answers the second delivery with an ItemAdded event, reporting <reported>
    When the coordinator processes the command
    Then business code is invoked twice, the second time with the events at sequences 1 to 3 as the window and a fresh conflict nonce
    And <outcome>
    And the command is never answered COORDINATOR_REASON_EXPECTED_SEQUENCE_STALE

    Examples:
      | reported                                                          | outcome                                                                                                          |
      | the conflict nonce of the second delivery as its resolved nonce   | the command succeeds and an ItemAdded event is persisted at sequence 4                                           |
      | the conflict nonce of the first delivery as its resolved nonce    | the command fails ABORTED with reason COORDINATOR_REASON_CONFLICT_UNRESOLVED and the command persists nothing    |
      | no resolved nonce                                                 | the command fails ABORTED with reason COORDINATOR_REASON_CONFLICT_UNRESOLVED and the command persists nothing    |

  @write_race @commutative
  @C-0723
  Scenario Outline: Commutative - a race lost at the write re-delivers the widened window, and unit overlap decides on it
    Given an AddItem command with conflict resolution COMMUTATIVE targeting sequence 1
    And the business process answers the first delivery with an ItemAdded event, reporting window units "items[pear]" and command units "items[apple]"
    And another write to the "order" stream lands at sequence 3 after the window is delivered and before the command is written
    And the business process answers the second delivery with an ItemAdded event, reporting window units <window> and command units "items[apple]"
    When the coordinator processes the command
    Then business code is invoked twice, the second time with the events at sequences 1 to 3 as the window and a fresh conflict nonce
    And <outcome>
    And the command is never answered COORDINATOR_REASON_EXPECTED_SEQUENCE_STALE

    Examples:
      | window                        | outcome                                                                                                       |
      | "items[pear]", "items[kiwi]"  | the command succeeds and an ItemAdded event is persisted at sequence 4                                        |
      | "items[pear]", "items[apple]" | the command fails ABORTED with reason COORDINATOR_REASON_CONFLICT_OVERLAP and the command persists nothing    |

  @write_race
  @C-0724
  Scenario Outline: A race lost at every write until the retry cap dead-letters the command
    Given the retry cap is reached after 3 deliveries
    And an AddItem command with conflict resolution <resolution> targeting sequence 1, sent through Handle by a caller waiting for the answer
    And the business process answers every delivery with an ItemAdded event, reporting <accepted>
    And another write to the "order" stream lands after each delivery's window is delivered and before the command is written
    When the coordinator processes the command
    Then business code is invoked 3 times, each time with a wider window and a fresh conflict nonce
    And the command is dead-lettered with reason COORDINATOR_REASON_RETRY_CAP_EXHAUSTED, attempts "3" and cap "3", and an alert is raised
    And the command persists nothing
    And the caller is answered INTERNAL with reason COORDINATOR_REASON_RETRY_CAP_EXHAUSTED, attempts "3" and cap "3"
    And the command is never answered COORDINATOR_REASON_EXPECTED_SEQUENCE_STALE

    Examples:
      | resolution        | accepted                                                              |
      | AGGREGATE_HANDLES | the conflict nonce it was delivered as its resolved nonce             |
      | COMMUTATIVE       | window units "items[pear]" and command units "items[apple]"           |

  @write_race
  @C-0719
  Scenario: A command without an expected sequence that loses a race at its write is re-run on the new head
    Given an AddItem command with no expected sequence and no conflict resolution
    And another write to the "order" stream lands at sequence 3 after the command is loaded and before it is written
    When the coordinator processes the command
    Then business code is invoked twice, the second time against the head at sequence 4
    And the command succeeds
    And an ItemAdded event is persisted at sequence 4

  @write_race
  @C-0725
  Scenario: A command without an expected sequence that loses a race at every write stops at the race re-run cap
    # Decision 14.46: the race re-run cap is its own, separate from the
    # retry cap, so the retry cap here is set higher and never reached.
    Given the race re-run cap is reached after 3 runs
    And the retry cap is reached after 5 deliveries
    And an AddItem command with no expected sequence and no conflict resolution, sent through Handle by a caller waiting for the answer
    And the business process answers every run with an ItemAdded event
    And another write to the "order" stream lands after each run's load and before the command is written
    When the coordinator processes the command
    Then business code is invoked 3 times, each time against a newer head
    And the command is dead-lettered with reason COORDINATOR_REASON_RETRY_CAP_EXHAUSTED, attempts "3" and cap "3", and an alert is raised
    And the command persists nothing
    And the caller is answered INTERNAL with reason COORDINATOR_REASON_RETRY_CAP_EXHAUSTED, attempts "3" and cap "3"
    And the command is never answered COORDINATOR_REASON_EXPECTED_SEQUENCE_STALE

  @write_race @fact
  @C-0726
  Scenario: A fact that loses a race at every write until the race re-run cap is parked, never refused
    # Decision 14.46: facts are never refused, so at the cap the sender is
    # answered PARKED, never COORDINATOR_REASON_RETRY_CAP_EXHAUSTED; the key
    # is the dead letter's reason.
    Given the race re-run cap is reached after 3 runs
    And the order aggregate declares fact ShipmentDispatched
    And a ShipmentDispatched fact for the "order" stream, sent through Handle by a client waiting for the answer
    And the ingest handler answers every run with a ShipmentDispatched event
    And another write to the "order" stream lands after each run's load and before the fact is written
    When the coordinator processes the fact
    Then the ingest handler is invoked 3 times, each time against a newer head
    And the fact is dead-lettered with reason COORDINATOR_REASON_RETRY_CAP_EXHAUSTED, attempts "3" and cap "3", and an alert is raised
    And the fact persists nothing
    And the client is answered OK with delivery PARKED and no events
    And the client is never answered COORDINATOR_REASON_RETRY_CAP_EXHAUSTED

  @write_race
  @C-0740
  Scenario: With a race re-run cap of 0, a command that loses a race at its write goes back to its caller as the store's conflict, keyed
    # Decision 14.46, amended: a cap of 0 is the opt-out for a caller that
    # owns its retries; the retry cap, set here, does not re-run a race.
    # Decision 14.51: the conflict is retryable, so it carries its key, for
    # the caller to report if its own retries are spent.
    Given the race re-run cap is 0
    And the retry cap is reached after 3 deliveries
    And an AddItem command with no expected sequence and no conflict resolution, sent through Handle by a caller waiting for the answer
    And another write to the "order" stream lands at sequence 3 after the command is loaded and before it is written
    When the coordinator processes the command
    Then business code is invoked once
    And the command persists nothing
    And nothing is dead-lettered and no alert is raised
    And the caller is answered UNAVAILABLE with reason COORDINATOR_REASON_STORE_CONFLICT, expected "3" and actual "4", with no wait before it
    And the caller is never answered COORDINATOR_REASON_RETRY_CAP_EXHAUSTED

  @write_race @fact
  @C-0741
  Scenario: With a race re-run cap of 0, a fact that loses a race at its write goes back to its sender as the store's conflict, never parked
    # Decision 14.46, amended: the opt-out holds for a fact too; the
    # coordinator parks only a fact it gave up on at a cap.
    Given the race re-run cap is 0
    And the order aggregate declares fact ShipmentDispatched
    And a ShipmentDispatched fact for the "order" stream, sent through Handle by a client waiting for the answer
    And the ingest handler answers with a ShipmentDispatched event
    And another write to the "order" stream lands at sequence 3 after the fact is loaded and before it is written
    When the coordinator processes the fact
    Then the ingest handler is invoked once
    And the fact persists nothing
    And nothing is dead-lettered and no alert is raised
    And the client is answered UNAVAILABLE with reason COORDINATOR_REASON_STORE_CONFLICT, expected "3" and actual "4", with no wait before it
    And the client is never answered OK with delivery PARKED

  @write_race @reaction
  @C-0742
  Scenario: With a race re-run cap of 0, a policy step that loses a race at its write holds the policy's position at its trigger
    # Decision 14.46, amended, with 14.37: a step's position moves past its
    # trigger only after the step is dead-lettered, and at a cap of 0 the
    # coordinator dead-letters nothing, whatever the retry cap says.
    Given the race re-run cap is 0
    And the retry cap is reached after 3 deliveries
    And a policy "PaymentToOrder" translating from "payment" to "order" that sends AddItem to "order" "o-1" on PaymentReceived, with no expected sequence and no conflict resolution
    And a PaymentReceived event at sequence 1 of a "payment" stream
    And another write to the "order" stream lands at sequence 3 after the step's command is loaded and before it is written
    When the policy "PaymentToOrder" runs on the PaymentReceived event
    Then business code is invoked once
    And the step persists nothing
    And nothing is dead-lettered and no alert is raised
    And the step fails UNAVAILABLE with reason COORDINATOR_REASON_STORE_CONFLICT, expected "3" and actual "4", with no wait before it
    And the policy's position on the "payment" stream stays at the PaymentReceived event, never moved past it
    And the policy's next catch-up runs the step again

  @write_race
  @C-0720
  Scenario: A write to another stream does not make a command stale
    Given an AddItem command with conflict resolution STRICT targeting sequence 3
    And another write lands on a different "order" root's stream after the command is loaded and before it is written
    When the coordinator processes the command
    Then the command succeeds
    And an ItemAdded event is persisted at sequence 3

  # ===========================================================================
  # Reaction commands: never stale
  # ===========================================================================

  @reaction
  @C-0458
  Scenario: A reaction command is decided against the head
    Given a policy-emitted AddItem command with no expected sequence and no conflict resolution
    When the coordinator processes the command
    Then the command succeeds
    And an ItemAdded event is persisted at sequence 3

  @reaction
  @C-0552
  Scenario: A reaction command that names an expected sequence is a reactor bug
    Given a policy-emitted AddItem command targeting sequence 1
    When the coordinator processes the command
    Then the command is not delivered to business code
    And the command is dead-lettered with reason COORDINATOR_REASON_POLICY_COMMAND_WITH_HANDLING, domain "order" and command_type "AddItem", and an alert is raised
    And no events are persisted

  @reaction
  @C-0459
  Scenario: A reaction command is guarded only by the destination's own decision
    Given a policy-emitted AddItem command with no expected sequence and no conflict resolution
    And business code rejects the command with code ORDER_CLOSED
    When the coordinator processes the command
    Then the command fails with the business rejection reason ORDER_CLOSED
    And no events are persisted

  @reaction
  @C-0460
  Scenario: A redelivered reaction command is applied once
    Given a policy-emitted AddItem command
    When the coordinator processes the command
    And the coordinator processes the same command again
    Then exactly one ItemAdded event is persisted
    And the second delivery returns the events of the first

  # ===========================================================================
  # Edge cases
  # ===========================================================================

  @conflict_resolution @edge_case
  @C-0174
  Scenario Outline: A new stream accepts sequence 0 under every resolution
    Given a new "order" aggregate with no events
    And a CreateOrder command with conflict resolution <resolution> targeting sequence 0
    When the coordinator processes the command
    Then the command succeeds
    And an OrderCreated event is persisted at sequence 0

    Examples:
      | resolution        |
      | COMMUTATIVE       |
      | STRICT            |
      | AGGREGATE_HANDLES |

  @conflict_resolution @edge_case
  @C-0175
  Scenario: A snapshot moves the head forward
    Given an "order" aggregate with a snapshot at sequence 50 and events at sequences 51 and 52
    And an AddItem command with conflict resolution STRICT targeting sequence 53
    When the coordinator processes the command
    Then the command succeeds
    And an ItemAdded event is persisted at sequence 53
