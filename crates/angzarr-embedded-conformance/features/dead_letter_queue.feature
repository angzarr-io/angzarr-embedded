Feature: Dead letter queue routing
  Messages the framework cannot process and must not drop are published as
  AngzarrDeadLetter to the per-domain topic "angzarr.dlq.{domain}", where
  domain is the domain of the dead letter's cover. A dead letter carries
  the rejected command (rejected_command), the events that failed
  (rejected_events) or a failed edition removal (failed_edition_removal), a reason (a Diagnostic: a code and its args, never
  rendered text; decision 14.37), exactly one rejection_details variant, chosen by
  its reason (the mapping on AngzarrDeadLetter.rejection_details in types.proto;
  decision 14.48), occurred_at, and the producing component's
  name and type ("aggregate" | "policy" | "projector" | "process_manager").
  The cover's correlation_id is the failed message's correlation_id.

  Sources of dead letters:
  - queued commands whose own expected sequence is stale (sequence_mismatch
    details)
  - queued commands refused with a typed refusal (command_refused details)
  - policy / PM / projector handler failures once retries are exhausted
    (processing_failed details)
  - claim-check payloads that cannot be retrieved, at the retry cap
    (payload_retrieval_failed details, COORDINATOR_REASON_PAYLOAD_UNAVAILABLE)
  - inputs whose event store keeps failing, at the retry cap
    (processing_failed details, COORDINATOR_REASON_STORE_UNAVAILABLE)

  Compensation is no longer part of the contract (RM-2), so its sources are
  gone: C-0448 (a compensation handler's RevocationResponse sending the
  rejected command to the dead letter queue) is retired.

  @C-0443
  Scenario: A queued command with a stale expected sequence is dead-lettered with mismatch details
    Given an "order" aggregate "order-1" at next_sequence 3
    When a ChangeShippingAddress command with conflict resolution STRICT targeting sequence 1 and correlation_id "0192f3a4-5b6c-7d8e-9f01-23456789ab07" is sent asynchronously
    Then a dead letter is published to "angzarr.dlq.order"
    And the dead letter carries the rejected command
    And the dead letter's command is addressed to domain "order" root "order-1" with correlation_id "0192f3a4-5b6c-7d8e-9f01-23456789ab07"
    And the dead letter's sequence_mismatch details are expected 1, actual 3
    And the dead letter's source_component_type is "aggregate"
    And the dead letter has occurred_at set

  @C-0738
  Scenario: A command whose business process cannot be reached is dead-lettered at the retry cap with the unavailable key
    # Decision 14.49: a retryable bug at its cap records its own key, as
    # last answered (14.48, amended), never COORDINATOR_REASON_RETRY_CAP_EXHAUSTED.
    Given an "order" aggregate "order-1" at next_sequence 3
    And the retry cap is reached after 3 attempts
    And the business process handling "order" cannot be reached, its connection refused
    When an AddItem command with no expected sequence is sent asynchronously and the coordinator processes it
    Then the coordinator attempts the delivery 3 times, backing off between attempts
    And a dead letter is published to "angzarr.dlq.order"
    And the dead letter carries the rejected command
    And the dead letter's reason has code COORDINATOR_REASON_HANDLER_UNAVAILABLE, arg domain "order" and arg cause
    And the dead letter's processing_failed details have failure code COORDINATOR_REASON_HANDLER_UNAVAILABLE, retry_count 2 and is_transient true
    And the dead letter carries no other rejection_details variant
    And an alert is raised
    And no events are persisted

  @C-0744
  Scenario: A command whose event store keeps failing is dead-lettered at the retry cap with the store key
    # Decision 14.51: a retryable failure carries a key, so the dead letter
    # at the cap records it, as last answered, never
    # COORDINATOR_REASON_RETRY_CAP_EXHAUSTED (14.48, amended).
    Given an "order" aggregate "order-1" at next_sequence 3
    And the retry cap is reached after 3 attempts
    And the "order" event store fails every write, its connection dropped
    When an AddItem command with no expected sequence is sent asynchronously and the coordinator processes it
    Then the coordinator attempts the command 3 times, backing off between attempts
    And a dead letter is published to "angzarr.dlq.order"
    And the dead letter carries the rejected command
    And the dead letter's reason has code COORDINATOR_REASON_STORE_UNAVAILABLE and arg cause
    And the dead letter's processing_failed details have failure code COORDINATOR_REASON_STORE_UNAVAILABLE, retry_count 2 and is_transient true
    And the dead letter carries no other rejection_details variant
    And an alert is raised
    And no events are persisted

  @C-0739
  Scenario: A decision that runs past the router's step limit is dead-lettered as a fatal bug
    # Decision 14.49: a limit a router may enforce, where a client library
    # lets a decision continue in follow-on steps.
    Given an "order" aggregate "order-1" at next_sequence 3, hosted by a router whose step limit is 16
    And the business process answers AddItem with an ItemAdded event and a follow-on step that always continues
    When an AddItem command with no expected sequence is sent asynchronously and the coordinator processes it
    Then the coordinator attempts the delivery once
    And a dead letter is published to "angzarr.dlq.order"
    And the dead letter carries the rejected command
    And the dead letter's reason has code ROUTER_REASON_TOO_MANY_STEPS and arg limit "16"
    And the dead letter's processing_failed details have failure code ROUTER_REASON_TOO_MANY_STEPS, retry_count 0 and is_transient false
    And the dead letter carries no other rejection_details variant
    And an alert is raised
    And no events are persisted

  @C-0444
  Scenario: A policy that keeps failing is dead-lettered with the events it could not process
    Given a policy "OrderFulfillment" subscribed to "order" whose handler fails with code INVENTORY_UNAVAILABLE
    And the policy's retry budget is 3 attempts
    When an OrderCreated event for "order-1" is delivered to the policy
    Then the policy handler is attempted 3 times
    And a dead letter is published to "angzarr.dlq.order"
    And the dead letter carries the OrderCreated event as rejected_events
    And the dead letter's processing_failed details have failure code INVENTORY_UNAVAILABLE and retry_count 2
    And the dead letter's source_component is "OrderFulfillment" with source_component_type "policy"

  @C-0445
  Scenario: A projector that keeps failing is dead-lettered
    Given a projector "OrderSummary" subscribed to "order" whose handler fails with code VIEW_STORE_UNAVAILABLE
    When an OrderCreated event for "order-1" is delivered to the projector and its retries are exhausted
    Then a dead letter is published to "angzarr.dlq.order"
    And the dead letter carries the OrderCreated event as rejected_events
    And the dead letter's source_component is "OrderSummary" with source_component_type "projector"

  @C-0446
  Scenario: A non-transient handler failure is dead-lettered without retries
    Given a policy "OrderFulfillment" subscribed to "order" whose handler fails permanently with code UNKNOWN_EVENT_VERSION
    When an OrderCreated event for "order-1" is delivered to the policy
    Then the policy handler is attempted once
    And the dead letter's processing_failed details have is_transient false and retry_count 0

  @C-0447
  Scenario: An unretrievable claim-check payload is dead-lettered at the retry cap with the payload key
    # Decision 14.51: a retryable failure carries a key, so the dead letter
    # at the cap records it.
    Given the retry cap is reached after 3 attempts
    And an OrderCreated event for "order-1" whose payload is a PayloadReference to "s3://payloads/abc.bin"
    And the payload store cannot return "s3://payloads/abc.bin"
    When the event is delivered to the policy "OrderFulfillment"
    Then the coordinator attempts the retrieval 3 times, backing off between attempts
    And a dead letter is published to "angzarr.dlq.order"
    And the dead letter's reason has code COORDINATOR_REASON_PAYLOAD_UNAVAILABLE, arg uri "s3://payloads/abc.bin" and arg cause
    And the dead letter's payload_retrieval_failed details have storage_type S3, uri "s3://payloads/abc.bin" and failure code COORDINATOR_REASON_PAYLOAD_UNAVAILABLE
    And the dead letter carries no other rejection_details variant
    And an alert is raised

  @C-0449
  Scenario: A dead letter is not published when processing succeeds
    Given a policy "OrderFulfillment" subscribed to "order" whose handler succeeds
    When an OrderCreated event for "order-1" is delivered to the policy
    Then no dead letter is published

  @C-0673
  Scenario: A dead letter carries a key and its args, never rendered text
    # Decision 14.37: text is rendered only where it is displayed, from a
    # catalog keyed by the code.
    Given a policy "OrderFulfillment" subscribed to "order" whose handler fails with code INVENTORY_UNAVAILABLE and arg sku "sku-1", and logs "inventory service down" for itself
    And the policy's retry budget is 1 attempt
    When an OrderCreated event for "order-1" is delivered to the policy
    Then a dead letter is published to "angzarr.dlq.order"
    And the dead letter's reason has code INVENTORY_UNAVAILABLE and arg sku "sku-1"
    And the dead letter's processing_failed details have failure code INVENTORY_UNAVAILABLE and arg sku "sku-1"
    And no field of the dead letter carries the text "inventory service down"

  @C-0730
  Scenario Outline: An asynchronous command ahead of the stream carries sequence mismatch details
    # Decision 14.48: the AHEAD key's head arg is the details' actual_sequence.
    Given an "order" aggregate "order-1" at next_sequence 3
    When a ChangeShippingAddress command with conflict resolution <resolution> targeting sequence 7 is sent asynchronously
    Then a dead letter is published to "angzarr.dlq.order"
    And the dead letter carries the rejected command
    And the dead letter's reason has code COORDINATOR_REASON_EXPECTED_SEQUENCE_AHEAD, arg expected "7" and arg head "3"
    And the dead letter's sequence_mismatch details are expected 7, actual 3
    And the dead letter carries no other rejection_details variant

    Examples:
      | resolution        |
      | STRICT            |
      | AGGREGATE_HANDLES |

  @C-0731
  Scenario: An asynchronous STRICT command behind the stream carries its stale key and sequence mismatch details
    Given an "order" aggregate "order-1" at next_sequence 3
    When a ChangeShippingAddress command with conflict resolution STRICT targeting sequence 1 is sent asynchronously
    Then a dead letter is published to "angzarr.dlq.order"
    And the dead letter carries the rejected command
    And the dead letter's reason has code COORDINATOR_REASON_EXPECTED_SEQUENCE_STALE, arg expected "1" and arg actual "3"
    And the dead letter's sequence_mismatch details are expected 1, actual 3
    And the dead letter carries no other rejection_details variant

  @C-0732
  Scenario Outline: A command dead-lettered at a retry cap carries event processing failed details
    # Decision 14.48: one variant for the key, whether or not the command
    # carries an expected sequence; retry_count is the retries, the attempts
    # arg less the first run (ProcessingFailedDetails.retry_count).
    Given an "order" aggregate "order-1" at next_sequence 3
    And the <cap> is reached after 3 runs
    And an AddItem command <handling> is sent asynchronously
    And the business process answers every run with an ItemAdded event<report>
    And another write to the "order" stream lands after each run's load and before the command is written
    When the coordinator processes the command
    Then a dead letter is published to "angzarr.dlq.order"
    And the dead letter carries the rejected command
    And the dead letter's reason has code COORDINATOR_REASON_RETRY_CAP_EXHAUSTED, arg attempts "3" and arg cap "3"
    And the dead letter's processing_failed details have failure code COORDINATOR_REASON_RETRY_CAP_EXHAUSTED, retry_count 2 and is_transient true
    And the dead letter carries no other rejection_details variant

    Examples:
      | cap             | handling                                                        | report                                                                  |
      | race re-run cap | with no expected sequence and no conflict resolution            |                                                                         |
      | retry cap       | with conflict resolution AGGREGATE_HANDLES targeting sequence 1 | , reporting the conflict nonce it was delivered as its resolved nonce   |

  @C-0733
  Scenario: An asynchronous command refused with a typed refusal keeps the refusal object
    # Decision 14.48, amended: the refusal object rides in command_refused,
    # whole, as the wire carried it.
    Given a "wallet" aggregate that refuses HoldFunds with FAILED_PRECONDITION reason INSUFFICIENT_AVAILABLE_FUNDS and the refusal object InsufficientFunds with available 200 and requested 500 when funds are short
    When a HoldFunds command for 500 on wallet "p-1" is sent asynchronously
    Then a dead letter is published to "angzarr.dlq.wallet"
    And the dead letter carries the rejected command
    And the dead letter's reason has code ROUTER_REASON_COMMAND_REFUSED and arg refusal_type naming InsufficientFunds
    And the dead letter's command_refused details have refusal_type naming InsufficientFunds and the InsufficientFunds refusal object packed, with available 200 and requested 500
    And the dead letter carries no other rejection_details variant
