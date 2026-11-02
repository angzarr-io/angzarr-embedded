Feature: Fact injection

  A process manager is a domain (decision 14.31): facts reach it as they
  reach an aggregate (timeouts, webhooks). A policy's business code never
  emits a fact; the one fact a policy delivers is a refusal fact (the
  standard CommandRefused, or a domestic fact its callback translates the
  refusal into), for its own source domain only (policy_refusal.feature).

  Retired with decision 14.31: C-0184, C-0188, C-0189 and C-0493, which had
  a policy inject facts into an aggregate (C-0610 now refuses that).

  Facts (events) are injected
  directly into target aggregates without going through command handling.
  Facts ARE sequenced and persisted to the event store - they differ from
  commands only in that they bypass validation and cannot be rejected.

  The receiving aggregate declares which event types it accepts as facts
  (ComponentOptions.facts); each is handled by its fact handler, which
  records the fact and may add events that flag it but cannot refuse it.
  A fact of an undeclared type is a wiring error: refused with
  INTERNAL / ROUTER_REASON_NO_FACT_HANDLER (a bug, dead-lettered and alerted; decision
  14.37) and nothing is persisted.

  Facts vs Commands vs Notifications:
  - Commands: Sequenced, validated, can be rejected
  - Facts: Sequenced, recorded by a declared fact handler, cannot be rejected
  - Notifications: NOT sequenced, used for coordination (e.g., compensation)

  Facts represent external realities:
  - Events that have already occurred elsewhere
  - State that the aggregate has no authority to reject
  - Cross-domain propagation where validation doesn't apply

  Sequences are 0-based: an aggregate with N events has next_sequence N.

  Background:
    Given the order aggregate declares facts ShipmentDispatched and PaymentCaptured
    And the inventory aggregate declares fact StockHoldRequested

  # ---------------------------------------------------------------------------
  # Cross-domain reality: the receiving aggregate records, never rejects
  # ---------------------------------------------------------------------------

  @C-0185
  Scenario: Fact receives the next sequence number
    Given an "order" aggregate with 3 existing events
    When a ShipmentDispatched fact is injected
    Then the fact is persisted with sequence number 3
    And subsequent events continue from sequence 4

  @C-0186
  Scenario: Fact is recorded even when a command with the same intent would be refused
    Given an order "order-2" that has been cancelled
    When a PaymentCaptured fact is injected into the "order" aggregate "order-2"
    Then the fact is persisted at the order's next sequence
    And no rejection is produced

  @C-0591
  Scenario: A fact addressed to a process manager is recorded on its stream and never refused
    Given a process manager "Fulfillment" owning domain "fulfillment" with an instance for correlation "0192a6b0-0000-7000-8000-000000000001"
    When a CarrierAssigned fact with external_id "carrier-C1" is sent to "fulfillment" root "0192a6b0-0000-7000-8000-000000000001"
    Then the process manager's events for it are appended to that instance's stream
    And each of those events records the fact's command_id as its causation and the fact's origin
    And the sender is answered APPLIED

  @C-0609
  Scenario: A fact carrying a causation_id is accepted by its trigger's own stream
    # The one fact with a causation_id is a policy's refusal fact, for the
    # exact stream of the policy's triggering event (decision 14.31).
    Given an "order" aggregate "order-1" that declares fact DeliveryRefused
    When a framework component sends a DeliveryRefused fact to "order" root "order-1" whose causation_id names an event of "order" aggregate "order-1"
    Then the fact is recorded at the order's next sequence

  @C-0610
  Scenario: A fact carrying a causation_id addressed to another domain is refused
    Given an "order" aggregate "order-1" that declares fact ShipmentDispatched
    When a framework component sends a ShipmentDispatched fact to "order" root "order-1" whose causation_id names an event of the "shipping" domain
    Then the fact is refused INTERNAL, and dead-lettered and alerted as a policy bug
    And no event is persisted to "order" aggregate "order-1"

  # ---------------------------------------------------------------------------
  # Error handling
  # ---------------------------------------------------------------------------

  @C-0492
  Scenario: A fact of a type the aggregate does not declare is refused and nothing is persisted
    Given an "order" aggregate with 3 existing events
    When an OrderRefunded fact with external_id "refund-R1" is injected into the "order" aggregate
    Then the injection fails with INTERNAL and angzarr error code ROUTER_REASON_NO_FACT_HANDLER
    And the "order" aggregate still has 3 events
    And external_id "refund-R1" is not recorded as processed

  @C-0190
  Scenario: Duplicate fact with same external_id is idempotent
    Given a fact with external_id "shipment-S1-dispatched"
    When the same fact is injected twice
    Then only one event is stored in the aggregate
    And the second injection reports already_processed
