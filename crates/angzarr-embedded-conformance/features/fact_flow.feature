Feature: Fact injection

  A process manager is a domain (decision 14.31): facts reach it as they
  reach an aggregate (timeouts, webhooks). A policy delivers facts of two
  kinds, told apart by their payload: the facts its destination declares,
  which its business code emits as it emits commands (policy_facts.feature;
  decision 14.69), and the refusal fact of a refused command (the standard
  CommandRefused, or a domestic fact its callback translates the refusal
  into), for the exact stream of its trigger only (policy_refusal.feature).
  Only a CommandRefused is held to its trigger's stream by the receiver; a
  translated refusal fact is addressed by the coordinator that issues it.

  C-0184, C-0188, C-0189 and C-0493 stay retired (decision 14.31): a
  policy's facts are policy_facts.feature's.

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
    # A policy's refusal fact, for the exact stream of the policy's
    # triggering event (decision 14.31).
    Given an "order" aggregate "order-1" that declares fact DeliveryRefused
    When a framework component sends a DeliveryRefused fact to "order" root "order-1" whose causation_id names an event of "order" aggregate "order-1"
    Then the fact is recorded at the order's next sequence

  @C-0610
  Scenario: A CommandRefused addressed to another stream than its trigger's is refused
    # A fact's kind is told by its payload (decision 14.69): a
    # CommandRefused is accepted only by the exact stream its trigger names.
    Given an "order" aggregate "order-1" that defines an on_refused handler
    When a framework component sends a CommandRefused fact to "order" root "order-1" whose trigger names an event of the "shipping" domain
    Then the fact is refused INTERNAL, and dead-lettered and alerted as a policy bug
    And no event is persisted to "order" aggregate "order-1"

  @C-0782
  Scenario: A policy's fact carrying a causation_id of another domain is accepted by the stream it is addressed to
    # A policy's fact for its destination (decision 14.69): its causation is
    # its trigger, in its source domain, and its command_id the reaction id
    # its policy's coordinator stamped, taken as given.
    Given an "order" aggregate "order-1" that declares fact ShipmentDispatched
    When a framework component sends a ShipmentDispatched fact to "order" root "order-1" whose causation_id names an event of the "shipping" domain, with the command_id its policy's coordinator stamped
    Then the fact is recorded at the order's next sequence
    And its events record that command_id as their causation

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

  @C-0786
  Scenario: A fact its business process answers with a rejection is dead-lettered, and its sender answered OK, PARKED
    # Owner's ruling (decision 14.72): a fact is never refused, so a
    # rejection its business process answers is that process's bug; the
    # fact is dead-lettered like any input that cannot be delivered.
    Given an "order" aggregate with 3 existing events
    And the "order" business process answers every ShipmentDispatched fact with a rejection, FAILED_PRECONDITION
    When a ShipmentDispatched fact is injected
    Then the fact is dead-lettered as rejected_command, and an alert is raised
    And the client is answered OK with delivery PARKED and no events
    And the "order" aggregate still has 3 events

  @C-0190
  Scenario: Duplicate fact with same external_id is idempotent
    Given a fact with external_id "shipment-S1-dispatched"
    When the same fact is injected twice
    Then only one event is stored in the aggregate
    And the second injection reports already_processed
