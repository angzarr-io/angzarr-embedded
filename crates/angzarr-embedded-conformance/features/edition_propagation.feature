# Coordinator-contract feature: edition propagation across cross-domain
# emissions. When a policy or process-manager handler receives an event in
# edition X, the coordinator stamps edition X onto every outgoing
# CommandBook / EventBook (a policy's commands and events, a process
# manager's process_events) before persistence or dispatch. The edition
# name propagates verbatim, and always overrides whatever edition the
# handler set. How an edition's streams are forked, and how forks
# propagate, is edition_forks.feature (decision 14.32).
#
# A process manager is a domain (decision 14.31): its inputs are commands
# (from policies, in their trigger's edition) and facts, and its events land
# in its input's edition, like any domain's. A policy's refusal fact keeps
# its trigger's edition.
#
# Coverage in core: Rust unit tests in
# src/orchestration/saga/local/tests.rs (C-0138..C-0142) and
# src/orchestration/process_manager/local/tests.rs (C-0144, before decision
# 14.31; C-0590 is new).
Feature: Edition propagation across cross-domain emissions
  As a policy / process-manager author
  I want the framework to guarantee that emitted commands and events
  inherit the source / trigger cover's edition
  So that timeline consistency holds across cross-domain boundaries
  without requiring me to manually stamp every outgoing cover

  # ---------------------------------------------------------------
  # Policy: source event → outgoing commands / events
  # ---------------------------------------------------------------

  @C-0138
  Scenario: Policy propagates source edition to outgoing commands
    Given a policy "OrderFulfillment" translating from "order" to "inventory"
    And the policy handles OrderCreated by emitting a ReserveStock command
    And the source event has edition "speculative"
    When an OrderCreated event is dispatched to the policy
    Then the emitted command's cover has edition "speculative"

  @C-0139
  Scenario: Policy propagates source edition to outgoing events
    Given a policy "OrderAudit" translating from "order" to "audit"
    And the policy handles OrderCreated by emitting an OrderObserved event
    And the source event has edition "speculative"
    When an OrderCreated event is dispatched to the policy
    Then the emitted event's cover has edition "speculative"

  @C-0140
  Scenario: A policy's command naming another edition than its trigger's is refused
    # policy.proto, PolicyResponse: no edition is overridden; a command into
    # any other edition than its trigger's is the policy's bug (decision
    # 14.66).
    Given a policy "OrderFulfillment" translating from "order" to "inventory"
    And the source event has edition "alpha"
    And the policy handler sets outgoing edition "beta"
    When an OrderCreated event is dispatched to the policy
    Then the policy's answer is refused INTERNAL with reason COORDINATOR_REASON_POLICY_COMMAND_OUTSIDE_EDITION, expected_edition "alpha", actual_edition "beta"
    And no command reaches "inventory"

  @C-0141
  Scenario: Policy propagates main-timeline (empty) edition unchanged
    Given a policy "OrderFulfillment" translating from "order" to "inventory"
    And the source event has no edition set
    When an OrderCreated event is dispatched to the policy
    Then the emitted command's cover has no edition set

  # C-0142 (an edition's divergences propagated) is retired: editions are
  # per-stream forks with no divergence records (decision 14.32; the fork
  # scenarios are in edition_forks.feature).

  # ---------------------------------------------------------------
  # Process Manager: input → its own events
  # ---------------------------------------------------------------

  @C-0144
  Scenario: A PM's events land in its input's edition
    Given a process manager "Fulfillment" owning domain "fulfillment"
    And the PM records an OrderTracked event on a StartFulfillment command
    And the command's cover has edition "speculative"
    When the StartFulfillment command is delivered to the PM
    Then every events book the PM answers has edition "speculative"

  @C-0590
  Scenario: A PM's events book naming another edition is refused
    # A PM's answer, like an aggregate's, is payloads for its own stream;
    # any other cover, another edition included, is a handler bug.
    Given a process manager "Fulfillment" owning domain "fulfillment"
    And the command's cover has edition "alpha"
    And the PM handler sets the edition "beta" on its answer's events book
    When a StartFulfillment command is delivered to the PM
    Then no event is persisted
    And the command is refused INTERNAL with reason COORDINATOR_REASON_EVENTS_NOT_ITS_STREAM, naming the stream and the event stream in edition "beta"
    And the command is dead-lettered and an alert is raised

  @C-0608
  Scenario: A policy's refusal fact carries its trigger's edition
    Given a policy "OrderToInventory" translating from "order" to "inventory" generated with ReserveStock in pass-through mode
    And the source event has edition "speculative"
    And the inventory aggregate refuses ReserveStock
    When an OrderCreated event is dispatched to the policy
    Then the refusal fact's cover has edition "speculative"

  # ---------------------------------------------------------------
  # Main timeline: unset, and spelled no other way
  # ---------------------------------------------------------------

  @C-0409
  Scenario Outline: The main timeline is spelled one way: unset
    # types.proto, Cover.edition: main is unset; an empty edition is
    # malformed, and "angzarr" names an edition like any other (decision
    # 14.66).
    Given an "order" aggregate "order-1" with 2 events on the main timeline
    When a client sends an AddItem command to "order-1" with edition <edition>
    Then <outcome>

    Examples:
      | edition   | outcome                                                                                                              |
      | unset     | an ItemAdded event is persisted at sequence 2                                                                        |
      | ""        | the command fails INVALID_ARGUMENT and nothing is persisted                                                          |
      | "angzarr" | the client is answered FAILED_PRECONDITION with reason COORDINATOR_REASON_EDITION_NOT_FORKED, edition "angzarr" |

  @C-0410
  Scenario: The main timeline cannot be deleted
    Given an "order" aggregate "order-1" with 2 events on the main timeline
    When DeleteEdition is requested with an empty name
    Then the request is refused INVALID_ARGUMENT because main is unnamed and cannot be deleted
    And the aggregate still has 2 events
