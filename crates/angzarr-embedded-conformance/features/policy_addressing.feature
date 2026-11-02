Feature: Who a policy's command addresses, and the correlation it carries
  A policy sends commands to foreign aggregates directly, and to process
  managers (decision 14.36, as amended). Every command or fact a policy
  sends, to any target, carries its trigger's correlation_id unchanged:
  correlation traces the causal chain, and a command carrying another is a
  policy bug (COORDINATOR_REASON_POLICY_COMMAND_OTHER_CORRELATION). A policy names its
  command's root for EVERY target, aggregate or process manager alike:
  nothing fills one in, the router never knows the target's kind, and a
  command with no root is a policy bug (COORDINATOR_REASON_POLICY_COMMAND_NO_ROOT). The
  policy's coordinator makes every one of these checks, per triggering
  page, on the commands the router returns unchecked (decision 14.42), and
  refuses a command for any domain but the descriptor's output domain.
  Setting a
  process manager's root to the trigger's correlation is a convention a
  policy may follow by writing it. The correlation ids below are UUIDs, as
  in every scenario (decision 14.18).

  Background:
    Given an "order" aggregate "order-1" on the main timeline
    And an "inventory" aggregate "inv-7" on the main timeline
    And a process manager "Fulfillment" owning domain "fulfillment" whose StartFulfillment command is a starting input

  @policy_addressing
  @C-0656
  Scenario: A policy's command to an aggregate lands on the root it names, with the trigger's correlation
    Given a policy "OrderToInventory" translating from "order" to "inventory" that sends ReserveStock to "inventory" "inv-7" on OrderPlaced
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000040" is recorded for order "order-1"
    Then "inventory" "inv-7" handles ReserveStock
    And the ReserveStock command carries correlation_id "0192a6b0-0000-7000-8000-000000000040"
    And the events it records carry correlation_id "0192a6b0-0000-7000-8000-000000000040"

  # C-0665 (an aggregate command naming no root) is merged into C-0657 and
  # retired: the rule is the same for every target (decision 14.36, as
  # amended).

  @policy_addressing
  @C-0657
  Scenario Outline: A policy's command naming no root is a policy bug, whatever its target
    Given a policy "<policy>" translating from "order" to "<domain>" that sends <command> with no root on OrderPlaced
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000041" is recorded for order "order-1"
    Then the <command> command is refused INTERNAL with reason COORDINATOR_REASON_POLICY_COMMAND_NO_ROOT, naming domain "<domain>" and command type <command>
    And it is dead-lettered and an alert is raised
    And no root is filled in for it, and nothing is appended to any "<domain>" stream

    Examples:
      | policy             | domain      | command          |
      | OrderToInventory   | inventory   | ReserveStock     |
      | OrderToFulfillment | fulfillment | StartFulfillment |

  @policy_addressing
  @C-0658
  Scenario: A policy's command to a process manager lands on the root the policy sets, keeping its trigger's correlation
    Given a policy "OrderToFulfillment" translating from "order" to "fulfillment" that sends StartFulfillment to "fulfillment" "0192a6b0-0000-7000-8000-0000000000aa" on OrderPlaced
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000042" is recorded for order "order-1"
    Then "Fulfillment" handles StartFulfillment on stream "fulfillment" root "0192a6b0-0000-7000-8000-0000000000aa"
    And nothing is appended to stream "fulfillment" root "0192a6b0-0000-7000-8000-000000000042"
    And the StartFulfillment command carries correlation_id "0192a6b0-0000-7000-8000-000000000042"
    And the events "Fulfillment" records carry correlation_id "0192a6b0-0000-7000-8000-000000000042"
    And nothing is dead-lettered and no alert is raised

  @policy_addressing
  @C-0659
  Scenario: One instance named by its root takes the inputs of several correlations, each keeping its own
    Given a policy "OrderToFulfillment" translating from "order" to "fulfillment" that sends StartFulfillment to "fulfillment" "0192a6b0-0000-7000-8000-0000000000aa" on OrderPlaced
    And "Fulfillment" also takes a RecordStockReserved command, which is not a starting input
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000043" is recorded for order "order-1"
    And a client sends a RecordStockReserved command to "fulfillment" root "0192a6b0-0000-7000-8000-0000000000aa" with correlation_id "0192a6b0-0000-7000-8000-000000000044"
    Then stream "fulfillment" root "0192a6b0-0000-7000-8000-0000000000aa" holds the events of both inputs
    And the StartFulfillment events carry correlation_id "0192a6b0-0000-7000-8000-000000000043"
    And the RecordStockReserved events carry correlation_id "0192a6b0-0000-7000-8000-000000000044"

  @policy_addressing
  @C-0660
  Scenario: One event fans out to an aggregate and a process manager, both commands keeping the trigger's correlation
    Given a policy "OrderToInventory" translating from "order" to "inventory" that sends ReserveStock to "inventory" "inv-7" on OrderPlaced
    And a policy "OrderToFulfillment" translating from "order" to "fulfillment" that sends StartFulfillment to "fulfillment" root the trigger's correlation on OrderPlaced
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000045" is recorded for order "order-1"
    Then "inventory" "inv-7" handles ReserveStock with correlation_id "0192a6b0-0000-7000-8000-000000000045"
    And "Fulfillment" handles StartFulfillment on stream "fulfillment" root "0192a6b0-0000-7000-8000-000000000045" with correlation_id "0192a6b0-0000-7000-8000-000000000045"
    And each command's causation_id is the OrderPlaced event of "order" "order-1"

  @policy_addressing
  @C-0666
  Scenario Outline: A policy's command carrying another correlation is a policy bug, whatever its target
    Given a policy "<policy>" translating from "order" to "<domain>" that sends <command> to "<domain>" "0192a6b0-0000-7000-8000-0000000000bb" with correlation_id "0192a6b0-0000-7000-8000-000000000099" on OrderPlaced
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000047" is recorded for order "order-1"
    Then the <command> command is refused INTERNAL with reason COORDINATOR_REASON_POLICY_COMMAND_OTHER_CORRELATION
    And it is dead-lettered and an alert is raised
    And nothing is appended to stream "<domain>" root "0192a6b0-0000-7000-8000-0000000000bb"

    Examples:
      | policy             | domain      | command          |
      | OrderToInventory   | inventory   | ReserveStock     |
      | OrderToFulfillment | fulfillment | StartFulfillment |

  # Moved from handler-contract (decision 14.42): the coordinator, not the
  # router, refuses a command outside the policy's one destination.
  @policy_addressing
  @C-0583
  Scenario: A policy's command for a domain other than its one destination is refused by its coordinator
    Given a policy "OrderToInventory" translating from "order" to "inventory", whose descriptor names output domain "inventory"
    And its business code answers an OrderPlaced event with a ReserveStock command for "inventory" "inv-7" and a ShipOrder command for "shipping"
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000048" is recorded for order "order-1"
    Then the policy's answer is refused INTERNAL with reason COORDINATOR_REASON_POLICY_COMMAND_OUTSIDE_DESTINATION, naming expected_domain "inventory" and actual_domain "shipping"
    And neither command is delivered
    And the OrderPlaced trigger is dead-lettered and an alert is raised

  # Decision 14.48, amended: every dead letter is keyed, so the identity
  # and Fact checks on a policy's reactions carry keys of their own.
  @policy_addressing
  @C-0734
  Scenario Outline: A policy's reaction that sets its own identity, is a Fact, or carries a fact origin is a policy bug
    Given a policy "OrderToInventory" translating from "order" to "inventory" whose business code answers an OrderPlaced event with a ReserveStock <reaction> for "inventory" "inv-7" that <fault>
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000049" is recorded for order "order-1"
    Then the policy's answer is refused INTERNAL with reason <reason>, naming domain "inventory" and command type ReserveStock<field>
    And nothing is delivered and nothing is appended to stream "inventory" root "inv-7"
    And the OrderPlaced trigger is dead-lettered with reason <reason> as rejected_events, and an alert is raised
    And the dead letter's processing_failed details have failure code <reason>, retry_count 0 and is_transient false

    Examples:
      | reaction | fault                                        | reason                                           | field                     |
      | command  | sets a command_id other than the stamped one | COORDINATOR_REASON_POLICY_COMMAND_OTHER_IDENTITY | , with field command_id   |
      | command  | sets a causation_id other than the trigger's | COORDINATOR_REASON_POLICY_COMMAND_OTHER_IDENTITY | , with field causation_id |
      | fact     | has a fact_origin                            | COORDINATOR_REASON_POLICY_COMMAND_IS_FACT        |                           |
      | command  | carries a fact_origin                        | COORDINATOR_REASON_POLICY_COMMAND_FACT_ORIGIN    |                           |

  @policy_addressing
  @C-0735
  Scenario: A reaction command carrying a fact origin that reaches its destination is dead-lettered there as a reactor bug
    Given a policy-emitted ReserveStock command for "inventory" "inv-7" that carries a fact_origin
    When the "inventory" coordinator processes the command
    Then the command is not delivered to business code
    And the command is dead-lettered with reason COORDINATOR_REASON_POLICY_COMMAND_FACT_ORIGIN, domain "inventory" and command_type "ReserveStock", as rejected_command, and an alert is raised
    And no events are persisted

  # Decision 14.49: conditions any coordinator may enforce on a policy's
  # reactions, each keyed.
  @policy_addressing
  @C-0736
  Scenario: A policy's reaction command for a domain its coordinator cannot route is a policy bug
    Given a policy "OrderToInventory" translating from "order" to "inventory", whose descriptor names output domain "inventory"
    And no component of "inventory" is registered with the policy's coordinator, and it knows no coordinator for "inventory"
    And its business code answers an OrderPlaced event with a ReserveStock command for "inventory" "inv-7"
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000050" is recorded for order "order-1"
    Then the policy's answer is refused INTERNAL with reason COORDINATOR_REASON_POLICY_COMMAND_UNKNOWN_DOMAIN, naming domain "inventory" and command type ReserveStock
    And nothing is delivered
    And the OrderPlaced trigger is dead-lettered with reason COORDINATOR_REASON_POLICY_COMMAND_UNKNOWN_DOMAIN as rejected_events, and an alert is raised
    And the dead letter's processing_failed details have failure code COORDINATOR_REASON_POLICY_COMMAND_UNKNOWN_DOMAIN, retry_count 0 and is_transient false

  @policy_addressing
  @C-0737
  Scenario: A chain of reactions deeper than the coordinator's limit stops at the limit
    # A limit a coordinator may enforce, where it follows the chain from
    # the input a caller sent (that input at depth 0).
    Given a coordinator whose chain depth limit is 8
    And a policy "OrderToOrder" translating from "order" to "order" whose business code answers every ItemAdded event with an AddItem command for the same order
    When an AddItem command for "order" "order-1" is handled
    Then the reaction at depth 9 is not delivered
    And the ItemAdded event that triggered it is dead-lettered with reason COORDINATOR_REASON_CHAIN_TOO_DEEP and arg limit "8" as rejected_events, and an alert is raised
    And the dead letter's processing_failed details have failure code COORDINATOR_REASON_CHAIN_TOO_DEEP, retry_count 0 and is_transient false
