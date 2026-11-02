Feature: Policy facts - a policy emits what its destination declares
  A policy translates its source's events into what its destination takes
  (decision 14.69): a command for a type the destination declares a
  command, which it may refuse (policy_refusal.feature), and a fact for a
  type it declares a fact (CommandOptions.fact, options.proto), which it
  records and never refuses. One reaction may hold both arms, in the order
  business code returned them; which arm a type takes is the destination's
  declaration, read by generated code, never business code's choice.

  The policy's coordinator checks a fact as it checks a command
  (policy_addressing.feature) and stamps it the same identity: its
  command_id is the reaction command id of its trigger, the policy's name
  and its index in the reaction's output, commands and facts counted in one
  index space; its fact_origin.external_id is that id's canonical text; its
  causation is the trigger and its correlation the trigger's. A fact whose
  handler fails is dead-lettered at its destination, and the policy's step
  completes. An arm its destination does not declare is a bug, refused at
  the destination. A policy may address its own domain, at another root.
  The correlation ids below are UUIDs, as in every scenario (decision
  14.18).

  Background:
    Given an "order" aggregate "order-1" on the main timeline
    And an "inventory" aggregate "inv-7" on the main timeline
    And the inventory aggregate declares fact StockHoldRequested

  @policy_facts
  @C-0775
  Scenario: A policy's fact for a type its destination declares is recorded with its reaction's identity
    Given a policy "OrderToInventory" translating from "order" to "inventory" that sends the fact StockHoldRequested to "inventory" "inv-7" on OrderPlaced
    When an OrderPlaced event with correlation "0192a6b0-0000-7000-8000-000000000060" is recorded for order "order-1"
    Then "inventory" "inv-7" records the StockHoldRequested fact
    And the fact's command_id is the reaction command id of that OrderPlaced event, the policy "OrderToInventory" and index 0
    And the fact's fact_origin external_id is that command_id's canonical text
    And the StockHoldRequested events carry correlation_id "0192a6b0-0000-7000-8000-000000000060"
    And nothing is dead-lettered and no alert is raised

  @policy_facts
  @C-0776
  Scenario: A reaction's commands and facts share one index space
    Given a policy "OrderToInventory" translating from "order" to "inventory" whose business code answers an OrderPlaced event with a ReserveStock command and then a StockHoldRequested fact for "inventory" "inv-7"
    When an OrderPlaced event is recorded for order "order-1"
    Then "inventory" "inv-7" handles ReserveStock
    And "inventory" "inv-7" records the StockHoldRequested fact
    And the ReserveStock command's command_id is the reaction command id of that OrderPlaced event, the policy "OrderToInventory" and index 0
    And the fact's command_id is the reaction command id of that OrderPlaced event, the policy "OrderToInventory" and index 1

  @policy_facts
  @C-0777
  Scenario: A type its destination comes to declare a fact keeps its reaction's id, so a re-run deduplicates
    # Owner's ruling (decision 14.69): one id for either arm, so a re-run
    # across the change is answered DUPLICATE.
    Given a policy "OrderToInventory" translating from "order" to "inventory" that sends ReserveStock to "inventory" "inv-7" on OrderPlaced
    And an OrderPlaced event was recorded for order "order-1" and its ReserveStock applied by "inventory" "inv-7"
    And "inventory" now declares ReserveStock a fact, and the policy sends it as one
    When the policy is run again over that OrderPlaced event
    Then the ReserveStock fact carries the command_id the ReserveStock command carried
    And it is answered DUPLICATE, and nothing new is appended to stream "inventory" root "inv-7"

  @policy_facts
  @C-0778
  Scenario: A policy's fact whose handler fails is dead-lettered at its destination, and the policy's step completes
    Given a policy "OrderToInventory" translating from "order" to "inventory" whose business code answers an OrderPlaced event with a StockHoldRequested fact and then a ReserveStock command for "inventory" "inv-7"
    And "inventory"'s StockHoldRequested handler fails with INTERNAL
    When an OrderPlaced event is recorded for order "order-1"
    Then the StockHoldRequested fact is dead-lettered as rejected_command, and an alert is raised
    And "inventory" "inv-7" handles ReserveStock
    And the OrderPlaced trigger is not dead-lettered
    And the policy "OrderToInventory"'s position on order "order-1" is past the OrderPlaced event

  @policy_facts
  @C-0779
  Scenario Outline: An arm its destination does not declare is refused there as a bug and dead-lettered
    # Owner's ruling (decision 14.69): the descriptor names no fact types,
    # so the policy's coordinator cannot check the arm; the destination's
    # router refuses one that does not match its handler's kind.
    Given a policy "OrderToInventory" translating from "order" to "inventory" that sends <type> as a <arm> to "inventory" "inv-7" on OrderPlaced
    When an OrderPlaced event is recorded for order "order-1"
    Then the <type> <arm> is refused INTERNAL with reason <reason> by "inventory", dead-lettered as rejected_command, and an alert is raised
    And nothing is appended to stream "inventory" root "inv-7"

    Examples:
      | type               | arm     | reason                             |
      | ReserveStock       | fact    | ROUTER_REASON_NO_FACT_HANDLER      |
      | StockHoldRequested | command | ROUTER_REASON_UNKNOWN_COMMAND_TYPE |

  @policy_facts
  @C-0780
  Scenario: A policy may address its own domain, at another root
    # Owner's ruling (decision 14.69): a same-domain policy is allowed.
    Given a policy "OrderToOrder" translating from "order" to "order" that sends AddItem to "order" "order-2" on OrderPlaced
    When an OrderPlaced event is recorded for order "order-1"
    Then "order" "order-2" handles AddItem
    And nothing is dead-lettered and no alert is raised
