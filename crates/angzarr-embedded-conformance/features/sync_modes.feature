Feature: Sync modes - how much downstream work a command waits for
  CommandRequest.sync_mode decides what HandleCommand has finished when it
  returns. Every mode persists the aggregate's events before returning (or
  returns the rejection). They differ in downstream work:

  - ASYNC: projectors and policies/PMs run asynchronously from the bus.
  - DECISION: as ASYNC; the point is that acceptance or rejection is the
    synchronous answer.
  - SIMPLE: projectors run synchronously and their projections are in the
    response; policies/PMs run asynchronously.

  An unset sync_mode (SYNC_MODE_UNSPECIFIED) is ASYNC. DECISION exists for
  callers that need only the accept/reject outcome synchronously.
  PageHeader.sync_mode on a CommandPage is that command's own mode. The
  caller's mode is a floor for everything its command sets off: a reaction
  command runs with the stronger of the caller's mode and its own
  (ASYNC < DECISION < SIMPLE). Its own mode can raise how much of it the
  caller waits for, never lower it. When unset the caller's mode applies.

  CASCADE, ISOLATED and the cascade error modes are deleted (decision 2.1,
  backlog RM-1): no mode waits for reactions, and every mode publishes the
  persisted events downstream. Their scenarios (C-0430, C-0431, C-0432,
  C-0433) are retired.

  Background:
    Given an "order" aggregate that accepts CreateOrder and rejects CancelOrder for unknown orders
    And a projector "OrderSummary" subscribed to "order"
    And a policy "OrderFulfillment" translating OrderCreated into a ReserveStock command for "inventory"

  @C-0425
  Scenario: ASYNC returns after persisting, with no projections
    When a CreateOrder command is handled with sync_mode ASYNC
    Then the response contains the OrderCreated event
    And the response contains no projections
    And the OrderCreated event is published to the bus
    And the OrderSummary projector and the OrderFulfillment policy receive it from the bus

  @C-0426
  Scenario: An unset sync_mode behaves as ASYNC
    When a CreateOrder command is handled with no sync_mode set
    Then the response contains the OrderCreated event
    And the response contains no projections

  @C-0427
  Scenario: DECISION surfaces acceptance synchronously and defers downstream work
    When a CreateOrder command is handled with sync_mode DECISION
    Then the response contains the OrderCreated event
    And the response contains no projections
    And the OrderCreated event is published to the bus

  @C-0428
  Scenario: DECISION surfaces rejection synchronously
    When a CancelOrder command for an unknown order is handled with sync_mode DECISION
    Then the command fails with the handler's rejection reason
    And no events are persisted

  @C-0429
  Scenario: SIMPLE waits for projectors only
    When a CreateOrder command is handled with sync_mode SIMPLE
    Then the response contains the OrderCreated event
    And the response contains the OrderSummary projection for the OrderCreated event
    And the OrderFulfillment policy has not run when the response is returned
    And the OrderCreated event is published to the bus

  # The scenarios that were here (C-0434, C-0435, C-0507, C-0508, C-0512)
  # had a process manager emit reaction commands with their own
  # PageHeader.sync_mode. A process manager sends no commands (decision
  # 14.31), so they are retired. What a policy's reaction command waits for
  # comes from the policy's configuration, never from the command (backlog
  # MD-5); the rest of this file awaits the rewrite in backlog DOC-5.

  @error_keys
  @C-0676
  Scenario: A rejection reaches the caller as the handler's key and args
    # Decision 14.37: the status message is the reason key; the coordinator
    # adds no text of its own.
    Given the CancelOrder handler rejects an unknown order with code ORDER_UNKNOWN and arg order "o-9"
    When a CancelOrder command for order "o-9" is handled with sync_mode DECISION
    Then the command fails FAILED_PRECONDITION with reason ORDER_UNKNOWN, which is also its status message
    And the rejection's error info carries order "o-9"

  @error_keys
  @C-0677
  Scenario: A failing synchronous projector is reported by its code and a diagnostic
    Given a projector "OrderSummary" whose handler fails UNAVAILABLE with code VIEW_STORE_UNAVAILABLE
    When a CreateOrder command is handled with sync_mode SIMPLE
    Then the command succeeds
    And the outcome's projector_failures holds "OrderSummary" with code UNAVAILABLE and diagnostic code VIEW_STORE_UNAVAILABLE
    And the projector failure carries no text
