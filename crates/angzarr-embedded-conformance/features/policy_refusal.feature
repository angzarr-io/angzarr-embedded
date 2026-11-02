Feature: Policy refusals - a refused command becomes one fact for the policy's own source domain
  A policy's target, an aggregate or a process manager alike, refuses a
  policy's command the way it refuses any command, optionally with a typed
  refusal object of its own domain, and records nothing. The policy then
  yields one unrefusable refusal fact for the exact stream of its
  triggering event (decision 2.2, as amended by 14.31 and 14.42). The
  coordinator calls the policy's OnRefused. By default the policy has no
  translation, and the COORDINATOR builds the standard CommandRefused {
  command, refusal, trigger }, the typed refusal object packed, which the
  source's on_refused_<Command> handles; where angzarr-cli selects
  translate, the policy's callback turns Refusal { event, command, response
  } into a domestic fact, so the source need not unpack a foreign object.
  Either way the coordinator addresses the fact and stamps its identity:
  the refused command's id. Both calls go through the outbox.

  Background:
    Given a "wallet" aggregate that refuses HoldFunds with FAILED_PRECONDITION reason INSUFFICIENT_AVAILABLE_FUNDS and the refusal object InsufficientFunds when funds are short
    And a process manager "BuyIn" owning domain "buy-in"
    And a policy "BuyInToWallet" translating from "buy-in" to "wallet" that builds HoldFunds from BuyInStarted

  @policy_refusal
  @C-0613
  Scenario: A refused command is passed through as CommandRefused and handled by the source's generated handler
    Given "BuyInToWallet" was generated with HoldFunds in pass-through mode
    And "BuyIn" defines an on_refused_HoldFunds handler
    And wallet "p-1" has too little available to hold 500
    When "BuyIn" records BuyInStarted for 500 from wallet "p-1" under correlation "0192a6b0-0000-7000-8000-000000000020"
    Then wallet "p-1" refuses HoldFunds and records nothing
    And "BuyInToWallet"'s OnRefused answers no translation
    And the coordinator builds a CommandRefused fact carrying only the HoldFunds sent, the InsufficientFunds refusal object packed and the BuyInStarted event's EventRef
    And it is delivered to "buy-in" root "0192a6b0-0000-7000-8000-000000000020"
    And "BuyIn"'s on_refused_HoldFunds handler unpacks InsufficientFunds and records its own events for it

  @policy_refusal
  @C-0614
  Scenario: A refused command is translated into a domestic fact by the policy's callback
    Given "BuyInToWallet" was generated with HoldFunds in translate mode, its callback returning the "buy-in" fact HoldFundsDeclined
    And wallet "p-1" has too little available to hold 500
    When "BuyIn" records BuyInStarted for 500 from wallet "p-1" under correlation "0192a6b0-0000-7000-8000-000000000021"
    Then "BuyInToWallet"'s callback is called with one Refusal holding the BuyInStarted event, the HoldFunds sent and the refusal response with the InsufficientFunds refusal object
    And a HoldFundsDeclined fact is delivered to "buy-in" root "0192a6b0-0000-7000-8000-000000000021"
    And "BuyIn" handles it as an ordinary fact, with no reference to the wallet's protos

  @policy_refusal
  @C-0615
  Scenario Outline: The refusal fact's id is the refused command's id, in either mode
    Given "BuyInToWallet" was generated with HoldFunds in <mode> mode
    And wallet "p-1" has too little available to hold 500
    When "BuyIn" records BuyInStarted for 500 from wallet "p-1" under correlation "0192a6b0-0000-7000-8000-000000000022"
    Then the refusal fact's command_id is the refused HoldFunds' command_id
    And its fact_origin external_id is that command_id's canonical text
    And its causation_id names the BuyInStarted event
    And its correlation_id is "0192a6b0-0000-7000-8000-000000000022"
    And wallet "p-1"'s stream holds no event with that command_id

    Examples:
      | mode         |
      | pass-through |
      | translate    |

  @policy_refusal
  @C-0616
  Scenario: The refusal fact replaces the command's outbox entry in one transaction
    Given "BuyInToWallet" was generated with HoldFunds in pass-through mode
    And "BuyIn" defines an on_refused_HoldFunds handler
    And wallet "p-1" has too little available to hold 500
    And the policy's coordinator crashes after OnRefused answers and before its transaction commits
    When the coordinator restarts
    Then HoldFunds is delivered again and refused again
    And "BuyIn" records the refusal once

  @policy_refusal
  @C-0617
  Scenario Outline: Only a business rejection yields a refusal fact, and the status decides which is one
    # Decision 14.37 (owner rulings, 2026-10-29): the gRPC status decides
    # the class. FAILED_PRECONDITION, INVALID_ARGUMENT, NOT_FOUND,
    # ALREADY_EXISTS, OUT_OF_RANGE and PERMISSION_DENIED are business
    # refusals, never retried; ABORTED is an immediate-return business
    # error, whose policy step the runtime reloads and re-runs under the
    # capped backoff; INTERNAL, UNKNOWN and DATA_LOSS are fatal bugs,
    # dead-lettered at once; the rest are retryable bugs, retried with
    # backoff and jitter up to a configured cap, then dead-lettered.
    # (Decision 14.37, owner's rulings 2026-10-29, latest: four classes.)
    Given wallet "p-1" answers HoldFunds with <status>
    When "BuyIn" records BuyInStarted for 500 from wallet "p-1"
    Then "BuyInToWallet"'s OnRefused <on_refused>
    And <outcome>

    Examples:
      | status           | on_refused                 | outcome                                                                                                      |
      | UNAVAILABLE      | is not called              | HoldFunds is retried with backoff up to its configured cap, then dead-lettered and alerted                   |
      | ABORTED          | is not called              | the policy step is reloaded and re-run with backoff up to its configured cap, then dead-lettered and alerted |
      | INVALID_ARGUMENT | is called with the refusal | HoldFunds is neither retried nor dead-lettered                                                               |
      | INTERNAL         | is not called              | HoldFunds is dead-lettered and an alert is raised                                                            |

  @policy_refusal
  @C-0618
  Scenario: A pass-through refusal with no handler at the source is dead-lettered for a human
    Given "BuyInToWallet" was generated with HoldFunds in pass-through mode
    And "BuyIn" defines no on_refused_HoldFunds handler
    And wallet "p-1" has too little available to hold 500
    When "BuyIn" records BuyInStarted for 500 from wallet "p-1"
    Then the CommandRefused fact is refused ROUTER_REASON_NO_REFUSAL_HANDLER, naming command_type HoldFunds, dead-lettered and alerted at run time
    And nothing is recorded in "buy-in" or "wallet"

  @policy_refusal
  @C-0619
  Scenario: A failing translating callback is dead-lettered and alerted
    Given "BuyInToWallet" was generated with HoldFunds in translate mode, its callback returning the "buy-in" fact HoldFundsDeclined
    And "BuyInToWallet"'s callback fails with INTERNAL
    And wallet "p-1" has too little available to hold 500
    When "BuyIn" records BuyInStarted for 500 from wallet "p-1"
    Then the refusal is dead-lettered and an alert is raised
    And nothing is recorded in "buy-in"

  @policy_refusal
  @C-0621
  Scenario Outline: A refusal fact for any stream but its trigger's is refused
    # A refusal fact is accepted only by the exact stream its trigger
    # EventRef names: domain, root and edition (decision 14.31).
    Given "BuyInToWallet" was generated with HoldFunds in translate mode, its callback returning the "buy-in" fact HoldFundsDeclined
    And "BuyInToWallet"'s callback returns a fact addressed to <stream>
    And wallet "p-1" has too little available to hold 500
    When "BuyIn" records BuyInStarted for 500 from wallet "p-1" under correlation "0192a6b0-0000-7000-8000-000000000023"
    Then the fact is refused as malformed, dead-lettered and alerted
    And nothing is recorded in <stream>

    Examples:
      | stream                                                                   |
      | domain "table"                                                           |
      | "buy-in" root "0192a6b0-0000-7000-8000-000000000099"                     |
      | "buy-in" root "0192a6b0-0000-7000-8000-000000000023" in edition "what-if" |

  @policy_refusal
  @C-0622
  Scenario: The mechanism is the same when the policy's target is a process manager
    Given a process manager "Fulfillment" owning domain "fulfillment" that refuses RecordStockReserved once completed
    And a policy "InventoryToFulfillment" translating from "inventory" to "fulfillment", generated with RecordStockReserved in pass-through mode
    And the "inventory" aggregate defines an on_refused_RecordStockReserved handler
    When "Fulfillment" refuses the RecordStockReserved built from StockReserved of item "sku-1"
    Then "Fulfillment" records nothing
    And a CommandRefused fact is delivered to "inventory" item "sku-1"

  @policy_refusal
  @C-0624
  Scenario: A command handler's typed refusal object reaches the policy's source, packed
    Given "BuyInToWallet" was generated with HoldFunds in pass-through mode
    And "BuyIn" defines an on_refused_HoldFunds handler
    And wallet "p-1" has 120 available and is asked to hold 500
    When "BuyIn" records BuyInStarted for 500 from wallet "p-1"
    Then wallet "p-1"'s HoldFunds handler returns the refusal object InsufficientFunds with available 120 and requested 500
    And the refused response the policy receives carries that InsufficientFunds as its refusal and no text
    And the CommandRefused fact's refusal unpacks to InsufficientFunds with available 120 and requested 500

  @policy_refusal
  @error_keys
  @C-0674
  Scenario: An untyped refusal reaches the policy as its code and args, never text
    # Decision 14.37: a rejection with no typed refusal object carries a
    # diagnostic code and its args; RefusalResponse.diagnostic holds them.
    Given "BuyInToWallet" was generated with HoldFunds in translate mode, its callback returning the "buy-in" fact HoldFundsDeclined
    And wallet "p-1" is closed, so its HoldFunds handler rejects with code ACCOUNT_CLOSED and arg wallet "p-1" and no refusal object
    When "BuyIn" records BuyInStarted for 500 from wallet "p-1"
    Then "BuyInToWallet"'s callback is called with a refusal response whose diagnostic has code ACCOUNT_CLOSED and arg wallet "p-1"
    And the refusal response carries no refusal object and no text

  @policy_refusal
  @C-0685
  Scenario Outline: A translated fact that is more than a payload is refused as a policy bug
    # The business process answers a translated fact's payload alone; the
    # coordinator addresses and identifies it (decision 14.42).
    Given "BuyInToWallet" was generated with HoldFunds in translate mode, its callback returning the "buy-in" fact HoldFundsDeclined
    And "BuyInToWallet"'s OnRefused answers a fact that <fault>
    And wallet "p-1" has too little available to hold 500
    When "BuyIn" records BuyInStarted for 500 from wallet "p-1"
    Then the fact is refused INTERNAL with reason <reason>, dead-lettered and alerted
    And nothing is recorded in "buy-in"

    Examples:
      | fault                   | reason                                     |
      | sets its own cover      | COORDINATOR_REASON_REFUSAL_FACT_ADDRESSED  |
      | sets its own handling   | COORDINATOR_REASON_REFUSAL_FACT_ADDRESSED  |
      | has no page             | COORDINATOR_REASON_REFUSAL_FACT_NO_PAGE    |
      | has a Command page      | COORDINATOR_REASON_REFUSAL_FACT_NOT_A_FACT |
      | sets its own command_id | COORDINATOR_REASON_REFUSAL_FACT_IDENTIFIED |
