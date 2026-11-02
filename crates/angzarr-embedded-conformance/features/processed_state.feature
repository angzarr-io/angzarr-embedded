Feature: Processed state - what a projector or a policy has processed outlives a purge
  The driver of a projector, or of a policy (a saga: a process manager is
  composed of policies, decision 14.31), keeps the record of what its
  handler has processed: every stream it has seen, and where it is on each
  (decision 14.68). The record is the driver's own: a purge removes a
  stream, its snapshot, its command ids and its dead letters, never the
  record. A driver finds a stream gone by comparing its record with the
  store's listing of streams, hands its handler the stream's gone book (its
  cover, no page, next_sequence 0), then forgets the stream. A purge made
  while no driver runs is found so at the next one.

  @C-0771
  Scenario: A projector's record of a stream outlives the stream's purge
    Given a projector "OrderSummary" of domain "order" that has processed order "order-1" to sequence 3
    When order "order-1" is purged
    Then the record of "OrderSummary" still holds order "order-1" at sequence 3

  @C-0772
  Scenario: A stream purged while no catch-up ran is handed gone at the next one
    Given a projector "OrderSummary" of domain "order" that has processed order "order-1" to sequence 3
    And order "order-1" is purged while "OrderSummary" is not running
    When "OrderSummary" catches up
    Then "OrderSummary" is handed order "order-1" as gone
    And the record of "OrderSummary" no longer holds order "order-1"

  @C-0773
  Scenario: A policy is told a purged source stream is gone
    Given a policy "OrderFulfillment" translating from "order" to "inventory" that has processed order "order-1" to sequence 2
    And order "order-1" is purged while "OrderFulfillment" is not running
    When "OrderFulfillment" catches up
    Then "OrderFulfillment" is handed order "order-1" as gone, and answers nothing
    And the record of "OrderFulfillment" no longer holds order "order-1"

  @C-0774
  Scenario: A stream the record holds and the store still lists is not gone
    Given a projector "OrderSummary" of domain "order" that has processed order "order-1" to sequence 3
    When "OrderSummary" catches up
    Then "OrderSummary" is handed nothing of order "order-1"
    And the record of "OrderSummary" still holds order "order-1" at sequence 3
