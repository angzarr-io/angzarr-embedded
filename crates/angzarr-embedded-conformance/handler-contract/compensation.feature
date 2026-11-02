Feature: Compensation - a process manager's rollback, kept by the client library
  A process manager that cannot finish undoes what it did (decision 14.74).
  Each step is paired in code with its undo, never in proto (decision 2.3):
  an event of the PM's own that its outbound policy turns into what the
  step's destination declares, a fact where it can (decision 14.69), so an
  undo is never refused and compensation never loops. The journal is the
  PM's own events, folded into its state: a step is completed once its
  reply is folded. The client library keeps the pairing and builds the
  rollback, so authors never write it twice.

  Bound by each client library's process-manager tooling, angzarr-embedded's
  markup (Rust) and client-python, so the two agree; it is no router's
  dispatch. The fixture is fixture/compensation.proto: the shipment process
  manager, whose steps are, in order, "reserve stock" (completed by
  StockReserved, undone by StockReleaseDue, which becomes ReleaseStock) and
  "charge payment" (PaymentCharged, undone by RefundDue, which becomes
  RefundPayment); its failed event is ShipmentFailed.

  Background:
    Given the shipment process manager's steps, registered at startup

  @C-0787
  Scenario: A rollback records the failure, then undoes each completed step, newest first
    Given the shipment has completed "reserve stock" and "charge payment"
    When it rolls back because "address undeliverable"
    Then it records ShipmentFailed "address undeliverable", RefundDue and StockReleaseDue, in that order

  @C-0788
  Scenario: A rollback undoes only the steps that completed
    Given the shipment has completed "reserve stock"
    When it rolls back because "payment declined"
    Then it records ShipmentFailed "payment declined" and StockReleaseDue, in that order

  @C-0789
  Scenario: A rollback before any step completed records the failure alone
    Given the shipment has completed no step
    When it rolls back because "out of stock"
    Then it records ShipmentFailed "out of stock" alone

  @C-0790
  Scenario: A reply that arrives after the rollback is recorded and undone at once
    # The one subtle case (steps in flight together): the tooling owns it,
    # so no author reinvents it.
    Given the shipment has completed "reserve stock" and rolled back
    When the reply completing "charge payment" arrives
    Then it records PaymentCharged and RefundDue, in that order

  @C-0791
  Scenario: A reply before any rollback is recorded alone
    Given the shipment has completed "reserve stock"
    When the reply completing "charge payment" arrives
    Then it records PaymentCharged alone

  @C-0792
  Scenario Outline: Registering a step checks its undo's target against the descriptor
    # Decision 14.74, the owner's rulings 6 and Q10: which event undoes which
    # step is known only in code, so the library checks at startup, when the
    # step is registered, and warns of an undo that may be refused.
    Given "reserve stock" is registered with an undo that becomes <target>
    When the shipment process manager starts
    Then <warning>

    Examples:
      | target                           | warning                                                                                                  |
      | "test.compensation.ReleaseStock" | registration reports no warning                                                                          |
      | "test.compensation.RestockItems" | registration warns that the undo of "reserve stock" targets the command "test.compensation.RestockItems" |

  @C-0793
  Scenario: An undo its destination refuses records CompensationFailed
    # Ruling 6: a refused undo makes the PM record CompensationFailed. The
    # undo here targets a command, which registration warned of (C-0792).
    Given "reserve stock" is registered with an undo that becomes "test.compensation.RestockItems"
    And the shipment has completed "reserve stock" and rolled back
    When the refusal of its "test.compensation.RestockItems" reaches the shipment
    Then it records CompensationFailed for "reserve stock"

  @C-0796
  Scenario: A refused undo's handling succeeds and dead-letters nothing
    # Owner decision 38 (a), 2026-11-02 (14.74): CompensationFailed is the
    # alert. A handling either records events or is dead-lettered, never
    # both, so the refusal is not dead-lettered; a human learns of it by
    # watching CompensationFailed, through a projector or subscriber.
    Given "reserve stock" is registered with an undo that becomes "test.compensation.RestockItems"
    And the shipment has completed "reserve stock" and rolled back
    When the refusal of its "test.compensation.RestockItems" reaches the shipment
    Then the handling succeeds
    And it records CompensationFailed for "reserve stock"
    And nothing is dead-lettered for the refusal
