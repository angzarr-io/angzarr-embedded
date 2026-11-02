Feature: Editions are per-stream forks, and the fork travels with the data
  An edition is a set of per-stream forks (decision 14.32). Forking an
  origin stream (D, main, R) into edition X appends an EditionCreated
  meta-event (io.angzarr.v1.meta) to (D, X, R) at the sequence it states,
  its parent_sequence: any at or below main's head for that stream (main's
  next sequence), never above it (decision 14.36). ForkStream always
  states it; an arrival's is the main head the system layer read. The fork
  point is per stream, and no cross-stream snapshot is promised; (D, X, R) reads
  main's events below that fork sequence, then X's own. The fork travels
  with the data: a policy's command in X that reaches a stream with no fork
  in X forks it on arrival. A command from outside to an unforked stream is
  refused until propagated. There is no edition registry: an edition is
  known while any creation event of it exists. Deletion is an event too:
  each stream it reaches records an EditionDeleted meta-event in sequence,
  the event propagates onward through the policy flows, and only then are
  that stream's rows of the edition removed, the event included. Main
  knows nothing of an edition.

  Background:
    Given a "player" aggregate "p-1" with 5 events on the main timeline
    And a policy "PlayerToTable" translating from "player" to "table" that sends SeatPlayer to "table" "t-1" on PlayerReady
    And a "table" aggregate "t-1" with 3 events on the main timeline

  @editions
  @C-0628
  Scenario: Forking an origin stream at main's next sequence appends the fork event there
    When ForkStream forks "player" "p-1" into edition "what-if" at parent_sequence 5
    Then "player" "p-1" in edition "what-if" holds an EditionCreated event at sequence 5
    And the EditionCreated event names edition "what-if" and parent_sequence 5, with no propagated_from and no forked_by
    And the main timeline of "player" "p-1" is unchanged

  @editions
  @C-0629
  Scenario: A forked stream reads main below its fork sequence, then its own events
    Given "player" "p-1" is forked into edition "what-if" at sequence 5
    And "player" "p-1" on the main timeline gains 2 more events
    And "player" "p-1" in edition "what-if" gains 1 event of its own
    When "player" "p-1" is loaded in edition "what-if"
    Then the loaded history is main's events 0 to 4, then the fork event at 5, then the edition's own event at 6
    And none of main's events at or after sequence 5 are in it

  @editions
  @C-0630
  Scenario: A policy command arriving at an unforked stream forks it, then is handled
    Given "player" "p-1" is forked into edition "what-if" at sequence 5
    And "table" "t-1" has no fork in edition "what-if"
    When "player" "p-1" records PlayerReady in edition "what-if"
    Then "table" "t-1" in edition "what-if" first gains an EditionCreated event at sequence 3
    And its propagated_from names the fork event of "player" "p-1" in edition "what-if"
    And its forked_by is the SeatPlayer command's command_id
    And the SeatPlayer command is then handled in edition "what-if"
    And no policy business code was called for either fork event

  @editions
  @C-0631
  Scenario: A stream is forked once, whatever arrives later
    Given "player" "p-1" is forked into edition "what-if"
    And "table" "t-1" was forked in edition "what-if" by an earlier SeatPlayer
    When "player" "p-1" records PlayerReady again in edition "what-if"
    Then "table" "t-1" in edition "what-if" still holds exactly one EditionCreated event
    And the new SeatPlayer command is handled in edition "what-if"

  # C-0632 (a propagation hop bound) is retired: the fork travels with the
  # data, which is its only bound (decision 14.32).

  @editions
  @C-0633
  Scenario Outline: An outside input to a stream not forked in the edition is refused until propagated
    Given "player" "p-1" is forked into edition "what-if"
    And "table" "t-9" with 4 events on the main timeline has no fork in edition "what-if"
    When a client sends <input> to "table" "t-9" in edition "what-if"
    Then the client is answered FAILED_PRECONDITION with reason EDITION_NOT_FORKED
    And "table" "t-9" holds no event in edition "what-if"

    Examples:
      | input       |
      | a command   |
      | a fact      |

  @editions
  @C-0634
  Scenario: A query of a stream not yet forked reads main and forks nothing
    Given "player" "p-1" is forked into edition "what-if"
    And "table" "t-9" with 4 events on the main timeline is not forked in "what-if"
    When "table" "t-9" is queried in edition "what-if"
    Then the result is main's 4 events of "table" "t-9"
    And "table" "t-9" holds no event in edition "what-if"

  @editions
  @C-0635
  Scenario Outline: An unknown edition is refused
    Given <history>
    When a command is sent to "player" "p-1" in edition "what-if"
    Then the command is refused NOT_FOUND
    And nothing is written in edition "what-if"

    Examples:
      | history                                                                      |
      | no fork event of edition "what-if" exists anywhere                           |
      | edition "what-if" was forked and then deleted with DeleteEdition             |

  @editions
  @C-0636
  Scenario: Editions are listed from the fork events, with no registry
    Given "player" "p-1" is forked into edition "what-if"
    And a SeatPlayer command has forked "table" "t-1" in edition "what-if"
    When ListEditions is called
    Then the answer lists edition "what-if" with one origin, the fork of "player" "p-1", and 2 forked streams

  @editions
  @C-0637
  Scenario: A delete is recorded after the in-flight work before it, in the origin's own domain
    Given "player" "p-1" is forked into edition "what-if"
    And "player" "p-1" recorded PlayerReady in edition "what-if", and its SeatPlayer is still in the policy's outbox
    When DeleteEdition deletes "what-if"
    Then "player" "p-1" in edition "what-if" records an EditionDeleted event after PlayerReady
    And the SeatPlayer command reaches "table" "t-1" before any deletion does, forking it and being handled in "what-if"
    And nothing polled, waited on a timer or retried

  @editions
  @C-0640
  Scenario: After a delete, a later arrival in the edition is refused and re-creates nothing
    Given edition "what-if" was deleted and no row of it remains
    When a policy command in edition "what-if" arrives at "table" "t-1"
    Then the command is refused NOT_FOUND
    And "table" "t-1" holds no event in edition "what-if"
    And ListEditions does not list edition "what-if"
    And only ForkStream can bring the name "what-if" back

  @editions
  @C-0641
  Scenario: A removal that fails is dead-lettered and alerted
    Given "player" "p-1" is forked into edition "what-if"
    And the edition store refuses to remove the rows of edition "what-if"
    When DeleteEdition deletes "what-if" and its EditionDeleted has propagated onward
    Then the failed removal is dead-lettered with reason COORDINATOR_REASON_EDITION_REMOVAL_FAILED and edition "what-if", and an alert is raised
    And the dead letter's failed_edition_removal is "player" "p-1"'s stream in edition "what-if" with its EditionDeleted event
    And the dead letter's processing_failed details have failure code COORDINATOR_REASON_EDITION_REMOVAL_FAILED

  @editions
  @C-0642
  Scenario Outline: Creating and deleting an edition leaves main byte-for-byte unchanged
    Given a snapshot of main's <part> for "player" "p-1" and "table" "t-1"
    When ForkStream forks "player" "p-1" into edition "what-if"
    And "player" "p-1" records PlayerReady in edition "what-if", forking "table" "t-1"
    And DeleteEdition deletes "what-if" and the deletion completes
    Then main's <part> are byte-for-byte unchanged

    Examples:
      | part                          |
      | streams                       |
      | policy outbox entries           |
      | policy and projector positions  |
      | projections                   |

  @editions
  @C-0643
  Scenario: Edition lifecycle events are never written to, or read from, main
    When ForkStream forks "player" "p-1" into edition "what-if"
    And ListEditions is called
    And DeleteEdition deletes "what-if"
    Then no EditionCreated or EditionDeleted event exists on any main stream
    And ListEditions and the existence check read only the edition store
    And main's coordinators, projectors and readers received nothing about edition "what-if"

  # C-0638 (a fork at one store-wide position) is retired: there is no
  # global position, and every fork is per stream (decision 14.35).

  @editions
  @C-0639
  Scenario: A process manager's instance stream is forked like any domain's
    Given a process manager "BuyIn" owning domain "buy-in" with an instance for correlation "0192a6b0-0000-7000-8000-000000000030"
    And a policy "PlayerToBuyIn" that sends a command to "buy-in" "0192a6b0-0000-7000-8000-000000000030" on PlayerReady
    And "player" "p-1" is forked into edition "what-if"
    When "player" "p-1" records PlayerReady in edition "what-if"
    Then "buy-in" "0192a6b0-0000-7000-8000-000000000030" in edition "what-if" first gains an EditionCreated event
    And the command is then handled in edition "what-if"

  @editions
  @C-0647
  Scenario: While an edition is being deleted, an arrival at a removed stream is refused
    Given "player" "p-1" and "table" "t-1" are forked into edition "what-if"
    And both have recorded their EditionDeleted events, and "player" "p-1" in edition "what-if" has been removed
    When a policy step in edition "what-if" writes its position on "player" "p-1", or a policy command arrives at it
    Then it is refused NOT_FOUND
    And "player" "p-1" holds no event in edition "what-if"

  @editions
  @C-0644
  Scenario: A delete is recorded, propagated onward, and only then removed, stream by stream
    Given "player" "p-1" is forked into edition "what-if"
    And a SeatPlayer command has forked "table" "t-1" in edition "what-if"
    When DeleteEdition deletes "what-if"
    Then "player" "p-1" in edition "what-if" records an EditionDeleted event
    And the policy "PlayerToTable" carries it to "table" "t-1", which records its own EditionDeleted event in edition "what-if"
    And the rows of "player" "p-1" in edition "what-if" are removed only after that policy step and its outbox entries have completed
    And the rows of "table" "t-1" in edition "what-if" are removed only after its EditionDeleted has propagated onward
    And no row of edition "what-if" remains, the EditionDeleted events included
    And ListEditions no longer lists edition "what-if"

  @editions
  @C-0645
  Scenario: A stream's rows are never removed before its EditionDeleted has propagated onward
    Given "player" "p-1" is forked into edition "what-if"
    And "player" "p-1" in edition "what-if" has recorded an EditionDeleted event
    And the policy step carrying it onward still has an outbox entry pending
    When the removal of "player" "p-1" in edition "what-if" is attempted
    Then nothing is removed
    And the removal happens once the outbox entry has been delivered

  @editions
  @C-0646
  Scenario: A stream the delete cannot reach keeps its rows, and the edition stays listed
    Given "player" "p-1" is forked into edition "what-if"
    And "table" "t-9" was forked into edition "what-if" with ForkStream, and no policy flow leads to it from "player"
    When DeleteEdition deletes "what-if" through the origin "player" "p-1" only
    Then the rows of "player" "p-1" in edition "what-if" are removed
    And "table" "t-9" in edition "what-if" keeps its rows
    And ListEditions still lists edition "what-if", for the user to act on

  @editions
  @C-0652
  Scenario: The angzarr system layer creates a stream in the edition when a policy's command arrives, then hands the command to the router
    Given "player" "p-1" is forked into edition "what-if"
    And "table" "t-1" has no row in edition "what-if"
    When a policy's command in edition "what-if" arrives at "table" "t-1"
    Then the system layer first writes an EditionCreated event to "table" "t-1" in edition "what-if"
    And only then hands the command to the router as an ordinary command in edition "what-if"
    And the router is given no EditionCreated

  @editions
  @C-0653
  Scenario: The angzarr system layer propagates a deletion along the streams the policies reached, skipping a stream with nothing left
    Given "player" "p-1" is forked into edition "what-if"
    And the policy "PlayerToTable" reached "table" "t-1" in edition "what-if"
    And "table" "t-3" holds no row of edition "what-if"
    When DeleteEdition deletes "what-if"
    Then the system layer records an EditionDeleted event in "player" "p-1" in edition "what-if"
    And the system layer writes "table" "t-1"'s own EditionDeleted event in edition "what-if", not as a policy's command
    And each stream's rows of edition "what-if" are removed only after its EditionDeleted has propagated onward
    And a deletion reaching "table" "t-3" is skipped, creating and forking nothing

  @editions
  @C-0654
  Scenario Outline: The angzarr system layer refuses, before the router is called
    Given "player" "p-1" is forked into edition "what-if"
    When <input>
    Then it is answered <status> with reason <reason>
    And nothing is written
    And the router is never called

    Examples:
      | input                                                                                               | status              | reason                  |
      | a client sends a command to "table" "t-9" in edition "what-if", which has no row there               | FAILED_PRECONDITION | EDITION_NOT_FORKED      |
      | a policy's command arrives at "table" "t-1" in edition "gone", which has been deleted               | NOT_FOUND           | EDITION_NOT_FOUND       |
      | a client sends a command whose payload is an io.angzarr.v1.meta.EditionDeleted to "player" "p-1"     | INVALID_ARGUMENT    | META_EVENT_FROM_OUTSIDE |
      | a client sends a command whose payload is an io.angzarr.v1.meta.EditionCreated to "player" "p-1"     | INVALID_ARGUMENT    | META_EVENT_FROM_OUTSIDE |

  @editions
  @C-0655
  Scenario Outline: Policies and handlers never see a meta-event
    Given a policy "PlayerToTable" with a handler for every "player" event
    And a projector with a handler for every "player" event
    When "player" "p-1" in edition "what-if" records an <meta-event> event
    Then the system layer delivers it to no policy, process manager, aggregate or projector handler
    And the router is never given it

    Examples:
      | meta-event     |
      | EditionCreated |
      | EditionDeleted |

  # ---------------------------------------------------------------
  # The fork point is explicit, per stream (decision 14.36)
  # ---------------------------------------------------------------

  @editions @fork_point
  @C-0661
  Scenario: A fork stated below main's head lands at its stated sequence and reads main below it
    When ForkStream forks "player" "p-1" into edition "what-if" at parent_sequence 3
    Then "player" "p-1" in edition "what-if" holds an EditionCreated event at sequence 3
    And the EditionCreated event names edition "what-if" and parent_sequence 3
    And "player" "p-1" loaded in edition "what-if" is main's events 0 to 2, then the fork event at 3
    And none of main's events at or after sequence 3 are in it
    And the main timeline of "player" "p-1" is unchanged

  @editions @fork_point
  @C-0662
  Scenario: A fork stated above main's head is a conflict, and writes nothing
    When ForkStream forks "player" "p-1" into edition "what-if" at parent_sequence 6
    Then the request is refused ABORTED
    And "player" "p-1" holds no event in edition "what-if"
    And no EditionCreated event of edition "what-if" exists anywhere

  @editions @fork_point
  @C-0663
  Scenario: An arrival's fork is at the main head the system layer read, even if main moves on before the write
    Given "player" "p-1" is forked into edition "what-if" at sequence 5
    And "table" "t-1" has no fork in edition "what-if"
    And the system layer reads main's head of "table" "t-1" as 3 when the SeatPlayer command arrives in edition "what-if"
    And "table" "t-1" on the main timeline gains 2 more events before the fork is written
    When the system layer writes the fork of "table" "t-1" in edition "what-if"
    Then "table" "t-1" in edition "what-if" holds an EditionCreated event at sequence 3, with parent_sequence 3
    And "table" "t-1" loaded in edition "what-if" holds main's events 0 to 2 only, then the fork event at 3
    And the SeatPlayer command is then handled in edition "what-if"
    And no policy business code saw a sequence
