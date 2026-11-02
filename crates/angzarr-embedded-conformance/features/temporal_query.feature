Feature: Temporal queries - state as of a sequence or a time
  A Query with a TemporalQuery selection returns the aggregate's history up
  to a point. The coordinator may start the returned EventBook from a
  snapshot, but only from one that cannot contain state from after the
  point; otherwise it returns every page from sequence 0. Folding the
  returned book always yields the same state as a full replay to the point.

  Background:
    Given an "order" aggregate with events at sequences 0 through 9
    And each event sequence N was created N minutes after "2026-01-01T10:00:00Z"

  # ---------------------------------------------------------------------------
  # as_of_sequence
  # ---------------------------------------------------------------------------

  @C-0402
  Scenario: as_of_sequence starts from a snapshot at or before the point
    Given the aggregate has a snapshot at sequence 4
    When I query the aggregate as of sequence 6
    Then the result carries the snapshot at sequence 4
    And the result contains pages at sequences 5 and 6 only

  @C-0403
  Scenario: as_of_sequence ignores a snapshot after the point
    Given the aggregate has a snapshot at sequence 8
    When I query the aggregate as of sequence 6
    Then the result carries no snapshot
    And the result contains pages at sequences 0 through 6

  @C-0404
  Scenario: as_of_sequence equal to the snapshot returns the snapshot alone
    Given the aggregate has a snapshot at sequence 6
    When I query the aggregate as of sequence 6
    Then the result carries the snapshot at sequence 6
    And the result contains no pages

  # ---------------------------------------------------------------------------
  # as_of_time
  # ---------------------------------------------------------------------------

  @C-0405
  Scenario: as_of_time starts from a snapshot persisted at or before the time
    Given the aggregate has a snapshot at sequence 4 created at "2026-01-01T10:04:30Z"
    When I query the aggregate as of time "2026-01-01T10:05:30Z"
    Then the result carries the snapshot at sequence 4
    And the result contains the page at sequence 5 only

  @C-0406
  Scenario: as_of_time ignores a snapshot persisted after the time
    Given the aggregate has a snapshot at sequence 4 created at "2026-01-01T10:07:00Z"
    When I query the aggregate as of time "2026-01-01T10:05:30Z"
    Then the result carries no snapshot
    And the result contains pages at sequences 0 through 5

  @C-0407
  Scenario: as_of_time ignores a snapshot without created_at
    Given the aggregate has a snapshot at sequence 4 with no created_at
    When I query the aggregate as of time "2026-01-01T10:05:30Z"
    Then the result carries no snapshot
    And the result contains pages at sequences 0 through 5

  @C-0408
  Scenario Outline: Snapshot and full replay fold to the same state
    Given the aggregate has a snapshot at sequence 4 created at "2026-01-01T10:04:30Z"
    When I query the aggregate as of <point>
    Then folding the result yields the same state as replaying sequences 0 through <last> without a snapshot

    Examples:
      | point                       | last |
      | sequence 6                  | 6    |
      | time "2026-01-01T10:05:30Z" | 5    |
