# The toy domain annals-sim's own tests run on. Nothing here is a game.
Feature: A brawl on the clock
  Fighters on two sides swing at each other until one side is down.

  Background:
    Given a seed of 7

  Scenario: The tougher side wins
    Given these fighters
      | hp   | side |
      | 1000 | 0    |
      | 5    | 1    |
    When the brawl runs to its end
    Then side 0 wins
    And fighter 1 is down
    And fighter 0 swung at least 1 time

  Scenario: Swings still in the air when it ends stay on the clock
    Given these fighters
      | hp | side |
      | 5  | 0    |
      | 60 | 1    |
    When the brawl runs to its end
    Then side 1 wins
    And something is left on the clock
