//! Thens about what business code was delivered: how often, the window
//! (the events from the expected sequence to the head), the nonce, the
//! snapshot, the head and the resolution it was handled under.

use std::collections::BTreeSet;

use cucumber::then;

use angzarr_embedded::v1::{ConflictResolution, ContextualCommand, CoordinatorReason};

use crate::tests::world::{resolution, Coordinator};

/// A delivery's expected sequence: the window's start.
fn expected(delivered: &ContextualCommand) -> u64 {
    delivered
        .command
        .as_ref()
        .and_then(|book| book.handling.as_ref())
        .and_then(|handling| handling.expected_sequence)
        .expect("a delivery's handling is resolved")
}

/// A delivery's head: its stream's next sequence.
fn head(delivered: &ContextualCommand) -> u64 {
    delivered
        .events
        .as_ref()
        .map_or(0, |events| events.next_sequence)
}

/// A delivery's window: the sequences of the events it was delivered from
/// its expected sequence on.
fn window(delivered: &ContextualCommand) -> Vec<u64> {
    let from = expected(delivered);
    let pages = delivered.events.as_ref().map_or(&[][..], |e| &e.pages[..]);
    let window: Vec<u64> = pages
        .iter()
        .map(|p| p.sequence)
        .filter(|s| *s >= from)
        .collect();
    assert_eq!(
        window,
        (from..head(delivered)).collect::<Vec<_>>(),
        "the window, whole, as events"
    );
    window
}

/// The order domain's deliveries.
fn seen(w: &mut Coordinator) -> Vec<ContextualCommand> {
    w.business("order").delivered()
}

/// The last delivery.
fn last(w: &mut Coordinator) -> ContextualCommand {
    seen(w).pop().expect("business code was invoked")
}

/// A count as a scenario says it.
fn count(text: &str) -> usize {
    match text {
        "once" => 1,
        "twice" => 2,
        n => n.trim_end_matches(" times").parse().expect("a count"),
    }
}

#[then(regex = r"^(?:business code is not invoked|the command is not delivered to business code)$")]
fn not_invoked(w: &mut Coordinator) {
    assert!(seen(w).is_empty(), "{:?}", seen(w));
}

#[then("business code is invoked")]
fn invoked(w: &mut Coordinator) {
    assert!(!seen(w).is_empty());
}

#[then(regex = r"^(?:business code|the ingest handler) is invoked (once|twice|\d+ times)$")]
fn invoked_times(w: &mut Coordinator, times: String) {
    assert_eq!(seen(w).len(), count(&times));
}

#[then("the coordinator does not re-run the command")]
fn not_rerun(w: &mut Coordinator) {
    assert_eq!(seen(w).len(), 1);
}

#[then(
    regex = r"^business code is invoked twice, the second time against the head at sequence (\d+)$"
)]
fn second_against(w: &mut Coordinator, at: u64) {
    let seen = seen(w);
    assert_eq!(seen.len(), 2);
    assert_eq!(head(&seen[1]), at);
}

#[then(
    regex = r"^business code is invoked twice, the second time with the events at sequences (\d+) to (\d+) as the window and a fresh conflict nonce$"
)]
fn second_window(w: &mut Coordinator, from: u64, to: u64) {
    let seen = seen(w);
    assert_eq!(seen.len(), 2);
    assert_eq!(window(&seen[1]), (from..=to).collect::<Vec<_>>());
    assert!(!seen[1].conflict_nonce.is_empty());
    assert_ne!(
        seen[1].conflict_nonce, seen[0].conflict_nonce,
        "a fresh nonce"
    );
}

#[then(
    regex = r"^business code is invoked (\d+) times, each time with a wider window and a fresh conflict nonce$"
)]
fn wider_windows(w: &mut Coordinator, times: usize) {
    let seen = seen(w);
    assert_eq!(seen.len(), times);
    let widths: Vec<usize> = seen.iter().map(|d| window(d).len()).collect();
    assert!(
        widths.windows(2).all(|p| p[0] < p[1]),
        "each wider: {widths:?}"
    );
    let nonces: BTreeSet<_> = seen.iter().map(|d| d.conflict_nonce.clone()).collect();
    assert_eq!(nonces.len(), times, "each nonce fresh");
}

#[then(
    regex = r"^(?:business code|the ingest handler) is invoked (\d+) times, each time against a newer head$"
)]
fn newer_heads(w: &mut Coordinator, times: usize) {
    let heads: Vec<u64> = seen(w).iter().map(head).collect();
    assert_eq!(heads.len(), times);
    assert!(
        heads.windows(2).all(|p| p[0] < p[1]),
        "each newer: {heads:?}"
    );
}

#[then(regex = r"^business code receives the events at sequences (\d+) and (\d+) as the window$")]
fn window_of_two(w: &mut Coordinator, first: u64, second: u64) {
    assert_eq!(window(&last(w)), vec![first, second]);
}

#[then("business code receives an empty window")]
fn empty_window(w: &mut Coordinator) {
    assert!(window(&last(w)).is_empty());
}

#[then("business code receives a conflict nonce")]
fn a_nonce(w: &mut Coordinator) {
    assert!(!last(w).conflict_nonce.is_empty());
}

#[then("business code received no conflict nonce")]
fn no_nonce(w: &mut Coordinator) {
    assert!(last(w).conflict_nonce.is_empty());
}

#[then("the two deliveries carry different conflict nonces")]
fn different_nonces(w: &mut Coordinator) {
    let seen = seen(w);
    assert_eq!(seen.len(), 2);
    assert_ne!(seen[0].conflict_nonce, seen[1].conflict_nonce);
}

#[then("business code receives no snapshot")]
fn no_snapshot(w: &mut Coordinator) {
    assert!(last(w).events.and_then(|e| e.snapshot).is_none());
}

/// Handled under STRICT, a stale command is refused with STRICT's own key
/// before business code; under any other resolution it is delivered so.
#[then(regex = r"^the command is handled under (\w+)$")]
fn handled_under(w: &mut Coordinator, name: String) {
    let resolution = resolution(&name);
    let Some(delivered) = seen(w).pop() else {
        assert_eq!(
            resolution,
            ConflictResolution::Strict,
            "refused before business code"
        );
        let stale = CoordinatorReason::ExpectedSequenceStale.as_str_name();
        assert_eq!(w.answer().reason(), Some(stale), "{:?}", w.answer());
        return;
    };
    let handling = delivered
        .command
        .and_then(|book| book.handling)
        .expect("handling");
    assert_eq!(handling.conflict_resolution(), resolution);
}

#[then(
    regex = r"^the outcome is (rejected with ABORTED before business code|delivered to business code with the window and a nonce)$"
)]
fn outcome_is(w: &mut Coordinator, outcome: String) {
    match outcome.starts_with("rejected") {
        true => {
            assert!(seen(w).is_empty());
            let status = w.answer().status();
            assert_eq!(status, Some(crate::tests::answer::status("ABORTED")));
        }
        false => {
            let delivered = last(w);
            assert!(!window(&delivered).is_empty());
            assert!(!delivered.conflict_nonce.is_empty());
            let resolution = delivered
                .command
                .and_then(|b| b.handling)
                .expect("handling");
            assert_ne!(resolution.conflict_resolution(), ConflictResolution::Strict);
        }
    }
}
