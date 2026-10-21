use super::*;

#[test]
fn order_is_tick_then_phase_then_side_then_scheduling() {
    let mut s = Scheduler::new();
    s.push(5, 1, 0, "late");
    s.push(2, 2, 0, "reveal");
    s.push(2, 0, 1, "impact, side 1");
    s.push(2, 0, 0, "impact, side 0, first");
    s.push(2, 0, 0, "impact, side 0, second");
    let order: Vec<&str> = std::iter::from_fn(|| s.pop().map(|(_, d)| d)).collect();
    assert_eq!(
        order,
        [
            "impact, side 0, first",
            "impact, side 0, second",
            "impact, side 1",
            "reveal",
            "late"
        ]
    );
}

#[test]
fn due_at_only_answers_for_the_first_tick() {
    let mut s = Scheduler::new();
    s.push(3, 0, 0, 'a');
    assert_eq!(s.next_tick(), Some(3));
    assert_eq!(s.due_at(2), None);
    assert_eq!(s.due_at(3), Some(&'a'));
}
