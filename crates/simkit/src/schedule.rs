//! A tick scheduler held in folded state, and the clock that drives it.
//!
//! The [`Scheduler`] lives **inside a domain's `State`**. Only `apply` changes
//! it: an event such as `JobScheduled` pushes, an event such as `DueTaken`
//! pops. Its sequence counter is folded too. So the queue at any point in a
//! log is exactly the fold of the log up to there, and a stream replayed from
//! any prefix — cut anywhere between commands — resumes with the same queue,
//! in the same order, and carries on exactly as the original did.
//!
//! Order is by [`Key`]: `(tick, phase, side, seq)`. The domain says what a
//! phase and a side are — resolve impacts before reveals, say, and break a
//! genuine tie between teams in an order chosen once per run — and `seq`
//! keeps everything else first-scheduled, first-run.

use std::collections::BTreeMap;

use es::{Domain, Tx};

/// A point on the clock. The domain says how long a tick is.
pub type Tick = u64;

/// Where something sits in the queue. Ordered field by field.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Key {
    pub tick: Tick,
    /// Order within a tick, by kind of work. Lower runs first.
    pub phase: u8,
    /// Order within a phase, by side. Lower runs first.
    pub side: u8,
    /// Order of scheduling: the tie-breaker of last resort.
    pub seq: u64,
}

/// Everything due on the clock, in the order it will run. Keep it in folded
/// state and change it only from `apply`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Scheduler<Due> {
    due: BTreeMap<Key, Due>,
    seq: u64,
}

impl<Due> Default for Scheduler<Due> {
    fn default() -> Self {
        Scheduler {
            due: BTreeMap::new(),
            seq: 0,
        }
    }
}

impl<Due> Scheduler<Due> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Put `due` on the clock. Call it from `apply` only.
    pub fn push(&mut self, tick: Tick, phase: u8, side: u8, due: Due) -> Key {
        self.seq += 1;
        let key = Key {
            tick,
            phase,
            side,
            seq: self.seq,
        };
        self.due.insert(key, due);
        key
    }

    /// Take the first thing due off the clock. Call it from `apply` only.
    pub fn pop(&mut self) -> Option<(Key, Due)> {
        self.due.pop_first()
    }

    /// The tick the next thing is due at.
    pub fn next_tick(&self) -> Option<Tick> {
        self.due.keys().next().map(|k| k.tick)
    }

    /// The first thing due, and where it sits.
    pub fn first(&self) -> Option<(&Key, &Due)> {
        self.due.iter().next()
    }

    /// The first thing due, if it is due at `tick`.
    pub fn due_at(&self, tick: Tick) -> Option<&Due> {
        self.first().filter(|(k, _)| k.tick == tick).map(|(_, d)| d)
    }

    /// Everything not yet taken, in the order it will run.
    pub fn pending(&self) -> impl Iterator<Item = (&Key, &Due)> {
        self.due.iter()
    }

    pub fn len(&self) -> usize {
        self.due.len()
    }

    pub fn is_empty(&self) -> bool {
        self.due.is_empty()
    }
}

/// A domain whose decide is driven by a [`Scheduler`] in its state.
pub trait Clock: Domain {
    /// What sits on the clock: a job record, a reveal, anything.
    type Due: Clone;

    /// The scheduler inside the folded state.
    fn scheduler(state: &Self::State) -> &Scheduler<Self::Due>;

    /// The event that takes the first thing due off the clock. Its `apply`
    /// must call [`Scheduler::pop`] exactly once — that is what makes taking
    /// part of the log, so a replay takes the same things.
    fn taken(tick: Tick) -> Self::Event;

    /// Do what came due. Emit through `tx`, schedule more by emitting the
    /// events whose `apply` pushes.
    fn run(tx: &mut Tx<Self>, tick: Tick, due: Self::Due);
}

/// What one [`advance`] did.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Advance {
    /// Everything due at `tick` ran: `taken` things in all, including any
    /// scheduled for the same tick while it ran.
    Ran { tick: Tick, taken: usize },
    /// Nothing is on the clock.
    Idle,
    /// The next thing due is at `tick`, past the limit. Nothing ran.
    Beyond(Tick),
}

/// Resolve everything due at the next scheduled tick, one at a time against
/// live state, and stop. Call it from a decide.
///
/// The clock jumps from one scheduled tick to the next: empty ticks are
/// never visited, with the same result as stepping every tick. Running a
/// whole tick before returning lets the caller check its end condition once
/// per instant, so simultaneous outcomes (mutual kills) stand.
///
/// Work that keeps scheduling more work at the same tick never returns; a
/// domain that allows zero-delay chains must cap them itself.
pub fn advance<D: Clock>(tx: &mut Tx<D>, limit: Tick) -> Advance {
    let tick = match D::scheduler(tx.state()).next_tick() {
        None => return Advance::Idle,
        Some(t) if t > limit => return Advance::Beyond(t),
        Some(t) => t,
    };
    let mut taken = 0;
    while let Some(due) = D::scheduler(tx.state()).due_at(tick).cloned() {
        let before = D::scheduler(tx.state()).len();
        tx.emit(D::taken(tick));
        debug_assert_eq!(
            D::scheduler(tx.state()).len() + 1,
            before,
            "the taken event's apply must pop the scheduler exactly once"
        );
        taken += 1;
        D::run(tx, tick, due);
    }
    Advance::Ran { tick, taken }
}

#[cfg(test)]
mod tests {
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
}
