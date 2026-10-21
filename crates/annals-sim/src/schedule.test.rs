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

#[test]
fn pending_lists_everything_in_run_order_and_empties() {
    let mut s = Scheduler::new();
    assert!(s.is_empty());
    let late = s.push(4, 0, 0, 'b');
    let early = s.push(1, 0, 0, 'a');
    assert!(!s.is_empty());
    assert_eq!(s.len(), 2);
    assert_eq!(
        s.pending().collect::<Vec<_>>(),
        [(&early, &'a'), (&late, &'b')]
    );
    s.pop();
    s.pop();
    assert!(s.is_empty());
}

/// A clock with nothing else on it: `Push` schedules a letter, `Advance`
/// runs [`advance`] and records what it did. A `'+'` that runs schedules a
/// `'.'` for the same tick, so a tick can grow while it runs.
mod ticker {
    use super::super::*;
    use annals::{Domain, Header, Stream, StreamId, Tx};
    use std::convert::Infallible;

    pub struct Ticker;

    #[derive(Default)]
    pub struct State {
        pub clock: Scheduler<char>,
        pub ran: Vec<(Tick, char)>,
        pub advanced: Vec<Advance>,
    }

    pub enum Cmd {
        Push(Tick, char),
        Advance(Tick),
    }

    #[derive(Clone)]
    pub enum Ev {
        Pushed(Tick, char),
        Taken,
        Ran(Tick, char),
        Advanced(Advance),
    }

    impl Domain for Ticker {
        type State = State;
        type Command = Cmd;
        type Event = Ev;
        type Rejection = Infallible;
        type Rules = ();
        type Ctx = ();

        fn decide(cmd: &Cmd, tx: &mut Tx<Self>) -> Result<(), Infallible> {
            match *cmd {
                Cmd::Push(tick, c) => tx.emit(Ev::Pushed(tick, c)),
                Cmd::Advance(limit) => {
                    let did = advance(tx, limit);
                    tx.emit(Ev::Advanced(did))
                }
            };
            Ok(())
        }

        fn apply(s: &mut State, e: &Ev, _: &()) {
            match *e {
                Ev::Pushed(tick, c) => {
                    s.clock.push(tick, 0, 0, c);
                }
                Ev::Taken => {
                    s.clock.pop();
                }
                Ev::Ran(tick, c) => s.ran.push((tick, c)),
                Ev::Advanced(did) => s.advanced.push(did),
            }
        }
    }

    impl Clock for Ticker {
        type Due = char;

        fn scheduler(s: &State) -> &Scheduler<char> {
            &s.clock
        }

        fn taken(_: Tick) -> Ev {
            Ev::Taken
        }

        fn run(tx: &mut Tx<Self>, tick: Tick, due: char) {
            tx.emit(Ev::Ran(tick, due));
            if due == '+' {
                tx.emit(Ev::Pushed(tick, '.'));
            }
        }
    }

    /// A ticker with `due` pushed, then advanced once per limit; what each
    /// advance did, and everything that ran.
    pub fn run(due: &[(Tick, char)], limits: &[Tick]) -> (Vec<Advance>, Vec<(Tick, char)>) {
        let header = Header {
            id: StreamId(0),
            domain: "ticker".to_string(),
            schema: 1,
            rules: "toy".to_string(),
        };
        let mut stream = Stream::<Ticker>::new(header);
        let mut exec = |cmd| {
            let Ok(_) = stream.execute(&cmd, &(), &mut ());
        };
        for &(tick, c) in due {
            exec(Cmd::Push(tick, c));
        }
        for &limit in limits {
            exec(Cmd::Advance(limit));
        }
        let s = stream.state();
        (s.advanced.clone(), s.ran.clone())
    }
}

#[test]
fn advance_runs_a_tick_at_the_limit_and_stops_short_of_one_past_it() {
    let (did, ran) = ticker::run(&[(3, 'a'), (5, 'b')], &[3, 4, 5, 5]);
    assert_eq!(
        did,
        [
            Advance::Ran { tick: 3, taken: 1 },
            Advance::Beyond(5),
            Advance::Ran { tick: 5, taken: 1 },
            Advance::Idle,
        ]
    );
    assert_eq!(ran, [(3, 'a'), (5, 'b')]);
}

#[test]
fn advance_counts_what_a_tick_scheduled_for_itself() {
    let (did, ran) = ticker::run(&[(2, '+'), (2, 'a'), (6, 'z')], &[9]);
    assert_eq!(did, [Advance::Ran { tick: 2, taken: 3 }]);
    assert_eq!(ran, [(2, '+'), (2, 'a'), (2, '.')]);
}
