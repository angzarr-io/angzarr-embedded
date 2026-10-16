//! The engine, proven on domains that have nothing to do with any game.

use std::convert::Infallible;
use std::fmt;

use annals::{
    project, Conflict, Domain, Envelope, EventStore, Header, InMemoryStore, Projection,
    ReplayError, Stream, StreamId, Tx,
};
use annals_diagnostic::{Arg, Diagnostic};

fn header(id: u64, domain: &str) -> Header {
    Header {
        id: StreamId(id),
        domain: domain.to_string(),
        schema: 1,
        rules: "test".to_string(),
    }
}

// ---------------------------------------------------------------- a bank account

struct Account;

#[derive(Default, Clone, PartialEq, Debug)]
struct Balance {
    open: bool,
    cents: i64,
}

enum AccountCmd {
    Open,
    Deposit(i64),
    Withdraw(i64),
}

#[derive(Clone, PartialEq, Debug)]
enum AccountEvent {
    Opened,
    Deposited(i64),
    Withdrew(i64),
}

#[derive(PartialEq, Debug)]
enum Refused {
    AlreadyOpen,
    NotOpen,
    NotPositive,
    Overdrawn { short: i64 },
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Refused::AlreadyOpen => f.write_str("the account is already open"),
            Refused::NotOpen => f.write_str("the account is not open"),
            Refused::NotPositive => f.write_str("the amount must be positive"),
            Refused::Overdrawn { short } => write!(f, "{short} cents short"),
        }
    }
}

impl Diagnostic for Refused {
    fn code(&self) -> &'static str {
        match self {
            Refused::AlreadyOpen => "toy-account-already-open",
            Refused::NotOpen => "toy-account-not-open",
            Refused::NotPositive => "toy-account-not-positive",
            Refused::Overdrawn { .. } => "toy-account-overdrawn",
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match *self {
            Refused::Overdrawn { short } => vec![("short", short.into())],
            _ => vec![],
        }
    }
}

impl Domain for Account {
    type State = Balance;
    type Command = AccountCmd;
    type Event = AccountEvent;
    type Rejection = Refused;
    type Rules = ();
    type Ctx = ();

    fn decide(cmd: &AccountCmd, tx: &mut Tx<Self>) -> Result<(), Refused> {
        let s = tx.state();
        match *cmd {
            AccountCmd::Open => {
                if s.open {
                    return Err(Refused::AlreadyOpen);
                }
                tx.emit(AccountEvent::Opened);
            }
            AccountCmd::Deposit(c) => {
                if !s.open {
                    return Err(Refused::NotOpen);
                }
                if c <= 0 {
                    return Err(Refused::NotPositive);
                }
                tx.emit(AccountEvent::Deposited(c));
            }
            AccountCmd::Withdraw(c) => {
                if !s.open {
                    return Err(Refused::NotOpen);
                }
                if c > s.cents {
                    return Err(Refused::Overdrawn { short: c - s.cents });
                }
                tx.emit(AccountEvent::Withdrew(c));
            }
        }
        Ok(())
    }

    fn apply(s: &mut Balance, e: &AccountEvent, _: &()) {
        match *e {
            AccountEvent::Opened => s.open = true,
            AccountEvent::Deposited(c) => s.cents += c,
            AccountEvent::Withdrew(c) => s.cents -= c,
        }
    }
}

fn run(cmds: &[AccountCmd]) -> Stream<Account> {
    let mut s = Stream::<Account>::new(header(1, "account"));
    for c in cmds {
        let _ = s.execute(c, &(), &mut ());
    }
    s
}

#[test]
fn a_command_emits_events_that_fold_into_state() {
    let s = run(&[
        AccountCmd::Open,
        AccountCmd::Deposit(500),
        AccountCmd::Withdraw(200),
    ]);
    assert_eq!(s.state().cents, 300);
    assert_eq!(s.len(), 3);
}

#[test]
fn a_rejected_command_leaves_no_trace() {
    let mut s = run(&[AccountCmd::Open, AccountCmd::Deposit(100)]);
    let refused = s
        .execute(&AccountCmd::Withdraw(250), &(), &mut ())
        .unwrap_err();
    assert_eq!(refused, Refused::Overdrawn { short: 150 });
    assert_eq!(refused.code(), "toy-account-overdrawn");
    assert_eq!(refused.to_string(), "150 cents short");
    assert_eq!(s.len(), 2);
    assert_eq!(s.state().cents, 100);
}

#[test]
fn replay_rebuilds_the_same_state() {
    let s = run(&[
        AccountCmd::Open,
        AccountCmd::Deposit(500),
        AccountCmd::Withdraw(900),
        AccountCmd::Withdraw(120),
    ]);
    let (h, events, state) = s.into_parts();
    let replayed = Stream::<Account>::replay(h, events, &()).unwrap();
    assert_eq!(*replayed.state(), state);
}

#[test]
fn state_at_is_the_state_before_that_event() {
    let s = run(&[
        AccountCmd::Open,
        AccountCmd::Deposit(500),
        AccountCmd::Withdraw(200),
    ]);
    assert_eq!(s.state_at(0, &()), Balance::default());
    assert_eq!(s.state_at(2, &()).cents, 500);
    assert_eq!(s.state_at(3, &()), *s.state());
    assert_eq!(s.state_at(99, &()), *s.state());
}

// ---------------------------------------------------------------- decide sees its own events

/// Pouring fills cups one at a time until the jug is empty or the cups are
/// full — each pour reads the state the previous pour left.
struct Jug;

#[derive(Default)]
struct Cups {
    jug: u32,
    cups: Vec<u32>,
}

enum JugCmd {
    Fill(u32, usize),
    Pour,
}

#[derive(Clone, Debug, PartialEq)]
enum JugEvent {
    Filled { jug: u32, cups: usize },
    Poured { cup: usize, amount: u32 },
}

const CUP: u32 = 3;

impl Domain for Jug {
    type State = Cups;
    type Command = JugCmd;
    type Event = JugEvent;
    type Rejection = Infallible;
    type Rules = ();
    type Ctx = ();

    fn decide(cmd: &JugCmd, tx: &mut Tx<Self>) -> Result<(), Infallible> {
        match *cmd {
            JugCmd::Fill(jug, cups) => {
                tx.emit(JugEvent::Filled { jug, cups });
            }
            JugCmd::Pour => loop {
                let s = tx.state();
                let Some(cup) = s.cups.iter().position(|c| *c < CUP) else {
                    break;
                };
                let amount = (CUP - s.cups[cup]).min(s.jug);
                if amount == 0 {
                    break;
                }
                tx.emit(JugEvent::Poured { cup, amount });
            },
        }
        Ok(())
    }

    fn apply(s: &mut Cups, e: &JugEvent, _: &()) {
        match *e {
            JugEvent::Filled { jug, cups } => {
                s.jug = jug;
                s.cups = vec![0; cups];
            }
            JugEvent::Poured { cup, amount } => {
                s.jug -= amount;
                s.cups[cup] += amount;
            }
        }
    }
}

#[test]
fn a_decide_sees_the_effects_of_its_own_events() {
    let mut s = Stream::<Jug>::new(header(2, "jug"));
    s.execute(&JugCmd::Fill(7, 3), &(), &mut ()).unwrap();
    let poured = s.execute(&JugCmd::Pour, &(), &mut ()).unwrap().len();
    assert_eq!(poured, 3);
    assert_eq!(s.state().cups, vec![3, 3, 1]);
    assert_eq!(s.state().jug, 0);
}

// ---------------------------------------------------------------- dice live in the context

/// A die rolled in decide, its face recorded in the event. Apply never sees
/// the die.
struct Dice;

#[derive(Default, Clone, PartialEq, Debug)]
struct Total(u32);

#[derive(Clone, PartialEq, Debug)]
struct Rolled {
    face: u32,
}

/// A deliberately crude die: a counter.
struct Die {
    next: u32,
    step: u32,
}

impl Die {
    fn roll(&mut self) -> u32 {
        self.next = (self.next + self.step) % 6;
        self.next + 1
    }
}

impl Domain for Dice {
    type State = Total;
    type Command = ();
    type Event = Rolled;
    type Rejection = Infallible;
    type Rules = ();
    type Ctx = Die;

    fn decide(_: &(), tx: &mut Tx<Self>) -> Result<(), Infallible> {
        let face = tx.ctx().roll();
        tx.emit(Rolled { face });
        Ok(())
    }

    fn apply(t: &mut Total, e: &Rolled, _: &()) {
        t.0 += e.face;
    }
}

#[test]
fn replaying_never_rolls_so_a_changed_die_changes_nothing() {
    let mut s = Stream::<Dice>::new(header(3, "dice"));
    let mut die = Die { next: 0, step: 1 };
    for _ in 0..10 {
        s.execute(&(), &(), &mut die).unwrap();
    }
    let total = s.state().clone();
    // The die is replaced by a different one; the log replays identically,
    // because the faces are in the events.
    let (h, events, _) = s.into_parts();
    let replayed = Stream::<Dice>::replay(h, events, &()).unwrap();
    assert_eq!(*replayed.state(), total);
}

// ---------------------------------------------------------------- causes

#[test]
fn events_carry_their_cause() {
    let mut s = Stream::<Account>::new(header(4, "account"));
    s.execute(&AccountCmd::Open, &(), &mut ()).unwrap();
    s.execute_caused(&AccountCmd::Deposit(5), Some(0), &(), &mut ())
        .unwrap();
    assert_eq!(s.events()[0].cause, None);
    assert_eq!(s.events()[1].cause, Some(0));
}

#[test]
fn replay_refuses_a_malformed_log() {
    let h = header(5, "account");
    let gap = vec![
        Envelope {
            seq: 0,
            cause: None,
            event: AccountEvent::Opened,
        },
        Envelope {
            seq: 2,
            cause: None,
            event: AccountEvent::Deposited(1),
        },
    ];
    assert_eq!(
        Stream::<Account>::replay(h.clone(), gap, &()).err(),
        Some(ReplayError::OutOfSequence { index: 1, found: 2 })
    );
    assert_eq!(
        ReplayError::OutOfSequence { index: 1, found: 2 }.to_string(),
        "event 1 claims sequence number 2, not 1"
    );
    let forward = vec![
        Envelope {
            seq: 0,
            cause: None,
            event: AccountEvent::Opened,
        },
        Envelope {
            seq: 1,
            cause: Some(1),
            event: AccountEvent::Deposited(1),
        },
    ];
    assert_eq!(
        Stream::<Account>::replay(h, forward, &()).err(),
        Some(ReplayError::CauseNotEarlier { seq: 1, cause: 1 })
    );
    assert_eq!(
        ReplayError::CauseNotEarlier { seq: 1, cause: 1 }.to_string(),
        "event 1 names event 1 as its cause, which is not earlier"
    );
}

// ---------------------------------------------------------------- validate, then emit

/// Emits, then rejects — the bug the engine refuses to let pass quietly.
struct Sloppy;

impl Domain for Sloppy {
    type State = u32;
    type Command = ();
    type Event = ();
    type Rejection = Refused;
    type Rules = ();
    type Ctx = ();

    fn decide(_: &(), tx: &mut Tx<Self>) -> Result<(), Refused> {
        tx.emit(());
        Err(Refused::NotOpen)
    }

    fn apply(n: &mut u32, _: &(), _: &()) {
        *n += 1;
    }
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "validate before you emit")]
fn rejecting_after_emitting_panics_in_debug() {
    let mut s = Stream::<Sloppy>::new(header(6, "sloppy"));
    let _ = s.execute(&(), &(), &mut ());
}

#[test]
#[cfg(not(debug_assertions))]
fn rejecting_after_emitting_keeps_log_and_state_together_in_release() {
    let mut s = Stream::<Sloppy>::new(header(6, "sloppy"));
    assert!(s.execute(&(), &(), &mut ()).is_err());
    // The event was applied, so it must be in the log too.
    assert_eq!(s.len(), 1);
    assert_eq!(*s.state(), 1);
}

// ---------------------------------------------------------------- store and projections

#[test]
fn the_store_holds_streams_and_refuses_stale_appends() {
    let s = run(&[AccountCmd::Open, AccountCmd::Deposit(40)]);
    let mut store = InMemoryStore::new();
    store.append(s.header(), 0, s.events()).unwrap();
    assert_eq!(
        store.append(s.header(), 0, s.events()),
        Err(Conflict {
            id: StreamId(1),
            expected: 0,
            actual: 2
        })
    );
    assert_eq!(
        store
            .append(s.header(), 0, s.events())
            .unwrap_err()
            .to_string(),
        "stream 1 holds 2 events, not the 0 expected: someone else appended first"
    );
    let (h, events) = store.read(StreamId(1)).unwrap();
    let replayed = Stream::<Account>::replay(h, events, &()).unwrap();
    assert_eq!(replayed.state().cents, 40);
    assert!(store.read(StreamId(99)).is_none());
}

#[derive(Default)]
struct Turnover {
    moved: i64,
}

impl Projection<AccountEvent> for Turnover {
    fn on(&mut self, e: &Envelope<AccountEvent>) {
        match e.event {
            AccountEvent::Deposited(c) | AccountEvent::Withdrew(c) => self.moved += c,
            AccountEvent::Opened => {}
        }
    }
}

#[test]
fn a_projection_is_a_fold_of_its_own() {
    let s = run(&[
        AccountCmd::Open,
        AccountCmd::Deposit(500),
        AccountCmd::Withdraw(200),
    ]);
    let mut t = Turnover::default();
    project(&mut t, s.events());
    assert_eq!(t.moved, 700);
}
