//! Toy domains for the proposal tests: an account whose fee comes from its
//! rules, and a jug whose pour reads its own staged events.

use std::convert::Infallible;
use std::fmt;

use annals::next::{
    Actor, CommandMeta, CorrelationId, Domain, Header, RequestId, Rulebook, StreamId, Timestamp, Tx,
};
use annals_diagnostic::{Arg, Diagnostic};

pub fn header(id: u64, domain: &str) -> Header {
    Header {
        id: StreamId::from(id),
        domain: domain.to_string(),
    }
}

/// Command `command`, from ann, at `at` microseconds.
pub fn meta(command: u128, at: i64) -> CommandMeta {
    CommandMeta::new(
        Timestamp(at),
        Some(Actor("pseudonym-ann".to_string())),
        RequestId(command),
    )
}

/// A command continuing an existing correlation.
pub fn meta_in(command: u128, at: i64, correlation: u128) -> CommandMeta {
    CommandMeta {
        correlation: CorrelationId(correlation),
        ..meta(command, at)
    }
}

// ---------------------------------------------------------------- an account

/// The rules: a fee taken from every withdrawal, in cents.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Fees {
    pub withdrawal: i64,
}

pub fn fees(version: &str, withdrawal: i64) -> Rulebook<Fees> {
    Rulebook::new(version, Fees { withdrawal })
}

pub struct Account;

#[derive(Default, Clone, PartialEq, Debug)]
pub struct Balance {
    pub open: bool,
    pub cents: i64,
}

pub enum AccountCmd {
    Open,
    Deposit(i64),
    Withdraw(i64),
    /// Deposits, then refuses when the balance passes `limit`: a decide
    /// that rejects after emitting, which the proposed kernel allows.
    DepositUpTo {
        cents: i64,
        limit: i64,
    },
}

#[derive(Clone, PartialEq, Debug)]
pub enum AccountEvent {
    Opened,
    Deposited(i64),
    Withdrew(i64),
}

#[derive(Clone, PartialEq, Debug)]
pub enum Refused {
    AlreadyOpen,
    NotOpen,
    Overdrawn { short: i64 },
    OverLimit,
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Refused::AlreadyOpen => f.write_str("the account is already open"),
            Refused::NotOpen => f.write_str("the account is not open"),
            Refused::Overdrawn { short } => write!(f, "{short} cents short"),
            Refused::OverLimit => f.write_str("over the limit"),
        }
    }
}

impl Diagnostic for Refused {
    fn code(&self) -> &'static str {
        match self {
            Refused::AlreadyOpen => "toy-account-already-open",
            Refused::NotOpen => "toy-account-not-open",
            Refused::Overdrawn { .. } => "toy-account-overdrawn",
            Refused::OverLimit => "toy-account-over-limit",
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
    type Rules = Fees;

    const NAME: &'static str = "account";
    const SCHEMA: u32 = 3;

    fn decide(cmd: &AccountCmd, tx: &mut Tx<'_, Self>) -> Result<(), Refused> {
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
                tx.emit(AccountEvent::Deposited(c));
            }
            AccountCmd::Withdraw(c) => {
                let total = c + tx.rules().withdrawal;
                if total > s.cents {
                    return Err(Refused::Overdrawn {
                        short: total - s.cents,
                    });
                }
                tx.emit(AccountEvent::Withdrew(c));
            }
            AccountCmd::DepositUpTo { cents, limit } => {
                tx.emit(AccountEvent::Deposited(cents));
                if tx.state().cents > limit {
                    return Err(Refused::OverLimit);
                }
            }
        }
        Ok(())
    }

    /// The fee is the rules': replaying under the wrong rules would give
    /// the wrong balance.
    fn apply(s: &mut Balance, e: &AccountEvent, rules: &Fees) {
        match *e {
            AccountEvent::Opened => s.open = true,
            AccountEvent::Deposited(c) => s.cents += c,
            AccountEvent::Withdrew(c) => s.cents -= c + rules.withdrawal,
        }
    }
}

// ---------------------------------------------------------------- a jug

/// Pouring fills cups one at a time until the jug is empty or the cups are
/// full: each pour reads the state the previous staged pour left.
pub struct Jug;

#[derive(Default, Clone, PartialEq, Debug)]
pub struct Cups {
    pub jug: u32,
    pub cups: Vec<u32>,
}

pub enum JugCmd {
    Fill(u32, usize),
    Pour,
}

#[derive(Clone, Debug, PartialEq)]
pub enum JugEvent {
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

    const NAME: &'static str = "jug";
    const SCHEMA: u32 = 1;

    fn decide(cmd: &JugCmd, tx: &mut Tx<'_, Self>) -> Result<(), Infallible> {
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

    fn apply(s: &mut Cups, e: &JugEvent, (): &()) {
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

pub fn no_rules() -> Rulebook<()> {
    Rulebook::new("none", ())
}

// ---------------------------------------------------------------- a hat

/// Draws a slip. The random value is sampled by the caller into the
/// command; the instant comes from the command's metadata. Both land in the
/// event's payload, which is all apply sees.
pub struct Hat;

#[derive(Default, Clone, PartialEq, Debug)]
pub struct Slips {
    pub slips: Vec<String>,
    pub last: Option<Drawn>,
}

pub enum HatCmd {
    Put(String),
    /// `roll` is the caller's random value.
    Draw {
        roll: u64,
    },
}

#[derive(Clone, PartialEq, Debug)]
pub struct Drawn {
    pub slip: String,
    pub roll: u64,
    /// Business time, copied from the command's metadata.
    pub at: i64,
}

#[derive(Clone, PartialEq, Debug)]
pub enum HatEvent {
    Put(String),
    Drawn(Drawn),
}

#[derive(Clone, PartialEq, Debug)]
pub struct Empty;

impl fmt::Display for Empty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the hat is empty")
    }
}

impl Diagnostic for Empty {
    fn code(&self) -> &'static str {
        "toy-hat-empty"
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![]
    }
}

impl Domain for Hat {
    type State = Slips;
    type Command = HatCmd;
    type Event = HatEvent;
    type Rejection = Empty;
    type Rules = ();

    const NAME: &'static str = "hat";
    const SCHEMA: u32 = 1;

    fn decide(cmd: &HatCmd, tx: &mut Tx<'_, Self>) -> Result<(), Empty> {
        match cmd {
            HatCmd::Put(slip) => {
                tx.emit(HatEvent::Put(slip.clone()));
            }
            HatCmd::Draw { roll } => {
                let slips = &tx.state().slips;
                if slips.is_empty() {
                    return Err(Empty);
                }
                let slip = slips[(*roll % slips.len() as u64) as usize].clone();
                let at = tx.meta().recorded_at.0;
                tx.emit(HatEvent::Drawn(Drawn {
                    slip,
                    roll: *roll,
                    at,
                }));
            }
        }
        Ok(())
    }

    fn apply(s: &mut Slips, e: &HatEvent, (): &()) {
        match e {
            HatEvent::Put(slip) => s.slips.push(slip.clone()),
            HatEvent::Drawn(drawn) => s.last = Some(drawn.clone()),
        }
    }
}
