//! Toy domains for the proposal tests: an account whose fee comes from its
//! rules and which ingests payments, a jug whose pour reads its own staged
//! events, a hat that draws, and a shelf that resolves its own conflicts.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::fmt;

use annals::next::{
    Actor, Book, BookOf, ConflictResolution, Correlation, Domain, EditionName, Origin, Provenance,
    RequestId, Root, Rulebook, Sampled, Stream, StreamKey, Timestamp, Tx,
};
use annals_diagnostic::{Arg, Diagnostic};

/// `domain`'s stream `id`, on the main timeline.
pub fn key(id: u64, domain: &str) -> StreamKey {
    StreamKey::main(domain, Root::from(id))
}

/// The edition `name`.
pub fn edition(name: &str) -> Option<EditionName> {
    Some(EditionName::new(name).unwrap())
}

/// An empty `D` stream `id`, on the main timeline.
pub fn stream<D: Domain>(id: u64) -> Stream<D> {
    Stream::new(None, Root::from(id))
}

/// The request `n`'s id.
pub fn request(n: u128) -> RequestId {
    RequestId(format!("req-{n}"))
}

/// The correlation `n`.
pub fn correlation(n: u128) -> Correlation {
    Correlation(format!("corr-{n}"))
}

/// Ann, giving no correlation.
pub fn ann() -> Provenance {
    Provenance {
        actor: Some(Actor("pseudonym-ann".to_string())),
        correlation: None,
    }
}

/// Ann, continuing the correlation `n`.
pub fn ann_in(n: u128) -> Provenance {
    Provenance {
        correlation: Some(correlation(n)),
        ..ann()
    }
}

/// What the application sampled at `at` microseconds: that instant, and a
/// minted correlation that names it.
pub fn at(at: i64) -> Sampled {
    Sampled {
        now: Timestamp(at),
        minted: Correlation(format!("minted-{at}")),
    }
}

/// `cmd` for the `D` stream `stream`, the first command of request `n`,
/// from ann, with no correlation.
pub fn book<D: Domain>(stream: &Stream<D>, cmd: D::Command, n: u128) -> BookOf<D> {
    Book::command(
        stream.key().clone(),
        cmd,
        Origin::request(request(n), 0),
        ann(),
    )
}

/// `cmd` for `key`, the first command of request `n`, from ann.
pub fn book_at<D: Domain>(key: &StreamKey, cmd: D::Command, n: u128) -> BookOf<D> {
    Book::command(key.clone(), cmd, Origin::request(request(n), 0), ann())
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
    /// A payment that arrived for an account that is not open: recorded,
    /// never refused.
    Unmatched(i64),
}

/// Facts from the payment provider.
#[derive(Clone, PartialEq, Debug)]
pub enum AccountFact {
    PaymentReceived(i64),
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
    type Fact = AccountFact;
    type Event = AccountEvent;
    type Rejection = Refused;
    type Rules = Fees;

    const NAME: &'static str = "account";

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

    /// A payment is a deposit if the account is open, and recorded as
    /// unmatched if not: the same situation a `Deposit` command is refused
    /// for, recorded instead.
    fn ingest(s: &Balance, fact: &AccountFact, _: &Fees) -> Vec<AccountEvent> {
        match *fact {
            AccountFact::PaymentReceived(c) if s.open => vec![AccountEvent::Deposited(c)],
            AccountFact::PaymentReceived(c) => vec![AccountEvent::Unmatched(c)],
        }
    }

    /// The fee is the rules': replaying under the wrong rules would give
    /// the wrong balance.
    fn apply(s: &mut Balance, e: &AccountEvent, rules: &Fees) {
        match *e {
            AccountEvent::Opened => s.open = true,
            AccountEvent::Deposited(c) => s.cents += c,
            AccountEvent::Withdrew(c) => s.cents -= c + rules.withdrawal,
            AccountEvent::Unmatched(_) => {}
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
    type Fact = Infallible;
    type Event = JugEvent;
    type Rejection = Infallible;
    type Rules = ();

    const NAME: &'static str = "jug";

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

    fn ingest(_: &Cups, fact: &Infallible, (): &()) -> Vec<JugEvent> {
        match *fact {}
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
/// command; the instant is the one the kernel records the event at. Both
/// land in the event's payload, which is all apply sees.
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
    /// Business time, copied from the recorded instant.
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
    type Fact = Infallible;
    type Event = HatEvent;
    type Rejection = Empty;
    type Rules = ();

    const NAME: &'static str = "hat";

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
                let at = tx.recorded_at().0;
                tx.emit(HatEvent::Drawn(Drawn {
                    slip,
                    roll: *roll,
                    at,
                }));
            }
        }
        Ok(())
    }

    fn ingest(_: &Slips, fact: &Infallible, (): &()) -> Vec<HatEvent> {
        match *fact {}
    }

    fn apply(s: &mut Slips, e: &HatEvent, (): &()) {
        match e {
            HatEvent::Put(slip) => s.slips.push(slip.clone()),
            HatEvent::Drawn(drawn) => s.last = Some(drawn.clone()),
        }
    }
}

// ---------------------------------------------------------------- a shelf

/// Counts per item. Its commands set one item's count; it can say which
/// items a change touched (so it supports COMMUTATIVE), and under
/// AGGREGATE_HANDLES its decide acknowledges the window only when told to.
pub struct Shelf;

#[derive(Default, Clone, PartialEq, Debug)]
pub struct Counts(pub BTreeMap<String, i64>);

#[derive(Clone, PartialEq, Debug)]
pub struct Set {
    pub item: String,
    pub count: i64,
    /// Under AGGREGATE_HANDLES: acknowledge the window
    /// (`resolve_conflicts`), or return without doing so (a handler bug).
    pub resolve: bool,
}

#[derive(Clone, PartialEq, Debug)]
pub struct WasSet {
    pub item: String,
    pub count: i64,
    /// How many events the window held, when decide was handed one.
    pub window: Option<usize>,
}

pub fn set(item: &str, count: i64) -> Set {
    Set {
        item: item.to_string(),
        count,
        resolve: true,
    }
}

impl Domain for Shelf {
    type State = Counts;
    type Command = Set;
    type Fact = Infallible;
    type Event = WasSet;
    type Rejection = Infallible;
    type Rules = ();

    const NAME: &'static str = "shelf";

    fn decide(cmd: &Set, tx: &mut Tx<'_, Self>) -> Result<(), Infallible> {
        let mut window = None;
        if let Some(conflicts) = tx.conflicts() {
            window = Some(conflicts.window.len());
            if cmd.resolve {
                tx.resolve_conflicts(conflicts.window, conflicts.nonce);
            }
        }
        tx.emit(WasSet {
            item: cmd.item.clone(),
            count: cmd.count,
            window,
        });
        Ok(())
    }

    fn ingest(_: &Counts, fact: &Infallible, (): &()) -> Vec<WasSet> {
        match *fact {}
    }

    fn apply(s: &mut Counts, e: &WasSet, (): &()) {
        s.0.insert(e.item.clone(), e.count);
    }

    /// Commutative by default: two writers to different items never
    /// conflict.
    fn conflict_resolution(_: &Set) -> ConflictResolution {
        ConflictResolution::Commutative
    }

    fn changed(before: &Counts, after: &Counts) -> Option<BTreeSet<String>> {
        let items = before.0.keys().chain(after.0.keys());
        Some(
            items
                .filter(|item| before.0.get(*item) != after.0.get(*item))
                .cloned()
                .collect(),
        )
    }
}
