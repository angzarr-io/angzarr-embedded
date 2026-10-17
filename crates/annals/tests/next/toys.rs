//! Toy domains for the proposal tests: an account whose fee comes from its
//! rules and which ingests payments, a jug whose pour reads its own staged
//! events, a hat that draws, and a shelf shaped like hatpick's `ListState`
//! whose conflict units stand in for generated code.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt;

use annals::next::{
    commutative, Actor, Book, BookOf, Changed, CommutativeState, ConflictResolution, Correlation,
    DeriveIds, Domain, EditionName, FactMismatch, Fingerprint, Key, Origin, Provenance, RequestId,
    Root, Rulebook, Sampled, Stream, StreamKey, Timestamp, Tx, UnitPath, Units, Uuid,
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

/// A UUID whose first byte is `tag` and whose last bytes are `n`: ids of
/// different kinds never collide.
pub fn tagged(tag: u8, n: u128) -> Uuid {
    let mut bytes = n.to_be_bytes();
    bytes[0] = tag;
    Uuid(bytes)
}

/// The client-supplied request `n`'s id.
pub fn request(n: u128) -> RequestId {
    RequestId(tagged(0x52, n))
}

/// The correlation `n`.
pub fn correlation(n: u128) -> Correlation {
    Correlation(tagged(0x43, n))
}

/// The request id the application minted at `at` (a UUIDv7's stand-in).
pub fn minted(at: i64) -> RequestId {
    RequestId(tagged(0x07, at as u128))
}

/// The fingerprint of payload `n`. The real one is angzarr's UUIDv5
/// (`annals_angzarr::fingerprint`); the kernel only compares.
pub fn fingerprint(n: u8) -> Fingerprint {
    Fingerprint(tagged(0xf1, n as u128))
}

/// The toys' derivations: a fact's correlation is its external id's bytes
/// under the tag 0xFC. The real one is UUIDv5 (`annals_angzarr::ids`); the
/// kernel only asks.
pub struct ToyIds;

impl DeriveIds for ToyIds {
    fn fact_correlation(&self, external_id: &str) -> Correlation {
        let mut bytes = [0; 16];
        bytes[0] = 0xfc;
        for (b, c) in bytes[1..].iter_mut().zip(external_id.bytes()) {
            *b = c;
        }
        Correlation(Uuid(bytes))
    }
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

/// What the application supplied at `at` microseconds: that instant, the
/// request id it minted then, and the toys' derivations.
pub fn at(at: i64) -> Sampled {
    Sampled {
        now: Timestamp(at),
        request: minted(at),
        derive: &ToyIds,
    }
}

/// `cmd` for the `D` stream `stream`, as request `n`, from ann, with no correlation; its payload's fingerprint is `n`'s.
pub fn book<D: Domain>(stream: &Stream<D>, cmd: D::Command, n: u128) -> BookOf<D> {
    book_at::<D>(stream.key(), cmd, n)
}

/// `cmd` for `key`, as request `n`, from ann.
pub fn book_at<D: Domain>(key: &StreamKey, cmd: D::Command, n: u128) -> BookOf<D> {
    Book::command(key.clone(), cmd, Origin::request(request(n)), ann())
        .fingerprinted(fingerprint(n as u8))
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
    /// A bank statement's closing balance, for reconciliation.
    Stated(i64),
}

/// Facts from the payment provider.
#[derive(Clone, PartialEq, Debug)]
pub enum AccountFact {
    PaymentReceived(i64),
    /// A statement line from the bank, keyed by the line's id. The bank
    /// re-signs each redelivery, so one line arrives as other bytes: the
    /// account answers a mismatch on it as a duplicate (and the
    /// application alerts), not as a refusal.
    Statement(i64),
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
            AccountFact::Statement(c) => vec![AccountEvent::Stated(c)],
        }
    }

    /// Payments keep the default (a payment id reused for another amount
    /// is refused); statements, re-signed on every redelivery, are answered
    /// as duplicates.
    fn fact_mismatch(fact: &AccountFact) -> FactMismatch {
        match fact {
            AccountFact::PaymentReceived(_) => FactMismatch::Refuse,
            AccountFact::Statement(_) => FactMismatch::Duplicate,
        }
    }

    /// The fee is the rules': replaying under the wrong rules would give
    /// the wrong balance.
    fn apply(s: &mut Balance, e: &AccountEvent, rules: &Fees) {
        match *e {
            AccountEvent::Opened => s.open = true,
            AccountEvent::Deposited(c) => s.cents += c,
            AccountEvent::Withdrew(c) => s.cents -= c + rules.withdrawal,
            AccountEvent::Unmatched(_) | AccountEvent::Stated(_) => {}
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

/// Shaped like hatpick's `ListState` in angzarr's worked example
/// (`options.proto`, `conflict_units`): items per key, the columns whole,
/// the settings whole, plus a field with no marker. Commutative by default.
/// Under AGGREGATE_HANDLES its decide acknowledges the window only when
/// told to.
pub struct Shelf;

/// The state, as if generated from
///
/// ```text
/// message ShelfState {
///   option (io.angzarr.v1.commutative_state) = true;
///   map<string, int64> items = 1 [(io.angzarr.v1.conflict) = {}];
///   repeated string columns = 2 [(io.angzarr.v1.conflict) = {}];
///   Settings settings = 3 [(io.angzarr.v1.conflict) = {}];
///   string notes = 4;  // unmarked: conflicts with everything
/// }
/// ```
#[derive(Default, Clone, PartialEq, Debug)]
pub struct ShelfState {
    pub items: BTreeMap<String, i64>,
    pub columns: Vec<String>,
    pub settings: Settings,
    pub notes: String,
}

#[derive(Default, Clone, PartialEq, Debug)]
pub struct Settings {
    pub cooldown_days: u32,
}

/// **A test fixture standing in for generated code**: what
/// `protoc-gen-angzarr-conflict` emits for `ShelfState`'s markers. Never
/// hand-write this for a real domain.
impl CommutativeState for ShelfState {
    fn changed(before: &Self, after: &Self) -> Units {
        let mut units = Units::new();
        let items = UnitPath::field("items");
        for item in before.items.keys().chain(after.items.keys()) {
            if before.items.get(item) != after.items.get(item) {
                units.insert(items.key(Key::Str(item)));
            }
        }
        if before.columns != after.columns {
            units.insert(UnitPath::field("columns"));
        }
        if before.settings != after.settings {
            units.insert(UnitPath::field("settings"));
        }
        if before.notes != after.notes {
            units.insert(UnitPath::field("notes").wildcard());
        }
        units
    }
}

pub enum ShelfCmd {
    Set(Set),
    Columns(Vec<String>),
    Cooldown(u32),
    Note(String),
}

#[derive(Clone, PartialEq, Debug)]
pub struct Set {
    pub item: String,
    pub count: i64,
    /// Under AGGREGATE_HANDLES: acknowledge the window
    /// (`resolve_conflicts`), or return without doing so (a handler bug).
    pub resolve: bool,
}

#[derive(Clone, PartialEq, Debug)]
pub enum ShelfEvent {
    WasSet(WasSet),
    ColumnsSet(Vec<String>),
    CooldownSet(u32),
    Noted(String),
}

#[derive(Clone, PartialEq, Debug)]
pub struct WasSet {
    pub item: String,
    pub count: i64,
    /// How many events the window held, when decide was handed one.
    pub window: Option<usize>,
}

pub fn set(item: &str, count: i64) -> ShelfCmd {
    ShelfCmd::Set(Set {
        item: item.to_string(),
        count,
        resolve: true,
    })
}

impl Domain for Shelf {
    type State = ShelfState;
    type Command = ShelfCmd;
    type Fact = Infallible;
    type Event = ShelfEvent;
    type Rejection = Infallible;
    type Rules = ();

    const NAME: &'static str = "shelf";

    const COMMUTATIVE: Option<Changed<ShelfState>> = commutative::<ShelfState>();

    fn decide(cmd: &ShelfCmd, tx: &mut Tx<'_, Self>) -> Result<(), Infallible> {
        let event = match cmd {
            ShelfCmd::Set(set) => {
                let mut window = None;
                if let Some(conflicts) = tx.conflicts() {
                    window = Some(conflicts.window.len());
                    if set.resolve {
                        tx.resolve_conflicts(conflicts.window, conflicts.nonce);
                    }
                }
                ShelfEvent::WasSet(WasSet {
                    item: set.item.clone(),
                    count: set.count,
                    window,
                })
            }
            ShelfCmd::Columns(columns) => ShelfEvent::ColumnsSet(columns.clone()),
            ShelfCmd::Cooldown(days) => ShelfEvent::CooldownSet(*days),
            ShelfCmd::Note(note) => ShelfEvent::Noted(note.clone()),
        };
        tx.emit(event);
        Ok(())
    }

    fn ingest(_: &ShelfState, fact: &Infallible, (): &()) -> Vec<ShelfEvent> {
        match *fact {}
    }

    fn apply(s: &mut ShelfState, e: &ShelfEvent, (): &()) {
        match e {
            ShelfEvent::WasSet(set) => {
                s.items.insert(set.item.clone(), set.count);
            }
            ShelfEvent::ColumnsSet(columns) => s.columns = columns.clone(),
            ShelfEvent::CooldownSet(days) => s.settings.cooldown_days = *days,
            ShelfEvent::Noted(note) => s.notes = note.clone(),
        }
    }

    /// Commutative by default: two writers to different items never
    /// conflict.
    fn conflict_resolution(_: &ShelfCmd) -> ConflictResolution {
        ConflictResolution::Commutative
    }
}
