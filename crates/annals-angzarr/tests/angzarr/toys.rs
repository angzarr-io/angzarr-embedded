//! Toy domains whose payloads are prost messages under versioned packages,
//! as hatpick's will be: an account, and a ledger a policy commands from it.
//!
//! The account's deposit has three versions:
//!
//! - `toy.account.v1.Deposit { amount }`: whole euros;
//! - `toy.account.v2.Deposited { cents }`;
//! - `toy.account.v3.Deposited { cents, currency }`: current.

use std::convert::Infallible;
use std::fmt;

use annals::next::{
    Actor, Book, BookOf, Committed, Correlation, Domain, EditionName, Origin, Policy, Position,
    Provenance, Reaction, RequestId, Root, Rulebook, Sampled, Seq, StreamKey, Timestamp, Tx, Uuid,
};
use annals_angzarr::ids::ANGZARR_IDS;
use annals_angzarr::{fingerprinted, pack, Decoder, Payload, PayloadError, Upcaster};
use annals_diagnostic::{Arg, Diagnostic, Report};
use prost_types::Any;

/// A message under `package`, with its `prost::Name`.
macro_rules! named {
    ($package:literal, $name:ident { $($field:ident : $ty:ty = $kind:ident / $tag:literal),* $(,)? }) => {
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct $name {
            $(#[prost($kind, tag = $tag)] pub $field: $ty,)*
        }

        impl ::prost::Name for $name {
            const NAME: &'static str = stringify!($name);
            const PACKAGE: &'static str = $package;
        }
    };
}

pub mod v1 {
    named!(
        "toy.account.v1",
        Deposit {
            amount: i64 = int64 / "1"
        }
    );
}

pub mod v2 {
    named!(
        "toy.account.v2",
        Deposited {
            cents: i64 = int64 / "1"
        }
    );
}

pub mod v3 {
    named!("toy.account.v3", Opened {});
    named!(
        "toy.account.v3",
        Deposited {
            cents: i64 = int64 / "1",
            currency: String = string / "2"
        }
    );
    named!(
        "toy.account.v3",
        Withdrew {
            cents: i64 = int64 / "1"
        }
    );
}

/// The account's facts, for fact books.
pub mod fact {
    named!(
        "toy.account.fact.v1",
        PaymentReceived {
            cents: i64 = int64 / "1"
        }
    );
}

/// The account's commands, for command books.
pub mod cmd {
    named!("toy.account.cmd.v1", Open {});
    named!(
        "toy.account.cmd.v1",
        Deposit {
            cents: i64 = int64 / "1"
        }
    );
}

pub mod ledger {
    named!(
        "toy.ledger.v1",
        Recorded {
            source_seq: u64 = uint64 / "1",
            cents: i64 = int64 / "2"
        }
    );
}

/// A type no account code knows: written by a later release, say.
pub mod v9 {
    named!(
        "toy.account.v9",
        Bonus {
            cents: i64 = int64 / "1"
        }
    );
}

// ---------------------------------------------------------------- the account

pub struct Account;

#[derive(Default, Clone, PartialEq, Debug)]
pub struct Balance {
    pub open: bool,
    pub cents: i64,
}

#[derive(Clone, PartialEq, Debug)]
pub enum AccountCmd {
    Open,
    Deposit(i64),
}

/// A payment from the provider: a fact.
#[derive(Clone, PartialEq, Debug)]
pub enum AccountFact {
    PaymentReceived(i64),
}

#[derive(Clone, PartialEq, Debug)]
pub enum AccountEvent {
    Opened,
    Deposited { cents: i64, currency: String },
    Withdrew(i64),
}

#[derive(Clone, PartialEq, Debug)]
pub struct NotOpen;

impl fmt::Display for NotOpen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the account is not open")
    }
}

impl Diagnostic for NotOpen {
    fn code(&self) -> &'static str {
        "toy-account-not-open"
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![]
    }
}

impl Domain for Account {
    type State = Balance;
    type Command = AccountCmd;
    type Fact = AccountFact;
    type Event = AccountEvent;
    type Rejection = NotOpen;
    type Rules = ();

    const NAME: &'static str = "account";

    fn decide(cmd: &AccountCmd, tx: &mut Tx<'_, Self>) -> Result<(), NotOpen> {
        match *cmd {
            AccountCmd::Open => {
                tx.emit(AccountEvent::Opened);
            }
            AccountCmd::Deposit(cents) => {
                if !tx.state().open {
                    return Err(NotOpen);
                }
                tx.emit(AccountEvent::Deposited {
                    cents,
                    currency: "EUR".to_string(),
                });
            }
        }
        Ok(())
    }

    fn ingest(_: &Balance, fact: &AccountFact, (): &()) -> Vec<AccountEvent> {
        match *fact {
            AccountFact::PaymentReceived(cents) => vec![AccountEvent::Deposited {
                cents,
                currency: "EUR".to_string(),
            }],
        }
    }

    fn apply(s: &mut Balance, e: &AccountEvent, (): &()) {
        match e {
            AccountEvent::Opened => s.open = true,
            AccountEvent::Deposited { cents, .. } => s.cents += cents,
            AccountEvent::Withdrew(cents) => s.cents -= cents,
        }
    }
}

fn account_events() -> Decoder<AccountEvent> {
    Decoder::new()
        .on::<v3::Opened>(|_| AccountEvent::Opened)
        .on::<v3::Deposited>(|m| AccountEvent::Deposited {
            cents: m.cents,
            currency: m.currency,
        })
        .on::<v3::Withdrew>(|m| AccountEvent::Withdrew(m.cents))
}

impl Payload for AccountEvent {
    fn to_any(&self) -> Any {
        match self {
            AccountEvent::Opened => pack(&v3::Opened {}),
            AccountEvent::Deposited { cents, currency } => pack(&v3::Deposited {
                cents: *cents,
                currency: currency.clone(),
            }),
            AccountEvent::Withdrew(cents) => pack(&v3::Withdrew { cents: *cents }),
        }
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        account_events().decode(any)
    }

    fn type_urls() -> Vec<String> {
        account_events().type_urls()
    }
}

fn account_commands() -> Decoder<AccountCmd> {
    Decoder::new()
        .on::<cmd::Open>(|_| AccountCmd::Open)
        .on::<cmd::Deposit>(|m| AccountCmd::Deposit(m.cents))
}

impl Payload for AccountCmd {
    fn to_any(&self) -> Any {
        match self {
            AccountCmd::Open => pack(&cmd::Open {}),
            AccountCmd::Deposit(cents) => pack(&cmd::Deposit { cents: *cents }),
        }
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        account_commands().decode(any)
    }

    fn type_urls() -> Vec<String> {
        account_commands().type_urls()
    }
}

fn account_facts() -> Decoder<AccountFact> {
    Decoder::new().on::<fact::PaymentReceived>(|m| AccountFact::PaymentReceived(m.cents))
}

impl Payload for AccountFact {
    fn to_any(&self) -> Any {
        match self {
            AccountFact::PaymentReceived(cents) => pack(&fact::PaymentReceived { cents: *cents }),
        }
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        account_facts().decode(any)
    }

    fn type_urls() -> Vec<String> {
        account_facts().type_urls()
    }
}

/// Euros to cents.
pub fn one_to_two(old: v1::Deposit) -> Result<v2::Deposited, Report> {
    Ok(v2::Deposited {
        cents: old.amount * 100,
    })
}

/// Every v2 deposit was in euros.
pub fn two_to_three(old: v2::Deposited) -> Result<v3::Deposited, Report> {
    Ok(v3::Deposited {
        cents: old.cents,
        currency: "EUR".to_string(),
    })
}

/// The account's upcaster: v1 → v2 → v3, registered oldest first.
pub fn account_upcaster() -> Upcaster {
    Upcaster::new("account-upcaster", "account")
        .rule::<v1::Deposit, v2::Deposited>(one_to_two)
        .rule::<v2::Deposited, v3::Deposited>(two_to_three)
}

// ---------------------------------------------------------------- the ledger

pub struct Ledger;

#[derive(Default, Clone, PartialEq, Debug)]
pub struct LedgerState {
    pub checkpoint: Option<Seq>,
    pub cents: i64,
}

#[derive(Clone, PartialEq, Debug)]
pub struct Record {
    pub source_seq: Seq,
    pub cents: i64,
}

impl Domain for Ledger {
    type State = LedgerState;
    type Command = Record;
    type Fact = Infallible;
    type Event = Record;
    type Rejection = Infallible;
    type Rules = ();

    const NAME: &'static str = "ledger";

    fn decide(cmd: &Record, tx: &mut Tx<'_, Self>) -> Result<(), Infallible> {
        tx.emit(cmd.clone());
        Ok(())
    }

    fn ingest(_: &LedgerState, fact: &Infallible, (): &()) -> Vec<Record> {
        match *fact {}
    }

    fn apply(s: &mut LedgerState, r: &Record, (): &()) {
        s.checkpoint = Some(r.source_seq);
        s.cents += r.cents;
    }
}

fn ledger_events() -> Decoder<Record> {
    Decoder::new().on::<ledger::Recorded>(|m| Record {
        source_seq: m.source_seq,
        cents: m.cents,
    })
}

impl Payload for Record {
    fn to_any(&self) -> Any {
        pack(&ledger::Recorded {
            source_seq: self.source_seq,
            cents: self.cents,
        })
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        ledger_events().decode(any)
    }

    fn type_urls() -> Vec<String> {
        ledger_events().type_urls()
    }
}

pub const LEDGER_ROOT: Root = Root(100);

/// Every account event, recorded in the one ledger.
pub struct Books;

impl Reaction for Books {
    type Source = Account;
    type Target = Ledger;
    type TargetId = ();

    const NAME: &'static str = "books";

    fn targets(_: &Balance) -> Vec<()> {
        vec![()]
    }

    fn root((): &()) -> Root {
        LEDGER_ROOT
    }

    fn position(ledger: &LedgerState) -> Position {
        ledger.checkpoint.map_or(Position::START, Position::at)
    }
}

impl Policy for Books {
    fn command((): &(), c: &Committed<'_, Account>) -> Record {
        let cents = match c.event() {
            AccountEvent::Deposited { cents, .. } => *cents,
            _ => 0,
        };
        Record {
            source_seq: c.seq(),
            cents,
        }
    }
}

// ---------------------------------------------------------------- helpers

pub fn rules() -> Rulebook<()> {
    Rulebook::new("none", ())
}

/// Account `id`, on the main timeline.
pub fn account(id: u64) -> StreamKey {
    StreamKey::main("account", Root::from(id))
}

pub fn ledger_key(edition: Option<EditionName>) -> StreamKey {
    StreamKey::new("ledger", edition, LEDGER_ROOT)
}

/// A UUID whose first byte is `tag` and whose last bytes are `n`.
pub fn tagged(tag: u8, n: u32) -> Uuid {
    let mut bytes = [0; 16];
    bytes[0] = tag;
    bytes[12..].copy_from_slice(&n.to_be_bytes());
    Uuid(bytes)
}

/// The client-supplied request `n`'s id.
pub fn request(n: u32) -> RequestId {
    RequestId(tagged(0x52, n))
}

/// The request id the application minted at `at` (a UUIDv7's stand-in).
pub fn minted(at: i64) -> RequestId {
    RequestId(tagged(0x07, at as u32))
}

/// Ann, giving no correlation.
pub fn ann() -> Provenance {
    Provenance {
        actor: Some(Actor("pseudonym-ann".to_string())),
        correlation: None,
    }
}

/// `cmd` for account `key`, the first command of request `n`, from ann,
/// fingerprinted as the adapter does.
pub fn book(key: &StreamKey, cmd: AccountCmd, n: u32) -> BookOf<Account> {
    fingerprinted(Book::command(
        key.clone(),
        cmd,
        Origin::request(request(n)),
        ann(),
    ))
}

/// What the application supplied at `at` microseconds: the instant, the
/// request id it minted, and angzarr's derivations.
pub fn at(at: i64) -> Sampled {
    Sampled {
        now: Timestamp(at),
        request: minted(at),
        derive: &ANGZARR_IDS,
    }
}

/// The workflow `n`'s correlation.
pub fn correlation(n: u32) -> Correlation {
    Correlation(tagged(0x43, n))
}

/// The registry of both domains.
pub fn registry() -> annals_angzarr::Registry {
    annals_angzarr::Registry::new()
        .domain::<Account>(account_upcaster())
        .domain::<Ledger>(Upcaster::new("ledger-upcaster", "ledger"))
}
