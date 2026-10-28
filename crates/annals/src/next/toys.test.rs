//! Toy domains for the kernel's tests, their payloads prost messages under
//! versioned packages, as hatpick's will be:
//!
//! - an **account**: no conflict units (STRICT by default), rules with a
//!   fee, facts from a payment provider, an old command type
//!   (`toy.account.cmd.v0.Credit`, whole euros) and an old event type
//!   (`toy.account.v0.Credited`) that upcast to the current ones;
//! - a **shelf**: shaped like hatpick's `ListState`, its state declaring
//!   conflict units (COMMUTATIVE by default), one command type configured
//!   STRICT;
//! - a **ledger** a policy commands from the account, which refuses large
//!   amounts, for the chain. Its state is a plain Rust type whose snapshot
//!   form is a prost message, as a generated domain state's is.

use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use crate::next::v1::{
    command_page, Command, CommandBook, CommandPage, ConflictResolution, Cover, EventRef, Fact,
    FactOrigin, Handling, StateDescriptor, StateOptions, Uuid,
};
use crate::next::{
    ids, pack, Any, Committed, Decoder, Domain, IngestCtx, Key, Payload, PayloadError, Policy,
    Registry, Rulebook, Sampled, State, Timestamp, Tx, UnitPath, Units, Upcaster,
};
use annals_diagnostic::{Arg, Diagnostic, Report};

/// A message under `package`, with its `prost::Name`.
macro_rules! named {
    ($package:literal, $name:ident { $($(#[$attr:meta])* $field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct $name {
            $($(#[$attr])* pub $field: $ty,)*
        }

        impl ::prost::Name for $name {
            const NAME: &'static str = stringify!($name);
            const PACKAGE: &'static str = $package;
        }
    };
}

// ---------------------------------------------------------------- ids

/// A UUID whose first byte is `tag` and whose last bytes are `n`: ids of
/// different kinds never collide.
pub fn tagged(tag: u8, n: u128) -> Uuid {
    let mut bytes = n.to_be_bytes();
    bytes[0] = tag;
    Uuid::from_bytes(bytes)
}

/// Stream root `n`.
pub fn root(n: u128) -> Uuid {
    tagged(0x0a, n)
}

/// The client-supplied command id `n`.
pub fn command_id(n: u128) -> Uuid {
    tagged(0xc1, n)
}

/// The correlation `n`.
pub fn correlation(n: u128) -> Uuid {
    tagged(0x43, n)
}

/// The command id the application minted at `seconds` (a UUIDv7's
/// stand-in: the kernel never inspects a version).
pub fn minted(seconds: i64) -> Uuid {
    tagged(0x07, seconds as u128)
}

/// The instant `seconds` after the epoch.
pub fn instant(seconds: i64) -> Timestamp {
    Timestamp { seconds, nanos: 0 }
}

/// What the application sampled at `seconds`: that instant and the command
/// id it minted then.
pub fn at(seconds: i64) -> Sampled {
    Sampled {
        now: instant(seconds),
        command_id: minted(seconds),
    }
}

// ---------------------------------------------------------------- books

/// `domain`'s stream `n`, on the main timeline.
pub fn cover(domain: &str, n: u128) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(root(n)),
        edition: None,
    }
}

/// `domain`'s stream `n` in `edition`.
pub fn cover_in(domain: &str, n: u128, edition: &str) -> Cover {
    Cover {
        edition: Some(edition.to_string()),
        ..cover(domain, n)
    }
}

/// A client's command to `cover`, with no ids and no handling.
pub fn command(cover: Cover, payload: Any) -> CommandBook {
    CommandBook {
        cover: Some(cover),
        page: Some(CommandPage {
            body: Some(command_page::Body::Command(Command {
                payload: Some(payload),
            })),
            ..CommandPage::default()
        }),
        handling: None,
    }
}

/// A fact to `cover` from `external_id`, with no ids and no handling.
pub fn fact(cover: Cover, payload: Any, external_id: &str) -> CommandBook {
    CommandBook {
        cover: Some(cover),
        page: Some(CommandPage {
            body: Some(command_page::Body::Fact(Fact {
                payload: Some(payload),
            })),
            fact_origin: Some(FactOrigin {
                external_id: external_id.to_string(),
                description: "toy payments webhook".to_string(),
            }),
            ..CommandPage::default()
        }),
        handling: None,
    }
}

/// Edits to a book, for building cases.
pub trait Edit: Sized {
    fn page_mut(&mut self) -> &mut CommandPage;
    fn book_mut(&mut self) -> &mut CommandBook;

    /// With the client-supplied command id `n`.
    fn id(mut self, n: u128) -> Self {
        self.page_mut().command_id = Some(command_id(n));
        self
    }

    /// In the correlation `n`.
    fn correlated(mut self, n: u128) -> Self {
        self.page_mut().correlation_id = Some(correlation(n));
        self
    }

    /// Expecting the stream at `sequence`, resolved by `resolution` if
    /// stale (`Unspecified`: by configuration).
    fn expecting(mut self, sequence: u64, resolution: ConflictResolution) -> Self {
        self.book_mut().handling = Some(Handling {
            expected_sequence: Some(sequence),
            conflict_resolution: resolution as i32,
        });
        self
    }

    /// Asking for `resolution`, with no expected sequence.
    fn resolved_by(mut self, resolution: ConflictResolution) -> Self {
        self.book_mut().handling = Some(Handling {
            expected_sequence: None,
            conflict_resolution: resolution as i32,
        });
        self
    }

    /// Claiming to react to `trigger`: only the kernel may.
    fn caused_by(mut self, trigger: EventRef) -> Self {
        self.page_mut().causation_id = Some(trigger);
        self
    }
}

impl Edit for CommandBook {
    fn page_mut(&mut self) -> &mut CommandPage {
        self.page.get_or_insert_with(CommandPage::default)
    }

    fn book_mut(&mut self) -> &mut CommandBook {
        self
    }
}

/// The registry every test runs: the three toys, each with an upcaster
/// that has no rules (registering the account's v0 rules is
/// [`registry_with_upcasters`]'s).
pub fn registry() -> Registry {
    let mut registry = Registry::new();
    registry
        .register::<Account>(fees(), Upcaster::new("account-upcaster", "account"))
        .unwrap();
    registry
        .register::<Shelf>(
            Rulebook::new("v1", ()),
            Upcaster::new("shelf-upcaster", "shelf"),
        )
        .unwrap();
    registry
        .register::<Ledger>(
            Rulebook::new("v1", ()),
            Upcaster::new("ledger-upcaster", "ledger"),
        )
        .unwrap();
    registry
}

/// [`registry`], the account upcasting its v0 command and event.
pub fn registry_with_upcasters() -> Registry {
    let mut registry = Registry::new();
    registry
        .register::<Account>(fees(), account_upcaster())
        .unwrap();
    registry
        .register::<Shelf>(
            Rulebook::new("v1", ()),
            Upcaster::new("shelf-upcaster", "shelf"),
        )
        .unwrap();
    registry
        .register::<Ledger>(
            Rulebook::new("v1", ()),
            Upcaster::new("ledger-upcaster", "ledger"),
        )
        .unwrap();
    registry
}

// ---------------------------------------------------------------- the account

pub mod account {
    use std::collections::BTreeMap;

    named!("toy.account.v1", AccountState {
        #[prost(bool, tag = "1")] open: bool,
        #[prost(int64, tag = "2")] cents: i64,
        #[prost(int64, tag = "3")] unmatched: i64,
        /// Received payments, by external id, with what ingest's context said.
        #[prost(btree_map = "string, message", tag = "4")] received: BTreeMap<String, super::account::Received>,
    });

    // Current events.
    named!("toy.account.v1", Opened {});
    named!(
        "toy.account.v1",
        Deposited {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    named!(
        "toy.account.v1",
        Withdrew {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    // A payment, ingested: what the fact said and what ingest's read-only
    // context said about it.
    named!(
        "toy.account.v1",
        Received {
            #[prost(int64, tag = "1")]
            cents: i64,
            #[prost(string, tag = "2")]
            external_id: String,
            #[prost(int64, tag = "3")]
            recorded_seconds: i64,
            #[prost(string, tag = "4")]
            rules_version: String,
            #[prost(bool, tag = "5")]
            matched: bool,
        }
    );

    /// The old deposit event: whole euros.
    pub mod v0 {
        named!(
            "toy.account.v0",
            Credited {
                #[prost(int64, tag = "1")]
                euros: i64,
            }
        );
    }

    // Commands.
    pub mod cmd {
        named!("toy.account.cmd.v1", Open {});
        named!(
            "toy.account.cmd.v1",
            Deposit {
                #[prost(int64, tag = "1")]
                cents: i64,
            }
        );
        named!(
            "toy.account.cmd.v1",
            Withdraw {
                #[prost(int64, tag = "1")]
                cents: i64,
            }
        );
        // Withdraws everything, or nothing if the balance is 0: a
        // decision that may emit no events.
        named!("toy.account.cmd.v1", Sweep {});

        /// The old command: whole euros. Upcast to `Deposit`
        /// (discouraged, here to pin that it works).
        pub mod v0 {
            named!(
                "toy.account.cmd.v0",
                Credit {
                    #[prost(int64, tag = "1")]
                    euros: i64,
                }
            );
        }
    }

    /// Facts from the payment provider.
    pub mod fact {
        named!(
            "toy.account.fact.v1",
            PaymentReceived {
                #[prost(int64, tag = "1")]
                cents: i64,
            }
        );
    }

    /// A type no account code knows: written by a later release, say.
    pub mod v9 {
        named!(
            "toy.account.v9",
            Bonus {
                #[prost(int64, tag = "1")]
                cents: i64,
            }
        );
    }
}

use account::AccountState;

/// The rules: a fee taken from every withdrawal, in cents. Version `v2` is
/// current; `v1` charged nothing.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Fees {
    pub withdrawal: i64,
}

pub fn fees() -> Rulebook<Fees> {
    Rulebook::new("v2", Fees { withdrawal: 1 }).with("v1", Fees { withdrawal: 0 })
}

/// v0 → v1 for the command and the event, both in cents.
pub fn account_upcaster() -> Upcaster {
    Upcaster::new("account-upcaster", "account")
        .rule::<account::cmd::v0::Credit, account::cmd::Deposit>(|c| {
            Ok(account::cmd::Deposit {
                cents: c.euros * 100,
            })
        })
        .rule::<account::v0::Credited, account::Deposited>(|c| {
            Ok(account::Deposited {
                cents: c.euros * 100,
            })
        })
}

/// No generated conflict units: the implicit base only, so STRICT is the
/// default. (A fixture standing in for generated code.)
impl State for AccountState {
    /// A prost state is its own snapshot form.
    type Snapshot = Self;

    fn to_snapshot(&self) -> Self {
        self.clone()
    }

    fn from_snapshot(snapshot: Self) -> Result<Self, Report> {
        Ok(snapshot)
    }

    fn descriptor() -> StateDescriptor {
        StateDescriptor {
            type_url: "/toy.account.v1.AccountState".to_string(),
            options: Some(StateOptions {
                conflict_resolutions: vec![
                    ConflictResolution::Strict as i32,
                    ConflictResolution::AggregateHandles as i32,
                ],
            }),
        }
    }
}

pub struct Account;

#[derive(Clone, PartialEq, Debug)]
pub enum AccountCmd {
    Open,
    Deposit(i64),
    Withdraw(i64),
    Sweep,
}

#[derive(Clone, PartialEq, Debug)]
pub enum AccountFact {
    PaymentReceived(i64),
}

#[derive(Clone, PartialEq, Debug)]
pub enum AccountEvent {
    Opened,
    Deposited(i64),
    Withdrew(i64),
    Received(account::Received),
}

impl Payload for AccountCmd {
    fn to_any(&self) -> Any {
        match self {
            AccountCmd::Open => pack(&account::cmd::Open {}),
            AccountCmd::Deposit(cents) => pack(&account::cmd::Deposit { cents: *cents }),
            AccountCmd::Withdraw(cents) => pack(&account::cmd::Withdraw { cents: *cents }),
            AccountCmd::Sweep => pack(&account::cmd::Sweep {}),
        }
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        account_commands().decode(any)
    }

    fn type_urls() -> Vec<String> {
        account_commands().type_urls()
    }
}

fn account_commands() -> Decoder<AccountCmd> {
    Decoder::new()
        .on::<account::cmd::Open>(|_| AccountCmd::Open)
        .on::<account::cmd::Deposit>(|d| AccountCmd::Deposit(d.cents))
        .on::<account::cmd::Withdraw>(|w| AccountCmd::Withdraw(w.cents))
        .on::<account::cmd::Sweep>(|_| AccountCmd::Sweep)
}

impl Payload for AccountFact {
    fn to_any(&self) -> Any {
        match self {
            AccountFact::PaymentReceived(cents) => {
                pack(&account::fact::PaymentReceived { cents: *cents })
            }
        }
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        account_facts().decode(any)
    }

    fn type_urls() -> Vec<String> {
        account_facts().type_urls()
    }
}

fn account_facts() -> Decoder<AccountFact> {
    Decoder::new().on::<account::fact::PaymentReceived>(|p| AccountFact::PaymentReceived(p.cents))
}

impl Payload for AccountEvent {
    fn to_any(&self) -> Any {
        match self {
            AccountEvent::Opened => pack(&account::Opened {}),
            AccountEvent::Deposited(cents) => pack(&account::Deposited { cents: *cents }),
            AccountEvent::Withdrew(cents) => pack(&account::Withdrew { cents: *cents }),
            AccountEvent::Received(r) => pack(r),
        }
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        account_events().decode(any)
    }

    fn type_urls() -> Vec<String> {
        account_events().type_urls()
    }
}

pub fn account_events() -> Decoder<AccountEvent> {
    Decoder::new()
        .on::<account::Opened>(|_| AccountEvent::Opened)
        .on::<account::Deposited>(|d| AccountEvent::Deposited(d.cents))
        .on::<account::Withdrew>(|w| AccountEvent::Withdrew(w.cents))
        .on::<account::Received>(AccountEvent::Received)
}

#[derive(Clone, PartialEq, Debug)]
pub enum Refused {
    AlreadyOpen,
    NotOpen,
    Overdrawn { short: i64 },
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Refused::AlreadyOpen => f.write_str("the account is already open"),
            Refused::NotOpen => f.write_str("the account is not open"),
            Refused::Overdrawn { short } => write!(f, "{short} cents short"),
        }
    }
}

impl Diagnostic for Refused {
    fn code(&self) -> &'static str {
        match self {
            Refused::AlreadyOpen => "toy-account-already-open",
            Refused::NotOpen => "toy-account-not-open",
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
    type State = AccountState;
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
            AccountCmd::Deposit(cents) => {
                if !s.open {
                    return Err(Refused::NotOpen);
                }
                tx.emit(AccountEvent::Deposited(cents));
            }
            AccountCmd::Withdraw(cents) => {
                let total = cents + tx.rules().withdrawal;
                if total > s.cents {
                    return Err(Refused::Overdrawn {
                        short: total - s.cents,
                    });
                }
                tx.emit(AccountEvent::Withdrew(cents));
            }
            AccountCmd::Sweep => {
                let all = s.cents - tx.rules().withdrawal;
                if all > 0 {
                    tx.emit(AccountEvent::Withdrew(all));
                }
            }
        }
        Ok(())
    }

    /// A payment is recorded whether or not the account is open (never
    /// refused), with what the read-only context says about it. A payment
    /// of a negative amount is a provider bug this toy does not handle:
    /// it panics, standing in for an ingest bug.
    fn ingest(
        s: &AccountState,
        fact: &AccountFact,
        ctx: &IngestCtx<'_, Self>,
    ) -> Vec<AccountEvent> {
        let AccountFact::PaymentReceived(cents) = *fact;
        assert!(cents >= 0, "toy ingest bug: a negative payment");
        // What the rest of the context says, pinned here: a wrong value is a
        // panic, so the ingest tests fail on it.
        assert_eq!(ctx.stream().domain, "account");
        assert_eq!(
            ctx.command_id(),
            &ids::fact_command_id(&ctx.source().external_id)
        );
        assert!(ctx.correlation_id().is_well_formed());
        assert_eq!(ctx.rules(), &Fees { withdrawal: 1 }, "the current rules");
        vec![AccountEvent::Received(account::Received {
            cents,
            external_id: ctx.source().external_id.clone(),
            recorded_seconds: ctx.recorded_at().seconds,
            rules_version: ctx.rules_version().to_string(),
            matched: s.open,
        })]
    }

    /// The fee is the rules': replaying under the wrong rules would give
    /// the wrong balance.
    fn apply(s: &mut AccountState, e: &AccountEvent, rules: &Fees) {
        match e {
            AccountEvent::Opened => s.open = true,
            AccountEvent::Deposited(cents) => s.cents += cents,
            AccountEvent::Withdrew(cents) => s.cents -= cents + rules.withdrawal,
            AccountEvent::Received(r) => {
                if r.matched {
                    s.cents += r.cents;
                } else {
                    s.unmatched += r.cents;
                }
                s.received.insert(r.external_id.clone(), r.clone());
            }
        }
    }
}

// ---------------------------------------------------------------- a shelf

pub mod shelf {
    use std::collections::BTreeMap;

    // The state, as if generated from
    //
    // ```text
    // message ShelfState {
    //   option (io.angzarr.v1.state) = { conflict_resolutions: [CONFLICT_RESOLUTION_COMMUTATIVE] };
    //   map<string, int64> items = 1 [(io.angzarr.v1.conflict) = {}];
    //   repeated string columns = 2 [(io.angzarr.v1.conflict) = {}];
    //   Settings settings = 3 [(io.angzarr.v1.conflict) = {}];
    //   string notes = 4;  // unmarked: a build warning, and it conflicts with everything
    // }
    // ```
    named!("toy.shelf.v1", ShelfState {
        #[prost(btree_map = "string, int64", tag = "1")] items: BTreeMap<String, i64>,
        #[prost(string, repeated, tag = "2")] columns: Vec<String>,
        #[prost(message, optional, tag = "3")] settings: Option<Settings>,
        #[prost(string, tag = "4")] notes: String,
    });
    named!(
        "toy.shelf.v1",
        Settings {
            #[prost(uint32, tag = "1")]
            cooldown_days: u32,
        }
    );

    // Commands.
    named!(
        "toy.shelf.cmd.v1",
        Set {
            #[prost(string, tag = "1")]
            item: String,
            #[prost(int64, tag = "2")]
            count: i64,
            /// Under AGGREGATE_HANDLES: acknowledge the window, or return
            /// without doing so (a handler bug).
            #[prost(bool, tag = "3")]
            resolve: bool,
        }
    );
    named!("toy.shelf.cmd.v1", Columns {
        #[prost(string, repeated, tag = "1")] columns: Vec<String>,
    });
    named!(
        "toy.shelf.cmd.v1",
        Cooldown {
            #[prost(uint32, tag = "1")]
            days: u32,
        }
    );
    // Configured STRICT, by command type, on a COMMUTATIVE state.
    named!(
        "toy.shelf.cmd.v1",
        Note {
            #[prost(string, tag = "1")]
            note: String,
        }
    );

    // Events.
    named!("toy.shelf.v1", WasSet {
        #[prost(string, tag = "1")] item: String,
        #[prost(int64, tag = "2")] count: i64,
        /// How many events the window held, when decide was handed one.
        #[prost(uint64, optional, tag = "3")] window: Option<u64>,
    });
    named!("toy.shelf.v1", ColumnsSet {
        #[prost(string, repeated, tag = "1")] columns: Vec<String>,
    });
    named!(
        "toy.shelf.v1",
        CooldownSet {
            #[prost(uint32, tag = "1")]
            days: u32,
        }
    );
    named!(
        "toy.shelf.v1",
        Noted {
            #[prost(string, tag = "1")]
            note: String,
        }
    );
}

pub use shelf::ShelfState;

/// **A test fixture standing in for generated code**: what angzarr's
/// generator emits for `ShelfState`'s markers. Never hand-write this for a
/// real domain.
impl State for ShelfState {
    /// A prost state is its own snapshot form.
    type Snapshot = Self;

    fn to_snapshot(&self) -> Self {
        self.clone()
    }

    fn from_snapshot(snapshot: Self) -> Result<Self, Report> {
        Ok(snapshot)
    }

    fn descriptor() -> StateDescriptor {
        StateDescriptor {
            type_url: "/toy.shelf.v1.ShelfState".to_string(),
            options: Some(StateOptions {
                conflict_resolutions: vec![
                    ConflictResolution::Commutative as i32,
                    ConflictResolution::Strict as i32,
                    ConflictResolution::AggregateHandles as i32,
                ],
            }),
        }
    }

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

pub struct Shelf;

#[derive(Clone, PartialEq, Debug)]
pub enum ShelfCmd {
    Set(shelf::Set),
    Columns(Vec<String>),
    Cooldown(u32),
    Note(String),
}

#[derive(Clone, PartialEq, Debug)]
pub enum ShelfEvent {
    WasSet(shelf::WasSet),
    ColumnsSet(Vec<String>),
    CooldownSet(u32),
    Noted(String),
}

/// Set `item` to `count`, acknowledging any window.
pub fn set(item: &str, count: i64) -> Any {
    pack(&shelf::Set {
        item: item.to_string(),
        count,
        resolve: true,
    })
}

impl Payload for ShelfCmd {
    fn to_any(&self) -> Any {
        match self {
            ShelfCmd::Set(set) => pack(set),
            ShelfCmd::Columns(columns) => pack(&shelf::Columns {
                columns: columns.clone(),
            }),
            ShelfCmd::Cooldown(days) => pack(&shelf::Cooldown { days: *days }),
            ShelfCmd::Note(note) => pack(&shelf::Note { note: note.clone() }),
        }
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        Decoder::new()
            .on::<shelf::Set>(ShelfCmd::Set)
            .on::<shelf::Columns>(|c| ShelfCmd::Columns(c.columns))
            .on::<shelf::Cooldown>(|c| ShelfCmd::Cooldown(c.days))
            .on::<shelf::Note>(|n| ShelfCmd::Note(n.note))
            .decode(any)
    }

    fn type_urls() -> Vec<String> {
        Vec::new()
    }
}

impl Payload for ShelfEvent {
    fn to_any(&self) -> Any {
        match self {
            ShelfEvent::WasSet(set) => pack(set),
            ShelfEvent::ColumnsSet(columns) => pack(&shelf::ColumnsSet {
                columns: columns.clone(),
            }),
            ShelfEvent::CooldownSet(days) => pack(&shelf::CooldownSet { days: *days }),
            ShelfEvent::Noted(note) => pack(&shelf::Noted { note: note.clone() }),
        }
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        Decoder::new()
            .on::<shelf::WasSet>(ShelfEvent::WasSet)
            .on::<shelf::ColumnsSet>(|c| ShelfEvent::ColumnsSet(c.columns))
            .on::<shelf::CooldownSet>(|c| ShelfEvent::CooldownSet(c.days))
            .on::<shelf::Noted>(|n| ShelfEvent::Noted(n.note))
            .decode(any)
    }

    fn type_urls() -> Vec<String> {
        Vec::new()
    }
}

impl Domain for Shelf {
    type State = ShelfState;
    type Command = ShelfCmd;
    type Fact = Infallible;
    type Event = ShelfEvent;
    type Rejection = Infallible;
    type Rules = ();

    const NAME: &'static str = "shelf";

    /// A note is free text a person reads as a whole against the items: a
    /// stale one is refused rather than merged.
    const CONFLICT_RESOLUTION_BY_COMMAND: &'static [(&'static str, ConflictResolution)] =
        &[("toy.shelf.cmd.v1.Note", ConflictResolution::Strict)];

    fn decide(cmd: &ShelfCmd, tx: &mut Tx<'_, Self>) -> Result<(), Infallible> {
        let event = match cmd {
            ShelfCmd::Set(set) => {
                let mut window = None;
                if let Some(conflicts) = tx.conflicts() {
                    window = Some(conflicts.window.len() as u64);
                    if set.resolve {
                        tx.resolve_conflicts(conflicts.window, conflicts.nonce);
                    }
                }
                ShelfEvent::WasSet(shelf::WasSet {
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

    fn ingest(_: &ShelfState, fact: &Infallible, _: &IngestCtx<'_, Self>) -> Vec<ShelfEvent> {
        match *fact {}
    }

    fn apply(s: &mut ShelfState, e: &ShelfEvent, (): &()) {
        match e {
            ShelfEvent::WasSet(set) => {
                s.items.insert(set.item.clone(), set.count);
            }
            ShelfEvent::ColumnsSet(columns) => s.columns = columns.clone(),
            ShelfEvent::CooldownSet(days) => {
                s.settings
                    .get_or_insert_with(Default::default)
                    .cooldown_days = *days
            }
            ShelfEvent::Noted(note) => s.notes = note.clone(),
        }
    }
}

// ---------------------------------------------------------------- a ledger

pub mod ledger {
    named!(
        "toy.ledger.v1",
        LedgerSnapshot {
            /// The next account sequence the ledger is owed: its position.
            #[prost(uint64, tag = "1")]
            next_source: u64,
            #[prost(int64, tag = "2")]
            total: i64,
        }
    );
    named!(
        "toy.ledger.cmd.v1",
        Record {
            #[prost(uint64, tag = "1")]
            source_sequence: u64,
            #[prost(int64, tag = "2")]
            cents: i64,
        }
    );
    named!(
        "toy.ledger.v1",
        Recorded {
            #[prost(uint64, tag = "1")]
            source_sequence: u64,
            #[prost(int64, tag = "2")]
            cents: i64,
        }
    );
}

/// The ledger's state, a domain type: its total is never negative, so a
/// snapshot claiming one is not a ledger.
#[derive(Clone, Default, PartialEq, Debug)]
pub struct LedgerState {
    /// The next account sequence the ledger is owed: its position.
    pub next_source: u64,
    pub total: i64,
}

/// A total below zero: the reason a ledger snapshot is refused.
#[derive(Clone, PartialEq, Debug)]
pub struct NegativeTotal {
    pub total: i64,
}

impl fmt::Display for NegativeTotal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a ledger total of {} is negative", self.total)
    }
}

impl Diagnostic for NegativeTotal {
    fn code(&self) -> &'static str {
        "toy-ledger-negative-total"
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![("total", self.total.into())]
    }
}

impl State for LedgerState {
    type Snapshot = ledger::LedgerSnapshot;

    fn descriptor() -> StateDescriptor {
        StateDescriptor {
            type_url: "/toy.ledger.v1.LedgerSnapshot".to_string(),
            options: Some(StateOptions {
                conflict_resolutions: vec![
                    ConflictResolution::Strict as i32,
                    ConflictResolution::AggregateHandles as i32,
                ],
            }),
        }
    }

    fn to_snapshot(&self) -> ledger::LedgerSnapshot {
        ledger::LedgerSnapshot {
            next_source: self.next_source,
            total: self.total,
        }
    }

    fn from_snapshot(snapshot: ledger::LedgerSnapshot) -> Result<Self, Report> {
        if snapshot.total < 0 {
            return Err(Report::of(&NegativeTotal {
                total: snapshot.total,
            }));
        }
        Ok(LedgerState {
            next_source: snapshot.next_source,
            total: snapshot.total,
        })
    }
}

pub struct Ledger;

/// Deposits above this are refused by the ledger: a rejection three steps
/// from nowhere, for the chain.
pub const LEDGER_LIMIT: i64 = 1_000;

#[derive(Clone, PartialEq, Debug)]
pub struct TooLarge;

impl fmt::Display for TooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("too large for the ledger")
    }
}

impl Diagnostic for TooLarge {
    fn code(&self) -> &'static str {
        "toy-ledger-too-large"
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![]
    }
}

impl Payload for ledger::Record {
    fn to_any(&self) -> Any {
        pack(self)
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        Decoder::new().on::<ledger::Record>(|r| r).decode(any)
    }

    fn type_urls() -> Vec<String> {
        vec!["/toy.ledger.cmd.v1.Record".to_string()]
    }
}

impl Payload for ledger::Recorded {
    fn to_any(&self) -> Any {
        pack(self)
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        Decoder::new().on::<ledger::Recorded>(|r| r).decode(any)
    }

    fn type_urls() -> Vec<String> {
        vec!["/toy.ledger.v1.Recorded".to_string()]
    }
}

impl Domain for Ledger {
    type State = LedgerState;
    type Command = ledger::Record;
    type Fact = Infallible;
    type Event = ledger::Recorded;
    type Rejection = TooLarge;
    type Rules = ();

    const NAME: &'static str = "ledger";

    fn decide(cmd: &ledger::Record, tx: &mut Tx<'_, Self>) -> Result<(), TooLarge> {
        if cmd.cents > LEDGER_LIMIT {
            return Err(TooLarge);
        }
        tx.emit(ledger::Recorded {
            source_sequence: cmd.source_sequence,
            cents: cmd.cents,
        });
        Ok(())
    }

    fn ingest(
        _: &LedgerState,
        fact: &Infallible,
        _: &IngestCtx<'_, Self>,
    ) -> Vec<ledger::Recorded> {
        match *fact {}
    }

    fn apply(s: &mut LedgerState, e: &ledger::Recorded, (): &()) {
        s.next_source = e.source_sequence + 1;
        s.total += e.cents;
    }
}

/// The ledger every account books into.
pub fn ledger_root() -> Uuid {
    ids::compute_root("ledger", "main").unwrap()
}

/// Account deposits → the ledger.
pub struct Bookkeeping;

impl Policy for Bookkeeping {
    type Source = Account;
    type Target = Ledger;
    type TargetId = ();

    const NAME: &'static str = "bookkeeping";

    fn targets(_: &Committed<'_, Account>) -> Vec<()> {
        vec![()]
    }

    fn root((): &()) -> Uuid {
        ledger_root()
    }

    fn command((): &(), committed: &Committed<'_, Account>) -> ledger::Record {
        let cents = match committed.event() {
            AccountEvent::Deposited(cents) => *cents,
            _ => 0,
        };
        ledger::Record {
            source_sequence: committed.sequence(),
            cents,
        }
    }
}

// ---------------------------------------------------------------- running

/// Handle `book` through the kernel over `store`, with no policies and no
/// projections, logging dead letters to nowhere the test reads.
pub fn handle<D: Domain>(
    store: &mut crate::next::InMemoryStore,
    registry: &Registry,
    book: CommandBook,
    sampled: &Sampled,
) -> Result<crate::next::Handled, crate::next::Failure<D::Rejection>> {
    let log = crate::next::MemoryDeadLetters::new();
    let kernel = crate::next::Kernel::new(registry, &log);
    let routes = crate::next::Routes::new();
    kernel.handle::<D, _, _>(store.begin(), &routes, book, sampled, &mut ())
}

/// Every payload in `books`, decoded as `E`, in order.
pub fn payloads<E: Payload>(books: &[crate::next::v1::EventBook]) -> Vec<E> {
    books
        .iter()
        .flat_map(|b| &b.pages)
        .map(|p| E::from_any(p.payload.as_ref().expect("a page has a payload")).unwrap())
        .collect()
}

/// An open account `n` holding `cents`, through the kernel: commands 1 and
/// 2 (open, then deposit) at seconds 10 and 11.
pub fn opened(store: &mut crate::next::InMemoryStore, registry: &Registry, n: u128, cents: i64) {
    handle::<Account>(
        store,
        registry,
        command(cover("account", n), AccountCmd::Open.to_any()).id(1),
        &at(10),
    )
    .unwrap();
    handle::<Account>(
        store,
        registry,
        command(cover("account", n), AccountCmd::Deposit(cents).to_any()).id(2),
        &at(11),
    )
    .unwrap();
}

/// Poll a future to completion with a waker that does nothing: enough for
/// futures that are ready at once.
pub fn ready<F: Future>(f: F) -> F::Output {
    fn noop(_: *const ()) {}
    fn clone(p: *const ()) -> RawWaker {
        RawWaker::new(p, &VTABLE)
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    // SAFETY: every vtable function ignores the data pointer.
    let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
    let mut f = pin!(f);
    match f.as_mut().poll(&mut Context::from_waker(&waker)) {
        Poll::Ready(out) => out,
        Poll::Pending => panic!("not ready at once"),
    }
}
