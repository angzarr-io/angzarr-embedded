//! A **wallet** and a **lamp** written entirely in annals-macros' markup,
//! beside a hand-written **tally**, in one registry; a **buy-in**, a
//! process manager, and the **sagas** between it and the wallet. All are
//! on annals as a dependent sees it, `::annals`, the path the expansion
//! names: not `crate::`, whose types are another instance's in this test
//! build.
//!
//! The wallet is the markup's whole shape: the instance holds the state
//! (backed by a [`Ledger`], which knows nothing of the markup, and an
//! immutable [`WalletConfig`]); pure `&self` handlers that take a
//! [`Context`] and return their events as a single event, a tuple, an
//! `Option`, a `Vec`, or an [`Emit`] with a continuation step; a command
//! handler declaring its typed refusal objects (`refuses`); a fact
//! handler, with no `Result`, that records a refusal as an event; `&mut
//! self` appliers; a rejection inferred from the command handlers; a
//! snapshot form restored onto the factory's instance; domain types, each
//! with the wire message it travels as. Both marked-up aggregates are
//! registered by `Registry::aggregate`, with their factories. The lamp is the
//! smallest aggregate: no snapshot form, and no rejection it can return.
//!
//! What is pinned: routing by type URL; the router applying what a handler
//! returns, in order; continuations (a step sees the state after the
//! events before it, a step's rejection refuses the whole decision, the
//! step limit, the idempotence helper); every field of the context; replay
//! building a fresh instance from the registry's factory before the handler
//! runs, and a resume keeping the factory's configuration; a rejection
//! committing nothing, a message only or a typed refusal object; a fact
//! recording its outcome where a command would be refused; snapshots with
//! and without a snapshot form; the generated `WalletHandler` trait and
//! constants; the hand-written domain working beside the generated ones;
//! the process manager as a domain (its root the correlation, its start
//! rule, its one context); the sagas' refusal pass-through and translate,
//! and the source's `#[on_refused]`; and the projector. What does not
//! compile is pinned by annals-macros' `compile_fail` examples.

use std::convert::Infallible;
use std::fmt;

use ::annals::next::markup::v1::RefusalResponse;
use ::annals::next::markup::{
    self, assert_step_idempotent, CommandRefused, Commands, Context, DomainMessage, Emit,
    ProcessManager, ProjectionContext, ReactionContext, Record, Refusal, Refuses, Refusing,
    Rejection, Saga, TypeName, MARKUP_RULES_VERSION, STEP_LIMIT,
};
use ::annals::next::v1::event_meta::Causation;
use ::annals::next::v1::{
    command_page, Command, CommandBook, CommandPage, ConflictResolution, Cover, EventBook,
    EventPage, EventRef, Fact, FactOrigin, StateDescriptor, StateOptions, Uuid,
};
use ::annals::next::{
    pack, type_name, Any, Decoder, Domain, Envelope, Executed, Failure, Handled, IngestCtx,
    KernelError, MemoryDeadLetters, Payload, PayloadError, Recovery, Registry, Rulebook, Sampled,
    State, Stream, Timestamp, Tx, Upcaster, WireError,
};
use annals_diagnostic::{Arg, Diagnostic, Report};
use annals_macros::{
    aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, projector,
    projects, reacts, saga,
};

// ---------------------------------------------------------------- the wire

/// The prost messages the toys' types travel as. (A fixture standing in
/// for angzarr's generated wire types.)
mod wire {
    use ::annals::next::Timestamp;

    macro_rules! message {
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
    pub(super) use message;

    message!(
        "toy.wallet.v1",
        WalletSnapshot {
            #[prost(bool, tag = "1")]
            open: bool,
            #[prost(bool, tag = "2")]
            closed: bool,
            #[prost(int64, tag = "3")]
            cents: i64,
            #[prost(message, optional, tag = "4")]
            last_moved_at: Option<Timestamp>,
            #[prost(bool, tag = "5")]
            rewarded: bool,
        }
    );

    message!(
        "toy.wallet.cmd.v1",
        Open {
            #[prost(int64, tag = "1")]
            bonus: i64,
        }
    );
    message!(
        "toy.wallet.cmd.v1",
        Deposit {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!(
        "toy.wallet.cmd.v1",
        Withdraw {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!(
        "toy.wallet.cmd.v1",
        Sweep {
            #[prost(int64, tag = "1")]
            chunk: i64,
        }
    );
    message!("toy.wallet.cmd.v1", Close {});
    message!("toy.wallet.cmd.v1", Note {});
    message!("toy.wallet.cmd.v1", Spin {});

    message!("toy.wallet.v1", Opened {});
    message!(
        "toy.wallet.v1",
        Deposited {
            #[prost(int64, tag = "1")]
            cents: i64,
            #[prost(message, optional, tag = "2")]
            at: Option<Timestamp>,
        }
    );
    message!(
        "toy.wallet.v1",
        Rewarded {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!(
        "toy.wallet.v1",
        Withdrew {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!("toy.wallet.v1", Closed {});
    message!(
        "toy.wallet.v1",
        Noted {
            #[prost(bytes = "vec", tag = "1")]
            root: Vec<u8>,
            #[prost(bytes = "vec", tag = "2")]
            command_id: Vec<u8>,
            #[prost(bytes = "vec", tag = "3")]
            correlation_id: Vec<u8>,
        }
    );
    message!("toy.wallet.v1", Spun {});

    message!(
        "toy.wallet.fact.v1",
        Payout {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!(
        "toy.wallet.fact.v1",
        Gift {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    // An old spelling of `Deposited`, before `at` was recorded: what an
    // upcaster reads.
    message!(
        "toy.wallet.v0",
        OldDeposited {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!(
        "toy.wallet.v1",
        PaidOut {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!(
        "toy.wallet.v1",
        PayoutRefused {
            #[prost(int64, tag = "1")]
            cents: i64,
            #[prost(int64, tag = "2")]
            short: i64,
        }
    );

    message!("toy.lamp.cmd.v1", SwitchOn {});
    message!("toy.lamp.v1", SwitchedOn {});

    message!(
        "toy.wallet.cmd.v1",
        Hold {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!(
        "toy.wallet.v1",
        Held {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    // The wallet's typed refusal objects for `Hold`.
    message!(
        "toy.wallet.v1",
        InsufficientFunds {
            #[prost(int64, tag = "1")]
            short: i64,
        }
    );
    message!("toy.wallet.v1", WalletClosed {});

    message!(
        "toy.buyin.cmd.v1",
        StartBuyIn {
            #[prost(bytes = "vec", tag = "1")]
            wallet: Vec<u8>,
            #[prost(int64, tag = "2")]
            cents: i64,
        }
    );
    message!(
        "toy.buyin.cmd.v1",
        RecordHeld {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!("toy.buyin.cmd.v1", BuyInExpired {});
    message!(
        "toy.buyin.cmd.v1",
        HoldLost {
            #[prost(int64, tag = "1")]
            short: i64,
        }
    );
    message!(
        "toy.buyin.v1",
        BuyInStarted {
            #[prost(bytes = "vec", tag = "1")]
            wallet: Vec<u8>,
            #[prost(int64, tag = "2")]
            cents: i64,
        }
    );
    message!(
        "toy.buyin.v1",
        BuyInHeld {
            #[prost(int64, tag = "1")]
            cents: i64,
        }
    );
    message!(
        "toy.buyin.v1",
        Expired {
            #[prost(string, tag = "1")]
            external_id: String,
        }
    );
    message!(
        "toy.buyin.v1",
        BuyInFailed {
            #[prost(int64, tag = "1")]
            short: i64,
        }
    );
    message!(
        "toy.buyin.v1",
        HoldWasLost {
            #[prost(int64, tag = "1")]
            short: i64,
        }
    );
}

// ---------------------------------------------------------------- the domain types

/// A domain type whose fields are its wire message's, one for one. (A
/// fixture standing in for angzarr-codegen's domain layer.)
macro_rules! domain_type {
    ($name:ident { $($field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Clone, PartialEq, Debug)]
        pub struct $name {
            $(pub $field: $ty,)*
        }

        impl DomainMessage for $name {
            type Wire = wire::$name;

            fn to_wire(&self) -> wire::$name {
                wire::$name { $($field: self.$field.clone(),)* }
            }

            #[allow(unused_variables)]
            fn from_wire(wire: wire::$name) -> Result<Self, WireError> {
                Ok($name { $($field: wire.$field,)* })
            }
        }
    };
}

domain_type!(Open { bonus: i64 });
domain_type!(Deposit { cents: i64 });
domain_type!(Withdraw { cents: i64 });
domain_type!(Sweep { chunk: i64 });
domain_type!(Close {});
domain_type!(Note {});
domain_type!(Spin {});
domain_type!(Opened {});
domain_type!(Rewarded { cents: i64 });
domain_type!(Withdrew { cents: i64 });
domain_type!(Closed {});
domain_type!(Noted {
    root: Vec<u8>,
    command_id: Vec<u8>,
    correlation_id: Vec<u8>,
});
domain_type!(Spun {});
domain_type!(Payout { cents: i64 });
domain_type!(Gift { cents: i64 });
domain_type!(PaidOut { cents: i64 });
domain_type!(PayoutRefused {
    cents: i64,
    short: i64
});
domain_type!(SwitchOn {});
domain_type!(SwitchedOn {});
domain_type!(Hold { cents: i64 });
domain_type!(Held { cents: i64 });
domain_type!(InsufficientFunds { short: i64 });
domain_type!(WalletClosed {});
domain_type!(StartBuyIn {
    wallet: Vec<u8>,
    cents: i64
});
domain_type!(RecordHeld { cents: i64 });
domain_type!(BuyInExpired {});
domain_type!(HoldLost { short: i64 });
domain_type!(BuyInStarted {
    wallet: Vec<u8>,
    cents: i64
});
domain_type!(BuyInHeld { cents: i64 });
domain_type!(Expired {
    external_id: String
});
domain_type!(BuyInFailed { short: i64 });
domain_type!(HoldWasLost { short: i64 });

/// A typed refusal object is a domain type and a diagnostic: its message
/// is the refusal's.
impl fmt::Display for InsufficientFunds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} cents short of the hold", self.short)
    }
}

impl Diagnostic for InsufficientFunds {
    fn code(&self) -> &'static str {
        "toy-wallet-insufficient-funds"
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![("short", self.short.into())]
    }
}

impl fmt::Display for WalletClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the wallet is closed")
    }
}

impl Diagnostic for WalletClosed {
    fn code(&self) -> &'static str {
        "toy-wallet-closed"
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![]
    }
}

/// A domain type that is not its wire message: `at` is required, where the
/// wire's is optional.
#[derive(Clone, PartialEq, Debug)]
pub struct Deposited {
    pub cents: i64,
    pub at: Timestamp,
}

impl DomainMessage for Deposited {
    type Wire = wire::Deposited;

    fn to_wire(&self) -> wire::Deposited {
        wire::Deposited {
            cents: self.cents,
            at: Some(self.at),
        }
    }

    fn from_wire(wire: wire::Deposited) -> Result<Self, WireError> {
        Ok(Deposited {
            cents: wire.cents,
            at: wire.at.ok_or(WireError::Missing {
                message: "toy.wallet.v1.Deposited",
                field: "at",
            })?,
        })
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum Refused {
    NotPositive { cents: i64 },
    AlreadyOpen,
    NotOpen,
    Overdrawn { short: i64 },
    NotEmpty { cents: i64 },
    OverLimit { cents: i64, limit: i64 },
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Refused::NotPositive { cents } => write!(f, "{cents} cents is not an amount"),
            Refused::AlreadyOpen => f.write_str("the wallet is already open"),
            Refused::NotOpen => f.write_str("the wallet is not open"),
            Refused::Overdrawn { short } => write!(f, "{short} cents short"),
            Refused::NotEmpty { cents } => write!(f, "{cents} cents are still in the wallet"),
            Refused::OverLimit { cents, limit } => {
                write!(f, "{cents} cents is over the limit of {limit}")
            }
        }
    }
}

impl Diagnostic for Refused {
    fn code(&self) -> &'static str {
        match self {
            Refused::NotPositive { .. } => "toy-wallet-not-positive",
            Refused::AlreadyOpen => "toy-wallet-already-open",
            Refused::NotOpen => "toy-wallet-not-open",
            Refused::Overdrawn { .. } => "toy-wallet-overdrawn",
            Refused::NotEmpty { .. } => "toy-wallet-not-empty",
            Refused::OverLimit { .. } => "toy-wallet-over-limit",
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match *self {
            Refused::NotPositive { cents } | Refused::NotEmpty { cents } => {
                vec![("cents", cents.into())]
            }
            Refused::Overdrawn { short } => vec![("short", short.into())],
            Refused::OverLimit { cents, limit } => {
                vec![("cents", cents.into()), ("limit", limit.into())]
            }
            Refused::AlreadyOpen | Refused::NotOpen => vec![],
        }
    }
}

// ---------------------------------------------------------------- the wallet

/// What the wallet holds, and the rules of moving it: one of the objects
/// behind the aggregate. It knows nothing of commands, events or the
/// markup.
#[derive(Clone, Default, PartialEq, Debug)]
pub struct Ledger {
    cents: i64,
    last_moved_at: Option<Timestamp>,
}

impl Ledger {
    fn balance(&self) -> i64 {
        self.cents
    }

    fn covers(&self, cents: i64) -> Result<(), Refused> {
        match cents > self.cents {
            true => Err(Refused::Overdrawn {
                short: cents - self.cents,
            }),
            false => Ok(()),
        }
    }

    /// `balance` in pieces of at most `chunk`, largest first.
    fn pieces(&self, chunk: i64) -> Vec<i64> {
        let mut left = self.cents;
        let mut pieces = Vec::new();
        while left > 0 {
            pieces.push(left.min(chunk));
            left -= chunk;
        }
        pieces
    }

    fn credit(&mut self, cents: i64, at: Timestamp) {
        self.cents += cents;
        self.last_moved_at = Some(at);
    }

    fn bonus(&mut self, cents: i64) {
        self.cents += cents;
    }

    fn debit(&mut self, cents: i64) {
        self.cents -= cents;
    }
}

/// Immutable reference configuration a wallet is built with: the most it
/// may hold. Not state: no event changes it, and no snapshot holds it.
#[derive(Clone, PartialEq, Debug)]
pub struct WalletConfig {
    limit: i64,
}

impl Default for WalletConfig {
    fn default() -> Self {
        WalletConfig { limit: 1000 }
    }
}

/// The balance at which a wallet is rewarded, once.
const REWARD_AT: i64 = 100;

/// The aggregate: its state is its fields, and the [`Ledger`] behind it
/// does the arithmetic. The aggregate orchestrates.
#[derive(Clone, Default, PartialEq, Debug)]
pub struct Wallet {
    open: bool,
    closed: bool,
    rewarded: bool,
    ledger: Ledger,
    config: WalletConfig,
}

impl Wallet {
    /// A fresh wallet with `config`: what the registry's factory builds.
    pub fn with(config: WalletConfig) -> Wallet {
        Wallet {
            config,
            ..Wallet::default()
        }
    }
}

/// The snapshot form, both ways (this and [`Wallet::restore`]): the macro
/// builds the `State` impl from them.
impl From<&Wallet> for wire::WalletSnapshot {
    fn from(wallet: &Wallet) -> Self {
        wire::WalletSnapshot {
            open: wallet.open,
            closed: wallet.closed,
            cents: wallet.ledger.cents,
            last_moved_at: wallet.ledger.last_moved_at,
            rewarded: wallet.rewarded,
        }
    }
}

impl Wallet {
    /// Take the state a snapshot holds, keeping this instance's
    /// configuration (the factory's), which no snapshot holds.
    fn restore(&mut self, snapshot: wire::WalletSnapshot) -> Result<(), WireError> {
        self.open = snapshot.open;
        self.closed = snapshot.closed;
        self.rewarded = snapshot.rewarded;
        self.ledger = Ledger {
            cents: snapshot.cents,
            last_moved_at: snapshot.last_moved_at,
        };
        Ok(())
    }
}

#[aggregate(domain = "wallet", snapshot = wire::WalletSnapshot)]
impl Wallet {
    /// Two events, a tuple: the router applies them in order.
    #[handles_cmd(Open)]
    fn open(&self, cmd: Open, ctx: &Context) -> Result<(Opened, Deposited), Refused> {
        if self.open {
            return Err(Refused::AlreadyOpen);
        }
        Ok((
            Opened {},
            Deposited {
                cents: cmd.bonus,
                at: ctx.recorded_at(),
            },
        ))
    }

    /// One event, stamped with the context's instant (the handler has no
    /// clock), then a continuation: whether the deposit is allowed, or
    /// earns the reward, depends on the balance after it.
    #[handles_cmd(Deposit)]
    fn deposit(&self, cmd: Deposit, ctx: &Context) -> Result<Emit<Self>, Refused> {
        if !self.open {
            return Err(Refused::NotOpen);
        }
        positive(cmd.cents)?;
        Ok(Emit::of(Deposited {
            cents: cmd.cents,
            at: ctx.recorded_at(),
        })
        .then(Self::settle))
    }

    /// A continuation step: run on the wallet with the deposit applied.
    /// Over the configured limit refuses the whole decision; at
    /// [`REWARD_AT`] or more, a wallet not yet rewarded earns a tenth.
    /// Idempotent: once rewarded, it emits nothing.
    fn settle(&self, _ctx: &Context) -> Result<Option<Rewarded>, Refused> {
        let cents = self.ledger.balance();
        if cents > self.config.limit {
            return Err(Refused::OverLimit {
                cents,
                limit: self.config.limit,
            });
        }
        Ok((cents >= REWARD_AT && !self.rewarded).then_some(Rewarded { cents: cents / 10 }))
    }

    #[handles_cmd(Withdraw)]
    fn withdraw(&self, cmd: Withdraw, _ctx: &Context) -> Result<Withdrew, Refused> {
        positive(cmd.cents)?;
        self.ledger.covers(cmd.cents)?;
        Ok(Withdrew { cents: cmd.cents })
    }

    /// A command whose refusals are typed objects (`refuses`): it returns
    /// its event, or one of the listed objects, and nothing else compiles.
    #[handles_cmd(Hold, refuses(InsufficientFunds, WalletClosed))]
    fn hold(&self, cmd: Hold, _ctx: &Context) -> Result<Held, Refuses<Self, Hold>> {
        if !self.open {
            return Err(WalletClosed {}.into());
        }
        match self.ledger.covers(cmd.cents) {
            Ok(()) => Ok(Held { cents: cmd.cents }),
            Err(_) => Err(InsufficientFunds {
                short: cmd.cents - self.ledger.balance(),
            }
            .into()),
        }
    }

    /// Any number of one event, a `Vec`, each decided from the state before
    /// any of them.
    #[handles_cmd(Sweep)]
    fn sweep(&self, cmd: Sweep, _ctx: &Context) -> Result<Vec<Withdrew>, Refused> {
        positive(cmd.chunk)?;
        Ok(self
            .ledger
            .pieces(cmd.chunk)
            .into_iter()
            .map(|cents| Withdrew { cents })
            .collect())
    }

    /// One event or none, an `Option`: closing a closed wallet is nothing.
    #[handles_cmd(Close)]
    fn close(&self, _cmd: Close, _ctx: &Context) -> Result<Option<Closed>, Refused> {
        match self.ledger.balance() {
            0 => Ok((!self.closed).then_some(Closed {})),
            cents => Err(Refused::NotEmpty { cents }),
        }
    }

    /// Records what the context says about the command: its stream, its id
    /// and its correlation.
    #[handles_cmd(Note)]
    fn note(&self, _cmd: Note, ctx: &Context) -> Result<Noted, Refused> {
        Ok(Noted {
            root: ctx.stream().root.clone().unwrap_or_default().value.to_vec(),
            command_id: ctx.command_id().value.to_vec(),
            correlation_id: ctx.correlation_id().value.to_vec(),
        })
    }

    /// A deliberate bug: a step that always continues, so the decision
    /// runs into the step limit.
    #[handles_cmd(Spin)]
    fn spin(&self, _cmd: Spin, _ctx: &Context) -> Result<Emit<Self>, Refused> {
        Ok(Emit::of(Spun {}).then(Self::spin_again))
    }

    fn spin_again(&self, _ctx: &Context) -> Result<Emit<Self>, Refused> {
        Ok(Emit::of(Spun {}).then(Self::spin_again))
    }

    /// A fact (sent by a saga): no `Result`. A payout the wallet cannot
    /// cover is not refused; its refusal is recorded.
    #[handles_fact(Payout)]
    fn payout(&self, fact: Payout, _ctx: &Context) -> Record<Self> {
        match self.ledger.covers(fact.cents) {
            Ok(()) => Record::of(PaidOut { cents: fact.cents }),
            Err(_) => Record::of(PayoutRefused {
                cents: fact.cents,
                short: fact.cents - self.ledger.balance(),
            }),
        }
    }

    /// A fact with a continuation: the gift is recorded, then whether it
    /// earns the reward is decided on the balance after it. Nothing in it
    /// can refuse: a gift over the limit is recorded all the same.
    #[handles_fact(Gift)]
    fn gift(&self, fact: Gift, ctx: &Context) -> Record<Self> {
        Record::of(Deposited {
            cents: fact.cents,
            at: ctx.recorded_at(),
        })
        .then(Self::reward_due)
    }

    /// A fact's continuation step: no `Result`.
    fn reward_due(&self, _ctx: &Context) -> Option<Rewarded> {
        let cents = self.ledger.balance();
        (cents >= REWARD_AT && !self.rewarded).then_some(Rewarded { cents: cents / 10 })
    }

    #[applies(Opened)]
    fn opened(&mut self, _event: Opened) {
        self.open = true;
    }

    #[applies(Deposited)]
    fn deposited(&mut self, event: Deposited) {
        self.ledger.credit(event.cents, event.at);
    }

    #[applies(Rewarded)]
    fn rewarded(&mut self, event: Rewarded) {
        self.rewarded = true;
        self.ledger.bonus(event.cents);
    }

    #[applies(Withdrew)]
    fn withdrew(&mut self, event: Withdrew) {
        self.ledger.debit(event.cents);
    }

    #[applies(Held)]
    fn held(&mut self, event: Held) {
        self.ledger.debit(event.cents);
    }

    #[applies(Closed)]
    fn closed(&mut self, _event: Closed) {
        self.closed = true;
    }

    #[applies(Noted)]
    fn noted(&mut self, _event: Noted) {}

    #[applies(Spun)]
    fn spun(&mut self, _event: Spun) {}

    #[applies(PaidOut)]
    fn paid_out(&mut self, event: PaidOut) {
        self.ledger.debit(event.cents);
    }

    #[applies(PayoutRefused)]
    fn payout_refused(&mut self, _event: PayoutRefused) {}
}

fn positive(cents: i64) -> Result<(), Refused> {
    match cents > 0 {
        true => Ok(()),
        false => Err(Refused::NotPositive { cents }),
    }
}

// ---------------------------------------------------------------- the lamp

/// The smallest aggregate: no snapshot form, and a rejection (inferred,
/// `Infallible`) it never returns.
#[derive(Clone, Default, PartialEq, Debug)]
pub struct Lamp {
    on: bool,
}

#[aggregate(domain = "lamp")]
impl Lamp {
    #[handles_cmd(SwitchOn)]
    fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
        Ok((!self.on).then_some(SwitchedOn {}))
    }

    #[applies(SwitchedOn)]
    fn switched_on(&mut self, _event: SwitchedOn) {
        self.on = true;
    }
}

// ---------------------------------------------------------------- the tally, by hand

wire::message!(
    "toy.tally.v1",
    TallyState {
        #[prost(uint64, tag = "1")]
        count: u64,
    }
);
wire::message!("toy.tally.cmd.v1", Count {});
wire::message!("toy.tally.v1", Counted {});

impl State for TallyState {
    type Snapshot = Self;

    fn to_snapshot(&self) -> Self {
        self.clone()
    }

    fn from_snapshot(snapshot: Self) -> Result<Self, Report> {
        Ok(snapshot)
    }

    fn descriptor() -> StateDescriptor {
        StateDescriptor {
            type_url: "/toy.tally.v1.TallyState".to_string(),
            options: Some(StateOptions {
                conflict_resolutions: vec![
                    ConflictResolution::Strict as i32,
                    ConflictResolution::AggregateHandles as i32,
                ],
            }),
        }
    }
}

impl Payload for Count {
    fn to_any(&self) -> Any {
        pack(self)
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        Decoder::new().on::<Count>(|c| c).decode(any)
    }

    fn type_urls() -> Vec<String> {
        Decoder::new().on::<Count>(|c| c).type_urls()
    }
}

impl Payload for Counted {
    fn to_any(&self) -> Any {
        pack(self)
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        Decoder::new().on::<Counted>(|c| c).decode(any)
    }

    fn type_urls() -> Vec<String> {
        Decoder::new().on::<Counted>(|c| c).type_urls()
    }
}

/// A domain whose `Domain` impl is written by hand, as any domain's may
/// be.
pub struct Tally;

impl Domain for Tally {
    type State = TallyState;
    type Command = Count;
    type Fact = Infallible;
    type Event = Counted;
    type Rejection = Infallible;
    type Rules = ();

    const NAME: &'static str = "tally";

    fn decide(_: &Count, tx: &mut Tx<'_, Self>) -> Result<(), Infallible> {
        tx.emit(Counted {});
        Ok(())
    }

    fn ingest(_: &TallyState, fact: &Infallible, _: &IngestCtx<'_, Self>) -> Vec<Counted> {
        match *fact {}
    }

    fn apply(state: &mut TallyState, _: &Counted, (): &()) {
        state.count += 1;
    }
}

// ---------------------------------------------------------------- running them

/// The marked-up aggregates, each registered with its factory, and the
/// hand-written tally, registered as before.
fn registry() -> Registry {
    configured(WalletConfig::default())
}

/// As [`registry`], with every wallet built with `config`.
fn configured(config: WalletConfig) -> Registry {
    let mut registry = Registry::new();
    registry
        .aggregate::<Wallet>(move || Wallet::with(config.clone()))
        .aggregate::<Lamp>(Lamp::default);
    registry
        .register::<Tally>(
            Rulebook::new("v1", ()),
            Upcaster::new("tally-upcaster", "tally"),
        )
        .unwrap();
    registry
}

fn root() -> Uuid {
    Uuid::from_bytes([0x0a; 16])
}

/// A domain value as the `Any` it travels as.
fn any<M: DomainMessage>(message: M) -> Any {
    pack(&message.to_wire())
}

fn at(seconds: i64) -> Timestamp {
    Timestamp { seconds, nanos: 0 }
}

/// Command `n` to `domain`'s stream, as a client sends it, with
/// `correlation` if given.
fn send(domain: &str, n: u8, payload: Any, correlation: Option<Uuid>) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(root()),
            edition: None,
        }),
        page: Some(CommandPage {
            body: Some(command_page::Body::Command(Command {
                payload: Some(payload),
            })),
            command_id: Some(Uuid::from_bytes([n; 16])),
            correlation_id: correlation,
            ..CommandPage::default()
        }),
        handling: None,
    }
}

/// Execute `book` (command `n`, sampled at `n` seconds) on `stream`: the
/// events it committed, as the `Any`s they are stored as, or why it was
/// refused.
fn execute<D: Domain>(
    stream: &mut Stream<D>,
    registry: &Registry,
    n: u8,
    book: CommandBook,
) -> Result<Vec<Any>, Failure<D::Rejection>> {
    let sampled = Sampled {
        now: at(i64::from(n)),
        command_id: Uuid::from_bytes([0x07; 16]),
    };
    let log = MemoryDeadLetters::new();
    match stream.execute(book, registry, &sampled, &log, &mut [])? {
        Executed::Applied(events) => Ok(events.iter().map(|e| e.event().to_any()).collect()),
        Executed::Duplicate(_) => panic!("command {n} is new"),
    }
}

/// A fact to `domain`'s stream from `external_id`, with no ids: the kernel
/// derives them.
fn fact(domain: &str, payload: Any, external_id: &str) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(root()),
            edition: None,
        }),
        page: Some(CommandPage {
            body: Some(command_page::Body::Fact(Fact {
                payload: Some(payload),
            })),
            fact_origin: Some(FactOrigin {
                external_id: external_id.to_string(),
                description: "toy payout".to_string(),
            }),
            ..CommandPage::default()
        }),
        handling: None,
    }
}

/// Run command `n` on `stream`, as [`execute`], with no correlation given.
fn run<D: Domain>(
    stream: &mut Stream<D>,
    registry: &Registry,
    n: u8,
    payload: Any,
) -> Result<Vec<Any>, Failure<D::Rejection>> {
    execute(stream, registry, n, send(D::NAME, n, payload, None))
}

fn wallet() -> Stream<Wallet> {
    Stream::new(root(), None).unwrap()
}

/// A wallet opened with `bonus` by command 1.
fn opened(registry: &Registry, bonus: i64) -> Stream<Wallet> {
    let mut wallet = wallet();
    run(&mut wallet, registry, 1, any(Open { bonus })).unwrap();
    wallet
}

/// The domain's own rejection, a message only, that `failure` is.
fn rejection<R: fmt::Debug>(failure: Failure<Rejection<R>>) -> R {
    match failure {
        Failure::Rejected(Rejection::Message(r)) => r,
        other => panic!("refused by the domain with a message, not {other:?}"),
    }
}

/// The wallet as its fields would be: open, not closed, not rewarded,
/// holding `cents`, last moved at `moved` seconds, configured by default.
fn holding(cents: i64, moved: i64) -> Wallet {
    Wallet {
        open: true,
        closed: false,
        rewarded: false,
        ledger: Ledger {
            cents,
            last_moved_at: Some(at(moved)),
        },
        config: WalletConfig::default(),
    }
}

fn context(n: u8) -> Context {
    Context::new(
        at(i64::from(n)),
        wallet().cover().clone(),
        Uuid::from_bytes([n; 16]),
        Uuid::from_bytes([n; 16]),
    )
}

mod route {
    use super::*;

    /// Each command, sent as the `Any` a client sends, reaches the handler
    /// its type URL names, as a domain value.
    #[test]
    fn each_type_url_goes_to_its_handler() {
        let registry = registry();
        let mut wallet = wallet();

        let open = run(&mut wallet, &registry, 1, any(Open { bonus: 5 }));
        let deposit = run(&mut wallet, &registry, 2, any(Deposit { cents: 50 }));
        let withdraw = run(&mut wallet, &registry, 3, any(Withdraw { cents: 20 }));

        assert_eq!(
            open,
            Ok(vec![
                any(Opened {}),
                any(Deposited {
                    cents: 5,
                    at: at(1)
                })
            ])
        );
        assert_eq!(
            deposit,
            Ok(vec![any(Deposited {
                cents: 50,
                at: at(2)
            })])
        );
        assert_eq!(withdraw, Ok(vec![any(Withdrew { cents: 20 })]));
    }

    /// A type URL no handler is marked for (a command the proto declares
    /// and the markup does not handle, or another domain's) is the kernel's
    /// unknown type, not a rejection, and reaches no handler.
    #[test]
    fn an_unmarked_type_url_is_an_unknown_type() {
        let registry = registry();
        let mut wallet = opened(&registry, 0);

        let refused = run(&mut wallet, &registry, 2, pack(&Count {}));

        match refused {
            Err(Failure::Kernel(KernelError::UnknownType { type_url, .. })) => {
                assert_eq!(type_url, "/toy.tally.cmd.v1.Count");
            }
            other => panic!("an unknown type, not {other:?}"),
        }
        assert_eq!(wallet.len(), 2, "nothing was committed");
    }

    /// The generated command and event sets read exactly the marked types'
    /// type URLs: what an upcast chain must end at.
    #[test]
    fn the_payload_sets_are_the_marked_types() {
        let mut commands = <<Wallet as Domain>::Command as Payload>::type_urls();
        let mut facts = <<Wallet as Domain>::Fact as Payload>::type_urls();
        let mut events = <<Wallet as Domain>::Event as Payload>::type_urls();
        commands.sort();
        facts.sort();
        events.sort();

        assert_eq!(
            commands,
            [
                "/toy.wallet.cmd.v1.Close",
                "/toy.wallet.cmd.v1.Deposit",
                "/toy.wallet.cmd.v1.Hold",
                "/toy.wallet.cmd.v1.Note",
                "/toy.wallet.cmd.v1.Open",
                "/toy.wallet.cmd.v1.Spin",
                "/toy.wallet.cmd.v1.Sweep",
                "/toy.wallet.cmd.v1.Withdraw",
            ]
        );
        assert_eq!(
            facts,
            ["/toy.wallet.fact.v1.Gift", "/toy.wallet.fact.v1.Payout"]
        );
        assert_eq!(
            events,
            [
                "/toy.wallet.v1.Closed",
                "/toy.wallet.v1.Deposited",
                "/toy.wallet.v1.Held",
                "/toy.wallet.v1.Noted",
                "/toy.wallet.v1.Opened",
                "/toy.wallet.v1.PaidOut",
                "/toy.wallet.v1.PayoutRefused",
                "/toy.wallet.v1.Rewarded",
                "/toy.wallet.v1.Spun",
                "/toy.wallet.v1.Withdrew",
            ]
        );
    }
}

mod emit {
    use super::*;

    /// A handler returns its event; the router applies it through the
    /// applier, so the next command sees it.
    #[test]
    fn a_returned_event_is_applied_by_the_router() {
        let registry = registry();
        let mut wallet = opened(&registry, 5);

        run(&mut wallet, &registry, 2, any(Deposit { cents: 50 })).unwrap();

        assert_eq!(wallet.state(), &holding(55, 2));
    }

    /// A tuple is recorded and applied in its order.
    #[test]
    fn a_tuple_is_applied_and_recorded_in_order() {
        let registry = registry();
        let mut wallet = wallet();

        let events = run(&mut wallet, &registry, 1, any(Open { bonus: 5 }));

        assert_eq!(
            events,
            Ok(vec![
                any(Opened {}),
                any(Deposited {
                    cents: 5,
                    at: at(1)
                })
            ])
        );
        assert_eq!(wallet.state(), &holding(5, 1));
    }

    /// A `Vec` is recorded and applied in its order, each event decided
    /// from the state before the command.
    #[test]
    fn a_vec_is_applied_and_recorded_in_order() {
        let registry = registry();
        let mut wallet = opened(&registry, 25);

        let events = run(&mut wallet, &registry, 2, any(Sweep { chunk: 10 }));

        assert_eq!(
            events,
            Ok(vec![
                any(Withdrew { cents: 10 }),
                any(Withdrew { cents: 10 }),
                any(Withdrew { cents: 5 }),
            ])
        );
        assert_eq!(wallet.state(), &holding(0, 1));
    }

    /// `Some` is one event; `None` is none, and commits nothing.
    #[test]
    fn an_option_is_one_event_or_none() {
        let registry = registry();
        let mut wallet = opened(&registry, 0);

        let first = run(&mut wallet, &registry, 2, any(Close {}));
        let again = run(&mut wallet, &registry, 3, any(Close {}));

        assert_eq!(first, Ok(vec![any(Closed {})]));
        assert_eq!(again, Ok(vec![]));
        assert_eq!(wallet.len(), 3, "the second close committed nothing");
        assert!(wallet.state().closed);
    }
}

mod context {
    use super::*;

    /// The handler's instant is the context's: the recorded instant the
    /// application sampled, not a clock.
    #[test]
    fn the_recorded_instant_reaches_the_handler() {
        let registry = registry();
        let mut wallet = opened(&registry, 0);

        let events = run(&mut wallet, &registry, 7, any(Deposit { cents: 1 }));

        assert_eq!(
            events,
            Ok(vec![any(Deposited {
                cents: 1,
                at: at(7)
            })])
        );
        assert_eq!(wallet.state().ledger.last_moved_at, Some(at(7)));
    }

    /// The stream, the command's id and its correlation (as sent) reach
    /// the handler.
    #[test]
    fn the_stream_command_id_and_correlation_reach_the_handler() {
        let registry = registry();
        let mut wallet = opened(&registry, 0);
        let correlation = Uuid::from_bytes([0x0c; 16]);

        let noted = execute(
            &mut wallet,
            &registry,
            2,
            send("wallet", 2, any(Note {}), Some(correlation.clone())),
        );

        assert_eq!(
            noted,
            Ok(vec![any(Noted {
                root: root().value.to_vec(),
                command_id: Uuid::from_bytes([2; 16]).value.to_vec(),
                correlation_id: correlation.value.to_vec(),
            })])
        );
    }

    /// With no correlation sent, the context's is the command's id.
    #[test]
    fn the_correlation_defaults_to_the_command_id() {
        let registry = registry();
        let mut wallet = opened(&registry, 0);

        let noted = run(&mut wallet, &registry, 2, any(Note {}));

        assert_eq!(
            noted,
            Ok(vec![any(Noted {
                root: root().value.to_vec(),
                command_id: Uuid::from_bytes([2; 16]).value.to_vec(),
                correlation_id: Uuid::from_bytes([2; 16]).value.to_vec(),
            })])
        );
    }
}

mod continuation {
    use super::*;

    /// The step runs on the wallet with the deposit applied: 120 deposited
    /// is at the reward's threshold only once it is in the balance, and
    /// both events are one command's.
    #[test]
    fn a_step_sees_the_state_after_the_events_before_it() {
        let registry = registry();
        let mut wallet = opened(&registry, 0);

        let events = run(&mut wallet, &registry, 2, any(Deposit { cents: 120 }));
        let again = run(&mut wallet, &registry, 3, any(Deposit { cents: 10 }));

        assert_eq!(
            events,
            Ok(vec![
                any(Deposited {
                    cents: 120,
                    at: at(2)
                }),
                any(Rewarded { cents: 12 }),
            ])
        );
        assert_eq!(
            again,
            Ok(vec![any(Deposited {
                cents: 10,
                at: at(3)
            })]),
            "rewarded once"
        );
        assert_eq!(wallet.len(), 5);
        assert_eq!(wallet.state().ledger.balance(), 142);
        assert!(wallet.state().rewarded);
    }

    /// A step's rejection refuses the whole decision: the deposit the
    /// handler returned is not committed either.
    #[test]
    fn a_rejection_in_a_step_refuses_the_whole_decision() {
        let registry = registry();
        let mut wallet = opened(&registry, 5);
        let before = wallet.state().clone();

        let refused = run(&mut wallet, &registry, 2, any(Deposit { cents: 1500 }));

        assert_eq!(
            refused.map_err(rejection),
            Err(Refused::OverLimit {
                cents: 1505,
                limit: 1000
            })
        );
        assert_eq!(wallet.len(), 2, "nothing was committed");
        assert_eq!(wallet.state(), &before, "nothing was applied");
    }

    /// A decision whose steps never stop is the kernel's `TooManySteps`,
    /// at the limit, with nothing committed.
    #[test]
    fn steps_past_the_limit_are_too_many() {
        let registry = registry();
        let mut wallet = opened(&registry, 0);

        let refused = run(&mut wallet, &registry, 2, any(Spin {}));

        match refused {
            Err(Failure::Kernel(KernelError::TooManySteps { stream, limit })) => {
                assert_eq!(limit, STEP_LIMIT);
                assert_eq!(&stream, wallet.cover());
            }
            other => panic!("too many steps, not {other:?}"),
        }
        assert_eq!(wallet.len(), 2, "nothing was committed");
    }

    /// The helper runs the step, applies what it emits, and finds that
    /// re-running it there emits nothing; it returns the instance after
    /// the first run.
    #[test]
    fn the_helper_passes_an_idempotent_step() {
        let mut unrewarded = holding(150, 1);
        unrewarded.rewarded = false;

        let after = assert_step_idempotent(unrewarded, Wallet::settle, &context(2));

        assert!(after.rewarded);
        assert_eq!(after.ledger.balance(), 165);
    }

    /// A step that tips on every run is not idempotent.
    #[test]
    #[should_panic(expected = "not idempotent")]
    fn the_helper_names_a_step_that_emits_again() {
        fn always_tips(_wallet: &Wallet, _ctx: &Context) -> Result<Rewarded, Refused> {
            Ok(Rewarded { cents: 1 })
        }

        assert_step_idempotent(holding(150, 1), always_tips, &context(2));
    }
}

mod construction {
    use super::*;

    /// Replay starts from the registered factory, not `Default`: the
    /// rebuilt wallet carries the factory's configuration, and its handlers
    /// and steps decide under it.
    #[test]
    fn replay_starts_from_the_registered_factory() {
        let mut original = opened(&registry(), 0);
        run(&mut original, &registry(), 2, any(Deposit { cents: 50 })).unwrap();
        let pages: Vec<_> = original.events().iter().map(Envelope::page).collect();
        let registry = configured(WalletConfig { limit: 60 });

        let mut rebuilt =
            Stream::<Wallet>::replay(original.cover().clone(), &pages, &registry).unwrap();
        let refused = run(&mut rebuilt, &registry, 3, any(Deposit { cents: 20 }));

        assert_eq!(rebuilt.state().config, WalletConfig { limit: 60 });
        assert_eq!(
            refused.map_err(rejection),
            Err(Refused::OverLimit {
                cents: 70,
                limit: 60
            })
        );
    }

    /// Resuming from a snapshot builds from the factory, then restores: the
    /// configuration, which no snapshot holds, survives.
    #[test]
    fn a_resumed_aggregate_keeps_its_factorys_configuration() {
        let registry = configured(WalletConfig { limit: 60 });
        let mut wallet =
            Stream::<Wallet>::replay(wallet().cover().clone(), &[], &registry).unwrap();
        run(&mut wallet, &registry, 1, any(Open { bonus: 0 })).unwrap();
        run(&mut wallet, &registry, 2, any(Deposit { cents: 50 })).unwrap();
        let snapshot = wallet.snapshot(&registry).expect("the wallet snapshots");

        let mut resumed = Stream::<Wallet>::resume(
            wallet.cover().clone(),
            &snapshot,
            wallet.last_recorded_at(),
            &[],
            &registry,
        )
        .unwrap();
        let refused = run(&mut resumed, &registry, 3, any(Deposit { cents: 20 }));

        assert_eq!(resumed.state(), wallet.state());
        assert_eq!(resumed.state().config, WalletConfig { limit: 60 });
        assert_eq!(
            refused.map_err(rejection),
            Err(Refused::OverLimit {
                cents: 70,
                limit: 60
            })
        );
    }
}

mod facts {
    use super::*;

    /// A fact's handler records its outcome either way: paid out when the
    /// wallet covers it, a recorded refusal when it does not.
    #[test]
    fn a_fact_handler_records_its_outcome() {
        let registry = registry();
        let mut wallet = opened(&registry, 10);

        let paid = execute(
            &mut wallet,
            &registry,
            2,
            fact("wallet", any(Payout { cents: 4 }), "payout-1"),
        );
        let refused = execute(
            &mut wallet,
            &registry,
            3,
            fact("wallet", any(Payout { cents: 30 }), "payout-2"),
        );

        assert_eq!(paid, Ok(vec![any(PaidOut { cents: 4 })]));
        assert_eq!(
            refused,
            Ok(vec![any(PayoutRefused {
                cents: 30,
                short: 24
            })])
        );
        assert_eq!(wallet.len(), 4, "both outcomes were committed");
        assert_eq!(wallet.state().ledger.balance(), 6);
    }

    /// Where a command is refused, a fact is not: the same amount from the
    /// same wallet is a rejection as a command and a recorded refusal as a
    /// fact.
    #[test]
    fn a_fact_is_recorded_where_a_command_is_refused() {
        let registry = registry();
        let mut wallet = opened(&registry, 10);

        let command = run(&mut wallet, &registry, 2, any(Withdraw { cents: 30 }));
        let recorded = execute(
            &mut wallet,
            &registry,
            3,
            fact("wallet", any(Payout { cents: 30 }), "payout-3"),
        );

        assert_eq!(
            command.map_err(rejection),
            Err(Refused::Overdrawn { short: 20 })
        );
        assert_eq!(
            recorded,
            Ok(vec![any(PayoutRefused {
                cents: 30,
                short: 20
            })])
        );
        assert_eq!(wallet.len(), 3);
    }
}

mod fact_continuations {
    use super::*;

    /// A fact's continuation runs on the state after its events, as a
    /// command's does, and cannot refuse: the gift that brings the wallet
    /// to the reward earns it, and a gift past the limit (which a deposit
    /// command's step refuses) is recorded all the same.
    #[test]
    fn a_facts_continuation_runs_after_its_events_and_cannot_refuse() {
        let registry = configured(WalletConfig { limit: 150 });
        let mut wallet = opened(&registry, 10);

        let rewarded = execute(
            &mut wallet,
            &registry,
            2,
            fact("wallet", any(Gift { cents: 95 }), "gift-1"),
        );
        let past_the_limit = execute(
            &mut wallet,
            &registry,
            3,
            fact("wallet", any(Gift { cents: 500 }), "gift-2"),
        );

        assert_eq!(
            rewarded,
            Ok(vec![
                any(Deposited {
                    cents: 95,
                    at: at(2)
                }),
                any(Rewarded { cents: 10 })
            ])
        );
        assert_eq!(
            past_the_limit,
            Ok(vec![any(Deposited {
                cents: 500,
                at: at(3)
            })]),
            "rewarded once; and recorded, never refused"
        );
    }
}

mod registration {
    use super::*;

    /// A marked-up aggregate's events are recorded under the markup's one
    /// rules version, whatever registered it.
    #[test]
    fn its_events_carry_the_markups_rules_version() {
        let registry = registry();
        let wallet = opened(&registry, 5);
        for envelope in wallet.events() {
            assert_eq!(envelope.rules_version(), MARKUP_RULES_VERSION);
        }
    }

    /// An upcaster given at registration reads the aggregate's old events:
    /// an old `Deposited` without its instant replays as one at second 0.
    #[test]
    fn an_upcaster_given_reads_its_old_events() {
        let mut registry = Registry::new();
        registry.aggregate::<Wallet>(Wallet::default).upcaster(
            Upcaster::new("wallet-upcaster", "wallet").rule(
                |old: wire::OldDeposited| -> Result<wire::Deposited, Report> {
                    Ok(wire::Deposited {
                        cents: old.cents,
                        at: Some(at(0)),
                    })
                },
            ),
        );
        let original = opened(&registry, 7);
        let mut pages: Vec<_> = original.events().iter().map(Envelope::page).collect();
        pages[1].payload = Some(pack(&wire::OldDeposited { cents: 7 }));

        let replayed =
            Stream::<Wallet>::replay(original.cover().clone(), &pages, &registry).unwrap();

        assert_eq!(
            replayed.state().ledger,
            Ledger {
                cents: 7,
                last_moved_at: Some(at(0)),
            }
        );
    }

    /// Registering one aggregate twice is a bug in the composition root:
    /// it panics there, at startup, rather than being an error to handle.
    #[test]
    #[should_panic(expected = "wallet")]
    fn registering_an_aggregate_twice_panics() {
        let mut registry = Registry::new();
        registry
            .aggregate::<Wallet>(Wallet::default)
            .aggregate::<Wallet>(Wallet::default);
    }
}

mod replay {
    use super::*;

    /// A stream rebuilt from its stored pages is a fresh instance with the
    /// events replayed through the appliers, and the next handler decides
    /// against it.
    #[test]
    fn a_fresh_instance_is_rebuilt_from_the_log_then_handles() {
        let registry = registry();
        let mut original = opened(&registry, 0);
        run(&mut original, &registry, 2, any(Deposit { cents: 50 })).unwrap();
        let pages: Vec<_> = original.events().iter().map(Envelope::page).collect();

        let mut rebuilt =
            Stream::<Wallet>::replay(original.cover().clone(), &pages, &registry).unwrap();
        let refused = run(&mut rebuilt, &registry, 3, any(Withdraw { cents: 60 }));
        let withdrew = run(&mut rebuilt, &registry, 4, any(Withdraw { cents: 20 }));

        assert_eq!(
            refused.map_err(rejection),
            Err(Refused::Overdrawn { short: 10 })
        );
        assert_eq!(withdrew, Ok(vec![any(Withdrew { cents: 20 })]));
        assert_eq!(rebuilt.state(), &holding(30, 2));
    }
}

mod snapshot {
    use super::*;

    /// With `snapshot = ..`, the state's snapshot is the user's conversion,
    /// under the message's type URL, and a stream resumed from it holds the
    /// same state.
    #[test]
    fn a_snapshot_form_round_trips_through_the_users_conversions() {
        let registry = registry();
        let mut wallet = opened(&registry, 0);
        run(&mut wallet, &registry, 2, any(Deposit { cents: 120 })).unwrap();

        let snapshot = wallet.snapshot(&registry).expect("the wallet snapshots");
        let resumed = Stream::<Wallet>::resume(
            wallet.cover().clone(),
            &snapshot,
            wallet.last_recorded_at(),
            &[],
            &registry,
        )
        .unwrap();

        let state = snapshot.state.clone().unwrap_or_default();
        assert_eq!(type_name(&state.type_url), "toy.wallet.v1.WalletSnapshot");
        assert_eq!(
            <Wallet as State>::descriptor().type_url,
            "/toy.wallet.v1.WalletSnapshot"
        );
        assert_eq!(
            wallet.state().to_snapshot(),
            wire::WalletSnapshot::from(wallet.state())
        );
        assert_eq!(resumed.state(), wallet.state());
    }

    /// Without `snapshot = ..`, the kernel takes none: every load replays.
    #[test]
    fn no_snapshot_form_means_no_snapshot() {
        let registry = registry();
        let mut lamp = Stream::<Lamp>::new(root(), None).unwrap();

        let on = run(&mut lamp, &registry, 1, any(SwitchOn {}));

        assert_eq!(on, Ok(vec![any(SwitchedOn {})]));
        assert!(lamp.snapshot(&registry).is_none());
    }
}

mod rejection {
    use super::*;

    /// A handler's `Err` is the domain's own rejection, as it returned it,
    /// and nothing is committed or applied. The rejection type is inferred
    /// from the handlers' `Result`s.
    #[test]
    fn a_rejection_is_typed_and_commits_nothing() {
        let registry = registry();
        let mut wallet = opened(&registry, 5);
        let before = wallet.state().clone();

        let again = run(&mut wallet, &registry, 2, any(Open { bonus: 5 }));
        let overdrawn = run(&mut wallet, &registry, 3, any(Withdraw { cents: 8 }));

        assert_eq!(again.map_err(rejection), Err(Refused::AlreadyOpen));
        assert_eq!(
            overdrawn.map_err(rejection),
            Err(Refused::Overdrawn { short: 3 })
        );
        assert_eq!(wallet.len(), 2, "nothing was committed");
        assert_eq!(wallet.state(), &before, "nothing was applied");
    }
}

mod generated {
    use super::*;

    fn deposited_through<H: WalletHandler>(handler: &mut H, event: Deposited) {
        handler.deposited(event);
    }

    fn withdraw_through<H: WalletHandler>(
        handler: &H,
        cmd: Withdraw,
        ctx: &Context,
    ) -> Result<Withdrew, Refused> {
        handler.withdraw(cmd, ctx)
    }

    /// `WalletHandler` names the marked set, and the wallet implements it
    /// by its own methods: through the trait, an applier and a handler do
    /// what the marked methods do.
    #[test]
    fn the_handler_trait_is_the_marked_methods() {
        let ctx = context(3);
        let mut through = Wallet {
            open: true,
            ..Wallet::default()
        };
        let mut direct = through.clone();

        deposited_through(
            &mut through,
            Deposited {
                cents: 10,
                at: at(2),
            },
        );
        direct.deposited(Deposited {
            cents: 10,
            at: at(2),
        });

        assert_eq!(through, direct);
        assert_eq!(
            withdraw_through(&through, Withdraw { cents: 4 }, &ctx),
            Ok(Withdrew { cents: 4 })
        );
        assert_eq!(
            withdraw_through(&through, Withdraw { cents: 11 }, &ctx),
            Err(Refused::Overdrawn { short: 1 })
        );
    }

    /// `HANDLES` (commands and facts) and `APPLIES` are the marked types'
    /// wire names, in marker order.
    #[test]
    fn handles_and_applies_are_the_marked_types_in_order() {
        let command = |name| TypeName {
            package: "toy.wallet.cmd.v1",
            name,
        };
        let event = |name| TypeName {
            package: "toy.wallet.v1",
            name,
        };

        assert_eq!(
            Wallet::HANDLES,
            [
                command("Open"),
                command("Deposit"),
                command("Withdraw"),
                command("Hold"),
                command("Sweep"),
                command("Close"),
                command("Note"),
                command("Spin"),
                TypeName {
                    package: "toy.wallet.fact.v1",
                    name: "Payout",
                },
                TypeName {
                    package: "toy.wallet.fact.v1",
                    name: "Gift",
                },
            ]
        );
        assert_eq!(
            Wallet::APPLIES,
            [
                event("Opened"),
                event("Deposited"),
                event("Rewarded"),
                event("Withdrew"),
                event("Held"),
                event("Closed"),
                event("Noted"),
                event("Spun"),
                event("PaidOut"),
                event("PayoutRefused"),
            ]
        );
    }
}

mod beside {
    use super::*;

    /// One registry holds generated domains and a hand-written one, and
    /// each decides and folds as its own impl says.
    #[test]
    fn a_hand_written_domain_works_beside_a_generated_one() {
        let registry = registry();
        let mut tally = Stream::<Tally>::new(root(), None).unwrap();
        let mut wallet = wallet();

        let counted = run(&mut tally, &registry, 1, pack(&Count {}));
        let opened = run(&mut wallet, &registry, 1, any(Open { bonus: 0 }));
        let counted_again = run(&mut tally, &registry, 2, pack(&Count {}));

        assert_eq!(counted, Ok(vec![pack(&Counted {})]));
        assert_eq!(
            opened,
            Ok(vec![
                any(Opened {}),
                any(Deposited {
                    cents: 0,
                    at: at(1)
                })
            ])
        );
        assert_eq!(counted_again, Ok(vec![pack(&Counted {})]));
        assert_eq!(tally.state().count, 2);
        assert!(wallet.state().open);
    }
}

// --------------------------------------- the process manager and sagas (red)
//
// PROPOSAL (decision 14.31; plan rulings 7, 9 and 13): A PROCESS MANAGER IS
// A DOMAIN. The buy-in takes commands (which sagas send it, and which it
// may refuse) and facts (a timeout; refusal facts), and records only its
// own events; it subscribes to nothing. Its root is the correlation, and
// only its declared starting inputs may start an instance. Two sagas carry
// events across: the buy-in's into holds on the wallet (BuyInWallet), and
// the wallet's holds back into the buy-in (WalletBuyIn). When the wallet
// refuses a hold (with a typed InsufficientFunds, recording nothing), the
// saga delivers one refusal fact to the buy-in: passed through as
// `CommandRefused`, handled by `#[on_refused(Hold)]`; or, where the saga
// has its own `#[on_refused(Hold)]` callback, the callback's domestic
// fact, `HoldLost`. The
// dispatch is the linked angzarr-router's, blocked until the router is on
// io.angzarr.v1: these pin the markup and the contract.
//
// RULED by the owner, 2026-10-28, and pinned below: (1, revised
// 2026-10-28) translate is selected by the PRESENCE of the saga's
// `#[on_refused(Cmd)]` callback, nothing else (no `translate(..)`
// argument), and the callback must return a fact of its source; (2) typed refusals as `Refuses<Self, Cmd>` in
// `markup::Rejection<R>`, the objects `Diagnostic`s; (3) one marker,
// `#[on_refused]`, for the source's handler and the saga's callback; (4)
// annals does not check that a PM declares a starting input (angzarr-cli
// warns), so nothing here pins it; (5) `Failure::Projection` with the
// committed books, `Recovery::Retry`; (6) a refusal with no handler: the
// chain commits, the fact is dead-lettered and alerted, the call is Ok;
// (7) the new error codes pinned as strings until their variants exist;
// (8) `Saga::on_refused` answers the fact's payload as `Any`; (9) a
// `CommandRefused` from outside the chain is accepted when its trigger
// names the receiving stream; (10) a saga names a PM target's root with
// the correlation.

/// A buy-in: a process across the wallet, keyed by its correlation.
#[derive(Clone, Default, PartialEq, Debug)]
pub struct BuyIn {
    started: bool,
    held: i64,
    failed: bool,
}

#[derive(Clone, PartialEq, Debug)]
pub enum BuyInRefused {
    AlreadyStarted,
}

impl fmt::Display for BuyInRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the buy-in has started already")
    }
}

impl Diagnostic for BuyInRefused {
    fn code(&self) -> &'static str {
        "toy-buyin-already-started"
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![]
    }
}

#[process_manager(domain = "buyin")]
impl BuyIn {
    /// The starting input: a new correlation's buy-in begins here.
    #[handles_cmd(StartBuyIn, starts)]
    fn start(&self, cmd: StartBuyIn, _ctx: &Context) -> Result<BuyInStarted, BuyInRefused> {
        match self.started {
            true => Err(BuyInRefused::AlreadyStarted),
            false => Ok(BuyInStarted {
                wallet: cmd.wallet,
                cents: cmd.cents,
            }),
        }
    }

    /// Not a starting input: refused for a correlation with no buy-in.
    #[handles_cmd(RecordHeld)]
    fn record_held(&self, cmd: RecordHeld, _ctx: &Context) -> Result<BuyInHeld, BuyInRefused> {
        Ok(BuyInHeld { cents: cmd.cents })
    }

    /// A timeout, a fact, not a starting one: skipped for a correlation
    /// with no buy-in. Records the fact's origin, from the one context.
    #[handles_fact(BuyInExpired)]
    fn expired(&self, _fact: BuyInExpired, ctx: &Context) -> Record<Self> {
        Record::of(Expired {
            external_id: ctx
                .fact_origin()
                .map(|origin| origin.external_id.clone())
                .unwrap_or_default(),
        })
    }

    /// The wallet refused the hold BuyInWallet sent: passed through, its
    /// typed refusal object unpacked here.
    #[on_refused(Hold)]
    fn hold_refused(&self, refused: CommandRefused<Hold>, _ctx: &Context) -> Record<Self> {
        Record::of(BuyInFailed {
            short: refused
                .refusal::<InsufficientFunds>()
                .map(|object| object.short)
                .unwrap_or_default(),
        })
    }

    /// The same refusal, translated by the saga into the buy-in's own
    /// fact: the buy-in never reads the wallet's refusal object.
    #[handles_fact(HoldLost)]
    fn hold_lost(&self, fact: HoldLost, _ctx: &Context) -> Record<Self> {
        Record::of(HoldWasLost { short: fact.short })
    }

    #[applies(BuyInStarted)]
    fn started(&mut self, _event: BuyInStarted) {
        self.started = true;
    }

    #[applies(BuyInHeld)]
    fn buy_in_held(&mut self, event: BuyInHeld) {
        self.held += event.cents;
    }

    #[applies(Expired)]
    fn expired_at(&mut self, _event: Expired) {}

    #[applies(BuyInFailed)]
    fn failed(&mut self, _event: BuyInFailed) {
        self.failed = true;
    }

    #[applies(HoldWasLost)]
    fn lost(&mut self, _event: HoldWasLost) {
        self.failed = true;
    }
}

/// The saga for (buy-in, wallet): a started buy-in holds its stake on the
/// wallet it names. Pass-through: a refused hold reaches the buy-in as
/// `CommandRefused`.
pub struct BuyInWallet;

#[saga(name = "buyin-wallet", source = BuyIn, target = Wallet)]
impl BuyInWallet {
    #[reacts(BuyInStarted)]
    fn started(&self, event: &BuyInStarted, _ctx: &ReactionContext) -> Commands<Wallet> {
        Commands::none().to(uuid(&event.wallet), Hold { cents: event.cents })
    }
}

/// As [`BuyInWallet`], with an `#[on_refused(Hold)]` callback, whose
/// presence selects translate for `Hold`: it reads the wallet's refusal
/// object and hands the buy-in its own fact.
pub struct TranslatingBuyInWallet;

#[saga(name = "buyin-wallet", source = BuyIn, target = Wallet)]
impl TranslatingBuyInWallet {
    #[reacts(BuyInStarted)]
    fn started(&self, event: &BuyInStarted, _ctx: &ReactionContext) -> Commands<Wallet> {
        Commands::none().to(uuid(&event.wallet), Hold { cents: event.cents })
    }

    #[on_refused(Hold)]
    fn hold_refused(&self, refusal: &Refusal<Hold>) -> HoldLost {
        HoldLost {
            short: refusal
                .refusal::<InsufficientFunds>()
                .map(|object| object.short)
                .unwrap_or(refusal.command().cents),
        }
    }
}

/// The saga for (wallet, buy-in): a hold on the wallet is recorded on the
/// buy-in of its correlation. The buy-in's root IS that correlation.
pub struct WalletBuyIn;

#[saga(name = "wallet-buyin", source = Wallet, target = BuyIn)]
impl WalletBuyIn {
    #[reacts(Held)]
    fn held(&self, event: &Held, ctx: &ReactionContext) -> Commands<BuyIn> {
        Commands::none().to(
            ctx.correlation_id().clone(),
            RecordHeld { cents: event.cents },
        )
    }
}

/// `bytes` as a root.
fn uuid(bytes: &[u8]) -> Uuid {
    Uuid {
        value: bytes.to_vec().into(),
    }
}

/// Correlation `n`: the root of its buy-in.
fn correlation(n: u8) -> Uuid {
    Uuid::from_bytes([0xc0 | n; 16])
}

/// The buy-in's stream for correlation `c`.
fn buy_in_cover(c: &Uuid) -> Cover {
    Cover {
        domain: "buyin".to_string(),
        root: Some(c.clone()),
        edition: None,
    }
}

/// A book of `body` for the stream at `cover`, command id `[n; 16]`,
/// with `correlation` if given and `origin` for a fact.
fn book(
    cover: Cover,
    n: u8,
    body: command_page::Body,
    correlation: Option<Uuid>,
    origin: Option<FactOrigin>,
) -> CommandBook {
    let fact = origin.is_some();
    CommandBook {
        cover: Some(cover),
        page: Some(CommandPage {
            body: Some(body),
            command_id: (!fact).then(|| Uuid::from_bytes([n; 16])),
            correlation_id: correlation,
            fact_origin: origin,
            ..CommandPage::default()
        }),
        handling: None,
    }
}

/// Command `payload`, numbered `n`, to the stream at `cover`, in
/// `correlation`.
fn command_to(cover: Cover, n: u8, payload: Any, correlation: Option<Uuid>) -> CommandBook {
    book(
        cover,
        n,
        command_page::Body::Command(Command {
            payload: Some(payload),
        }),
        correlation,
        None,
    )
}

/// Fact `payload` from `external_id`, to the stream at `cover`.
fn fact_to(cover: Cover, payload: Any, external_id: &str) -> CommandBook {
    book(
        cover,
        0,
        command_page::Body::Fact(Fact {
            payload: Some(payload),
        }),
        None,
        Some(FactOrigin {
            external_id: external_id.to_string(),
            description: "toy".to_string(),
        }),
    )
}

/// Handle `book` (sampled at `n` seconds) through the kernel and `store`
/// on the async path, as an application does; `log` gets the dead
/// letters.
fn handle_in<D: Domain>(
    store: &mut ::annals::next::InMemoryStore,
    registry: &Registry,
    log: &MemoryDeadLetters,
    n: u8,
    book: CommandBook,
) -> Result<Handled, Failure<D::Rejection>> {
    let sampled = Sampled {
        now: at(i64::from(n)),
        command_id: Uuid::from_bytes([0x70 | n; 16]),
    };
    ready(
        ::annals::next::Kernel::new(registry, log).handle_async::<D, _, _>(
            ::annals::next::Ready(store.begin()),
            &::annals::next::Routes::new(),
            book,
            &sampled,
            &mut (),
        ),
    )
}

/// The stored pages of the stream at `cover`; `None` when it has none.
fn pages(store: &mut ::annals::next::InMemoryStore, cover: &Cover) -> Option<Vec<EventPage>> {
    use ::annals::next::EventStore;
    store.read(cover).unwrap().map(|slice| slice.pages)
}

/// The type names of the events the buy-in of correlation `c` recorded;
/// `None` when it has no stream.
fn buy_in_events(store: &mut ::annals::next::InMemoryStore, c: &Uuid) -> Option<Vec<String>> {
    pages(store, &buy_in_cover(c)).map(|pages| {
        pages
            .iter()
            .map(|p| type_name(&p.payload.as_ref().unwrap().type_url).to_string())
            .collect()
    })
}

/// The buy-in events, by name.
fn buy_in(names: &[&str]) -> Option<Vec<String>> {
    Some(names.iter().map(|n| format!("toy.buyin.v1.{n}")).collect())
}

/// The wallet and the buy-in, with no saga between them.
fn buy_ins() -> Registry {
    let mut registry = registry();
    registry.process_manager::<BuyIn>(BuyIn::default);
    registry
}

/// The wallet, the buy-in, both sagas (`translating` picks the buy-in
/// saga's mode).
fn flows(translating: bool) -> Registry {
    let mut registry = buy_ins();
    registry.saga(WalletBuyIn);
    match translating {
        true => registry.saga(TranslatingBuyInWallet),
        false => registry.saga(BuyInWallet),
    };
    registry
}

/// `flows`, a store, and a wallet at [`root`] opened with `bonus`
/// (command 1, in a correlation of its own).
fn funded(translating: bool, bonus: i64) -> (Registry, ::annals::next::InMemoryStore) {
    let registry = flows(translating);
    let mut store = ::annals::next::InMemoryStore::new();
    let log = MemoryDeadLetters::new();
    handle_in::<Wallet>(
        &mut store,
        &registry,
        &log,
        1,
        send("wallet", 1, any(Open { bonus }), Some(correlation(15))),
    )
    .unwrap();
    (registry, store)
}

/// StartBuyIn for `cents` from the wallet at [`root`], as command `n` to
/// the buy-in of correlation `c`.
fn start(c: &Uuid, n: u8, cents: i64) -> CommandBook {
    command_to(
        buy_in_cover(c),
        n,
        any(StartBuyIn {
            wallet: root().value.to_vec(),
            cents,
        }),
        Some(c.clone()),
    )
}

mod process_manager {
    use super::*;

    /// A starting command for a correlation with no buy-in starts one, at
    /// the root that IS the correlation: no derivation.
    #[test]
    fn a_starting_command_starts_an_instance_at_its_correlation() {
        let registry = buy_ins();
        let mut store = ::annals::next::InMemoryStore::new();
        let log = MemoryDeadLetters::new();
        let started =
            handle_in::<BuyIn>(&mut store, &registry, &log, 2, start(&correlation(1), 2, 0));
        assert!(
            started.is_ok(),
            "{:?}",
            started.err().map(|e| e.to_string())
        );
        assert_eq!(
            buy_in_events(&mut store, &correlation(1)).map(|names| names[0].clone()),
            Some("toy.buyin.v1.BuyInStarted".to_string())
        );
    }

    /// A command that is not a starting one, for a correlation with no
    /// instance, is refused (`PM_NO_INSTANCE`, a business refusal) and
    /// records nothing. Its code is pinned as a string (RULED, 7).
    #[test]
    fn a_non_starting_command_for_no_instance_is_refused() {
        let registry = buy_ins();
        let mut store = ::annals::next::InMemoryStore::new();
        let log = MemoryDeadLetters::new();
        let held = command_to(
            buy_in_cover(&correlation(2)),
            2,
            any(RecordHeld { cents: 5 }),
            Some(correlation(2)),
        );
        let Err(refused) = handle_in::<BuyIn>(&mut store, &registry, &log, 2, held) else {
            panic!("a command for no instance is refused");
        };
        assert_eq!(refused.code(), "annals-next-pm-no-instance");
        assert_eq!(refused.recovery(), Recovery::Business);
        assert_eq!(buy_in_events(&mut store, &correlation(2)), None);
    }

    /// A fact that is not a starting one, for a correlation with no
    /// instance, is skipped silently: applied, nothing recorded, nothing
    /// dead-lettered.
    #[test]
    fn a_non_starting_fact_for_no_instance_is_skipped() {
        let registry = buy_ins();
        let mut store = ::annals::next::InMemoryStore::new();
        let log = MemoryDeadLetters::new();
        let expired = fact_to(
            buy_in_cover(&correlation(3)),
            any(BuyInExpired {}),
            "timeout-3",
        );
        let skipped = handle_in::<BuyIn>(&mut store, &registry, &log, 2, expired);
        assert!(
            matches!(skipped, Ok(Handled::Applied(_))),
            "{:?}",
            skipped.err().map(|e| e.to_string())
        );
        assert_eq!(buy_in_events(&mut store, &correlation(3)), None);
        assert!(log.lines().is_empty());
    }

    /// A command whose correlation is set to anything but its cover's root
    /// names no instance: malformed. Its code is pinned as a string
    /// (RULED, 7).
    #[test]
    fn a_correlation_other_than_the_root_is_malformed() {
        let registry = buy_ins();
        let mut store = ::annals::next::InMemoryStore::new();
        let log = MemoryDeadLetters::new();
        let mut astray = start(&correlation(4), 2, 0);
        astray.page.as_mut().unwrap().correlation_id = Some(correlation(5));
        let Err(refused) = handle_in::<BuyIn>(&mut store, &registry, &log, 2, astray) else {
            panic!("a correlation that is not the root is malformed");
        };
        assert_eq!(refused.code(), "annals-next-malformed-root-not-correlation");
        assert_eq!(refused.recovery(), Recovery::Invalid);
    }

    /// A correlation left unset is given the root: the buy-in's events
    /// carry it.
    #[test]
    fn an_unset_correlation_is_given_the_root() {
        let registry = buy_ins();
        let mut store = ::annals::next::InMemoryStore::new();
        let log = MemoryDeadLetters::new();
        let mut unset = start(&correlation(6), 2, 0);
        unset.page.as_mut().unwrap().correlation_id = None;
        handle_in::<BuyIn>(&mut store, &registry, &log, 2, unset).unwrap();
        let pages = pages(&mut store, &buy_in_cover(&correlation(6))).expect("the buy-in");
        assert_eq!(
            pages[0]
                .meta
                .as_ref()
                .and_then(|m| m.correlation_id.clone()),
            Some(correlation(6))
        );
    }

    /// The one handler context carries a fact's identity: its origin
    /// reaches the buy-in's fact handler.
    #[test]
    fn a_facts_origin_reaches_its_handler() {
        let registry = buy_ins();
        let mut store = ::annals::next::InMemoryStore::new();
        let log = MemoryDeadLetters::new();
        handle_in::<BuyIn>(&mut store, &registry, &log, 2, start(&correlation(7), 2, 0)).unwrap();
        let expired = fact_to(
            buy_in_cover(&correlation(7)),
            any(BuyInExpired {}),
            "timeout-7",
        );
        let Ok(Handled::Applied(recorded)) =
            handle_in::<BuyIn>(&mut store, &registry, &log, 3, expired)
        else {
            panic!("the timeout is applied");
        };
        assert_eq!(
            recorded.books()[0]
                .pages
                .last()
                .and_then(|p| p.payload.clone()),
            Some(any(Expired {
                external_id: "timeout-7".to_string()
            }))
        );
    }

    /// A command sent again with its id is a duplicate, as on any domain:
    /// nothing runs or is recorded twice.
    #[test]
    fn a_command_sent_again_is_a_duplicate() {
        let registry = buy_ins();
        let mut store = ::annals::next::InMemoryStore::new();
        let log = MemoryDeadLetters::new();
        handle_in::<BuyIn>(&mut store, &registry, &log, 2, start(&correlation(8), 2, 0)).unwrap();
        let again =
            handle_in::<BuyIn>(&mut store, &registry, &log, 3, start(&correlation(8), 2, 0));
        assert!(matches!(again, Ok(Handled::Duplicate(_))));
        assert_eq!(
            buy_in_events(&mut store, &correlation(8)),
            buy_in(&["BuyInStarted"])
        );
    }

    /// `STARTS` names the starting inputs, `HANDLES` every command and
    /// fact, and `REFUSALS` the commands whose refusals it handles, each in
    /// marker order.
    #[test]
    fn its_constants_name_its_inputs() {
        let name = |name| TypeName {
            package: "toy.buyin.cmd.v1",
            name,
        };
        assert_eq!(<BuyIn as ProcessManager>::STARTS, [name("StartBuyIn")]);
        assert_eq!(
            BuyIn::HANDLES,
            [
                name("StartBuyIn"),
                name("RecordHeld"),
                name("BuyInExpired"),
                name("HoldLost"),
            ]
        );
        assert_eq!(
            BuyIn::REFUSALS,
            [TypeName {
                package: "toy.wallet.cmd.v1",
                name: "Hold",
            }]
        );
    }

    #[test]
    #[should_panic(expected = "buyin")]
    fn registering_a_process_manager_twice_panics() {
        let mut registry = registry();
        registry
            .process_manager::<BuyIn>(BuyIn::default)
            .process_manager::<BuyIn>(BuyIn::default);
    }
}

mod saga_registration {
    use super::*;

    #[test]
    #[should_panic(expected = "buyin-wallet")]
    fn registering_a_saga_twice_panics() {
        let mut registry = registry();
        registry.saga(BuyInWallet).saga(BuyInWallet);
    }
}

mod typed_refusals {
    use super::*;

    /// A handler with `refuses` refuses with one of its typed objects, as
    /// the caller receives it; the wallet records nothing (RULED, 2).
    #[test]
    fn a_typed_refusal_carries_its_object_and_records_nothing() {
        let registry = registry();
        let mut wallet = opened(&registry, 10);
        let refused = run(&mut wallet, &registry, 2, any(Hold { cents: 30 }));
        match refused {
            Err(Failure::Rejected(Rejection::Typed(typed))) => {
                assert_eq!(
                    typed.object::<InsufficientFunds>(),
                    Some(InsufficientFunds { short: 20 })
                );
                assert_eq!(typed.object::<WalletClosed>(), None);
            }
            other => panic!("a typed refusal, not {other:?}"),
        }
        assert_eq!(wallet.len(), 2, "nothing was recorded");
    }

    /// As a refused saga receives it: the object packed, and its message.
    #[test]
    fn a_typed_refusals_response_packs_its_object_and_message() {
        let registry = registry();
        let mut wallet = wallet();
        let refused = run(&mut wallet, &registry, 1, any(Hold { cents: 1 }));
        let Err(Failure::Rejected(rejection)) = refused else {
            panic!("refused");
        };
        assert_eq!(
            rejection.response(),
            RefusalResponse {
                refusal: Some(any(WalletClosed {})),
                message: "the wallet is closed".to_string(),
            }
        );
    }

    /// A handler without `refuses` refuses with the domain's own rejection,
    /// a message only: no object.
    #[test]
    fn a_message_only_refusal_has_no_object() {
        let registry = registry();
        let mut wallet = opened(&registry, 10);
        let Err(Failure::Rejected(rejection)) =
            run(&mut wallet, &registry, 2, any(Withdraw { cents: 30 }))
        else {
            panic!("refused");
        };
        assert_eq!(
            rejection,
            Rejection::Message(Refused::Overdrawn { short: 20 })
        );
        assert_eq!(
            rejection.response(),
            RefusalResponse {
                refusal: None,
                message: "20 cents short".to_string(),
            }
        );
    }

    /// `REFUSES` names a command's typed refusal objects, in order.
    #[test]
    fn refuses_names_the_commands_refusal_objects() {
        let object = |name| TypeName {
            package: "toy.wallet.v1",
            name,
        };
        assert_eq!(
            <Wallet as Refusing<Hold>>::REFUSES,
            [object("InsufficientFunds"), object("WalletClosed")]
        );
    }
}

/// The buy-in's first event at correlation `c` (sequence 0), as a saga's
/// trigger.
fn trigger(c: &Uuid) -> EventRef {
    EventRef {
        stream: Some(buy_in_cover(c)),
        sequence: 0,
    }
}

/// A refusal of a hold for `cents`, sent by the buy-in saga for the
/// buy-in at correlation `c`, with `response`.
fn hold_refusal(c: &Uuid, cents: i64, response: RefusalResponse) -> markup::v1::Refusal {
    markup::v1::Refusal {
        event: Some(EventBook {
            cover: Some(buy_in_cover(c)),
            pages: vec![EventPage {
                sequence: 0,
                payload: Some(any(BuyInStarted {
                    wallet: root().value.to_vec(),
                    cents,
                })),
                ..EventPage::default()
            }],
            ..EventBook::default()
        }),
        command: Some(send("wallet", 9, any(Hold { cents }), Some(c.clone()))),
        response: Some(response),
    }
}

fn insufficient(short: i64) -> RefusalResponse {
    RefusalResponse {
        refusal: Some(any(InsufficientFunds { short })),
        message: format!("{short} cents short of the hold"),
    }
}

mod saga_refusals {
    use super::*;

    /// Pass-through (the default): the generated `on_refused` answers
    /// angzarr's `CommandRefused { command, refusal, trigger }`, the
    /// refused command as sent, its typed object still packed, the trigger
    /// the triggering event; the message stays behind. The answer is the
    /// fact's payload, as `Any` (RULED, 8).
    #[test]
    fn pass_through_builds_command_refused_for_the_source() {
        let refusal = hold_refusal(&correlation(1), 30, insufficient(20));
        let fact = BuyInWallet.on_refused(&refusal);
        assert_eq!(
            fact,
            pack(&markup::v1::CommandRefused {
                command: refusal.command.clone(),
                refusal: Some(any(InsufficientFunds { short: 20 })),
                trigger: Some(trigger(&correlation(1))),
            })
        );
    }

    /// Translate (selected by the callback's presence): it reads the typed
    /// object and answers the source's own fact (RULED, 1 and 3).
    #[test]
    fn translate_answers_the_callbacks_domestic_fact() {
        let refusal = hold_refusal(&correlation(1), 30, insufficient(20));
        assert_eq!(
            TranslatingBuyInWallet.on_refused(&refusal),
            any(HoldLost { short: 20 })
        );
    }

    /// A saga's constants: what it reacts to, and what it translates, the
    /// latter derived from its `#[on_refused]` callbacks (RULED, 1).
    #[test]
    fn its_constants_name_its_reactions_and_translations() {
        let started = TypeName {
            package: "toy.buyin.v1",
            name: "BuyInStarted",
        };
        let hold = TypeName {
            package: "toy.wallet.cmd.v1",
            name: "Hold",
        };
        assert_eq!(<BuyInWallet as Saga>::REACTS, [started]);
        assert_eq!(<BuyInWallet as Saga>::TRANSLATES, []);
        assert_eq!(<TranslatingBuyInWallet as Saga>::TRANSLATES, [hold]);
    }

    /// The source's `#[on_refused(Hold)]` is chosen by the refused
    /// command's type, and reads the typed object: a `CommandRefused` of a
    /// hold, delivered to the buy-in its trigger names, records the
    /// failure. Delivered from outside the chain, it is accepted because
    /// its trigger names the receiving stream (RULED, 9).
    #[test]
    fn on_refused_dispatches_by_the_refused_commands_type() {
        let registry = buy_ins();
        let mut store = ::annals::next::InMemoryStore::new();
        let log = MemoryDeadLetters::new();
        handle_in::<BuyIn>(
            &mut store,
            &registry,
            &log,
            2,
            start(&correlation(2), 2, 30),
        )
        .unwrap();
        let refused = BuyInWallet.on_refused(&hold_refusal(&correlation(2), 30, insufficient(20)));
        handle_in::<BuyIn>(
            &mut store,
            &registry,
            &log,
            3,
            fact_to(buy_in_cover(&correlation(2)), refused, "refusal-2"),
        )
        .unwrap();
        assert_eq!(
            buy_in_events(&mut store, &correlation(2)),
            buy_in(&["BuyInStarted", "BuyInFailed"])
        );
    }

    /// A refusal fact is accepted only by the exact stream its trigger
    /// names: delivered anywhere else, it is malformed (RULED, 7 and 9).
    #[test]
    fn a_refusal_fact_for_another_stream_is_malformed() {
        let registry = buy_ins();
        let mut store = ::annals::next::InMemoryStore::new();
        let log = MemoryDeadLetters::new();
        handle_in::<BuyIn>(
            &mut store,
            &registry,
            &log,
            2,
            start(&correlation(3), 2, 30),
        )
        .unwrap();
        let elsewhere =
            BuyInWallet.on_refused(&hold_refusal(&correlation(4), 30, insufficient(20)));
        let Err(refused) = handle_in::<BuyIn>(
            &mut store,
            &registry,
            &log,
            3,
            fact_to(buy_in_cover(&correlation(3)), elsewhere, "refusal-3"),
        ) else {
            panic!("a refusal fact for another stream is malformed");
        };
        assert_eq!(refused.code(), "annals-next-malformed-refusal-misaddressed");
        assert_eq!(
            buy_in_events(&mut store, &correlation(3)),
            buy_in(&["BuyInStarted"])
        );
    }
}

mod refusal_flow {
    use super::*;

    /// A saga's command the target accepts flows on: the buy-in's hold is
    /// recorded on the wallet, and the wallet's saga records it back on
    /// the buy-in of its correlation, all in one chain: the saga names the
    /// buy-in's root with the correlation (RULED, 10).
    #[test]
    fn an_accepted_command_flows_through_both_sagas() {
        let (registry, mut store) = funded(false, 50);
        let log = MemoryDeadLetters::new();
        handle_in::<BuyIn>(
            &mut store,
            &registry,
            &log,
            2,
            start(&correlation(1), 2, 30),
        )
        .unwrap();
        assert_eq!(
            buy_in_events(&mut store, &correlation(1)),
            buy_in(&["BuyInStarted", "BuyInHeld"])
        );
        let wallet = pages(&mut store, wallet().cover()).expect("the wallet");
        assert_eq!(
            wallet.last().and_then(|p| p.payload.clone()),
            Some(any(Held { cents: 30 }))
        );
    }

    /// Pass-through: the wallet refuses the hold and records nothing; the
    /// saga's `CommandRefused` reaches the buy-in's `#[on_refused(Hold)]`,
    /// which records the failure; the chain commits.
    #[test]
    fn a_refused_command_reaches_the_sources_on_refused_handler() {
        let (registry, mut store) = funded(false, 10);
        let log = MemoryDeadLetters::new();
        let handled = handle_in::<BuyIn>(
            &mut store,
            &registry,
            &log,
            2,
            start(&correlation(2), 2, 30),
        );
        assert!(
            handled.is_ok(),
            "{:?}",
            handled.err().map(|e| e.to_string())
        );
        assert_eq!(
            buy_in_events(&mut store, &correlation(2)),
            buy_in(&["BuyInStarted", "BuyInFailed"])
        );
        assert_eq!(
            pages(&mut store, wallet().cover()).map(|p| p.len()),
            Some(2),
            "the wallet recorded nothing of the hold"
        );
    }

    /// The refusal fact's id is the refused command's id, and its origin's
    /// external id that id's canonical text.
    #[test]
    fn the_refusal_facts_id_is_the_refused_commands() {
        let (registry, mut store) = funded(false, 10);
        let log = MemoryDeadLetters::new();
        handle_in::<BuyIn>(
            &mut store,
            &registry,
            &log,
            2,
            start(&correlation(3), 2, 30),
        )
        .unwrap();
        let pages = pages(&mut store, &buy_in_cover(&correlation(3))).expect("the buy-in");
        let hold_id =
            ::annals::next::ids::reaction_command_id(&trigger(&correlation(3)), "buyin-wallet", 0)
                .unwrap();
        let meta = pages[1].meta.clone().expect("meta");
        assert_eq!(meta.causation, Some(Causation::CommandId(hold_id.clone())));
        assert_eq!(
            meta.fact.map(|origin| origin.external_id),
            Some(hold_id.to_string())
        );
    }

    /// Translate: the saga's callback hands the buy-in its own fact,
    /// which its `#[handles_fact(HoldLost)]` records.
    #[test]
    fn translate_delivers_the_callbacks_domestic_fact() {
        let (registry, mut store) = funded(true, 10);
        let log = MemoryDeadLetters::new();
        handle_in::<BuyIn>(
            &mut store,
            &registry,
            &log,
            2,
            start(&correlation(4), 2, 30),
        )
        .unwrap();
        assert_eq!(
            buy_in_events(&mut store, &correlation(4)),
            buy_in(&["BuyInStarted", "HoldWasLost"])
        );
    }

    /// A refusal with no path: a hold on the wallet outside any buy-in is
    /// recorded, the wallet's saga sends `RecordHeld` to a correlation with
    /// no buy-in, which refuses it, and the wallet handles no refusal of
    /// `RecordHeld`. The chain commits (the hold stays committed), the
    /// fact is dead-lettered, and the call is Ok (RULED, 6).
    #[test]
    fn a_refusal_with_no_handler_is_dead_lettered() {
        let (registry, mut store) = funded(false, 50);
        let log = MemoryDeadLetters::new();
        let handled = handle_in::<Wallet>(
            &mut store,
            &registry,
            &log,
            2,
            send("wallet", 2, any(Hold { cents: 5 }), Some(correlation(5))),
        );
        assert!(
            handled.is_ok(),
            "{:?}",
            handled.err().map(|e| e.to_string())
        );
        assert_eq!(
            pages(&mut store, wallet().cover())
                .and_then(|p| p.last().and_then(|p| p.payload.clone())),
            Some(any(Held { cents: 5 }))
        );
        let lines = log.lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains(" to wallet "), "{lines:?}");
    }
}

// ------------------------------------------------- the projector (red)
//
// PROPOSAL (ruling B2): ONE projector instance per process, registered
// with its ports, run AFTER COMMIT and awaited on the caller's runtime
// before the call returns, so a failure reaches the caller as an error;
// the events stay committed. Dispatch blocked on the linked router, as
// the PM's.

/// The projector's port: the balances it wrote, and whether to fail.
#[derive(Clone, Default)]
pub struct BalancePort {
    lines: std::sync::Arc<std::sync::Mutex<Vec<(u32, i64)>>>,
    fail: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

/// Writes each deposit, numbered by how many THIS instance has written:
/// one instance across streams numbers them on.
pub struct Balances {
    port: BalancePort,
    written: u32,
}

#[projector(name = "balances", source = Wallet)]
impl Balances {
    #[projects(Deposited)]
    async fn deposited(
        &mut self,
        event: &Deposited,
        _ctx: &ProjectionContext,
    ) -> Result<(), Report> {
        if self.port.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Report::of(&Refused::NotOpen));
        }
        self.written += 1;
        self.port
            .lines
            .lock()
            .unwrap()
            .push((self.written, event.cents));
        Ok(())
    }
}

/// Poll `f` to completion with a waker that does nothing: enough for the
/// in-memory store, which is ready at once.
fn ready<F: std::future::Future>(f: F) -> F::Output {
    use std::task::{Context as Cx, Poll, RawWaker, RawWakerVTable, Waker};
    fn noop(_: *const ()) {}
    fn clone(p: *const ()) -> RawWaker {
        RawWaker::new(p, &VTABLE)
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    // SAFETY: every vtable function ignores the data pointer.
    let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
    let mut f = std::pin::pin!(f);
    match f.as_mut().poll(&mut Cx::from_waker(&waker)) {
        Poll::Ready(out) => out,
        Poll::Pending => panic!("not ready at once"),
    }
}

/// Handle `payload` as command `n` to the wallet at `root`, on the async
/// path, awaited.
fn handle_async(
    store: &mut ::annals::next::InMemoryStore,
    registry: &Registry,
    n: u8,
    payload: Any,
    root: Uuid,
) -> Result<Handled, Failure<Rejection<Refused>>> {
    let sampled = Sampled {
        now: at(i64::from(n)),
        command_id: Uuid::from_bytes([n; 16]),
    };
    let mut book = send("wallet", n, payload, None);
    book.cover.as_mut().unwrap().root = Some(root);
    let log = MemoryDeadLetters::new();
    ready(
        ::annals::next::Kernel::new(registry, &log).handle_async::<Wallet, _, _>(
            ::annals::next::Ready(store.begin()),
            &::annals::next::Routes::new(),
            book,
            &sampled,
            &mut (),
        ),
    )
}

mod projector {
    use super::*;

    fn projected(port: &BalancePort) -> Registry {
        let mut registry = registry();
        registry.projector(Balances {
            port: port.clone(),
            written: 0,
        });
        registry
    }

    /// Run after the commit and awaited: the read model holds the deposit
    /// once the call returns.
    #[test]
    fn it_runs_after_commit_and_is_awaited_before_the_call_returns() {
        let port = BalancePort::default();
        let registry = projected(&port);
        let mut store = ::annals::next::InMemoryStore::new();
        handle_async(&mut store, &registry, 1, any(Open { bonus: 5 }), root()).unwrap();
        assert_eq!(*port.lines.lock().unwrap(), [(1, 5)]);
    }

    /// One instance per process: two streams' deposits reach the same
    /// instance, which numbers them on.
    #[test]
    fn one_instance_serves_every_stream() {
        let port = BalancePort::default();
        let registry = projected(&port);
        let mut store = ::annals::next::InMemoryStore::new();
        handle_async(&mut store, &registry, 1, any(Open { bonus: 5 }), root()).unwrap();
        let other = Uuid::from_bytes([0x0b; 16]);
        handle_async(&mut store, &registry, 2, any(Open { bonus: 7 }), other).unwrap();
        assert_eq!(*port.lines.lock().unwrap(), [(1, 5), (2, 7)]);
    }

    /// An event it does not mark is skipped.
    #[test]
    fn an_unmarked_event_is_skipped() {
        let port = BalancePort::default();
        let registry = projected(&port);
        let mut store = ::annals::next::InMemoryStore::new();
        handle_async(&mut store, &registry, 1, any(Open { bonus: 5 }), root()).unwrap();
        handle_async(&mut store, &registry, 2, any(Withdraw { cents: 5 }), root()).unwrap();
        assert_eq!(*port.lines.lock().unwrap(), [(1, 5)]);
    }

    /// A failure is the caller's ERROR, raised after the commit: a
    /// `Failure::Projection` naming the projector and carrying what the
    /// chain committed, and those events stay committed; its recovery is
    /// a retry (RULED, 5).
    #[test]
    fn a_failure_is_the_callers_error_and_the_events_stay_committed() {
        use ::annals::next::EventStore;
        let port = BalancePort::default();
        port.fail.store(true, std::sync::atomic::Ordering::SeqCst);
        let registry = projected(&port);
        let mut store = ::annals::next::InMemoryStore::new();
        let failed = handle_async(&mut store, &registry, 1, any(Open { bonus: 5 }), root());
        let Err(Failure::Projection {
            committed,
            projector,
            error,
        }) = failed
        else {
            panic!("a failing projection is the caller's Failure::Projection");
        };
        assert_eq!(projector, "balances");
        assert_eq!(error.code(), "toy-wallet-not-open");
        assert_eq!(
            committed
                .iter()
                .map(|book| book.pages.len())
                .collect::<Vec<_>>(),
            [2],
            "the wallet's book: Opened and Deposited"
        );
        let stored = store.read(wallet().cover()).unwrap().expect("the stream");
        assert_eq!(stored.pages.len(), 2, "and they stay committed");
        assert_eq!(
            Failure::<Rejection<Refused>>::Projection {
                committed,
                projector,
                error,
            }
            .recovery(),
            Recovery::Retry
        );
    }

    #[test]
    #[should_panic(expected = "balances")]
    fn registering_a_projector_twice_panics() {
        let port = BalancePort::default();
        let mut registry = projected(&port);
        registry.projector(Balances { port, written: 0 });
    }
}
