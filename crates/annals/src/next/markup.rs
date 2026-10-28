//! What annals-macros' expansion names: the runtime under a marked-up
//! aggregate or process manager (`#[aggregate]`, `#[process_manager]`,
//! `#[handles_cmd]`, `#[handles_fact]`, `#[on_refused]`, `#[applies]`),
//! saga (`#[saga]`, `#[reacts]`) and projector (`#[projector]`,
//! `#[projects]`); see annals-macros' crate docs. A hand-written
//! [`Domain`] needs none of it.
//!
//! - [`Context`]: a domain handler's third argument, what it may read
//!   besides itself and its input: one context, an aggregate's and a
//!   process manager's alike.
//! - [`DomainMessage`]: a domain type (angzarr-codegen's domain layer) and
//!   the prost message it travels as, so the router decodes by type URL
//!   into domain types and never hands a handler a wire type.
//! - [`Emits`] and [`Emit`]: what a command handler's `Ok` may be. A shape
//!   (`E`, a tuple, `Option<E>`, `Vec<E>`) is events the router applies, in
//!   order; [`Emit::then`] adds a continuation step the router runs on the
//!   instance after them, within the same decision.
//! - [`Record`] and [`Records`]: what a FACT handler returns: as [`Emit`],
//!   but its continuation steps return no `Result`.
//! - [`Rejection`], [`Refusing`], [`Refuses`] and [`TypedRefusal`]: a
//!   command's refusal, a message only or one of its declared typed
//!   refusal objects.
//! - [`CommandRefused`] and [`Refusal`]: a refusal fact as its source's
//!   `#[on_refused]` handler reads it, and a refusal as a translating
//!   saga's callback reads it.
//! - [`ProcessManager`]: what sets a process manager, a domain, apart: its
//!   root is the correlation, and its starting inputs.
//! - [`Saga`], [`Commands`] and [`ReactionContext`]: a saga, its output
//!   and its handler's context. [`Decides`] and [`Ingests`] say what a
//!   domain takes, for the checks on a saga.
//! - [`AsyncProjector`] and [`ProjectionContext`]: a projector.
//! - [`MARKUP_RULES_VERSION`]: the rules version every marked-up
//!   domain's events are recorded under.
//! - [`STEP_LIMIT`] and [`assert_step_idempotent`]: the cap on a decision's
//!   continuation steps, and the test helper for a step.
//! - [`TypeName`]: a message's package and name, as the generated
//!   constants list them.
//! - [`NoSnapshot`]: the snapshot form of a domain that has none.
//! - [`v1`]: angzarr's refusal messages, until annals' angzarr-proto has
//!   them.
//!
//! Every component here assumes nothing about distribution: the same
//! marked-up aggregate, process manager, saga or projector runs in
//! annals' process or as its own service, with no change to its code.
//!
//! **A proposal.** The bodies marked as stubs are `todo!()`; annals'
//! `next/decorated.test.rs` pins them.

use std::fmt;

use angzarr_proto::v1::{Cover, EventBook, EventRef, FactOrigin, Uuid};
use prost_types::{Any, Timestamp};

use super::domain::Domain;
use super::wire::WireError;

/// The error a generated `State::from_snapshot` returns, re-exported so
/// the expansion needs no dependency of its user's.
pub use annals_diagnostic::Report;

/// What a handler, or a continuation step, reads besides the instance and
/// its input, all read-only: the ONE handler context of every domain, an
/// aggregate's and a process manager's alike. The instant its events will
/// be recorded at (sampled by the application and injected, so the handler
/// stays pure: there is no wall clock), the stream it decides (for a
/// process manager, its root is the correlation), and the input's
/// identity: its command id (a client's, the one the application minted,
/// a saga's derived one, or, for a fact, the one derived from its origin;
/// a refusal fact's is the refused command's), its correlation, and, for a
/// fact, its origin.
#[derive(Clone, PartialEq, Debug)]
pub struct Context {
    recorded_at: Timestamp,
    stream: Cover,
    command_id: Uuid,
    correlation_id: Uuid,
    fact_origin: Option<FactOrigin>,
}

impl Context {
    /// A context for a handler's own unit tests; the router builds the real
    /// one from the input's `Tx`.
    pub fn new(
        recorded_at: Timestamp,
        stream: Cover,
        command_id: Uuid,
        correlation_id: Uuid,
    ) -> Context {
        Context {
            recorded_at,
            stream,
            command_id,
            correlation_id,
            fact_origin: None,
        }
    }

    /// This context, for a fact from `origin`.
    pub fn with_fact_origin(self, origin: FactOrigin) -> Context {
        Context {
            fact_origin: Some(origin),
            ..self
        }
    }

    /// When the events will be recorded (non-decreasing within the stream).
    pub fn recorded_at(&self) -> Timestamp {
        self.recorded_at
    }

    /// The stream being decided.
    pub fn stream(&self) -> &Cover {
        &self.stream
    }

    /// The input's command id.
    pub fn command_id(&self) -> &Uuid {
        &self.command_id
    }

    /// The input's correlation: as sent, else its command id (for a process
    /// manager, else its root).
    pub fn correlation_id(&self) -> &Uuid {
        &self.correlation_id
    }

    /// A fact's origin; `None` for a command.
    pub fn fact_origin(&self) -> Option<&FactOrigin> {
        self.fact_origin.as_ref()
    }
}

/// A domain type and the wire message it travels as. angzarr-codegen's
/// domain layer emits the impl beside each domain type; a prost message
/// does not implement it, so a handler or applier taking a wire type does
/// not compile.
pub trait DomainMessage: Clone + Send + Sync + Sized + 'static {
    /// The prost message: its [`prost::Name`] is the type URL the router
    /// routes by.
    type Wire: prost::Message + prost::Name + Default;

    /// This value on the wire.
    fn to_wire(&self) -> Self::Wire;

    /// The domain value a decoded message holds, or why it holds none.
    fn from_wire(wire: Self::Wire) -> Result<Self, WireError>;
}

/// A continuation step: a pure `&self` method of the aggregate (or a
/// closure over one), run on the instance with every event so far applied.
/// It refuses with anything that converts into the domain's
/// [`Rejection`]: the domain's own message-only rejection, or a
/// [`Refuses`] choice.
type Step<A> = Box<dyn FnOnce(&A, &Context) -> Result<Emit<A>, <A as Domain>::Rejection>>;

/// Events for the router to apply, in order, and optionally one
/// continuation step to run on the instance after them.
///
/// ```text
/// fn deposit(&self, cmd: Deposit, ctx: &Context) -> Result<Emit<Self>, Refused> {
///     Ok(Emit::of(Deposited { .. }).then(Self::settle))
/// }
///
/// fn settle(&self, ctx: &Context) -> Result<Option<Rewarded>, Refused> { .. }
/// ```
///
/// The router applies the events, calls the step on the updated instance,
/// applies what it returns, and repeats while there is a continuation. All
/// of it is **one decision**: a rejection from the handler or from any step
/// refuses it all, and what is committed is every step's events, in order,
/// in one commit. More than [`STEP_LIMIT`] steps is a bug, and the kernel's
/// [`KernelError::TooManySteps`](super::KernelError::TooManySteps).
///
/// A continuation is the only way a later event is decided from the state
/// after an earlier one. Applying an event never sets off processing: an
/// applier only assigns state.
pub struct Emit<A: Domain> {
    events: Vec<A::Event>,
    next: Option<Step<A>>,
}

impl<A: Domain> Emit<A> {
    /// These events, and no continuation.
    pub fn of(events: impl Emits<A>) -> Emit<A> {
        let _ = events;
        todo!("annals::next::markup::Emit is a stub")
    }

    /// Run `step` on the instance once these events are applied. The step
    /// may itself return an `Emit` with a continuation.
    pub fn then<S, O, E>(self, step: S) -> Emit<A>
    where
        S: FnOnce(&A, &Context) -> Result<O, E> + 'static,
        O: Emits<A>,
        E: Into<A::Rejection>,
    {
        let _ = (self.events, self.next, step);
        todo!("annals::next::markup::Emit is a stub")
    }
}

impl<A: Domain> fmt::Debug for Emit<A>
where
    A::Event: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Emit")
            .field("events", &self.events)
            .field("then", &self.next.is_some())
            .finish()
    }
}

/// What a handler's or a step's `Ok` may be, for aggregate `A`:
///
/// - a domain event `T`: one event;
/// - a tuple `(T1, T2)`, `(T1, T2, T3)`, `(T1, T2, T3, T4)`: a fixed set,
///   in order;
/// - `Option<T>`: one or none;
/// - `Vec<T>`: any number of one type;
/// - an [`Emit`]: events and a continuation.
///
/// A variable list of different types is not a shape (deferred).
pub trait Emits<A: Domain> {
    /// The events and the continuation, if any.
    fn into_emit(self) -> Emit<A>;
}

impl<A: Domain> Emits<A> for Emit<A> {
    fn into_emit(self) -> Emit<A> {
        todo!("annals::next::markup::Emits is a stub")
    }
}

impl<A: Domain, T: DomainMessage + Into<A::Event>> Emits<A> for T {
    fn into_emit(self) -> Emit<A> {
        todo!("annals::next::markup::Emits is a stub")
    }
}

impl<A, T1, T2> Emits<A> for (T1, T2)
where
    A: Domain,
    T1: DomainMessage + Into<A::Event>,
    T2: DomainMessage + Into<A::Event>,
{
    fn into_emit(self) -> Emit<A> {
        todo!("annals::next::markup::Emits is a stub")
    }
}

impl<A, T1, T2, T3> Emits<A> for (T1, T2, T3)
where
    A: Domain,
    T1: DomainMessage + Into<A::Event>,
    T2: DomainMessage + Into<A::Event>,
    T3: DomainMessage + Into<A::Event>,
{
    fn into_emit(self) -> Emit<A> {
        todo!("annals::next::markup::Emits is a stub")
    }
}

impl<A, T1, T2, T3, T4> Emits<A> for (T1, T2, T3, T4)
where
    A: Domain,
    T1: DomainMessage + Into<A::Event>,
    T2: DomainMessage + Into<A::Event>,
    T3: DomainMessage + Into<A::Event>,
    T4: DomainMessage + Into<A::Event>,
{
    fn into_emit(self) -> Emit<A> {
        todo!("annals::next::markup::Emits is a stub")
    }
}

impl<A: Domain, T: DomainMessage + Into<A::Event>> Emits<A> for Option<T> {
    fn into_emit(self) -> Emit<A> {
        todo!("annals::next::markup::Emits is a stub")
    }
}

impl<A: Domain, T: DomainMessage + Into<A::Event>> Emits<A> for Vec<T> {
    fn into_emit(self) -> Emit<A> {
        todo!("annals::next::markup::Emits is a stub")
    }
}

/// What a FACT handler (`#[handles_fact]`) returns: events for the router
/// to apply, in order, and optionally one continuation step, as [`Emit`],
/// but nothing in it can refuse. A fact already happened ("if it can't be
/// refused, it's a fact"), so its handler returns no `Result`, and neither
/// does any step: a step is `fn(&self, ctx: &Context) -> R`, `R` a shape
/// ([`Records`]) or another `Record`. Same router semantics as [`Emit`]:
/// one decision, every step's events committed together, [`STEP_LIMIT`].
///
/// ```text
/// fn payout(&self, fact: Payout, ctx: &Context) -> Record<Self> {
///     Record::of(PaidOut { .. }).then(Self::emptied)
/// }
///
/// fn emptied(&self, ctx: &Context) -> Option<Emptied> { .. }
/// ```
///
/// (Accepted default of 2026-10-26: a fact handler's continuation steps
/// are infallible. A separate type, not `Emit`, so a fallible step in a
/// fact's chain does not compile.)
///
/// **A proposal**: its bodies are `todo!()`.
pub struct Record<A: Domain> {
    events: Vec<A::Event>,
    next: Option<RecordStep<A>>,
}

/// A fact's continuation step: infallible.
type RecordStep<A> = Box<dyn FnOnce(&A, &Context) -> Record<A>>;

impl<A: Domain> Record<A> {
    /// These events, and no continuation.
    pub fn of(events: impl Records<A>) -> Record<A> {
        let _ = events;
        todo!("annals::next::markup::Record is a stub")
    }

    /// Run `step` on the instance once these events are applied. The step
    /// may itself return a `Record` with a continuation.
    pub fn then<S, O>(self, step: S) -> Record<A>
    where
        S: FnOnce(&A, &Context) -> O + 'static,
        O: Records<A>,
    {
        let _ = (self.events, self.next, step);
        todo!("annals::next::markup::Record is a stub")
    }
}

impl<A: Domain> fmt::Debug for Record<A>
where
    A::Event: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Record")
            .field("events", &self.events)
            .field("then", &self.next.is_some())
            .finish()
    }
}

/// What a fact handler, or a fact's continuation step, may return: the
/// [`Emits`] shapes (an event, a tuple of up to four, `Option`, `Vec`) or
/// a [`Record`]; never an [`Emit`], whose steps may refuse.
pub trait Records<A: Domain> {
    /// The events and the continuation, if any.
    fn into_record(self) -> Record<A>;
}

impl<A: Domain> Records<A> for Record<A> {
    fn into_record(self) -> Record<A> {
        self
    }
}

macro_rules! records_shape {
    ($([$($t:ident),+] $shape:ty;)*) => {$(
        impl<A: Domain, $($t: DomainMessage + Into<A::Event>),+> Records<A> for $shape {
            fn into_record(self) -> Record<A> {
                todo!("annals::next::markup::Records is a stub")
            }
        }
    )*};
}

records_shape! {
    [T] T;
    [T1, T2] (T1, T2);
    [T1, T2, T3] (T1, T2, T3);
    [T1, T2, T3, T4] (T1, T2, T3, T4);
    [T] Option<T>;
    [T] Vec<T>;
}

/// The rules version every marked-up domain's events (an aggregate's or a
/// process manager's) are recorded under
/// ([`Registry::aggregate`](super::Registry::aggregate) and
/// [`Registry::process_manager`](super::Registry::process_manager)
/// register it):
/// fixed, because the markup has no rulebook of its own (`Rules = ()`).
/// What changes how old events read is an upcaster
/// ([`Aggregate::upcaster`](super::Aggregate::upcaster)), not a
/// rules version. (Accepted default of 2026-10-26.)
pub const MARKUP_RULES_VERSION: &str = "markup-1";

/// A process manager written in the markup (`#[process_manager]`). A
/// process manager IS A DOMAIN: its inputs are commands (which sagas send
/// it, and which it may refuse) and facts (timeouts, webhooks, refusal
/// facts, which it never refuses); its only output is its own events. It
/// subscribes to nothing: the saga for (another domain, the PM's domain)
/// turns that domain's events into commands to it. The macro gives it the
/// same `Domain` and `State` impls an `#[aggregate]` gets; this trait adds
/// the two things that set a PM apart. Registered with
/// [`Registry::process_manager`](super::Registry::process_manager).
///
/// - **Its root is the correlation**, unchanged. A command or fact
///   addressed to it names the instance in its cover: a `correlation_id`
///   left unset is given the root, and one set to anything else is
///   malformed (`annals-next-malformed-root-not-correlation`).
/// - **The start rule.** Only an input type it declares as starting
///   (`#[handles_cmd(C, starts)]`, `#[handles_fact(F, starts)]`) may start
///   an instance. Any other input for a correlation with no instance is
///   not delivered, decided before the handler by the type after
///   upcasting: a FACT is skipped silently (applied, no events, nothing
///   dead-lettered), and a COMMAND is refused (`annals-next-pm-no-instance`,
///   a business refusal, so a saga's refused command yields its refusal
///   fact like any other), recording nothing.
///
/// **A proposal**: the macro emits it with `STARTS` empty; the dispatch is
/// the linked angzarr-router's (blocked).
pub trait ProcessManager: Domain<State = Self, Rules = ()> {
    /// The input types (commands and facts) that may start an instance,
    /// in marker order: angzarr's `ComponentDescriptor.starting_types`.
    const STARTS: &'static [TypeName];
}

/// That `A` decides command `C`: `#[handles_cmd(C)]` on its impl. What a
/// saga's [`Commands`] is checked against. Emitted by the macro, one per
/// command.
pub trait Decides<C: DomainMessage>: Domain {}

/// That `A` ingests fact `F`: `#[handles_fact(F)]` on its impl. What a
/// translating saga's callback is checked against: it must return a fact
/// its source ingests. Emitted by the macro, one per fact.
pub trait Ingests<F: DomainMessage>: Domain {}

// ---------------------------------------------------------------- refusals

/// The typed refusal objects `A`'s handler for command `C` may refuse
/// with: `#[handles_cmd(C, refuses(T1, T2, ..))]`, the markup's
/// `CommandOptions.refuses`. The macro emits it for each such command,
/// with [`Refusing::Refuses`] a choice of exactly those types, each
/// `From` its type (and the choice `Into` the domain's [`Rejection`]): a
/// handler returning `Err(T.into())` for a `T` not listed does not
/// compile.
pub trait Refusing<C: DomainMessage>: Domain {
    /// The choice of the listed refusal objects.
    type Refuses: Into<TypedRefusal>;
    /// The listed types, in order.
    const REFUSES: &'static [TypeName];
}

/// What a handler with `refuses(..)` returns as its `Err`:
/// `Result<Held, Refuses<Self, Hold>>`.
pub type Refuses<A, C> = <A as Refusing<C>>::Refuses;

/// A typed refusal object, packed: one of a command's `refuses` types (a
/// [`DomainMessage`] of the command's own domain, and a
/// [`Diagnostic`](annals_diagnostic::Diagnostic), whose message is the
/// refusal's), with its diagnostic.
///
/// **A proposal**: its constructor and readers are stubs.
#[derive(Clone, PartialEq, Debug)]
pub struct TypedRefusal {
    object: Any,
    report: Report,
}

impl TypedRefusal {
    /// `object`, packed, with its diagnostic.
    pub fn of<T: DomainMessage + annals_diagnostic::Diagnostic>(object: &T) -> TypedRefusal {
        let _ = object;
        todo!("annals::next::markup::TypedRefusal is a stub")
    }

    /// The object, as `T`, if it is one.
    pub fn object<T: DomainMessage>(&self) -> Option<T> {
        let _ = (&self.object, &self.report);
        todo!("annals::next::markup::TypedRefusal is a stub")
    }
}

/// A marked-up domain's `Domain::Rejection`: a handler's refusal, as the
/// caller (or a refused saga) receives it.
///
/// - [`Rejection::Message`]: a command handler without `refuses` refused
///   with the domain's own rejection `R` (inferred from those handlers, or
///   given by `rejection = ..`), a message only, as is.
/// - [`Rejection::Typed`]: a handler with `refuses` refused with one of
///   its listed objects.
///
/// Either way the refusing domain records nothing.
#[derive(Clone, PartialEq, Debug)]
pub enum Rejection<R> {
    Message(R),
    Typed(TypedRefusal),
}

/// A message-only refusal: a handler's or a step's `Err` of the domain's
/// own rejection.
impl<R> From<R> for Rejection<R> {
    fn from(rejection: R) -> Self {
        Rejection::Message(rejection)
    }
}

impl<R: annals_diagnostic::Diagnostic> Rejection<R> {
    /// As a refused saga receives it: the typed object packed (unset for a
    /// message), and the en-US message.
    ///
    /// **A proposal**: a stub.
    pub fn response(&self) -> v1::RefusalResponse {
        todo!("annals::next::markup::Rejection::response is a stub")
    }
}

impl<R: fmt::Display> fmt::Display for Rejection<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Rejection::Message(rejection) => rejection.fmt(f),
            Rejection::Typed(typed) => typed.report.fmt(f),
        }
    }
}

impl<R: annals_diagnostic::Diagnostic> annals_diagnostic::Diagnostic for Rejection<R> {
    fn code(&self) -> &'static str {
        match self {
            Rejection::Message(rejection) => rejection.code(),
            Rejection::Typed(typed) => typed.report.code(),
        }
    }

    fn args(&self) -> Vec<(&'static str, annals_diagnostic::Arg)> {
        match self {
            Rejection::Message(rejection) => rejection.args(),
            Rejection::Typed(typed) => typed.report.args(),
        }
    }
}

/// The pass-through refusal fact, as the saga's SOURCE domain's handler
/// for it reads it: `#[on_refused(C)]`, an ordinary, infallible fact
/// handler, `fn(&self, refused: CommandRefused<C>, ctx: &Context) ->
/// Record<Self>` (or a shape). The macro dispatches angzarr's
/// `CommandRefused` fact by the refused command's type to the handler for
/// it: which refusals a domain handles is inferred from its handlers, so a
/// `CommandRefused` for a command it has no handler for is caught at run
/// time (`annals-next-no-refusal-handler`, dead-lettered).
///
/// **A proposal**: its readers of the object are stubs.
#[derive(Clone, PartialEq, Debug)]
pub struct CommandRefused<C> {
    command: C,
    target: Cover,
    refusal: Option<Any>,
    trigger: EventRef,
}

impl<C> CommandRefused<C> {
    /// A refusal fact for a handler's own unit tests; the router builds the
    /// real one from angzarr's `CommandRefused`.
    pub fn new(command: C, target: Cover, refusal: Option<Any>, trigger: EventRef) -> Self {
        CommandRefused {
            command,
            target,
            refusal,
            trigger,
        }
    }

    /// The refused command, as sent.
    pub fn command(&self) -> &C {
        &self.command
    }

    /// The stream that refused it.
    pub fn target(&self) -> &Cover {
        &self.target
    }

    /// The typed refusal object, as `T`, if the target refused with one
    /// and it is a `T`.
    pub fn refusal<T: DomainMessage>(&self) -> Option<T> {
        let _ = &self.refusal;
        todo!("annals::next::markup::CommandRefused is a stub")
    }

    /// The saga's triggering event: a page of this stream.
    pub fn trigger(&self) -> &EventRef {
        &self.trigger
    }
}

/// What a TRANSLATING saga's callback reads (`#[on_refused(C)]` on a
/// `#[saga(.., translate(C))]`): angzarr's `Refusal { event, command,
/// response }`, the command decoded. The callback returns a fact of the
/// saga's source domain, so the source never refers to the target's
/// types.
///
/// **A proposal**: its readers of the object are stubs.
#[derive(Clone, PartialEq, Debug)]
pub struct Refusal<C> {
    event: EventBook,
    command: C,
    response: v1::RefusalResponse,
}

impl<C> Refusal<C> {
    /// A refusal for a callback's own unit tests; the saga's generated
    /// `on_refused` builds the real one.
    pub fn new(event: EventBook, command: C, response: v1::RefusalResponse) -> Self {
        Refusal {
            event,
            command,
            response,
        }
    }

    /// The triggering event: a book of the source stream holding the one
    /// event the saga reacted to.
    pub fn event(&self) -> &EventBook {
        &self.event
    }

    /// The refused command, as sent.
    pub fn command(&self) -> &C {
        &self.command
    }

    /// The typed refusal object, as `T`, if the target refused with one
    /// and it is a `T`.
    pub fn refusal<T: DomainMessage>(&self) -> Option<T> {
        todo!("annals::next::markup::Refusal is a stub")
    }

    /// The refusal's message.
    pub fn message(&self) -> &str {
        &self.response.message
    }
}

// ---------------------------------------------------------------- sagas

/// A saga written in the markup (`#[saga]`): stateless translation of its
/// ONE source domain's events into commands for its ONE target domain (an
/// aggregate's or a process manager's). Registered with
/// [`Registry::saga`](super::Registry::saga).
///
/// When a target refuses one of its commands, the target records nothing
/// and the saga delivers ONE unrefusable fact of the refusal to the exact
/// stream of its triggering event, in its source domain, whose id is the
/// refused command's id ([`Saga::on_refused`]). By default the generated
/// code passes it through as angzarr's `CommandRefused { command,
/// refusal, trigger }`, which the source handles in `#[on_refused(C)]`;
/// for the commands the saga's markup selects for TRANSLATE
/// (`#[saga(.., translate(C, ..))]`), its own `#[on_refused(C)]` callback
/// returns a domestic fact instead. The mode is chosen in the markup, at
/// compile time (angzarr-cli's refusal switch; annals' scaffold writes the
/// attribute): nothing about it is visible at run time beyond which fact
/// arrives.
///
/// **A proposal**: the macro emits it with empty constants and a stub
/// `on_refused`; the dispatch is the linked angzarr-router's (blocked).
pub trait Saga: Send + Sync + 'static {
    /// The saga's name: the component its commands' ids are derived from.
    const NAME: &'static str;
    /// The one domain whose events it reacts to, and to which it delivers
    /// its refusal facts.
    type Source: Domain;
    /// The one domain it sends commands to.
    type Target: Domain;
    /// The events it reacts to, in marker order.
    const REACTS: &'static [TypeName];
    /// The commands selected for translate, in order.
    const TRANSLATES: &'static [TypeName];

    /// angzarr's `SagaService.OnRefused`: the refusal fact's payload for
    /// the saga's source, packed. Pass-through: angzarr's `CommandRefused
    /// { command, refusal, trigger }`, `refusal` the response's object
    /// still packed, `trigger` the triggering event's `EventRef`. Translate:
    /// the callback's domestic fact. The chain addresses it to the
    /// trigger's stream and stamps its identity.
    fn on_refused(&self, refusal: &v1::Refusal) -> Any;
}

/// What a saga's `#[reacts(E)]` handler returns: commands for its target
/// `T`, each with the root of the stream it is for. For a process manager
/// that is the trigger's correlation ([`ReactionContext::correlation_id`]);
/// any other root is a saga bug. Each command's type must be one `T`
/// decides ([`Decides`]).
///
/// **A proposal**: its bodies are `todo!()`.
pub struct Commands<T: Domain> {
    orders: Vec<(Uuid, Any)>,
    target: std::marker::PhantomData<fn() -> T>,
}

impl<T: Domain> Commands<T> {
    /// No commands.
    pub fn none() -> Commands<T> {
        todo!("annals::next::markup::Commands is a stub")
    }

    /// These commands and `command`, for the stream of `T` at `root`.
    pub fn to<C>(self, root: Uuid, command: C) -> Commands<T>
    where
        C: DomainMessage,
        T: Decides<C>,
    {
        let _ = (self.orders, self.target, root, command);
        todo!("annals::next::markup::Commands is a stub")
    }
}

/// What a saga's handler reads besides the event: the triggering event's
/// `EventRef` (its stream and sequence), its correlation and when it was
/// recorded.
#[derive(Clone, PartialEq, Debug)]
pub struct ReactionContext {
    trigger: EventRef,
    correlation_id: Uuid,
    recorded_at: Timestamp,
}

impl ReactionContext {
    /// A context for a saga's own unit tests; the router builds the real
    /// one from the committed event.
    pub fn new(trigger: EventRef, correlation_id: Uuid, recorded_at: Timestamp) -> Self {
        ReactionContext {
            trigger,
            correlation_id,
            recorded_at,
        }
    }

    /// The event reacted to.
    pub fn trigger(&self) -> &EventRef {
        &self.trigger
    }

    /// The event's correlation: the root of a process manager's instance.
    pub fn correlation_id(&self) -> &Uuid {
        &self.correlation_id
    }

    /// When the event was recorded.
    pub fn recorded_at(&self) -> Timestamp {
        self.recorded_at
    }
}

/// angzarr's refusal messages (`io.angzarr.v1`'s `CommandRefused`,
/// `RefusalResponse` and `Refusal`, decision 14.31), wire-identical, field
/// for field. The angzarr-proto revision annals pins predates them; when
/// annals moves to one that has them, these are angzarr-proto's and this
/// module goes.
pub mod v1 {
    use angzarr_proto::v1::{CommandBook, EventBook, EventRef};
    use prost_types::Any;

    /// The standard refusal fact a pass-through saga delivers to its
    /// source.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct CommandRefused {
        /// The refused command, as delivered (identity stamped).
        #[prost(message, optional, tag = "1")]
        pub command: Option<CommandBook>,
        /// The target's typed refusal object, packed; unset if none.
        #[prost(message, optional, tag = "2")]
        pub refusal: Option<Any>,
        /// The saga's triggering event: the stream the fact is for.
        #[prost(message, optional, tag = "3")]
        pub trigger: Option<EventRef>,
    }

    impl prost::Name for CommandRefused {
        const NAME: &'static str = "CommandRefused";
        const PACKAGE: &'static str = "io.angzarr.v1";
    }

    /// A target's refusal, as the refused saga receives it.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct RefusalResponse {
        /// The typed refusal object, packed; unset if none.
        #[prost(message, optional, tag = "1")]
        pub refusal: Option<Any>,
        /// The refusal's message.
        #[prost(string, tag = "2")]
        pub message: String,
    }

    impl prost::Name for RefusalResponse {
        const NAME: &'static str = "RefusalResponse";
        const PACKAGE: &'static str = "io.angzarr.v1";
    }

    /// `SagaService.OnRefused`'s input.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Refusal {
        /// The triggering event's book.
        #[prost(message, optional, tag = "1")]
        pub event: Option<EventBook>,
        /// The refused command, as delivered.
        #[prost(message, optional, tag = "2")]
        pub command: Option<CommandBook>,
        /// The target's refusal.
        #[prost(message, optional, tag = "3")]
        pub response: Option<RefusalResponse>,
    }

    impl prost::Name for Refusal {
        const NAME: &'static str = "Refusal";
        const PACKAGE: &'static str = "io.angzarr.v1";
    }
}

/// A projector written in the markup (`#[projector]`): one source domain,
/// one instance per process, run after commit and awaited before the call
/// returns. A failure is the caller's error,
/// [`Failure::Projection`](super::Failure::Projection), carrying what was
/// committed: the events stay committed. Projectors run on the async
/// command path, the only one (async everywhere), so there is no guard
/// against blocking on a synchronous one. Registered with
/// [`Registry::projector`](super::Registry::projector).
///
/// **A proposal**: the macro emits it with the name and source; the
/// dispatch is the linked angzarr-router's (blocked).
pub trait AsyncProjector: Send + 'static {
    /// Named in the error a failure raises.
    const NAME: &'static str;
    /// The one domain whose events it projects.
    type Source: Domain;
}

/// What a projector's handler reads besides the event: the stream it is
/// from, its sequence there and when it was recorded.
///
/// **A proposal**: a stub.
#[derive(Clone, PartialEq, Debug)]
pub struct ProjectionContext {
    stream: Cover,
    sequence: u64,
    recorded_at: Timestamp,
}

impl ProjectionContext {
    /// The source stream.
    pub fn stream(&self) -> &Cover {
        &self.stream
    }

    /// The event's sequence in its stream.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// When the event was recorded.
    pub fn recorded_at(&self) -> Timestamp {
        self.recorded_at
    }
}

/// The most continuation steps one decision may run, after its handler.
/// A chain that would run more is a bug (a step that always continues),
/// refused as [`KernelError::TooManySteps`](super::KernelError::TooManySteps)
/// with nothing committed.
pub const STEP_LIMIT: usize = 16;

/// The test helper for a continuation step: runs `step` on `instance`,
/// applies what it emits (following its continuations, as the router
/// does, up to [`STEP_LIMIT`]), then runs `step` again on the result and
/// panics, saying the step is not idempotent and naming what it emitted,
/// unless it emits nothing and continues nowhere. Returns the
/// instance after the first run. A step must be idempotent on its own
/// result, or a chain through it never settles.
pub fn assert_step_idempotent<A, S, O, E>(instance: A, step: S, ctx: &Context) -> A
where
    A: Domain<State = A, Rules = ()>,
    A::Event: fmt::Debug,
    S: Fn(&A, &Context) -> Result<O, E>,
    O: Emits<A>,
    E: Into<A::Rejection> + fmt::Debug,
{
    let _ = (instance, step, ctx);
    todo!("annals::next::markup::assert_step_idempotent is a stub")
}

/// A message's protobuf package and name, as constants: what `HANDLES` and
/// `APPLIES` hold, one per marked type, read off its wire message's
/// [`prost::Name`].
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct TypeName {
    pub package: &'static str,
    pub name: &'static str,
}

impl TypeName {
    /// `M`'s package and name.
    pub const fn of<M: prost::Name>() -> TypeName {
        TypeName {
            package: M::PACKAGE,
            name: M::NAME,
        }
    }

    /// `package.Name`: what type URLs are compared by
    /// ([`type_name`](super::type_name)).
    pub fn full_name(&self) -> String {
        match self.package.is_empty() {
            true => self.name.to_string(),
            false => format!("{}.{}", self.package, self.name),
        }
    }

    /// `/package.Name`, as angzarr writes type URLs.
    pub fn type_url(&self) -> String {
        format!("/{}", self.full_name())
    }
}

/// The snapshot form of a marked-up aggregate with no `snapshot = ..`: its
/// [`State`](super::State) impl says
/// [`SNAPSHOTS`](super::State::SNAPSHOTS)` = false`, so the kernel never
/// takes one, and this message is never written.
#[derive(Clone, PartialEq, prost::Message)]
pub struct NoSnapshot {}

impl prost::Name for NoSnapshot {
    const NAME: &'static str = "NoSnapshot";
    const PACKAGE: &'static str = "annals.next.markup";
}
