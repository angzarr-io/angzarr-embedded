//! What angzarr-embedded-macros' expansion names: the runtime under a marked-up
//! aggregate or process manager (`#[aggregate]`, `#[process_manager]`,
//! `#[handles_cmd]`, `#[handles_fact]`, `#[on_refused]`, `#[applies]`),
//! policy (`#[policy]`, `#[on_event]`) and projector (`#[projector]`,
//! `#[projects]`); see angzarr-embedded-macros' crate docs. A hand-written
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
//!   policy's callback reads it.
//! - [`ProcessManager`]: what sets a process manager, a domain, apart: its
//!   root is the correlation, and its starting inputs.
//! - [`Policy`], [`Commands`] and [`PolicyContext`]: a policy, its output
//!   and its handler's context. [`Decides`] and [`Ingests`] say what a
//!   domain takes, for the checks on a policy.
//! - [`AsyncProjector`] and [`ProjectionContext`]: a projector.
//! - [`MARKUP_RULES_VERSION`]: the rules version every marked-up
//!   domain's events are recorded under.
//! - [`STEP_LIMIT`] and [`assert_step_idempotent`]: the cap on a decision's
//!   continuation steps, and the test helper for a step.
//! - [`TypeName`]: a message's package and name, as the generated
//!   constants list them.
//! - [`NoSnapshot`]: the snapshot form of a domain that has none.
//! - [`v1`]: angzarr's refusal messages, until angzarr-embedded's angzarr-proto has
//!   them.
//!
//! Every component here assumes nothing about distribution: the same
//! marked-up aggregate, process manager, policy or projector runs in
//! angzarr-embedded's process or as its own service, with no change to its code.
//!
//! angzarr-embedded's `next/decorated.test.rs` runs all of it.

use std::fmt;

use angzarr_proto::v1::{Cover, EventBook, EventRef, FactOrigin, Uuid};
use prost_types::{Any, Timestamp};

use super::domain::Domain;
use super::error::PayloadError;
use super::payload::Payload as _;
use super::wire::WireError;

/// The error a generated `State::from_snapshot` returns, re-exported so
/// the expansion needs no dependency of its user's.
pub use angzarr_embedded_diagnostic::Report;

/// What a handler, or a continuation step, reads besides the instance and
/// its input, all read-only: the ONE handler context of every domain, an
/// aggregate's and a process manager's alike. The instant its events will
/// be recorded at (sampled by the application and injected, so the handler
/// stays pure: there is no wall clock), the stream it decides (for a
/// process manager, its root is the correlation), and the input's
/// identity: its command id (a client's, the one the application minted,
/// a policy's derived one, or, for a fact, the one derived from its origin;
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
        events.into_emit()
    }

    /// Run `step` on the instance once these events are applied. The step
    /// may itself return an `Emit` with a continuation.
    pub fn then<S, O, E>(self, step: S) -> Emit<A>
    where
        S: FnOnce(&A, &Context) -> Result<O, E> + 'static,
        O: Emits<A>,
        E: Into<A::Rejection>,
    {
        self.push(Box::new(move |instance, ctx| {
            step(instance, ctx)
                .map(Emits::into_emit)
                .map_err(Into::into)
        }))
    }

    /// `step` after this emit's continuation, if it has one, else as it.
    fn push(self, step: Step<A>) -> Emit<A> {
        let next: Step<A> = match self.next {
            None => step,
            Some(first) => {
                Box::new(move |instance, ctx| first(instance, ctx).map(|emit| emit.push(step)))
            }
        };
        Emit {
            events: self.events,
            next: Some(next),
        }
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
        self
    }
}

/// These events, with no continuation.
fn emitted<A: Domain>(events: Vec<A::Event>) -> Emit<A> {
    Emit { events, next: None }
}

impl<A: Domain, T: DomainMessage + Into<A::Event>> Emits<A> for T {
    fn into_emit(self) -> Emit<A> {
        emitted(vec![self.into()])
    }
}

impl<A, T1, T2> Emits<A> for (T1, T2)
where
    A: Domain,
    T1: DomainMessage + Into<A::Event>,
    T2: DomainMessage + Into<A::Event>,
{
    fn into_emit(self) -> Emit<A> {
        emitted(vec![self.0.into(), self.1.into()])
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
        emitted(vec![self.0.into(), self.1.into(), self.2.into()])
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
        emitted(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
        ])
    }
}

impl<A: Domain, T: DomainMessage + Into<A::Event>> Emits<A> for Option<T> {
    fn into_emit(self) -> Emit<A> {
        emitted(self.into_iter().map(Into::into).collect())
    }
}

impl<A: Domain, T: DomainMessage + Into<A::Event>> Emits<A> for Vec<T> {
    fn into_emit(self) -> Emit<A> {
        emitted(self.into_iter().map(Into::into).collect())
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
/// A fact handler's continuation steps are infallible: a separate type,
/// not `Emit`, so a fallible step in a fact's chain does not compile.
pub struct Record<A: Domain> {
    events: Vec<A::Event>,
    next: Option<RecordStep<A>>,
}

/// A fact's continuation step: infallible.
type RecordStep<A> = Box<dyn FnOnce(&A, &Context) -> Record<A>>;

impl<A: Domain> Record<A> {
    /// These events, and no continuation.
    pub fn of(events: impl Records<A>) -> Record<A> {
        events.into_record()
    }

    /// Run `step` on the instance once these events are applied. The step
    /// may itself return a `Record` with a continuation.
    pub fn then<S, O>(self, step: S) -> Record<A>
    where
        S: FnOnce(&A, &Context) -> O + 'static,
        O: Records<A>,
    {
        self.push(Box::new(move |instance, ctx| {
            step(instance, ctx).into_record()
        }))
    }

    /// `step` after this record's continuation, if it has one, else as it.
    fn push(self, step: RecordStep<A>) -> Record<A> {
        let next: RecordStep<A> = match self.next {
            None => step,
            Some(first) => Box::new(move |instance, ctx| first(instance, ctx).push(step)),
        };
        Record {
            events: self.events,
            next: Some(next),
        }
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
                let Emit { events, .. } = Emits::<A>::into_emit(self);
                Record { events, next: None }
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
/// rules version.
pub const MARKUP_RULES_VERSION: &str = "markup-1";

/// A process manager written in the markup (`#[process_manager]`). A
/// process manager IS A DOMAIN: its inputs are commands (which policies send
/// it, and which it may refuse) and facts (timeouts, webhooks, refusal
/// facts, which it never refuses); its only output is its own events. It
/// subscribes to nothing: the policy for (another domain, the PM's domain)
/// turns that domain's events into commands to it. The macro gives it the
/// same `Domain` and `State` impls an `#[aggregate]` gets; this trait adds
/// the two things that set a PM apart. Registered with
/// [`Registry::process_manager`](super::Registry::process_manager).
///
/// - **Its root is the correlation**, unchanged. A command or fact
///   addressed to it names the instance in its cover: a `correlation_id`
///   left unset is given the root, and one set to anything else is
///   malformed (`angzarr-embedded-next-malformed-root-not-correlation`).
/// - **The start rule.** Only an input type it declares as starting
///   (`#[handles_cmd(C, starts)]`, `#[handles_fact(F, starts)]`) may start
///   an instance. Any other input for a correlation with no instance is
///   not delivered, decided before the handler by the type after
///   upcasting: a FACT is skipped silently (applied, no events, nothing
///   dead-lettered), and a COMMAND is refused (`angzarr-embedded-next-pm-no-instance`,
///   a business refusal, so a policy's refused command yields its refusal
///   fact like any other), recording nothing.
///
/// A process manager whose `STARTS` is empty can never start an instance;
/// angzarr-embedded does not refuse one (no compile error, no check): the warning is
/// angzarr-cli's.
///
/// angzarr-embedded applies both rules: the first in [`check`](super::check), the
/// second in the generated `Domain` impl, before the handler.
pub trait ProcessManager: Domain<State = Self, Rules = ()> {
    /// The input types (commands and facts) that may start an instance,
    /// in marker order: angzarr's `ComponentDescriptor.starting_types`.
    const STARTS: &'static [TypeName];
}

/// That `A` decides command `C`: `#[handles_cmd(C)]` on its impl. What a
/// policy's [`Commands`] is checked against. Emitted by the macro, one per
/// command.
pub trait Decides<C: DomainMessage>: Domain {}

/// That `A` ingests fact `F`: `#[handles_fact(F)]` on its impl. What a
/// translating policy's callback is checked against: it must return a fact
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
/// [`Diagnostic`](angzarr_embedded_diagnostic::Diagnostic), whose message is the
/// refusal's), with its diagnostic.
#[derive(Clone, PartialEq, Debug)]
pub struct TypedRefusal {
    object: Any,
    report: Report,
}

impl TypedRefusal {
    /// `object`, packed, with its diagnostic.
    pub fn of<T: DomainMessage + angzarr_embedded_diagnostic::Diagnostic>(
        object: &T,
    ) -> TypedRefusal {
        TypedRefusal {
            object: __pack(object),
            report: Report::of(object),
        }
    }

    /// The object, as `T`, if it is one.
    pub fn object<T: DomainMessage>(&self) -> Option<T> {
        __decode(&self.object).ok().flatten()
    }
}

/// A marked-up domain's `Domain::Rejection`: a handler's refusal, as the
/// caller (or a refused policy) receives it.
///
/// - [`Rejection::Message`]: a command handler without `refuses` refused
///   with the domain's own rejection `R` (inferred from those handlers, or
///   given by `rejection = ..`), a message only, as is.
/// - [`Rejection::Typed`]: a handler with `refuses` refused with one of
///   its listed objects.
///
/// - [`Rejection::NoInstance`]: a command for a process manager's
///   correlation with no instance, of a type that does not start one.
///
/// Either way the refusing domain records nothing.
#[derive(Clone, PartialEq, Debug)]
pub enum Rejection<R> {
    Message(R),
    Typed(TypedRefusal),
    NoInstance { stream: Cover },
}

/// A message-only refusal: a handler's or a step's `Err` of the domain's
/// own rejection.
impl<R> From<R> for Rejection<R> {
    fn from(rejection: R) -> Self {
        Rejection::Message(rejection)
    }
}

impl<R: angzarr_embedded_diagnostic::Diagnostic> Rejection<R> {
    /// As a refused policy receives it: the typed object packed (unset for a
    /// message), and the en-US message.
    pub fn response(&self) -> v1::RefusalResponse {
        v1::RefusalResponse {
            refusal: self.refusal().cloned(),
            message: self.to_string(),
        }
    }
}

impl<R> Rejection<R> {
    /// The typed refusal object, packed: `None` unless [`Rejection::Typed`].
    pub fn refusal(&self) -> Option<&Any> {
        match self {
            Rejection::Typed(typed) => Some(&typed.object),
            Rejection::Message(_) | Rejection::NoInstance { .. } => None,
        }
    }
}

impl<R: fmt::Display> fmt::Display for Rejection<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Rejection::Message(rejection) => rejection.fmt(f),
            Rejection::Typed(typed) => typed.report.fmt(f),
            Rejection::NoInstance { stream } => write!(
                f,
                "process manager stream {} has no instance, and only a starting input starts one",
                super::error::cover_text(stream)
            ),
        }
    }
}

impl<R: angzarr_embedded_diagnostic::Diagnostic> angzarr_embedded_diagnostic::Diagnostic
    for Rejection<R>
{
    fn code(&self) -> &'static str {
        match self {
            Rejection::Message(rejection) => rejection.code(),
            Rejection::Typed(typed) => typed.report.code(),
            Rejection::NoInstance { .. } => "angzarr-embedded-next-pm-no-instance",
        }
    }

    fn args(&self) -> Vec<(&'static str, angzarr_embedded_diagnostic::Arg)> {
        match self {
            Rejection::Message(rejection) => rejection.args(),
            Rejection::Typed(typed) => typed.report.args(),
            Rejection::NoInstance { stream } => {
                vec![("stream", super::error::cover_text(stream).into())]
            }
        }
    }
}

/// The pass-through refusal fact, as the policy's SOURCE domain's handler
/// for it reads it: `#[on_refused(C)]`, an ordinary, infallible fact
/// handler, `fn(&self, refused: CommandRefused<C>, ctx: &Context) ->
/// Record<Self>` (or a shape). The macro dispatches angzarr's
/// `CommandRefused` fact by the refused command's type to the handler for
/// it: which refusals a domain handles is inferred from its handlers, so a
/// `CommandRefused` for a command it has no handler for is caught at run
/// time (`angzarr-embedded-next-no-refusal-handler`, dead-lettered).
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
        __decode(self.refusal.as_ref()?).ok().flatten()
    }

    /// The policy's triggering event: a page of this stream.
    pub fn trigger(&self) -> &EventRef {
        &self.trigger
    }
}

/// What a policy's translating callback reads (`#[on_refused(C)]` on the
/// policy's impl): angzarr's `Refusal { event, command,
/// response }`, the command decoded. The callback returns a fact of the
/// policy's source domain, so the source never refers to the target's
/// types.
#[derive(Clone, PartialEq, Debug)]
pub struct Refusal<C> {
    event: EventBook,
    command: C,
    response: v1::RefusalResponse,
}

impl<C> Refusal<C> {
    /// A refusal for a callback's own unit tests; the policy's generated
    /// `on_refused` builds the real one.
    pub fn new(event: EventBook, command: C, response: v1::RefusalResponse) -> Self {
        Refusal {
            event,
            command,
            response,
        }
    }

    /// The triggering event: a book of the source stream holding the one
    /// event the policy reacted to.
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
        __decode(self.response.refusal.as_ref()?).ok().flatten()
    }

    /// The refusal's message.
    pub fn message(&self) -> &str {
        &self.response.message
    }
}

// ---------------------------------------------------------------- policies

/// A policy: the kernel's one [`Policy`](super::Policy) trait, which
/// `#[policy]` implements (answering refusals) and a hand-written policy
/// implements as it is.
pub use super::policy::Policy;

/// What a policy's `#[on_event(E)]` handler returns: commands for its target
/// `T`, each with the root of the stream it is for. For a process manager
/// that is the trigger's correlation ([`PolicyContext::correlation_id`]);
/// any other root is a policy bug. Each command's type must be one
/// `T` decides ([`Decides`]).
pub struct Commands<T: Domain> {
    orders: Vec<(Uuid, Any)>,
    target: std::marker::PhantomData<fn() -> T>,
}

impl<T: Domain> Commands<T> {
    /// No commands.
    pub fn none() -> Commands<T> {
        Commands {
            orders: Vec::new(),
            target: std::marker::PhantomData,
        }
    }

    /// These commands and `command`, for the stream of `T` at `root`.
    pub fn to<C>(self, root: Uuid, command: C) -> Commands<T>
    where
        C: DomainMessage,
        T: Decides<C>,
    {
        let mut orders = self.orders;
        orders.push((root, __pack(&command)));
        Commands {
            orders,
            target: self.target,
        }
    }

    /// Each command, packed, with the root of its stream, in order.
    pub fn orders(&self) -> &[(Uuid, Any)] {
        &self.orders
    }

    /// These commands and `command`, one of `T`'s, for the stream of `T` at
    /// `root`: a hand-written policy's way to send one.
    pub fn send(self, root: Uuid, command: &T::Command) -> Commands<T> {
        let mut orders = self.orders;
        orders.push((root, command.to_any()));
        Commands {
            orders,
            target: self.target,
        }
    }

    /// As [`Commands::orders`], owned.
    pub(crate) fn into_orders(self) -> Vec<(Uuid, Any)> {
        self.orders
    }
}

/// What a policy's handler reads besides the event: the triggering event's
/// `EventRef` (its stream and sequence), its correlation and when it was
/// recorded.
#[derive(Clone, PartialEq, Debug)]
pub struct PolicyContext {
    trigger: EventRef,
    correlation_id: Uuid,
    recorded_at: Timestamp,
}

impl PolicyContext {
    /// A context for a policy's own unit tests; the router builds the real
    /// one from the committed event.
    pub fn new(trigger: EventRef, correlation_id: Uuid, recorded_at: Timestamp) -> Self {
        PolicyContext {
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
/// for field. The angzarr-proto revision angzarr-embedded pins predates them; when
/// angzarr-embedded moves to one that has them, these are angzarr-proto's and this
/// module goes.
pub mod v1 {
    use angzarr_proto::v1::{CommandBook, EventBook, EventRef};
    use prost_types::Any;

    /// The standard refusal fact a pass-through policy delivers to its
    /// source.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct CommandRefused {
        /// The refused command, as delivered (identity stamped).
        #[prost(message, optional, tag = "1")]
        pub command: Option<CommandBook>,
        /// The target's typed refusal object, packed; unset if none.
        #[prost(message, optional, tag = "2")]
        pub refusal: Option<Any>,
        /// The policy's triggering event: the stream the fact is for.
        #[prost(message, optional, tag = "3")]
        pub trigger: Option<EventRef>,
    }

    impl prost::Name for CommandRefused {
        const NAME: &'static str = "CommandRefused";
        const PACKAGE: &'static str = "io.angzarr.v1";
    }

    /// A target's refusal, as the refused policy receives it.
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

    /// `PolicyService.OnRefused`'s input.
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
/// [`Registry::projector`](super::Registry::projector). Several
/// projectors on one domain each receive every committed event of it they
/// mark; one's failure does not stop the others.
pub trait AsyncProjector: Send + 'static {
    /// Named in the error a failure raises.
    const NAME: &'static str;
    /// The one domain whose events it projects.
    type Source: Domain;
    /// The projected events, in marker order.
    const PROJECTS: &'static [TypeName];

    /// Project `event` (an event of its source, packed), from its
    /// `#[projects]` handler for the event's type; `None` for a type it
    /// does not mark. Generated.
    fn project<'a>(
        &'a mut self,
        event: &'a Any,
        ctx: &'a ProjectionContext,
    ) -> Option<Projecting<'a>>;
}

/// A projector handler's run: awaited by the kernel after the commit.
pub type Projecting<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Report>> + Send + 'a>>;

/// What a projector's handler reads besides the event: the stream it is
/// from, its sequence there and when it was recorded.
#[derive(Clone, PartialEq, Debug)]
pub struct ProjectionContext {
    stream: Cover,
    sequence: u64,
    recorded_at: Timestamp,
}

impl ProjectionContext {
    /// A context for a projector's own unit tests; the kernel builds the
    /// real one from the committed event.
    pub fn new(stream: Cover, sequence: u64, recorded_at: Timestamp) -> Self {
        ProjectionContext {
            stream,
            sequence,
            recorded_at,
        }
    }

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
    let mut instance = instance;
    let first = match step(&instance, ctx) {
        Ok(first) => first.into_emit(),
        Err(rejection) => panic!("the step refused: {rejection:?}"),
    };
    let mut emit = first;
    for _ in 0..=STEP_LIMIT {
        for event in std::mem::take(&mut emit.events) {
            A::apply(&mut instance, &event, &());
        }
        let Some(next) = emit.next.take() else {
            break;
        };
        emit = match next(&instance, ctx) {
            Ok(emit) => emit,
            Err(rejection) => panic!("the step's continuation refused: {rejection}"),
        };
    }
    let again = match step(&instance, ctx) {
        Ok(again) => again.into_emit(),
        Err(rejection) => panic!("the step refused on its own result: {rejection:?}"),
    };
    if !again.events.is_empty() || again.next.is_some() {
        panic!(
            "the step is not idempotent: re-run on its own result, it emitted {:?}{}",
            again.events,
            if again.next.is_some() {
                " and continued"
            } else {
                ""
            }
        );
    }
    instance
}

// ---------------------------------------------------------------- the glue
//
// What angzarr-embedded-macros' expansion calls: public so the expansion can name
// them, hidden because nothing else should.

/// `value` packed as its wire message.
#[doc(hidden)]
pub fn __pack<T: DomainMessage>(value: &T) -> Any {
    super::payload::pack(&value.to_wire())
}

/// `T`'s type URL.
#[doc(hidden)]
pub fn __type_url<T: DomainMessage>() -> String {
    format!("/{}", <T::Wire as prost::Name>::full_name())
}

/// `any` as a `T`, if it is one: `None` for another type; a payload of
/// `T`'s type that does not decode, or that is not a valid `T`, is the
/// error.
#[doc(hidden)]
pub fn __decode<T: DomainMessage>(any: &Any) -> Result<Option<T>, PayloadError> {
    if super::payload::type_name(&any.type_url) != <T::Wire as prost::Name>::full_name() {
        return Ok(None);
    }
    let wire = <T::Wire as prost::Message>::decode(&any.value[..]).map_err(|error| {
        PayloadError::Decode {
            type_url: any.type_url.clone(),
            error,
        }
    })?;
    T::from_wire(wire)
        .map(Some)
        .map_err(|error| PayloadError::Invalid {
            type_url: any.type_url.clone(),
            reason: Report::of(&error),
        })
}

/// The full name of angzarr's `CommandRefused`.
fn command_refused() -> String {
    <v1::CommandRefused as prost::Name>::full_name()
}

/// `CommandRefused`'s type URL: a payload set with `#[on_refused]`
/// handlers decodes it.
#[doc(hidden)]
pub fn __command_refused_url() -> String {
    format!("/{}", command_refused())
}

/// `any`, angzarr's `CommandRefused`, as the refusal of a `C`, if it is
/// one: `None` for another payload type, or a refusal of another command.
#[doc(hidden)]
pub fn __refused<C: DomainMessage>(any: &Any) -> Result<Option<CommandRefused<C>>, PayloadError> {
    if super::payload::type_name(&any.type_url) != command_refused() {
        return Ok(None);
    }
    let wire = <v1::CommandRefused as prost::Message>::decode(&any.value[..]).map_err(|error| {
        PayloadError::Decode {
            type_url: any.type_url.clone(),
            error,
        }
    })?;
    let book = wire.command.unwrap_or_default();
    let Some(command) = command_payload(&book) else {
        return Ok(None);
    };
    let Some(command) = __decode::<C>(command)? else {
        return Ok(None);
    };
    Ok(Some(CommandRefused {
        command,
        target: book.cover.unwrap_or_default(),
        refusal: wire.refusal,
        trigger: wire.trigger.unwrap_or_default(),
    }))
}

/// The refused command's type URL in `any`, angzarr's `CommandRefused`:
/// what a payload set names when it has no handler for that refusal.
#[doc(hidden)]
pub fn __refused_command_url(any: &Any) -> Option<String> {
    if super::payload::type_name(&any.type_url) != command_refused() {
        return None;
    }
    let wire = <v1::CommandRefused as prost::Message>::decode(&any.value[..]).ok()?;
    command_payload(wire.command.as_ref()?).map(|payload| payload.type_url.clone())
}

/// A command book's payload, if it holds a command.
fn command_payload(book: &angzarr_proto::v1::CommandBook) -> Option<&Any> {
    match book.page.as_ref()?.body.as_ref()? {
        angzarr_proto::v1::command_page::Body::Command(command) => command.payload.as_ref(),
        angzarr_proto::v1::command_page::Body::Fact(_) => None,
    }
}

/// The context a command handler reads, from its `Tx`.
#[doc(hidden)]
pub fn __context<A: Domain>(tx: &super::domain::Tx<'_, A>) -> Context {
    Context::new(
        tx.recorded_at,
        tx.stream.clone(),
        tx.command_id.clone(),
        tx.correlation_id.clone(),
    )
}

/// The context a fact handler reads, from its `IngestCtx`.
#[doc(hidden)]
pub fn __fact_context<A: Domain>(ctx: &super::domain::IngestCtx<'_, A>) -> Context {
    Context::new(
        ctx.recorded_at,
        ctx.stream.clone(),
        ctx.command_id.clone(),
        ctx.correlation_id.clone(),
    )
    .with_fact_origin(ctx.source.clone())
}

/// Whether the stream a command is decided on holds an instance: any
/// event at all.
#[doc(hidden)]
pub fn __decided_instance<A: Domain>(tx: &super::domain::Tx<'_, A>) -> bool {
    tx.head > 0
}

/// Whether the stream a fact is ingested into holds an instance.
#[doc(hidden)]
pub fn __ingested_instance<A: Domain>(ctx: &super::domain::IngestCtx<'_, A>) -> bool {
    ctx.head > 0
}

/// The refusal of a command that does not start a process manager, for a
/// correlation with no instance.
#[doc(hidden)]
pub fn __no_instance<A: Domain, R>(tx: &super::domain::Tx<'_, A>) -> Rejection<R> {
    Rejection::NoInstance {
        stream: tx.stream.clone(),
    }
}

/// A command handler's result, run as one decision: its events emitted
/// through `tx` (folded into the staged instance), then each continuation
/// step on the staged instance, until there is none. A rejection anywhere
/// refuses it all; more than [`STEP_LIMIT`] steps is the kernel's
/// [`KernelError::TooManySteps`](super::KernelError::TooManySteps).
#[doc(hidden)]
pub fn __decide<A, O, E>(
    tx: &mut super::domain::Tx<'_, A>,
    ctx: &Context,
    handled: Result<O, E>,
) -> Result<(), A::Rejection>
where
    A: Domain<State = A>,
    O: Emits<A>,
    E: Into<A::Rejection>,
{
    let mut emit = handled.map_err(Into::into)?.into_emit();
    let mut steps = 0;
    loop {
        for event in std::mem::take(&mut emit.events) {
            tx.emit(event);
        }
        let Some(next) = emit.next.take() else {
            return Ok(());
        };
        steps += 1;
        if steps > STEP_LIMIT {
            tx.fault = Some(super::error::KernelError::TooManySteps {
                stream: tx.stream.clone(),
                limit: STEP_LIMIT,
            });
            return Ok(());
        }
        emit = next(tx.state(), ctx)?;
    }
}

/// A fact handler's result, run as one decision on a working copy of
/// `state`: its events, then each continuation step's on the copy with
/// them applied. Infallible; more than [`STEP_LIMIT`] steps is a bug,
/// which panics (the kernel's `IngestPanicked`, dead-lettered).
#[doc(hidden)]
pub fn __ingest<A, R>(state: &A, ctx: &Context, recorded: R) -> Vec<A::Event>
where
    A: Domain<State = A, Rules = ()> + Clone,
    R: Records<A>,
{
    let mut working = state.clone();
    let mut events = Vec::new();
    let mut record = recorded.into_record();
    let mut steps = 0;
    loop {
        for event in std::mem::take(&mut record.events) {
            A::apply(&mut working, &event, &());
            events.push(event);
        }
        let Some(next) = record.next.take() else {
            return events;
        };
        steps += 1;
        assert!(
            steps <= STEP_LIMIT,
            "the fact on stream {} ran more than its limit of {STEP_LIMIT} continuation steps",
            super::error::cover_text(ctx.stream())
        );
        record = next(&working, ctx);
    }
}

/// angzarr's pass-through refusal fact: `CommandRefused { command,
/// refusal, trigger }`, the refusal object still packed and the trigger
/// the triggering event's `EventRef`, packed.
#[doc(hidden)]
pub fn __pass_through(refusal: &v1::Refusal) -> Any {
    let trigger = refusal.event.as_ref().map(|book| EventRef {
        stream: book.cover.clone(),
        sequence: book.pages.first().map_or(0, |page| page.sequence),
    });
    super::payload::pack(&v1::CommandRefused {
        command: refusal.command.clone(),
        refusal: refusal.response.as_ref().and_then(|r| r.refusal.clone()),
        trigger,
    })
}

/// `refusal` as a translating callback for command `C` reads it, if the
/// refused command is a `C`.
#[doc(hidden)]
pub fn __translating<C: DomainMessage>(refusal: &v1::Refusal) -> Option<Refusal<C>> {
    let command = __decode::<C>(command_payload(refusal.command.as_ref()?)?).ok()??;
    Some(Refusal {
        event: refusal.event.clone().unwrap_or_default(),
        command,
        response: refusal.response.clone().unwrap_or_default(),
    })
}

/// A translating callback's fact, packed: one its source `S` ingests.
#[doc(hidden)]
pub fn __domestic<S: Ingests<F>, F: DomainMessage>(fact: &F) -> Any {
    __pack(fact)
}

/// A projector handler's result, as the kernel awaits it.
#[doc(hidden)]
pub fn __projecting<'a, F>(run: F) -> Projecting<'a>
where
    F: std::future::Future<Output = Result<(), Report>> + Send + 'a,
{
    Box::pin(run)
}

/// The state descriptor of a marked-up domain whose snapshot form is `S`:
/// its type URL, with STRICT and AGGREGATE_HANDLES.
#[doc(hidden)]
pub fn __descriptor<S: prost::Name>() -> angzarr_proto::v1::StateDescriptor {
    angzarr_proto::v1::StateDescriptor {
        type_url: format!("/{}", S::full_name()),
        options: Some(angzarr_proto::v1::StateOptions {
            conflict_resolutions: vec![
                angzarr_proto::v1::ConflictResolution::Strict as i32,
                angzarr_proto::v1::ConflictResolution::AggregateHandles as i32,
            ],
        }),
    }
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
    const PACKAGE: &'static str = "angzarr_embedded.next.markup";
}
