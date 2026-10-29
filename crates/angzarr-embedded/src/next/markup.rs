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
//! - [`Failed`] and [`BusinessStatus`]: what a command handler fails
//!   with, a business refusal (under a status of the business set) or a
//!   bug.
//! - [`CommandRefused`] and [`Refusal`]: a refusal fact as its source's
//!   `#[on_refused]` handler reads it, and a refusal as a translating
//!   policy's callback reads it.
//! - [`ProcessManager`]: what sets a process manager, a domain, apart: its
//!   starting inputs.
//! - [`Routed`]: a marked-up domain's handlers, registered on
//!   angzarr-router's table.
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

use angzarr_proto::v1::{Cover, EventBook, EventRef, Uuid};
use prost_types::{Any, Timestamp};

use super::domain::Domain;
use super::error::PayloadError;
use super::payload::Payload as _;
use super::wire::WireError;

/// The error a generated `State::from_snapshot` returns, re-exported so
/// the expansion needs no dependency of its user's.
pub use angzarr_embedded_diagnostic::Report;

/// angzarr-router's context of the input a handler decides, and its
/// conflict under AGGREGATE_HANDLES: a [`Context`]'s `command`.
pub use angzarr_router::domain::{CommandContext, Conflict};

/// What a handler, or a continuation step, reads besides the instance and
/// its input, all read-only: the ONE handler context of every domain, an
/// aggregate's and a process manager's alike.
///
/// - [`Context::command`]: angzarr-router's whole
///   [`CommandContext`](angzarr_router::domain::CommandContext), as the
///   router hands it to the handler: `cover` (the stream's domain and
///   root), `next_sequence`, `had_prior_events`, `expected_sequence`,
///   `conflict_resolution`, `conflict` (under AGGREGATE_HANDLES, the window
///   the command did not see and the nonce the handler hands back through
///   `resolve_conflicts`), and the input's identity: `command_id` (a
///   client's, the one the application minted, a policy's derived one, or,
///   for a fact, the one derived from its origin; a refusal fact's is the
///   refused command's), `correlation_id` and, for a fact, `fact_origin`.
///   Nothing the router knows of the input is withheld, and nothing it
///   knows is kept twice.
/// - [`Context::recorded_at`]: what the router does not carry, the instant
///   the events will be recorded at (sampled by the application and
///   injected, so the handler stays pure: there is no wall clock).
#[derive(Clone, Debug)]
pub struct Context {
    recorded_at: Timestamp,
    /// angzarr-router's context of the input this handler decides.
    pub command: CommandContext,
}

impl Context {
    /// A context for a handler's own unit tests, recorded at `recorded_at`
    /// with the router's default context ([`Context::with_command`] sets
    /// it); the kernel builds the real one from what angzarr-router hands
    /// the handler.
    pub fn new(recorded_at: Timestamp) -> Context {
        Context {
            recorded_at,
            command: CommandContext::default(),
        }
    }

    /// This context, with `command` as the router's context of the input.
    pub fn with_command(self, command: CommandContext) -> Context {
        Context { command, ..self }
    }

    /// When the events will be recorded (non-decreasing within the stream).
    pub fn recorded_at(&self) -> Timestamp {
        self.recorded_at
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
/// - **Its root is the one its input names.** A command or fact addressed
///   to it names the instance in its cover, whatever its correlation (one
///   instance may take inputs of several correlations); a `correlation_id`
///   left unset is given the root.
/// - **The start rule**, angzarr-router's. Only an input type it declares
///   as starting (`#[handles_cmd(C, starts)]`, `#[handles_fact(F, starts)]`)
///   may start an instance. Any other input for a stream with no instance is
///   not delivered, decided before the handler by the type after
///   upcasting: a FACT is skipped silently (applied, no events, nothing
///   dead-lettered), and a COMMAND is refused (`PM_NO_INSTANCE`, forwarded
///   as `angzarr-embedded-next-pm-no-instance`, a business refusal, so a
///   policy's refused command yields its refusal fact like any other),
///   recording nothing.
///
/// A process manager whose `STARTS` is empty can never start an instance;
/// angzarr-embedded does not refuse one (no compile error, no check): the warning is
/// angzarr-cli's.
///
/// The kernel registers it on the router's process-manager table, which
/// applies the start rule.
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
            Rejection::Message(_) => None,
        }
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

impl<R: angzarr_embedded_diagnostic::Diagnostic> angzarr_embedded_diagnostic::Diagnostic
    for Rejection<R>
{
    fn code(&self) -> &'static str {
        match self {
            Rejection::Message(rejection) => rejection.code(),
            Rejection::Typed(typed) => typed.report.code(),
        }
    }

    fn args(&self) -> Vec<(&'static str, angzarr_embedded_diagnostic::Arg)> {
        match self {
            Rejection::Message(rejection) => rejection.args(),
            Rejection::Typed(typed) => typed.report.args(),
        }
    }
}

/// What a command handler fails with: one of the two classes, a business
/// refusal or a bug (decision 14.37), `R` the domain's own rejection.
///
/// - [`Failed::Rejected`]: the domain said no. It goes out as a business
///   refusal under `status` (FAILED_PRECONDITION unless the handler says
///   otherwise, [`Failed::with_status`]), the refusal's code and args
///   beside, and reaches the issuing policy's refusal path.
/// - [`Failed::Bug`]: a defect, never a business answer. It goes out as
///   INTERNAL UNHANDLED_HANDLER_ERROR, the report's code as its `cause` and
///   each arg as `cause.<arg>`, and the input is dead-lettered.
///
/// A handler that only refuses keeps returning `R` (or a `Refuses`
/// choice): each converts into a `Failed` by `From`, so `?` on a
/// `Result<_, R>` works inside a handler returning `Failed<R>`.
#[derive(Clone, PartialEq, Debug)]
pub enum Failed<R> {
    Rejected {
        rejection: Rejection<R>,
        status: BusinessStatus,
    },
    Bug(Report),
}

/// The statuses a business refusal may go out under: the business set.
/// Any other status is a fault.
#[derive(Copy, Clone, PartialEq, Eq, Debug, Default)]
pub enum BusinessStatus {
    #[default]
    FailedPrecondition,
    InvalidArgument,
    NotFound,
    AlreadyExists,
    OutOfRange,
    PermissionDenied,
}

impl<R> Failed<R> {
    /// A bug, keyed by `diagnostic`'s code, with its args.
    pub fn bug(diagnostic: &dyn angzarr_embedded_diagnostic::Diagnostic) -> Failed<R> {
        Failed::Bug(Report::of(diagnostic))
    }

    /// This refusal under `status`; a bug is unchanged.
    pub fn with_status(self, status: BusinessStatus) -> Failed<R> {
        match self {
            Failed::Rejected { rejection, .. } => Failed::Rejected { rejection, status },
            bug @ Failed::Bug(_) => bug,
        }
    }
}

/// A message-only refusal, FAILED_PRECONDITION: a handler's `Err` (or
/// `?`) of the domain's own rejection.
impl<R> From<R> for Failed<R> {
    fn from(rejection: R) -> Self {
        Failed::Rejected {
            rejection: Rejection::Message(rejection),
            status: BusinessStatus::default(),
        }
    }
}

/// The pass-through refusal fact, as the policy's SOURCE domain's handler
/// for it reads it: `#[on_refused(C)]`, a fact handler,
/// `fn(&self, refused: CommandRefused<C>, ctx: &Context) ->
/// Result<Record<Self>, E>` (or a shape), `ctx` the router's context of
/// the refusal fact. It cannot refuse; its `Err`, any `Diagnostic`, is a
/// bug, its policy's failure
/// ([`StepError::PolicyFailed`](super::StepError::PolicyFailed)). The macro dispatches angzarr's
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
/// response }`, the command decoded, beside the refused command's
/// [`CommandContext`]. The callback returns `Result` of a fact of the
/// policy's source domain, so the source never refers to the target's
/// types; its `Err` is a bug, dead-lettered as
/// [`StepError::PolicyFailed`](super::StepError::PolicyFailed).
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
/// implements as it is. Policies should not fail for business reasons:
/// they react to events the application's own domains created, so an `Err`
/// is a bug, dead-lettered as
/// [`StepError::PolicyFailed`](super::StepError::PolicyFailed).
pub use super::policy::Policy;

/// What a policy's `#[on_event(E)]` handler returns as its `Ok` (its `Err`
/// is any [`Diagnostic`](angzarr_embedded_diagnostic::Diagnostic): a bug,
/// dead-lettered as
/// [`StepError::PolicyFailed`](super::StepError::PolicyFailed)): commands for its target
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
/// `RefusalResponse` and `Refusal`, decision 14.31), as angzarr-router
/// carries them.
pub mod v1 {
    pub use angzarr_router::pb::{CommandRefused, Refusal, RefusalResponse};
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

/// A marked-up domain's handlers, as angzarr-router dispatches them: what
/// `#[aggregate]` and `#[process_manager]` generate, each marked handler
/// registered on the router's table for the domain (`on_command`,
/// `on_fact_with_context`, `on_refused`; a process manager's starting
/// inputs `starts`).
pub trait Routed: Domain {
    /// `table`, with every marked handler registered on it.
    fn route(
        table: angzarr_router::domain::DomainDispatch<Self::State>,
    ) -> angzarr_router::domain::DomainDispatch<Self::State>;
}

/// The context a handler reads: the router's, and the instant its events
/// will be recorded at.
fn context_of(cctx: angzarr_router::domain::CommandContext) -> Context {
    Context::new(super::route::recorded_at()).with_command(cctx)
}

/// The input `any` as a `T`, or the router's refusal of a payload that does
/// not decode as the type it names.
fn input<T: DomainMessage>(any: &Any) -> Result<T, angzarr_router::error::HandlerError> {
    match __decode::<T>(any) {
        Ok(Some(value)) => Ok(value),
        Ok(None) | Err(_) => Err(angzarr_router::error::HandlerError::Coded(
            angzarr_router::error::CodedError::invalid_argument(
                angzarr_router::error::codes::ANY_DECODE_FAILED,
                "the payload does not decode as its type",
                [(
                    angzarr_router::error::extras::TYPE_URL.to_string(),
                    any.type_url.clone(),
                )],
            ),
        )),
    }
}

/// `events`, packed, as the book a handler answers the router with.
fn book<A: Domain>(events: Vec<A::Event>) -> EventBook {
    EventBook {
        pages: events
            .iter()
            .map(|event| angzarr_proto::v1::EventPage {
                payload: Some(event.to_any()),
                ..Default::default()
            })
            .collect(),
        ..EventBook::default()
    }
}

/// `events` as the router records a fact: the first event as the record,
/// the rest as its flags; nothing, as the record of no event.
fn fact_record<A: Domain>(events: Vec<A::Event>) -> angzarr_router::domain::FactRecord {
    let mut events = events.iter().map(|event| event.to_any());
    let fact = events.next().unwrap_or_else(|| Any {
        type_url: super::route::NOTHING.to_string(),
        value: Vec::new(),
    });
    angzarr_router::domain::FactRecord {
        fact,
        flags: events.collect(),
    }
}

/// A command handler's result, run as one decision: its events, then each
/// continuation step on a working copy of `state` with every event so far
/// applied, until there is none. A rejection anywhere refuses it all, as
/// the router carries a business refusal; more than [`STEP_LIMIT`] steps is
/// the kernel's [`KernelError::TooManySteps`](super::KernelError::TooManySteps).
#[doc(hidden)]
pub fn __command<A, R, C, O, E>(
    any: &Any,
    state: &A,
    cctx: angzarr_router::domain::CommandContext,
    handler: impl FnOnce(&A, C, &Context) -> Result<O, E>,
) -> Result<Option<EventBook>, angzarr_router::error::HandlerError>
where
    A: Domain<State = A, Rules = (), Rejection = Rejection<R>> + Clone,
    R: angzarr_embedded_diagnostic::Diagnostic,
    C: DomainMessage,
    O: Emits<A>,
    E: Into<Failed<R>>,
{
    let refuse =
        |rejection: Rejection<R>| super::route::rejection(&rejection, rejection.refusal().cloned());
    let ctx = context_of(cctx);
    let command = input::<C>(any)?;
    let mut emit = handler(state, command, &ctx)
        .map_err(|e| match e.into() {
            Failed::Rejected { rejection, .. } => refuse(rejection),
            Failed::Bug(report) => super::route::rejection(&report, None),
        })?
        .into_emit();
    let mut working = state.clone();
    let mut events = Vec::new();
    let mut steps = 0;
    loop {
        for event in std::mem::take(&mut emit.events) {
            A::apply(&mut working, &event, &());
            events.push(event);
        }
        let Some(next) = emit.next.take() else {
            return Ok(Some(book::<A>(events)));
        };
        steps += 1;
        if steps > STEP_LIMIT {
            return Err(angzarr_router::error::HandlerError::Coded(
                super::route::internal(
                    super::route::TOO_MANY_STEPS,
                    "the decision ran more continuation steps than its limit",
                    [],
                ),
            ));
        }
        emit = next(&working, &ctx).map_err(refuse)?;
    }
}

/// A fact handler's result, run as one decision on a working copy of
/// `state`: its events, then each continuation step's on the copy with
/// them applied. Infallible; more than [`STEP_LIMIT`] steps is a bug,
/// which panics (the kernel's `IngestPanicked`, dead-lettered).
#[doc(hidden)]
pub fn __fact<A, F, Rr>(
    any: &Any,
    state: &A,
    cctx: angzarr_router::domain::CommandContext,
    handler: impl FnOnce(&A, F, &Context) -> Rr,
) -> Result<angzarr_router::domain::FactRecord, angzarr_router::error::HandlerError>
where
    A: Domain<State = A, Rules = ()> + Clone,
    F: DomainMessage,
    Rr: Records<A>,
{
    let ctx = context_of(cctx);
    let fact = input::<F>(any)?;
    Ok(fact_record::<A>(record(
        state,
        &ctx,
        handler(state, fact, &ctx),
    )))
}

/// A passed-through refusal's handler (`#[on_refused(C)]` on a domain),
/// for the router's `CommandRefused` of a `C`, with the router's context
/// of the refusal fact: run as a fact handler's. Its error is a bug, the
/// policy's failure, as the router carries it
/// ([`StepError::PolicyFailed`](super::StepError::PolicyFailed)).
#[doc(hidden)]
pub fn __on_refused<A, C, Rr, E>(
    refused: &v1::CommandRefused,
    state: &A,
    cctx: CommandContext,
    handler: impl FnOnce(&A, CommandRefused<C>, &Context) -> Result<Rr, E>,
) -> Result<angzarr_router::domain::FactRecord, angzarr_router::error::HandlerError>
where
    A: Domain<State = A, Rules = ()> + Clone,
    C: DomainMessage,
    Rr: Records<A>,
    E: angzarr_embedded_diagnostic::Diagnostic,
{
    let ctx = context_of(cctx);
    let book = refused.command.clone().unwrap_or_default();
    let command = input::<C>(command_payload(&book).unwrap_or(&Any::default()))?;
    let refused = CommandRefused {
        command,
        target: book.cover.unwrap_or_default(),
        refusal: refused.refusal.clone(),
        trigger: refused.trigger.clone().unwrap_or_default(),
    };
    let recorded =
        handler(state, refused, &ctx).map_err(|failure| super::route::policy_failed(&failure))?;
    Ok(fact_record::<A>(record(state, &ctx, recorded)))
}

/// A fact's record run as one decision on a working copy of `state`.
fn record<A, Rr>(state: &A, ctx: &Context, recorded: Rr) -> Vec<A::Event>
where
    A: Domain<State = A, Rules = ()> + Clone,
    Rr: Records<A>,
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
            super::error::cover_text(&ctx.command.cover.clone().unwrap_or_default())
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

/// `refusal` as a translating callback for command `C` reads it: `None` if
/// the refused command is not a `C`; one of `C`'s type that does not decode
/// is the error, a bug (the policy's failure), as an `#[on_event]`'s
/// undecodable event is.
#[doc(hidden)]
pub fn __translating<C: DomainMessage>(
    refusal: &v1::Refusal,
) -> Result<Option<Refusal<C>>, Report> {
    let Some(payload) = refusal.command.as_ref().and_then(command_payload) else {
        return Ok(None);
    };
    let Some(command) = __decode::<C>(payload).map_err(|error| Report::of(&error))? else {
        return Ok(None);
    };
    Ok(Some(Refusal {
        event: refusal.event.clone().unwrap_or_default(),
        command,
        response: refusal.response.clone().unwrap_or_default(),
    }))
}

/// A translating callback's answer, as `Policy::on_refused` gives it: its
/// fact, one its source `S` ingests, packed; or its error as a report (its
/// code the dead letter's reason, its args the letter's metadata).
#[doc(hidden)]
pub fn __domestic<S: Ingests<F>, F: DomainMessage, E: angzarr_embedded_diagnostic::Diagnostic>(
    answer: Result<F, E>,
) -> Result<Any, Report> {
    answer
        .map(|fact| __pack(&fact))
        .map_err(|error| Report::of(&error))
}

/// A policy's `#[on_event]` handler's answer, as `Policy::on_event` gives
/// it: its commands, or its error as a report (its code the dead letter's
/// reason, its args the letter's metadata).
#[doc(hidden)]
pub fn __answered<T: Domain, E: angzarr_embedded_diagnostic::Diagnostic>(
    answer: Result<Commands<T>, E>,
) -> Result<Option<Commands<T>>, Report> {
    answer.map(Some).map_err(|error| Report::of(&error))
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
