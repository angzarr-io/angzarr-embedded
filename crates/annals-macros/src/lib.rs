//! Decorators over hand-written components. [`aggregate`] and
//! [`process_manager`] generate, from the impl's [`handles_cmd`],
//! [`handles_fact`], [`on_refused`] and [`applies`] markers, the runtime
//! glue that runs a domain on annals (its `annals::next::Domain` and
//! `annals::next::State` impls, routing by type URL), a nameable
//! `<Name>Handler` trait, and its constants. [`saga`] generates a saga's
//! `annals::next::markup::Saga`, its refusal pass-through included, and
//! [`projector`] a projector's `AsyncProjector`. Hand-writing `Domain`
//! stays first-class; this is a way of writing one, not a layer over it.
//!
//! Every component written here assumes nothing about distribution: the
//! same aggregate, process manager, saga or projector runs in annals'
//! process or as its own service, with no change to its code.
//!
//! **A proposal.** The macros are stubs: they emit the generated items
//! with `todo!()` bodies and empty constants, and check nothing, so the
//! tests that pin the expansion (annals' `next/decorated.test.rs`, and the
//! `compile_fail` examples here) fail until it is written. Dispatch is the
//! linked angzarr-router's, blocked until the router is on io.angzarr.v1.
//!
//! # Where it sits
//!
//! 1. The proto declares the messages and the component: `(component)` on
//!    the anchor, `(command){component, emits, refuses, starts}` on each
//!    command and fact, `(event){component}` on each event.
//! 2. `angzarr scaffold rust` writes the empty decorated impl, once: the
//!    `#[aggregate]` (or `#[process_manager]`) impl, one
//!    `#[handles_cmd(Cmd)]` stub per command typed from its `emits` and
//!    `refuses` (and marked `starts` where the proto says so), one
//!    `#[applies(Event)]` stub per event, `todo!()` bodies; and, for a saga,
//!    the `#[saga]` impl, with an `#[on_refused(Cmd)]` callback stub for
//!    each command angzarr-cli's refusal switch selects for translate. The file is the user's and is never
//!    overwritten; `--missing` appends only the stubs that are absent.
//!    Every handler it writes returns its events; a continuation step
//!    (below) is the user's to add, where a later event depends on an
//!    earlier one's state.
//! 3. The user writes the bodies.
//! 4. This macro generates the glue, the trait and the constants.
//!
//! A command the proto declares and the markup does not handle reaches the
//! router as a type URL it does not know, and is refused at run time as the
//! kernel's unknown type, like any type no domain reads.
//!
//! # The markup
//!
//! ```text
//! #[aggregate(domain = "wallet", snapshot = WalletSnapshot)]
//! impl Wallet {
//!     #[handles_cmd(Deposit)]
//!     fn deposit(&self, cmd: Deposit, ctx: &Context) -> Result<Emit<Self>, Refused> { .. }
//!
//!     #[handles_cmd(Hold, refuses(InsufficientFunds, WalletClosed))]
//!     fn hold(&self, cmd: Hold, ctx: &Context) -> Result<Held, Refuses<Self, Hold>> { .. }
//!
//!     #[handles_fact(Payout)] // a fact: no Result, anywhere
//!     fn payout(&self, fact: Payout, ctx: &Context) -> Record<Self> { .. }
//!
//!     #[applies(Deposited)]
//!     fn deposited(&mut self, event: Deposited) { .. }
//! }
//! ```
//!
//! - [`aggregate`], on an inherent impl:
//!   - `domain`: the `Domain::NAME`, a string literal;
//!   - `snapshot` (optional): the prost message the aggregate is
//!     snapshotted as;
//!   - `rejection` (optional): the domain's own, message-only rejection.
//!     By default it is inferred from the `Result` types of the command
//!     handlers without `refuses`, which must agree (`Infallible` when
//!     there are none);
//!   - `vis` (optional, default `pub`): the visibility of the generated
//!     `<Name>Handler` trait. An impl does not carry its type's
//!     visibility, so the scaffold writes the struct's here.
//! - [`process_manager`], on an inherent impl: the same arguments; see
//!   *Process managers*.
//! - [`handles_cmd`]`(Cmd)`, one per command (a request: refusable):
//!   `fn(&self, cmd: Cmd, ctx: &Context) -> Result<R, E>`, where `R` is an
//!   `Emits` shape: an event `E`, a tuple `(E1, E2, ..)` of up to four,
//!   `Option<E>`, `Vec<E>`, or an `Emit<Self>` with a continuation whose
//!   steps are fallible too. A variable list of different types is not a
//!   shape (deferred). `E` is the domain's own rejection, or, with
//!   `refuses(T, ..)`, `Refuses<Self, Cmd>` (see *Typed refusals*).
//!   On a process manager, `starts` marks a starting input.
//! - [`handles_fact`]`(Fact)`, one per fact (a reaction's or the outside
//!   world's: not refusable; "if it can't be refused, it's a fact"):
//!   `fn(&self, fact: Fact, ctx: &Context) -> R`, with no `Result`, where
//!   `R` is a shape or a `Record<Self>`, whose continuation steps return no
//!   `Result` either. A refusal is an event the handler records. On a
//!   process manager, `starts` marks a starting input.
//! - [`on_refused`]`(Cmd)`, one per command of another domain that a saga
//!   sent on this domain's behalf and whose refusals it handles: a fact
//!   handler, `fn(&self, refused: CommandRefused<Cmd>, ctx: &Context) ->
//!   R` (see *Refusals*).
//!
//!   The marker says which a method is, and the macro holds the signature
//!   to it: a `#[handles_cmd]` without a `Result`, a `#[handles_fact]` or
//!   `#[on_refused]` with one (or with an `Emit`, whose steps may refuse),
//!   does not compile.
//! - [`applies`]`(Event)`, one per event: `fn(&mut self, event: Event)`.
//!
//! There are no other markers on a domain. Stateless rules are not a
//! marker: refusing has no side effects, because the application creates
//! what a command needs inside the same unit of work its events commit in.
//!
//! ## The instance is the aggregate
//!
//! The aggregate's state is its own fields: `Domain::State` is the
//! aggregate type, and the macro generates its `State` impl. There is no
//! separate state parameter.
//!
//! The one aggregate may be, and often should be, **backed by several
//! objects**: its fields are other types with their own methods (a
//! ledger, a schedule, a set of holds), and the aggregate is the gateway
//! and orchestrator over them. Handlers ask those objects and decide;
//! appliers hand each event to the objects it changes. Only the aggregate
//! is marked up; the objects behind it know nothing of commands, events or
//! annals.
//!
//! ## Pure handlers
//!
//! A handler's only effect is the events it returns. It takes `&self`, so
//! it reads the instance and cannot assign to it (a handler that assigns
//! a field does not compile); it has no clock (the time is
//! `Context::recorded_at`, sampled by the application and injected) and
//! does no I/O. The **router** alone changes the instance: it applies the
//! returned events, in order, through the appliers, and records them.
//! Appliers are the only code that assigns state, and applying an event
//! never sets off further processing.
//!
//! **Interior mutability is unsupported.** A `Cell`, `RefCell`, `Mutex`,
//! atomic or anything else that lets `&self` change the instance breaks
//! event sourcing: the change is in no event, so replay and a retry do not
//! reproduce it. `&self` is the only enforcement; nothing checks for it
//! at run time.
//!
//! ## Continuations: events that depend on earlier ones
//!
//! A handler decides from the state before any of its events. Where a
//! later event must be decided from the state after an earlier one, the
//! handler returns `Emit::of(e).then(Self::step)`. A step is a pure
//! `&self` method, `fn(&self, ctx: &Context) -> Result<R, E>`, with `R`
//! any `Emits` shape and `E` anything that converts into the domain's
//! rejection (its own, or a `Refuses` choice), so a step may chain
//! `.then(..)` again.
//!
//! The router applies the events, calls the step on the updated instance,
//! applies what it returns, and repeats until there is no continuation.
//! All of it is **one decision**: a rejection from the handler or any step
//! refuses it all, and every step's events are committed together, in
//! order, in one commit. A decision that runs more than
//! `annals::next::markup::STEP_LIMIT` steps is a bug (a step that always
//! continues): the kernel's `TooManySteps`, nothing committed. A step must
//! be idempotent on its own result (re-run there, it emits nothing);
//! `annals::next::markup::assert_step_idempotent` is the test helper.
//!
//! angzarr's client-side router runs continuations with the same
//! semantics: events applied, step called on the updated instance, one
//! decision, one commit, the same cap.
//!
//! ## Commands and facts
//!
//! If it can't be refused, it is a fact. A **command** (a request: a user,
//! a CLI, an API, a saga) may be refused, so its handler returns
//! `Result`, and a refusal goes back to the caller, the domain recording
//! nothing. A **fact** (a timeout, a webhook, an import, a refusal fact
//! from a saga) already happened and is never refused: its handler
//! returns no `Result` and always records an outcome, a refusal included,
//! as an event (`PayoutRefused`). A fact's step cannot fail; only a bug
//! can. The router hands a fact to
//! `Domain::ingest`, a command to `Domain::decide`.
//!
//! A fact's continuations are infallible too: its handler returns a
//! `Record<Self>`, `Record::of(e).then(Self::step)`, whose step is
//! `fn(&self, ctx: &Context) -> R` with no `Result`, `R` a shape or
//! another `Record`. Same semantics as `Emit`'s (applied, then the step on
//! the updated instance, one decision, the same cap), but nothing in it
//! can refuse.
//!
//! ## Typed refusals
//!
//! A command's handler may refuse with a business-specific, TYPED refusal
//! object, the markup's spelling of the proto's `(command).refuses`:
//! `#[handles_cmd(Hold, refuses(InsufficientFunds, WalletClosed))]`. Each
//! listed type is a domain type of the command's own domain (a
//! `DomainMessage`, never an event) and an `annals_diagnostic::Diagnostic`,
//! whose message is the refusal's. The handler's `Err` is then
//! `Refuses<Self, Hold>`, a choice the macro builds of exactly those
//! types, each `From` its type: `Err(InsufficientFunds { .. }.into())`. An
//! object not listed does not compile.
//!
//! A command handler without `refuses` refuses with the domain's own
//! rejection, a message only. Either way the domain's `Domain::Rejection`
//! is `markup::Rejection<R>`: `Message(R)`, the domain's own rejection as
//! returned, or `Typed`, the object packed with its diagnostic. A refused
//! saga receives either as angzarr's `RefusalResponse { refusal, message
//! }` (`Rejection::response`). On angzarr the glue converts it with
//! `Into<CommandRejectedError>`, packing the object into the status
//! details.
//!
//! ## Process managers
//!
//! A process manager IS A DOMAIN: [`process_manager`] takes the same
//! arguments and markers as [`aggregate`] and generates the same
//! expansion. Its inputs are commands (which sagas send it, and which it
//! may refuse) and facts (timeouts, webhooks, refusal facts); its only
//! output is its own events. It has no triggers: it subscribes to no
//! domain, and the saga for (another domain, the PM's domain) turns that
//! domain's events into commands to it. It sends no commands: to act on
//! another domain it records an event carrying the command's data, and
//! the saga for (the PM's domain, that domain) builds the command. Where a
//! workflow's logic belongs together, the PM and its sagas run in one
//! annals process (`annals::next::Kernel::settle`),
//! each saga hop a step committed on its own.
//!
//! What sets it apart, `markup::ProcessManager`:
//!
//! - **Its root is the correlation**, unchanged. An input addressed to it
//!   names the instance in its cover; a correlation left unset is given
//!   the root, and one set to anything else is malformed.
//! - **The start rule.** Only an input marked `starts`
//!   (`#[handles_cmd(StartBuyIn, starts)]`, `#[handles_fact(F, starts)]`)
//!   may start an instance; `STARTS` lists them. Any other input for a
//!   correlation with no instance is not delivered: a fact is skipped
//!   silently, a command refused (`PM_NO_INSTANCE`), recording nothing.
//!   `starts` on an aggregate's handler does not compile. A process
//!   manager that marks no input `starts` can never start an instance,
//!   yet annals does not refuse it: the check is angzarr-cli's, a
//!   generator warning, because a proc macro on stable Rust cannot warn,
//!   and a compile error would be stricter than the spec.
//!
//! Deduplication is by command id and fact id, before the handler runs,
//! as for any domain, and it has the one handler context every domain has.
//!
//! ## Sagas
//!
//! ```text
//! #[saga(name = "buyin-wallet", source = BuyIn, target = Wallet)]
//! impl BuyInWallet {
//!     #[on_event(BuyInStarted)]
//!     fn started(&self, event: &BuyInStarted, ctx: &SagaContext) -> Commands<Wallet> { .. }
//! }
//! ```
//!
//! - [`saga`], on an inherent impl: `name` (the component its commands'
//!   ids are derived from), `source` and `target` (each a domain:
//!   one source, one target).
//! - [`on_event`]`(Event)`, one per event of the source it handles:
//!   `fn(&self, event: &Event, ctx: &SagaContext) -> Commands<Target>`,
//!   pure and infallible, each command one the target decides, with the
//!   root of its stream (for a process manager, `ctx.correlation_id()`).
//!
//! ## Refusals
//!
//! When a target refuses a saga's command, it records nothing. The saga
//! then delivers ONE unrefusable fact of the refusal to its OWN source
//! domain, at the exact stream of its triggering event, whose id is the
//! refused command's id; the source decides what the refusal means and
//! records its own events. By one of two paths, chosen per command:
//!
//! - **Pass-through** (the default). The saga's generated
//!   `Saga::on_refused` answers angzarr's standard `CommandRefused {
//!   command, refusal, trigger }`, the typed refusal object still packed.
//!   The source handles it in `#[on_refused(Cmd)]`, chosen by the refused
//!   command's type, unpacking the object
//!   (`refused.refusal::<InsufficientFunds>()`); it imports the target's
//!   types to do so. Which refusals a domain handles is inferred from its
//!   `#[on_refused]` handlers (`REFUSALS` lists them): one with no handler
//!   is caught at run time: the steps before it stay committed, the
//!   refusal fact is dead-lettered and alerted, and the call returns Ok, as
//!   on distributed angzarr. A `CommandRefused` from outside a saga is
//!   accepted when its trigger names the receiving stream.
//! - **Translate**, for each command the saga has a callback for:
//!   `#[on_refused(Hold)] fn(&self, refusal: &Refusal<Hold>) -> F` on the
//!   saga's impl reads the typed object and returns `F`, a fact its source
//!   ingests, so the source never refers to the target's types. A callback
//!   returning anything else does not compile. One marker,
//!   `#[on_refused(Cmd)]`, names both the source's handler and the saga's
//!   callback.
//!
//! The callback's PRESENCE is the selection: nothing else marks the mode,
//! and `TRANSLATES` lists the commands the saga has callbacks for. It is
//! fixed at compile time, as angzarr-cli's refusal switch is angzarr's:
//! in Rust the switch means the scaffold writes the callback stub, or does
//! not. Nothing about it is visible at run time beyond which fact arrives.
//!
//! The trade-off: deleting a saga's callback
//! silently reverts that command to pass-through. The source then receives
//! `CommandRefused`, and if it has no `#[on_refused]` handler for that
//! command, the refusal is a run-time dead letter, not a compile error.
//!
//! ## Context, rejections and domain types
//!
//! Every domain handler and step takes the one `Context`: the instant its
//! events will be recorded at, the stream it decides, and the input's
//! identity (its command id, its correlation, and a fact's origin).
//!
//! Commands and events are **domain types** (angzarr-codegen's domain
//! layer), each a `DomainMessage` naming the prost message it travels as.
//! A handler never sees a wire type; a handler or applier taking one does
//! not compile.
//!
//! ## Construction, replay and snapshots
//!
//! A marked-up aggregate is registered with its factory, which is its
//! whole registration: `registry.aggregate::<Wallet>(Wallet::default)`, or
//! `registry.aggregate::<Wallet>(move || Wallet::with(config.clone()))` for
//! one that carries immutable reference configuration; a process manager
//! with `registry.process_manager::<BuyIn>(BuyIn::default)`, a saga with
//! `registry.saga(BuyInWallet)`. The kernel builds a fresh instance per
//! stream from the factory, replays the stream's events through the
//! appliers, then calls the handler on the result. No instance outlives
//! its input, and none is shared between streams.
//!
//! With `snapshot = Msg`, the user writes `From<&Wallet> for Msg` and
//! `fn restore(&mut self, snapshot: Msg) -> Result<(), E>` (`E` a
//! `Diagnostic`), and the macro generates the `State` impl from them, with
//! `Msg`'s type URL as the state's and STRICT and AGGREGATE_HANDLES as its
//! resolutions. Resuming builds a fresh instance from the factory, then
//! restores it, so the configuration the snapshot does not hold survives.
//! Without `snapshot`, the state has no snapshot form (`State::SNAPSHOTS`
//! is `false`), and every load replays from the start.
//!
//! # The expansion
//!
//! The impl above is kept as written, markers included (each is inert),
//! and gets beside it:
//!
//! ```text
//! // The command, fact and event sets, by type URL, kept out of user
//! // code: a module no path names, holding one variant per marked type.
//! mod __wallet_markup {
//!     pub enum Command { Deposit(Deposit), .. }
//!     pub enum Fact { Payout(Payout), .. } // and angzarr's CommandRefused, by
//!                                          // the refused command's type
//!     pub enum Event { Deposited(Deposited), .. }
//!     // impl Payload for each: decode by type URL (the type's
//!     // DomainMessage::Wire), then DomainMessage::from_wire.
//!     // impl From<Deposited> for Event, and so on.
//! }
//!
//! // For each command with `refuses`: the choice, `From` each listed
//! // type, `Into` the domain's rejection.
//! pub enum __WalletHoldRefuses { InsufficientFunds(InsufficientFunds), .. }
//! impl Refusing<Hold> for Wallet { type Refuses = __WalletHoldRefuses; const REFUSES = ..; }
//!
//! // What a saga is checked against.
//! impl Decides<Deposit> for Wallet {} // one per command
//! impl Ingests<Payout> for Wallet {}  // one per fact
//!
//! impl ::annals::next::State for Wallet {
//!     type Snapshot = WalletSnapshot; // or markup::NoSnapshot, with SNAPSHOTS = false
//!     // to_snapshot: From<&Wallet>; restore: Wallet::restore;
//!     // from_snapshot: Default, then restore
//!     ..
//! }
//!
//! impl ::annals::next::Domain for Wallet {
//!     type State = Self;
//!     type Command = __wallet_markup::Command;
//!     type Fact = __wallet_markup::Fact;
//!     type Event = __wallet_markup::Event;
//!     type Rejection = markup::Rejection<Refused>; // Refused inferred
//!     type Rules = ();
//!     const NAME: &'static str = "wallet";
//!
//!     fn decide(command: &Command, tx: &mut Tx<'_, Self>) -> Result<(), Rejection<Refused>> {
//!         let ctx = Context::new(tx.recorded_at(), tx.stream().clone(),
//!                                tx.command_id().clone(), tx.correlation_id().clone());
//!         let mut emit = match command {
//!             Command::Deposit(cmd) => Emits::into_emit(tx.state().deposit(cmd.clone(), &ctx)?),
//!             ..
//!         };
//!         // Apply each event to the staged instance (tx.emit records it);
//!         // while there is a continuation, run it on tx.state() and loop,
//!         // up to STEP_LIMIT steps.
//!         ..
//!     }
//!
//!     fn ingest(state: &Self, fact: &Fact, ctx: &IngestCtx<'_, Self>) -> Vec<Event> {
//!         // As decide, on a working copy of `state`: the fact handler's
//!         // (or the refusal handler's) events, then its continuations'
//!         // (which return no Result).
//!         ..
//!     }
//!
//!     fn apply(state: &mut Self, event: &Event, (): &()) {
//!         match event {
//!             Event::Deposited(event) => state.deposited(event.clone()),
//!             ..
//!         }
//!     }
//! }
//!
//! // A process manager's, besides: impl ProcessManager for BuyIn { const STARTS = ..; }
//!
//! /// The domain's handler set, as a type, with the `vis` given: every
//! /// marked method's signature, implemented by delegating to it.
//! pub trait WalletHandler {
//!     fn deposit(&self, cmd: Deposit, ctx: &Context) -> Result<Emit<Wallet>, Refused>;
//!     fn deposited(&mut self, event: Deposited);
//!     ..
//! }
//! impl WalletHandler for Wallet { .. }
//!
//! impl Wallet {
//!     /// The handled commands and facts, the commands whose refusals it
//!     /// handles, and the applied events, in marker order.
//!     pub const HANDLES: &'static [TypeName] = &[TypeName::of::<<Deposit as DomainMessage>::Wire>(), ..];
//!     pub const REFUSALS: &'static [TypeName] = &[..];
//!     pub const APPLIES: &'static [TypeName] = &[..];
//! }
//! ```
//!
//! A saga's expansion is its `markup::Saga` impl: `NAME`, `Source`,
//! `Target`, `EVENTS`, `TRANSLATES`, and `on_refused`, which unpacks the
//! refused command and either builds the pass-through `CommandRefused` or
//! calls the translating callback.
//!
//! **Refused by the macro**, each with a message saying why: a second
//! handler or applier for one type; a handler taking `&mut self`;
//! message-only command handlers whose `Result`s disagree on the
//! rejection; a marker's type that is not its method's parameter type; a
//! marked method whose shape is not the one above; a `refuses` type that is
//! not a domain type; `starts` on an aggregate; a saga's `#[on_refused]`
//! callback returning anything but a fact of its source; a saga handler's
//! commands for another domain than its target.
//!
//! Which of the messages is a fact is the marker's to say, and the
//! signature is checked against it. Checking the marker against the
//! message's declared kind in the proto is an open question.
//!
//! **Not on annals.** `Rules` is `()`. [`rejected`] and [`state_factory`],
//! angzarr-macros' rejection handlers and state factories, are a
//! `compile_error!` here: a refusal reaches the saga's source as a fact,
//! handled in `#[on_refused]`, and the instance is built by `Default` or
//! the registry's factory, as above.
//!
//! # Examples
//!
//! An aggregate that compiles (the control for the examples below):
//!
//! ```no_run
//! # use std::convert::Infallible;
//! # use annals::next::markup::{Context, DomainMessage};
//! # use annals::next::WireError;
//! # use annals_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_cmd(SwitchOff)]
//!     fn switch_off(&self, _cmd: SwitchOff, _ctx: &Context) -> Result<SwitchedOff, Infallible> {
//!         Ok(SwitchedOff)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(SwitchedOff)]
//!     fn off(&mut self, _event: SwitchedOff) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Two handlers for `SwitchOn` do not compile: which one decides is not a
//! question the markup leaves open.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{Context, DomainMessage};
//! # use annals::next::WireError;
//! # use annals_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on_again(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Infallible> {
//!         Ok(SwitchedOn)
//!     }
//!
//!     #[handles_cmd(SwitchOff)]
//!     fn switch_off(&self, _cmd: SwitchOff, _ctx: &Context) -> Result<SwitchedOff, Infallible> {
//!         Ok(SwitchedOff)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(SwitchedOff)]
//!     fn off(&mut self, _event: SwitchedOff) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor do handlers that disagree on the rejection: the domain has one.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{Context, DomainMessage};
//! # use annals::next::WireError;
//! # use annals_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_cmd(SwitchOff)]
//!     fn switch_off(&self, _cmd: SwitchOff, _ctx: &Context) -> Result<SwitchedOff, WireError> {
//!         Ok(SwitchedOff)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(SwitchedOff)]
//!     fn off(&mut self, _event: SwitchedOff) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor does a handler taking the wire type: a prost message is not a
//! `DomainMessage`.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{Context, DomainMessage};
//! # use annals::next::WireError;
//! # use annals_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_cmd(wire::SwitchOff)]
//!     fn switch_off(&self, _cmd: wire::SwitchOff, _ctx: &Context) -> Result<SwitchedOff, Infallible> {
//!         Ok(SwitchedOff)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(SwitchedOff)]
//!     fn off(&mut self, _event: SwitchedOff) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor does a handler that assigns to the instance: it has `&self`.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{Context, DomainMessage};
//! # use annals::next::WireError;
//! # use annals_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_cmd(SwitchOff)]
//!     fn switch_off(&self, _cmd: SwitchOff, _ctx: &Context) -> Result<SwitchedOff, Infallible> {
//!         self.on = false;
//!         Ok(SwitchedOff)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(SwitchedOff)]
//!     fn off(&mut self, _event: SwitchedOff) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor does a handler that takes `&mut self` to get round that.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{Context, DomainMessage};
//! # use annals::next::WireError;
//! # use annals_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_cmd(SwitchOff)]
//!     fn switch_off(&mut self, _cmd: SwitchOff, _ctx: &Context) -> Result<SwitchedOff, Infallible> {
//!         self.on = false;
//!         Ok(SwitchedOff)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(SwitchedOff)]
//!     fn off(&mut self, _event: SwitchedOff) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! The handler trait has the `vis` given: a `pub(self)` trait is not
//! reachable outside its module.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{Context, DomainMessage};
//! # use annals::next::WireError;
//! # use annals_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! mod lamp {
//!     use super::*;
//!
//!     #[derive(Clone, Default)]
//!     pub struct Lamp {
//!         on: bool,
//!     }
//!
//!     #[aggregate(domain = "lamp", vis = pub(self))]
//!     impl Lamp {
//!         #[handles_cmd(SwitchOn)]
//!         fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!             Ok((!self.on).then_some(SwitchedOn))
//!         }
//!
//!         #[handles_cmd(SwitchOff)]
//!         fn switch_off(&self, _cmd: SwitchOff, _ctx: &Context) -> Result<SwitchedOff, Infallible> {
//!             Ok(SwitchedOff)
//!         }
//!
//!         #[applies(SwitchedOn)]
//!         fn on(&mut self, _event: SwitchedOn) {
//!             self.on = true;
//!         }
//!
//!         #[applies(SwitchedOff)]
//!         fn off(&mut self, _event: SwitchedOff) {
//!             self.on = false;
//!         }
//!     }
//! }
//!
//! fn handler_set<H: lamp::LampHandler>() {}
//!
//! fn main() {}
//! ```
//!
//! A fact handler returning a `Result` does not compile: a fact cannot be
//! refused, so its handler has nothing to refuse with.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{Context, DomainMessage, Record};
//! # use annals::next::WireError;
//! # use annals_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, PowerCut, SwitchedOn, WentDark);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, PowerCut, SwitchedOn, WentDark);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Result<WentDark, Infallible> {
//!         Ok(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor does a command handler without one: a command may be refused, so
//! its handler says how.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{Context, DomainMessage, Record};
//! # use annals::next::WireError;
//! # use annals_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, PowerCut, SwitchedOn, WentDark);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, PowerCut, SwitchedOn, WentDark);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Option<SwitchedOn> {
//!         (!self.on).then_some(SwitchedOn)
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor a fact handler returning an `Emit`, whose continuation steps may
//! refuse: a fact's steps are a `Record`'s, infallible.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{Context, DomainMessage, Emit};
//! # use annals::next::WireError;
//! # use annals_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, PowerCut, SwitchedOn, WentDark);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, PowerCut, SwitchedOn, WentDark);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Emit<Self> {
//!         Emit::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor one `#[handles]` for both kinds: the marker says which a method is.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{Context, DomainMessage, Record};
//! # use annals::next::WireError;
//! # use annals_macros::{aggregate, applies, handles};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, PowerCut, SwitchedOn, WentDark);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, PowerCut, SwitchedOn, WentDark);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor does a `#[validates]` marker: there is none.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{Context, DomainMessage};
//! # use annals::next::WireError;
//! # use annals_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[validates(SwitchOn)]
//!     fn bright_enough(_cmd: &SwitchOn) -> Result<(), Infallible> {
//!         Ok(())
//!     }
//!
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_cmd(SwitchOff)]
//!     fn switch_off(&self, _cmd: SwitchOff, _ctx: &Context) -> Result<SwitchedOff, Infallible> {
//!         Ok(SwitchedOff)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(SwitchedOff)]
//!     fn off(&mut self, _event: SwitchedOff) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor does a `#[rejected]` handler: a refusal reaches the saga's source
//! as a fact, handled in `#[on_refused]`.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{Context, DomainMessage};
//! # use annals::next::WireError;
//! # use annals_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! use annals_macros::rejected;
//!
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_cmd(SwitchOff)]
//!     fn switch_off(&self, _cmd: SwitchOff, _ctx: &Context) -> Result<SwitchedOff, Infallible> {
//!         Ok(SwitchedOff)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(SwitchedOff)]
//!     fn off(&mut self, _event: SwitchedOff) {
//!         self.on = false;
//!     }
//!
//!     #[rejected(domain = "grid", command = "Supply")]
//!     fn supply_refused(&self) {}
//! }
//! ```
//!
//! Nor does a `#[state_factory]`: the registry holds the factory.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{Context, DomainMessage};
//! # use annals::next::WireError;
//! # use annals_macros::{aggregate, applies, handles_cmd, handles_fact};
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchOff, SwitchedOn, SwitchedOff);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! use annals_macros::state_factory;
//!
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn)]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<Option<SwitchedOn>, Infallible> {
//!         Ok((!self.on).then_some(SwitchedOn))
//!     }
//!
//!     #[handles_cmd(SwitchOff)]
//!     fn switch_off(&self, _cmd: SwitchOff, _ctx: &Context) -> Result<SwitchedOff, Infallible> {
//!         Ok(SwitchedOff)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(SwitchedOff)]
//!     fn off(&mut self, _event: SwitchedOff) {
//!         self.on = false;
//!     }
//!
//!     #[state_factory]
//!     fn fresh() -> Lamp {
//!         Lamp::default()
//!     }
//! }
//! ```
//!
//! A domain, a process manager and a saga that compile (the control for the
//! examples below): typed refusals, a starting input, a refusal handler and
//! a pass-through saga.
//!
//! ```no_run
//! # use std::convert::Infallible;
//! # use annals::next::markup::{
//! #     CommandRefused, Commands, Context, DomainMessage, SagaContext, Record, Refusal, Refuses,
//! # };
//! # use annals::next::WireError;
//! # use annals_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, saga,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl annals_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &'static str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, annals_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn, refuses(Burnt))]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Refuses<Self, SwitchOn>> {
//!         match self.on {
//!             true => Err(Burnt.into()),
//!             false => Ok(SwitchedOn),
//!         }
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//!
//! #[process_manager(domain = "timer")]
//! impl Timer {
//!     #[handles_cmd(StartTimer, starts)]
//!     fn start(&self, _cmd: StartTimer, _ctx: &Context) -> Result<TimerStarted, Infallible> {
//!         Ok(TimerStarted)
//!     }
//!
//!     #[handles_fact(Ring)]
//!     fn ring(&self, _fact: Ring, _ctx: &Context) -> Record<Self> {
//!         Record::of(Rang)
//!     }
//!
//!     #[handles_fact(LampLost)]
//!     fn lamp_lost(&self, _fact: LampLost, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(&self, _refused: CommandRefused<SwitchOn>, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[applies(TimerStarted)]
//!     fn started(&mut self, _event: TimerStarted) {}
//!
//!     #[applies(Rang)]
//!     fn rang(&mut self, _event: Rang) {
//!         self.rang = true;
//!     }
//!
//!     #[applies(Lost)]
//!     fn lost(&mut self, _event: Lost) {}
//! }
//!
//! #[saga(name = "timer-lamp", source = Timer, target = Lamp)]
//! impl TimerLamp {
//!     #[on_event(Rang)]
//!     fn rang(&self, _event: &Rang, ctx: &SagaContext) -> Commands<Lamp> {
//!         Commands::none().to(ctx.correlation_id().clone(), SwitchOn)
//!     }
//! }
//! ```
//!
//! A handler refusing with an object its `refuses` does not list does not
//! compile: the choice holds only the listed types.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{
//! #     CommandRefused, Commands, Context, DomainMessage, SagaContext, Record, Refusal, Refuses,
//! # };
//! # use annals::next::WireError;
//! # use annals_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, saga,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl annals_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &'static str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, annals_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn, refuses(Burnt))]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Refuses<Self, SwitchOn>> {
//!         match self.on {
//!             true => Err(Fused.into()),
//!             false => Ok(SwitchedOn),
//!         }
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor a handler with `refuses` whose `Err` is not its choice: the
//! signature is held to the marker.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{
//! #     CommandRefused, Commands, Context, DomainMessage, SagaContext, Record, Refusal, Refuses,
//! # };
//! # use annals::next::WireError;
//! # use annals_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, saga,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl annals_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &'static str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, annals_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn, refuses(Burnt))]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Burnt> {
//!         match self.on {
//!             true => Err(Burnt),
//!             false => Ok(SwitchedOn),
//!         }
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! Nor a `refuses` naming a wire type: a refusal object is a domain type
//! (of the command's own domain), as an event is.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{
//! #     CommandRefused, Commands, Context, DomainMessage, SagaContext, Record, Refusal, Refuses,
//! # };
//! # use annals::next::WireError;
//! # use annals_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, saga,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl annals_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &'static str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, annals_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn, refuses(wire::Burnt))]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Refuses<Self, SwitchOn>> {
//!         match self.on {
//!             true => Err(wire::Burnt {}.into()),
//!             false => Ok(SwitchedOn),
//!         }
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! `starts` is a process manager's: on an aggregate's handler it does not
//! compile.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{
//! #     CommandRefused, Commands, Context, DomainMessage, SagaContext, Record, Refusal, Refuses,
//! # };
//! # use annals::next::WireError;
//! # use annals_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, saga,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl annals_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &'static str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, annals_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn, starts, refuses(Burnt))]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Refuses<Self, SwitchOn>> {
//!         match self.on {
//!             true => Err(Burnt.into()),
//!             false => Ok(SwitchedOn),
//!         }
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//! ```
//!
//! A process manager is a domain: it is named by `domain`, as an
//! aggregate is, not by a `name`.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{
//! #     CommandRefused, Commands, Context, DomainMessage, SagaContext, Record, Refusal, Refuses,
//! # };
//! # use annals::next::WireError;
//! # use annals_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, saga,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl annals_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &'static str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, annals_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[process_manager(name = "timer")]
//! impl Timer {
//!     #[handles_cmd(StartTimer, starts)]
//!     fn start(&self, _cmd: StartTimer, _ctx: &Context) -> Result<TimerStarted, Infallible> {
//!         Ok(TimerStarted)
//!     }
//!
//!     #[handles_fact(Ring)]
//!     fn ring(&self, _fact: Ring, _ctx: &Context) -> Record<Self> {
//!         Record::of(Rang)
//!     }
//!
//!     #[handles_fact(LampLost)]
//!     fn lamp_lost(&self, _fact: LampLost, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(&self, _refused: CommandRefused<SwitchOn>, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[applies(TimerStarted)]
//!     fn started(&mut self, _event: TimerStarted) {}
//!
//!     #[applies(Rang)]
//!     fn rang(&mut self, _event: Rang) {
//!         self.rang = true;
//!     }
//!
//!     #[applies(Lost)]
//!     fn lost(&mut self, _event: Lost) {}
//! }
//! ```
//!
//! A process manager has no triggers: `#[on]` is gone. Another domain's
//! events reach it as commands, through a saga.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{
//! #     CommandRefused, Commands, Context, DomainMessage, SagaContext, Record, Refusal, Refuses,
//! # };
//! # use annals::next::WireError;
//! # use annals_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, saga,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl annals_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &'static str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, annals_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! use annals_macros::on;
//!
//! #[process_manager(domain = "timer")]
//! impl Timer {
//!     #[handles_cmd(StartTimer, starts)]
//!     fn start(&self, _cmd: StartTimer, _ctx: &Context) -> Result<TimerStarted, Infallible> {
//!         Ok(TimerStarted)
//!     }
//!
//!     #[on(Ring)]
//!     fn ring(&self, _fact: Ring, _ctx: &Context) -> Record<Self> {
//!         Record::of(Rang)
//!     }
//!
//!     #[handles_fact(LampLost)]
//!     fn lamp_lost(&self, _fact: LampLost, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(&self, _refused: CommandRefused<SwitchOn>, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[applies(TimerStarted)]
//!     fn started(&mut self, _event: TimerStarted) {}
//!
//!     #[applies(Rang)]
//!     fn rang(&mut self, _event: Rang) {
//!         self.rang = true;
//!     }
//!
//!     #[applies(Lost)]
//!     fn lost(&mut self, _event: Lost) {}
//! }
//! ```
//!
//! Nor is there a root derived for it: its root is the correlation,
//! unchanged.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{
//! #     CommandRefused, Commands, Context, DomainMessage, SagaContext, Record, Refusal, Refuses,
//! # };
//! # use annals::next::WireError;
//! # use annals_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, saga,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl annals_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &'static str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, annals_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! use annals::next::markup::process_manager_root;
//! ```
//!
//! A refusal handler (`#[on_refused]` on a domain) is a fact handler: it
//! cannot refuse, so a `Result` does not compile.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{
//! #     CommandRefused, Commands, Context, DomainMessage, SagaContext, Record, Refusal, Refuses,
//! # };
//! # use annals::next::WireError;
//! # use annals_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, saga,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl annals_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &'static str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, annals_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[process_manager(domain = "timer")]
//! impl Timer {
//!     #[handles_cmd(StartTimer, starts)]
//!     fn start(&self, _cmd: StartTimer, _ctx: &Context) -> Result<TimerStarted, Infallible> {
//!         Ok(TimerStarted)
//!     }
//!
//!     #[handles_fact(Ring)]
//!     fn ring(&self, _fact: Ring, _ctx: &Context) -> Record<Self> {
//!         Record::of(Rang)
//!     }
//!
//!     #[handles_fact(LampLost)]
//!     fn lamp_lost(&self, _fact: LampLost, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(&self, _refused: CommandRefused<SwitchOn>, _ctx: &Context) -> Result<Lost, Infallible> {
//!         Ok(Lost)
//!     }
//!
//!     #[applies(TimerStarted)]
//!     fn started(&mut self, _event: TimerStarted) {}
//!
//!     #[applies(Rang)]
//!     fn rang(&mut self, _event: Rang) {
//!         self.rang = true;
//!     }
//!
//!     #[applies(Lost)]
//!     fn lost(&mut self, _event: Lost) {}
//! }
//! ```
//!
//! A saga translating a command's refusals: its `#[on_refused(SwitchOn)]`
//! callback is the selection, and returns the source's own fact (the control
//! for the two below).
//!
//! ```no_run
//! # use std::convert::Infallible;
//! # use annals::next::markup::{
//! #     CommandRefused, Commands, Context, DomainMessage, SagaContext, Record, Refusal, Refuses,
//! # };
//! # use annals::next::WireError;
//! # use annals_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, saga,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl annals_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &'static str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, annals_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn, refuses(Burnt))]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Refuses<Self, SwitchOn>> {
//!         match self.on {
//!             true => Err(Burnt.into()),
//!             false => Ok(SwitchedOn),
//!         }
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//!
//! #[process_manager(domain = "timer")]
//! impl Timer {
//!     #[handles_cmd(StartTimer, starts)]
//!     fn start(&self, _cmd: StartTimer, _ctx: &Context) -> Result<TimerStarted, Infallible> {
//!         Ok(TimerStarted)
//!     }
//!
//!     #[handles_fact(Ring)]
//!     fn ring(&self, _fact: Ring, _ctx: &Context) -> Record<Self> {
//!         Record::of(Rang)
//!     }
//!
//!     #[handles_fact(LampLost)]
//!     fn lamp_lost(&self, _fact: LampLost, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(&self, _refused: CommandRefused<SwitchOn>, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[applies(TimerStarted)]
//!     fn started(&mut self, _event: TimerStarted) {}
//!
//!     #[applies(Rang)]
//!     fn rang(&mut self, _event: Rang) {
//!         self.rang = true;
//!     }
//!
//!     #[applies(Lost)]
//!     fn lost(&mut self, _event: Lost) {}
//! }
//!
//! #[saga(name = "timer-lamp", source = Timer, target = Lamp)]
//! impl TimerLamp {
//!     #[on_event(Rang)]
//!     fn rang(&self, _event: &Rang, ctx: &SagaContext) -> Commands<Lamp> {
//!         Commands::none().to(ctx.correlation_id().clone(), SwitchOn)
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(&self, _refusal: &Refusal<SwitchOn>) -> LampLost {
//!         LampLost
//!     }
//! }
//! ```
//!
//! Nor a callback returning anything but a fact its source ingests: here,
//! one of the source's events.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{
//! #     CommandRefused, Commands, Context, DomainMessage, SagaContext, Record, Refusal, Refuses,
//! # };
//! # use annals::next::WireError;
//! # use annals_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, saga,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl annals_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &'static str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, annals_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn, refuses(Burnt))]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Refuses<Self, SwitchOn>> {
//!         match self.on {
//!             true => Err(Burnt.into()),
//!             false => Ok(SwitchedOn),
//!         }
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//!
//! #[process_manager(domain = "timer")]
//! impl Timer {
//!     #[handles_cmd(StartTimer, starts)]
//!     fn start(&self, _cmd: StartTimer, _ctx: &Context) -> Result<TimerStarted, Infallible> {
//!         Ok(TimerStarted)
//!     }
//!
//!     #[handles_fact(Ring)]
//!     fn ring(&self, _fact: Ring, _ctx: &Context) -> Record<Self> {
//!         Record::of(Rang)
//!     }
//!
//!     #[handles_fact(LampLost)]
//!     fn lamp_lost(&self, _fact: LampLost, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(&self, _refused: CommandRefused<SwitchOn>, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[applies(TimerStarted)]
//!     fn started(&mut self, _event: TimerStarted) {}
//!
//!     #[applies(Rang)]
//!     fn rang(&mut self, _event: Rang) {
//!         self.rang = true;
//!     }
//!
//!     #[applies(Lost)]
//!     fn lost(&mut self, _event: Lost) {}
//! }
//!
//! #[saga(name = "timer-lamp", source = Timer, target = Lamp)]
//! impl TimerLamp {
//!     #[on_event(Rang)]
//!     fn rang(&self, _event: &Rang, ctx: &SagaContext) -> Commands<Lamp> {
//!         Commands::none().to(ctx.correlation_id().clone(), SwitchOn)
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(&self, _refusal: &Refusal<SwitchOn>) -> Lost {
//!         Lost
//!     }
//! }
//! ```
//!
//! Nor a `translate(..)` argument: the callback's presence is the
//! selection, and nothing else marks the mode.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{
//! #     CommandRefused, Commands, Context, DomainMessage, SagaContext, Record, Refusal, Refuses,
//! # };
//! # use annals::next::WireError;
//! # use annals_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, saga,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl annals_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &'static str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, annals_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn, refuses(Burnt))]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Refuses<Self, SwitchOn>> {
//!         match self.on {
//!             true => Err(Burnt.into()),
//!             false => Ok(SwitchedOn),
//!         }
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//!
//! #[process_manager(domain = "timer")]
//! impl Timer {
//!     #[handles_cmd(StartTimer, starts)]
//!     fn start(&self, _cmd: StartTimer, _ctx: &Context) -> Result<TimerStarted, Infallible> {
//!         Ok(TimerStarted)
//!     }
//!
//!     #[handles_fact(Ring)]
//!     fn ring(&self, _fact: Ring, _ctx: &Context) -> Record<Self> {
//!         Record::of(Rang)
//!     }
//!
//!     #[handles_fact(LampLost)]
//!     fn lamp_lost(&self, _fact: LampLost, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(&self, _refused: CommandRefused<SwitchOn>, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[applies(TimerStarted)]
//!     fn started(&mut self, _event: TimerStarted) {}
//!
//!     #[applies(Rang)]
//!     fn rang(&mut self, _event: Rang) {
//!         self.rang = true;
//!     }
//!
//!     #[applies(Lost)]
//!     fn lost(&mut self, _event: Lost) {}
//! }
//!
//! #[saga(name = "timer-lamp", source = Timer, target = Lamp, translate(SwitchOn))]
//! impl TimerLamp {
//!     #[on_event(Rang)]
//!     fn rang(&self, _event: &Rang, ctx: &SagaContext) -> Commands<Lamp> {
//!         Commands::none().to(ctx.correlation_id().clone(), SwitchOn)
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(&self, _refusal: &Refusal<SwitchOn>) -> LampLost {
//!         LampLost
//!     }
//! }
//! ```
//!
//! A saga sends to its one target: commands for another domain do not
//! compile.
//!
//! ```compile_fail
//! # use std::convert::Infallible;
//! # use annals::next::markup::{
//! #     CommandRefused, Commands, Context, DomainMessage, SagaContext, Record, Refusal, Refuses,
//! # };
//! # use annals::next::WireError;
//! # use annals_macros::{
//! #     aggregate, applies, handles_cmd, handles_fact, on_refused, process_manager, on_event, saga,
//! # };
//! # // A compile-only fixture: nothing here runs.
//! # pub mod wire {
//! #     macro_rules! wire {
//! #         ($($name:ident),*) => {$(
//! #             #[derive(Clone, PartialEq, ::prost::Message)]
//! #             pub struct $name {}
//! #             impl ::prost::Name for $name {
//! #                 const NAME: &'static str = stringify!($name);
//! #                 const PACKAGE: &'static str = "lamp.v1";
//! #             }
//! #         )*};
//! #     }
//! #     wire!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! #     wire!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # }
//! # macro_rules! domain {
//! #     ($($name:ident),*) => {$(
//! #         #[derive(Clone, Debug)]
//! #         pub struct $name;
//! #         impl DomainMessage for $name {
//! #             type Wire = wire::$name;
//! #             fn to_wire(&self) -> wire::$name { unimplemented!() }
//! #             fn from_wire(_: wire::$name) -> Result<Self, WireError> { unimplemented!() }
//! #         }
//! #     )*};
//! # }
//! # domain!(SwitchOn, SwitchedOn, PowerCut, WentDark, Burnt, Fused);
//! # domain!(StartTimer, TimerStarted, Ring, Rang, LampLost, Lost);
//! # macro_rules! refusal {
//! #     ($($name:ident),*) => {$(
//! #         impl std::fmt::Display for $name {
//! #             fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//! #                 f.write_str(stringify!($name))
//! #             }
//! #         }
//! #         impl annals_diagnostic::Diagnostic for $name {
//! #             fn code(&self) -> &'static str { "lamp-refused" }
//! #             fn args(&self) -> Vec<(&'static str, annals_diagnostic::Arg)> { vec![] }
//! #         }
//! #     )*};
//! # }
//! # refusal!(Burnt, Fused);
//! # #[derive(Clone, Default)]
//! # pub struct Lamp { on: bool }
//! # #[derive(Clone, Default)]
//! # pub struct Timer { rang: bool }
//! # pub struct TimerLamp;
//! #[aggregate(domain = "lamp")]
//! impl Lamp {
//!     #[handles_cmd(SwitchOn, refuses(Burnt))]
//!     fn switch_on(&self, _cmd: SwitchOn, _ctx: &Context) -> Result<SwitchedOn, Refuses<Self, SwitchOn>> {
//!         match self.on {
//!             true => Err(Burnt.into()),
//!             false => Ok(SwitchedOn),
//!         }
//!     }
//!
//!     #[handles_fact(PowerCut)]
//!     fn power_cut(&self, _fact: PowerCut, _ctx: &Context) -> Record<Self> {
//!         Record::of(WentDark)
//!     }
//!
//!     #[applies(SwitchedOn)]
//!     fn on(&mut self, _event: SwitchedOn) {
//!         self.on = true;
//!     }
//!
//!     #[applies(WentDark)]
//!     fn dark(&mut self, _event: WentDark) {
//!         self.on = false;
//!     }
//! }
//!
//! #[process_manager(domain = "timer")]
//! impl Timer {
//!     #[handles_cmd(StartTimer, starts)]
//!     fn start(&self, _cmd: StartTimer, _ctx: &Context) -> Result<TimerStarted, Infallible> {
//!         Ok(TimerStarted)
//!     }
//!
//!     #[handles_fact(Ring)]
//!     fn ring(&self, _fact: Ring, _ctx: &Context) -> Record<Self> {
//!         Record::of(Rang)
//!     }
//!
//!     #[handles_fact(LampLost)]
//!     fn lamp_lost(&self, _fact: LampLost, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[on_refused(SwitchOn)]
//!     fn switch_on_refused(&self, _refused: CommandRefused<SwitchOn>, _ctx: &Context) -> Record<Self> {
//!         Record::of(Lost)
//!     }
//!
//!     #[applies(TimerStarted)]
//!     fn started(&mut self, _event: TimerStarted) {}
//!
//!     #[applies(Rang)]
//!     fn rang(&mut self, _event: Rang) {
//!         self.rang = true;
//!     }
//!
//!     #[applies(Lost)]
//!     fn lost(&mut self, _event: Lost) {}
//! }
//!
//! #[saga(name = "timer-lamp", source = Timer, target = Lamp)]
//! impl TimerLamp {
//!     #[on_event(Rang)]
//!     fn rang(&self, _event: &Rang, ctx: &SagaContext) -> Commands<Timer> {
//!         Commands::none().to(ctx.correlation_id().clone(), StartTimer)
//!     }
//! }
//! ```
//!

//! # Retry
//!
//! Retrying a command whose stream moved is a decorator around the kernel,
//! not part of this markup. It re-runs the whole pipeline (a fresh
//! instance, the replay, the handler and its steps), which is safe because
//! they are pure and the router's applies are a fold of what they
//! returned.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as Tokens};
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream, Parser};
use syn::punctuated::Punctuated;
use syn::{
    Error, Expr, GenericArgument, Ident, ImplItem, ItemImpl, PathArguments, ReturnType, Signature,
    Token, Type, Visibility,
};

/// Marks an inherent impl as an aggregate, and generates its annals
/// `State` and `Domain` impls, its `<Name>Handler` trait and its
/// constants (see the crate docs).
///
/// Arguments: `domain = "<name>"` (the `Domain::NAME`); optionally
/// `snapshot = <prost message>`, `rejection = <type>` (inferred from the
/// command handlers without `refuses` when absent) and `vis =
/// <visibility>` (the trait's; `pub` when absent).
#[proc_macro_attribute]
pub fn aggregate(attr: TokenStream, item: TokenStream) -> TokenStream {
    stub(attr.into(), item.into(), Kind::Aggregate)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Marks an inherent impl as a PROCESS MANAGER: a domain, marked up as an
/// aggregate is (the same arguments, markers and expansion), plus
/// `markup::ProcessManager`: its root is the correlation, and only an
/// input marked `starts` may start an instance (see the crate docs).
#[proc_macro_attribute]
pub fn process_manager(attr: TokenStream, item: TokenStream) -> TokenStream {
    stub(attr.into(), item.into(), Kind::ProcessManager)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Marks a command handler (fallible): `#[handles_cmd(Cmd)]`, with
/// optionally `refuses(T, ..)` (its typed refusal objects) and, on a
/// process manager, `starts`. Inert: [`aggregate`] and [`process_manager`]
/// read it.
#[proc_macro_attribute]
pub fn handles_cmd(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Marks a fact handler (infallible: no `Result`, and a `Record`'s steps
/// none either): `#[handles_fact(Fact)]`, with optionally, on a process
/// manager, `starts`. Inert: [`aggregate`] and [`process_manager`] read
/// it.
#[proc_macro_attribute]
pub fn handles_fact(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// On a domain: the handler of the refusal of the saga command `Cmd` it
/// caused, passed through (`#[on_refused(Cmd)]`, an infallible fact
/// handler of `markup::CommandRefused<Cmd>`). On a saga: the translating
/// callback for `Cmd`, returning a fact of the source. Inert: the
/// component's macro reads it.
#[proc_macro_attribute]
pub fn on_refused(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Marks an event applier. Inert: [`aggregate`] and [`process_manager`]
/// read it.
#[proc_macro_attribute]
pub fn applies(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// angzarr-macros' rejection handler, which annals refuses: a refusal
/// reaches its saga's source as a fact, handled in `#[on_refused]`. (A
/// stub: inert.)
#[proc_macro_attribute]
pub fn rejected(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// angzarr-macros' state factory, which annals refuses: the registry holds
/// the factory. (A stub: inert.)
#[proc_macro_attribute]
pub fn state_factory(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Marks an inherent impl as a SAGA: `#[saga(name = "..", source = <domain>,
/// target = <domain>)]` (see the crate docs). Generates `markup::Saga`:
/// its refusal pass-through, and translate for each command its
/// `#[on_refused]` callbacks name.
#[proc_macro_attribute]
pub fn saga(attr: TokenStream, item: TokenStream) -> TokenStream {
    flow_stub(attr.into(), item.into(), Flow::Saga)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Marks a saga's handler for one event of its source. Inert: [`saga`]
/// reads it.
#[proc_macro_attribute]
pub fn on_event(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Marks an inherent impl as a PROJECTOR (PROPOSAL, red), the one impure
/// kind:
///
/// ```text
/// #[projector(name = "balances", source = Wallet)]
/// impl Balances {
///     #[projects(Deposited)]
///     async fn deposited(&mut self, event: &Deposited, ctx: &ProjectionContext) -> Result<(), Report> { .. }
/// }
/// ```
///
/// One source domain. ONE instance per process (ruling B2), registered
/// with its ports: `registry.projector(Balances::new(port))`. The router
/// runs it AFTER COMMIT, awaited before the call returns, so a failure
/// reaches the caller as `Failure::Projection`, carrying what was
/// committed; its position advances per stream on success only. Unmarked
/// events are skipped. It runs on the async command path, the only one,
/// so nothing guards against blocking on a synchronous path.
///
/// The stub keeps the impl and emits `markup::AsyncProjector` with its
/// name and source, and an empty `PROJECTS`.
#[proc_macro_attribute]
pub fn projector(attr: TokenStream, item: TokenStream) -> TokenStream {
    flow_stub(attr.into(), item.into(), Flow::Projector)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Marks a projector's handler for one event. Inert: [`projector`] reads
/// it.
#[proc_macro_attribute]
pub fn projects(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Which flow [`flow_stub`] expands.
#[derive(Clone, Copy, PartialEq)]
enum Flow {
    Saga,
    Projector,
}

/// One `key = value` argument of `#[saga]` or `#[projector]`.
struct FlowArg {
    key: Ident,
    value: Expr,
}

impl Parse for FlowArg {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let key: Ident = input.parse()?;
        input.parse::<Token![=]>()?;
        Ok(FlowArg {
            key,
            value: input.parse()?,
        })
    }
}

/// The stub expansion of a saga or a projector: the impl as given (its
/// markers inert), its trait with the name, the domains and empty
/// constants. It dispatches nothing.
fn flow_stub(attr: Tokens, item: Tokens, flow: Flow) -> syn::Result<Tokens> {
    let args = Punctuated::<FlowArg, Token![,]>::parse_terminated.parse2(attr)?;
    let (mut name, mut source, mut target) = (None, None, None);
    for FlowArg { key, value } in args {
        match (key.to_string().as_str(), flow) {
            ("name", _) => name = Some(value),
            ("source", _) => source = Some(value),
            ("target", Flow::Saga) => target = Some(value),
            _ => return Err(Error::new_spanned(key, usage(flow))),
        }
    }
    let name = name.ok_or_else(|| Error::new(Span::call_site(), usage(flow)))?;
    let source = source.ok_or_else(|| Error::new(Span::call_site(), usage(flow)))?;
    let imp: ItemImpl = syn::parse2(item)?;
    let self_ty = &imp.self_ty;
    let generated = match flow {
        Flow::Saga => {
            let target = target.ok_or_else(|| Error::new(Span::call_site(), usage(flow)))?;
            quote! {
                impl ::annals::next::markup::Saga for #self_ty {
                    const NAME: &'static str = #name;
                    type Source = #source;
                    type Target = #target;
                    const EVENTS: &'static [::annals::next::markup::TypeName] = &[];
                    const TRANSLATES: &'static [::annals::next::markup::TypeName] = &[];

                    fn on_refused(
                        &self,
                        _refusal: &::annals::next::markup::v1::Refusal,
                    ) -> ::annals::next::Any {
                        ::core::todo!("{}", #STUB)
                    }
                }
            }
        }
        Flow::Projector => quote! {
            impl ::annals::next::markup::AsyncProjector for #self_ty {
                const NAME: &'static str = #name;
                type Source = #source;
            }

            impl #self_ty {
                /// The projected events, in marker order.
                pub const PROJECTS: &'static [::annals::next::markup::TypeName] = &[];
            }
        },
    };
    Ok(quote! {
        // The stub calls no marked method.
        #[allow(dead_code)]
        #imp

        #generated
    })
}

/// What a flow's arguments are.
fn usage(flow: Flow) -> &'static str {
    match flow {
        Flow::Saga => "#[saga] takes `name = \"..\"`, `source = <domain>` and `target = <domain>`",
        Flow::Projector => "#[projector] takes `name = \"..\"` and `source = <aggregate>`",
    }
}

const STUB: &str = "annals-macros is a stub";

/// Which domain [`stub`] expands.
#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Aggregate,
    ProcessManager,
}

/// One `key = value` argument of `#[aggregate]` or `#[process_manager]`.
enum Arg {
    Domain(Expr),
    Snapshot(Type),
    Rejection(Type),
    Vis(Visibility),
}

impl Parse for Arg {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let key: Ident = input.parse()?;
        input.parse::<Token![=]>()?;
        match key.to_string().as_str() {
            "domain" => Ok(Arg::Domain(input.parse()?)),
            "snapshot" => Ok(Arg::Snapshot(input.parse()?)),
            "rejection" => Ok(Arg::Rejection(input.parse()?)),
            "vis" => Ok(Arg::Vis(input.parse()?)),
            _ => Err(Error::new_spanned(
                key,
                "takes `domain = \"..\"`, and optionally `snapshot = <message>`, \
                 `rejection = <type>` and `vis = <visibility>`",
            )),
        }
    }
}

/// A handler marker's arguments: `#[handles_cmd(Cmd, starts, refuses(T,
/// ..))]`, every one after the type optional.
struct Marker {
    ty: Type,
    starts: bool,
    refuses: Vec<Type>,
}

impl Parse for Marker {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let ty: Type = input.parse()?;
        let (mut starts, mut refuses) = (false, Vec::new());
        while !input.is_empty() {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            let key: Ident = input.parse()?;
            match key.to_string().as_str() {
                "starts" => starts = true,
                "refuses" => {
                    let types;
                    syn::parenthesized!(types in input);
                    refuses.extend(Punctuated::<Type, Token![,]>::parse_terminated(&types)?);
                }
                _ => return Err(Error::new_spanned(
                    key,
                    "a handler marker takes its type, then optionally `starts` and `refuses(..)`",
                )),
            }
        }
        Ok(Marker {
            ty,
            starts,
            refuses,
        })
    }
}

/// The `R` of `-> Result<_, R>`, if the signature returns a `Result`.
fn rejection_of(sig: &Signature) -> Option<Type> {
    let ReturnType::Type(_, ty) = &sig.output else {
        return None;
    };
    let Type::Path(path) = &**ty else {
        return None;
    };
    let last = path.path.segments.last()?;
    if last.ident != "Result" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &last.arguments else {
        return None;
    };
    match args.args.iter().nth(1)? {
        GenericArgument::Type(rejection) => Some(rejection.clone()),
        _ => None,
    }
}

/// `tokens` with every `Self` replaced by `self_ty`.
fn own_type(tokens: Tokens, self_ty: &Type) -> Tokens {
    use proc_macro2::{Group, TokenTree};
    tokens
        .into_iter()
        .map(|token| match token {
            TokenTree::Ident(ident) if ident == "Self" => quote!(#self_ty),
            TokenTree::Group(group) => {
                let mut inner = Group::new(group.delimiter(), own_type(group.stream(), self_ty));
                inner.set_span(group.span());
                TokenTree::Group(inner).into()
            }
            other => other.into(),
        })
        .collect()
}

/// The last segment of a type's path, as an identifier part.
fn last_ident(ty: &Type) -> Option<&Ident> {
    match ty {
        Type::Path(path) => path.path.segments.last().map(|s| &s.ident),
        _ => None,
    }
}

/// An empty payload set: `Payload` with `todo!()` bodies.
fn empty_set(name: &Ident) -> Tokens {
    quote! {
        #[derive(Clone, Debug)]
        pub enum #name {}

        impl ::annals::next::Payload for #name {
            fn to_any(&self) -> ::annals::next::Any {
                match *self {}
            }
            fn from_any(
                _any: &::annals::next::Any,
            ) -> ::core::result::Result<Self, ::annals::next::PayloadError> {
                ::core::todo!("{}", #STUB)
            }
            fn type_urls() -> ::std::vec::Vec<::std::string::String> {
                ::core::todo!("{}", #STUB)
            }
        }
    }
}

/// The stub expansion of an aggregate or a process manager: the impl as
/// given, and every generated item with a `todo!()` body or an empty
/// constant. It reads the arguments, the self type, the marked methods'
/// signatures (for the trait), the markers' types (for `Decides`,
/// `Ingests`, `Refusing` and the `From` impls into the event set) and the
/// first message-only command handler's rejection; it checks nothing,
/// dispatches nothing (its one call is `State::restore` to the user's
/// `restore`), and gives the trait `pub` whatever `vis` says.
fn stub(attr: Tokens, item: Tokens, kind: Kind) -> syn::Result<Tokens> {
    let args = Punctuated::<Arg, Token![,]>::parse_terminated.parse2(attr)?;
    let (mut domain, mut rejection, mut snapshot) = (None, None, None);
    for arg in args {
        match arg {
            Arg::Domain(value) => domain = Some(value),
            Arg::Rejection(value) => rejection = Some(value),
            Arg::Snapshot(value) => snapshot = Some(value),
            // The stub gives the trait `pub` regardless.
            Arg::Vis(vis) => drop(vis),
        }
    }
    let domain = domain.ok_or_else(|| Error::new(Span::call_site(), "needs `domain = \"..\"`"))?;

    let imp: ItemImpl = syn::parse2(item)?;
    let self_ty = &imp.self_ty;
    let name =
        last_ident(self_ty).ok_or_else(|| Error::new_spanned(self_ty, "goes on `impl <Type>`"))?;
    let handler = format_ident!("{}Handler", name);
    let module = format_ident!("__{}_markup", name.to_string().to_lowercase());
    // Each marked method's signature and its marker's arguments.
    let marked_with = |marker: &'static str| {
        imp.items.iter().filter_map(move |item| match item {
            ImplItem::Fn(f) => f
                .attrs
                .iter()
                .find(|a| a.path().is_ident(marker))
                .map(|a| (&f.sig, a.parse_args::<Marker>())),
            _ => None,
        })
    };
    let mut markers = Vec::new();
    for marker in ["handles_cmd", "handles_fact", "on_refused", "applies"] {
        for (sig, args) in marked_with(marker) {
            markers.push((marker, sig, args?));
        }
    }
    let rejection = match rejection {
        Some(rejection) => rejection,
        None => markers
            .iter()
            .filter(|(marker, _, args)| *marker == "handles_cmd" && args.refuses.is_empty())
            .find_map(|(_, sig, _)| rejection_of(sig))
            .unwrap_or_else(|| syn::parse_quote!(::core::convert::Infallible)),
    };
    let rejection = quote!(::annals::next::markup::Rejection<#rejection>);
    // In the trait, `Self` is the implementor: the domain's own type is
    // what the signatures mean.
    let declared = markers
        .iter()
        .map(|(_, sig, _)| own_type(quote!(#sig), self_ty));
    let delegated = markers
        .iter()
        .map(|(_, sig, _)| quote!(#sig { ::core::todo!("{}", #STUB) }));
    let typed = |wanted: &'static str| {
        markers
            .iter()
            .filter(move |(marker, _, _)| *marker == wanted)
            .map(|(_, _, args)| &args.ty)
    };
    let from_events = typed("applies").map(|event| {
        quote! {
            impl ::core::convert::From<#event> for #module::Event {
                fn from(_event: #event) -> Self {
                    ::core::todo!("{}", #STUB)
                }
            }
        }
    });
    let decides = typed("handles_cmd")
        .map(|command| quote!(impl ::annals::next::markup::Decides<#command> for #self_ty {}));
    let ingests = typed("handles_fact")
        .map(|fact| quote!(impl ::annals::next::markup::Ingests<#fact> for #self_ty {}));
    let mut refusing = Vec::new();
    for (_, _, args) in markers
        .iter()
        .filter(|(marker, _, args)| *marker == "handles_cmd" && !args.refuses.is_empty())
    {
        let command = &args.ty;
        let choice = format_ident!(
            "__{}{}Refuses",
            name,
            last_ident(command).ok_or_else(|| Error::new_spanned(command, "names no type"))?
        );
        let variants = args
            .refuses
            .iter()
            .map(|ty| last_ident(ty).ok_or_else(|| Error::new_spanned(ty, "names no type")))
            .collect::<syn::Result<Vec<_>>>()?;
        let types = &args.refuses;
        refusing.push(quote! {
            #[doc(hidden)]
            #[allow(non_camel_case_types, dead_code)]
            pub enum #choice {
                #(#variants(#types),)*
            }

            #(
                impl ::core::convert::From<#types> for #choice {
                    fn from(object: #types) -> Self {
                        #choice::#variants(object)
                    }
                }
            )*

            impl ::core::convert::From<#choice> for ::annals::next::markup::TypedRefusal {
                fn from(_choice: #choice) -> Self {
                    ::core::todo!("{}", #STUB)
                }
            }

            impl ::core::convert::From<#choice> for #rejection {
                fn from(_choice: #choice) -> Self {
                    ::core::todo!("{}", #STUB)
                }
            }

            impl ::annals::next::markup::Refusing<#command> for #self_ty {
                type Refuses = #choice;
                const REFUSES: &'static [::annals::next::markup::TypeName] = &[];
            }
        });
    }
    let (command_set, fact_set, event_set) = (
        empty_set(&format_ident!("Command")),
        empty_set(&format_ident!("Fact")),
        empty_set(&format_ident!("Event")),
    );
    // With a snapshot form, `restore` is the user's (the one call the stub
    // makes); without, there is nothing to restore.
    let (snapshot_ty, snapshots, restore) = match snapshot {
        Some(snapshot) => (
            quote!(#snapshot),
            quote!(),
            quote!(<#self_ty>::restore(self, snapshot)
                .map_err(|error| ::annals::next::markup::Report::of(&error))),
        ),
        None => (
            quote!(::annals::next::markup::NoSnapshot),
            quote!(
                const SNAPSHOTS: bool = false;
            ),
            quote!({
                let _ = snapshot;
                ::core::todo!("{}", #STUB)
            }),
        ),
    };
    // The stub reads which inputs start an instance and does nothing with
    // them.
    let _starting = markers.iter().filter(|(_, _, args)| args.starts).count();
    let process_manager = match kind {
        Kind::Aggregate => quote!(),
        Kind::ProcessManager => quote! {
            impl ::annals::next::markup::ProcessManager for #self_ty {
                const STARTS: &'static [::annals::next::markup::TypeName] = &[];
            }
        },
    };

    Ok(quote! {
        // The stub calls no marked method.
        #[allow(dead_code)]
        #imp

        #[doc(hidden)]
        mod #module {
            #command_set
            #fact_set
            #event_set
        }

        impl ::annals::next::State for #self_ty {
            type Snapshot = #snapshot_ty;

            #snapshots

            fn to_snapshot(&self) -> #snapshot_ty {
                ::core::todo!("{}", #STUB)
            }

            fn from_snapshot(
                _snapshot: #snapshot_ty,
            ) -> ::core::result::Result<Self, ::annals::next::markup::Report> {
                ::core::todo!("{}", #STUB)
            }

            fn restore(
                &mut self,
                snapshot: #snapshot_ty,
            ) -> ::core::result::Result<(), ::annals::next::markup::Report> {
                #restore
            }

            fn descriptor() -> ::annals::next::v1::StateDescriptor {
                ::core::todo!("{}", #STUB)
            }
        }

        #(#from_events)*
        #(#decides)*
        #(#ingests)*
        #(#refusing)*

        impl ::annals::next::Domain for #self_ty {
            type State = Self;
            type Command = #module::Command;
            type Fact = #module::Fact;
            type Event = #module::Event;
            type Rejection = #rejection;
            type Rules = ();

            const NAME: &'static str = #domain;

            fn decide(
                _command: &#module::Command,
                _tx: &mut ::annals::next::Tx<'_, Self>,
            ) -> ::core::result::Result<(), #rejection> {
                ::core::todo!("{}", #STUB)
            }

            fn ingest(
                _state: &Self,
                _fact: &#module::Fact,
                _ctx: &::annals::next::IngestCtx<'_, Self>,
            ) -> ::std::vec::Vec<#module::Event> {
                ::core::todo!("{}", #STUB)
            }

            fn apply(_state: &mut Self, _event: &#module::Event, _rules: &()) {
                ::core::todo!("{}", #STUB)
            }
        }

        #process_manager

        /// The domain's handler set: every marked method.
        pub trait #handler {
            #(#declared;)*
        }

        #[allow(unused_variables)]
        impl #handler for #self_ty {
            #(#delegated)*
        }

        impl #self_ty {
            /// The handled commands and facts, in marker order.
            pub const HANDLES: &'static [::annals::next::markup::TypeName] = &[];
            /// The commands whose refusals it handles (`#[on_refused]`),
            /// in marker order.
            pub const REFUSALS: &'static [::annals::next::markup::TypeName] = &[];
            /// The applied events, in marker order.
            pub const APPLIES: &'static [::annals::next::markup::TypeName] = &[];
        }
    })
}
