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
//! annals' `next/decorated.test.rs` runs the expansion; the
//! `compile_fail` examples here pin what it refuses. annals does not run a
//! saga yet: that comes with the kernel's committed-step dispatch.
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
//! process, each saga hop a step committed on its own.
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
//!   is caught at run time: the steps before it stay committed, and the
//!   refusal fact is dead-lettered and alerted. A `CommandRefused` from outside a saga is
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
use quote::{format_ident, quote, ToTokens};
use syn::parse::{Parse, ParseStream, Parser};
use syn::punctuated::Punctuated;
use syn::{
    Error, Expr, FnArg, GenericArgument, Ident, ImplItem, ImplItemFn, ItemImpl, Pat, PathArguments,
    ReturnType, Signature, Token, Type, Visibility,
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
    domain(attr.into(), item.into(), Kind::Aggregate)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Marks an inherent impl as a PROCESS MANAGER: a domain, marked up as an
/// aggregate is (the same arguments, markers and expansion), plus
/// `markup::ProcessManager`: its root is the correlation, and only an
/// input marked `starts` may start an instance (see the crate docs).
#[proc_macro_attribute]
pub fn process_manager(attr: TokenStream, item: TokenStream) -> TokenStream {
    domain(attr.into(), item.into(), Kind::ProcessManager)
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
/// reaches its saga's source as a fact, handled in `#[on_refused]`.
#[proc_macro_attribute]
pub fn rejected(_attr: TokenStream, _item: TokenStream) -> TokenStream {
    Error::new(
        Span::call_site(),
        "annals has no #[rejected]: a refusal reaches the saga's source as a fact, \
         handled in #[on_refused(Cmd)]",
    )
    .into_compile_error()
    .into()
}

/// angzarr-macros' state factory, which annals refuses: the registry holds
/// the factory.
#[proc_macro_attribute]
pub fn state_factory(_attr: TokenStream, _item: TokenStream) -> TokenStream {
    Error::new(
        Span::call_site(),
        "annals has no #[state_factory]: register the factory, \
         `registry.aggregate::<A>(factory)`",
    )
    .into_compile_error()
    .into()
}

/// Marks an inherent impl as a SAGA: `#[saga(name = "..", source = <domain>,
/// target = <domain>)]` (see the crate docs). Generates `markup::Saga`:
/// its refusal pass-through, and translate for each command its
/// `#[on_refused]` callbacks name.
#[proc_macro_attribute]
pub fn saga(attr: TokenStream, item: TokenStream) -> TokenStream {
    flow(attr.into(), item.into(), Flow::Saga)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Marks a saga's handler for one event of its source. Inert: [`saga`]
/// reads it.
#[proc_macro_attribute]
pub fn on_event(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Marks an inherent impl as a PROJECTOR, the one impure kind:
///
/// ```text
/// #[projector(name = "balances", source = Wallet)]
/// impl Balances {
///     #[projects(Deposited)]
///     async fn deposited(&mut self, event: &Deposited, ctx: &ProjectionContext) -> Result<(), Report> { .. }
/// }
/// ```
///
/// One source domain. ONE instance per process, registered with its
/// ports: `registry.projector(Balances::new(port))`. The kernel's async
/// path runs it AFTER COMMIT, awaited before the call returns, so a
/// failure reaches the caller as `Failure::Projection`, carrying what was
/// committed, and does not stop the other projectors on the domain.
/// Unmarked events are skipped. It runs on the async command path, the
/// only one, so nothing guards against blocking on a synchronous path.
#[proc_macro_attribute]
pub fn projector(attr: TokenStream, item: TokenStream) -> TokenStream {
    flow(attr.into(), item.into(), Flow::Projector)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Marks a projector's handler for one event. Inert: [`projector`] reads
/// it.
#[proc_macro_attribute]
pub fn projects(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

const MARKUP: &str = "::annals::next::markup";

/// `::annals::next::markup`, as a path.
fn markup() -> Tokens {
    MARKUP.parse().expect("a path")
}

/// Which flow [`flow`] expands.
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
        if !input.peek(Token![=]) {
            return Err(Error::new_spanned(
                key,
                "#[saga] and #[projector] take `key = value` arguments only; a saga's \
                 translate mode is its `#[on_refused(Cmd)]` callback's presence",
            ));
        }
        input.parse::<Token![=]>()?;
        Ok(FlowArg {
            key,
            value: input.parse()?,
        })
    }
}

/// What a flow's arguments are.
fn usage(flow: Flow) -> &'static str {
    match flow {
        Flow::Saga => "#[saga] takes `name = \"..\"`, `source = <domain>` and `target = <domain>`",
        Flow::Projector => "#[projector] takes `name = \"..\"` and `source = <aggregate>`",
    }
}

/// A marked method: its marker's name, the method, and the marker's type.
struct Marked<'a> {
    marker: &'static str,
    method: &'a ImplItemFn,
    args: Marker,
}

/// The methods of `imp` marked with one of `markers`, in impl order. A
/// method with two markers is refused.
fn marked<'a>(imp: &'a ItemImpl, markers: &[&'static str]) -> syn::Result<Vec<Marked<'a>>> {
    let mut found = Vec::new();
    for item in &imp.items {
        let ImplItem::Fn(method) = item else {
            continue;
        };
        let mut on = method.attrs.iter().filter_map(|a| {
            markers
                .iter()
                .find(|m| a.path().is_ident(m))
                .map(|m| (*m, a))
        });
        let Some((marker, attr)) = on.next() else {
            continue;
        };
        if let Some((_, second)) = on.next() {
            return Err(Error::new_spanned(second, "a method takes one marker"));
        }
        found.push(Marked {
            marker,
            method,
            args: attr.parse_args::<Marker>()?,
        });
    }
    Ok(found)
}

/// The expansion of a saga or a projector: the impl as given (its markers
/// inert) and its trait.
fn flow(attr: Tokens, item: Tokens, flow: Flow) -> syn::Result<Tokens> {
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
    let m = markup();
    let generated = match flow {
        Flow::Saga => {
            let target = target.ok_or_else(|| Error::new(Span::call_site(), usage(flow)))?;
            let methods = marked(&imp, &["on_event", "on_refused"])?;
            let events: Vec<_> = methods.iter().filter(|x| x.marker == "on_event").collect();
            let callbacks: Vec<_> = methods
                .iter()
                .filter(|x| x.marker == "on_refused")
                .collect();
            for x in &methods {
                refuse_extras(x, false)?;
            }
            let event_types: Vec<_> = events.iter().map(|x| &x.args.ty).collect();
            let event_methods: Vec<_> = events.iter().map(|x| &x.method.sig.ident).collect();
            let commands: Vec<_> = callbacks.iter().map(|x| &x.args.ty).collect();
            let callback_methods: Vec<_> = callbacks.iter().map(|x| &x.method.sig.ident).collect();
            quote! {
                impl #m::Saga for #self_ty {
                    const NAME: &'static str = #name;
                    type Source = #source;
                    type Target = #target;
                    const EVENTS: &'static [#m::TypeName] = &[
                        #(#m::TypeName::of::<<#event_types as #m::DomainMessage>::Wire>(),)*
                    ];
                    const TRANSLATES: &'static [#m::TypeName] = &[
                        #(#m::TypeName::of::<<#commands as #m::DomainMessage>::Wire>(),)*
                    ];

                    fn on_refused(&self, refusal: &#m::v1::Refusal) -> ::annals::next::Any {
                        #(
                            if let ::core::option::Option::Some(refusal) =
                                #m::__translating::<#commands>(refusal)
                            {
                                return #m::__domestic::<<Self as #m::Saga>::Source, _>(
                                    &<#self_ty>::#callback_methods(self, &refusal),
                                );
                            }
                        )*
                        #m::__pass_through(refusal)
                    }

                    fn on_event(
                        &self,
                        event: &::annals::next::Any,
                        ctx: &#m::SagaContext,
                    ) -> ::core::option::Option<#m::Commands<<Self as #m::Saga>::Target>> {
                        #(
                            if let ::core::result::Result::Ok(::core::option::Option::Some(event)) =
                                #m::__decode::<#event_types>(event)
                            {
                                return ::core::option::Option::Some(
                                    <#self_ty>::#event_methods(self, &event, ctx),
                                );
                            }
                        )*
                        let _ = (event, ctx);
                        ::core::option::Option::None
                    }
                }
            }
        }
        Flow::Projector => {
            let methods = marked(&imp, &["projects"])?;
            for x in &methods {
                refuse_extras(x, false)?;
            }
            let types: Vec<_> = methods.iter().map(|x| &x.args.ty).collect();
            let names: Vec<_> = methods.iter().map(|x| &x.method.sig.ident).collect();
            quote! {
                impl #m::AsyncProjector for #self_ty {
                    const NAME: &'static str = #name;
                    type Source = #source;
                    const PROJECTS: &'static [#m::TypeName] = &[
                        #(#m::TypeName::of::<<#types as #m::DomainMessage>::Wire>(),)*
                    ];

                    fn project<'a>(
                        &'a mut self,
                        event: &'a ::annals::next::Any,
                        ctx: &'a #m::ProjectionContext,
                    ) -> ::core::option::Option<#m::Projecting<'a>> {
                        #(
                            match #m::__decode::<#types>(event) {
                                ::core::result::Result::Ok(::core::option::Option::Some(event)) => {
                                    return ::core::option::Option::Some(#m::__projecting(
                                        async move { <#self_ty>::#names(self, &event, ctx).await },
                                    ));
                                }
                                ::core::result::Result::Err(error) => {
                                    return ::core::option::Option::Some(#m::__projecting(
                                        async move { ::core::result::Result::Err(#m::Report::of(&error)) },
                                    ));
                                }
                                ::core::result::Result::Ok(::core::option::Option::None) => {}
                            }
                        )*
                        let _ = (event, ctx);
                        ::core::option::Option::None
                    }
                }
            }
        }
    };
    Ok(quote! {
        #imp

        #generated
    })
}

/// Which domain [`domain`] expands.
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

/// A marker's arguments: `#[handles_cmd(Cmd, starts, refuses(T, ..))]`,
/// every one after the type optional.
struct Marker {
    ty: Type,
    starts: Option<Ident>,
    refuses: Vec<Type>,
    refuses_key: Option<Ident>,
}

impl Parse for Marker {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let ty: Type = input.parse()?;
        let mut marker = Marker {
            ty,
            starts: None,
            refuses: Vec::new(),
            refuses_key: None,
        };
        while !input.is_empty() {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            let key: Ident = input.parse()?;
            match key.to_string().as_str() {
                "starts" => marker.starts = Some(key),
                "refuses" => {
                    let types;
                    syn::parenthesized!(types in input);
                    marker
                        .refuses
                        .extend(Punctuated::<Type, Token![,]>::parse_terminated(&types)?);
                    marker.refuses_key = Some(key);
                }
                _ => return Err(Error::new_spanned(
                    key,
                    "a handler marker takes its type, then optionally `starts` and `refuses(..)`",
                )),
            }
        }
        Ok(marker)
    }
}

/// Refuse `starts` (unless `starts_allowed`) and `refuses` where they mean
/// nothing.
fn refuse_extras(x: &Marked<'_>, starts_allowed: bool) -> syn::Result<()> {
    let starts_allowed = starts_allowed && matches!(x.marker, "handles_cmd" | "handles_fact");
    if let (Some(starts), false) = (&x.args.starts, starts_allowed) {
        return Err(Error::new_spanned(
            starts,
            match x.marker {
                "handles_cmd" | "handles_fact" => {
                    "`starts` is a process manager's: only its inputs start an instance; \
                     an aggregate's handler takes none"
                }
                _ => "`starts` marks a process manager's command or fact handler only",
            },
        ));
    }
    if let (Some(refuses), true) = (&x.args.refuses_key, x.marker != "handles_cmd") {
        return Err(Error::new_spanned(
            refuses,
            "`refuses` names a command handler's typed refusal objects: a fact is never refused",
        ));
    }
    Ok(())
}

/// The last segment of a type's path.
fn last_ident(ty: &Type) -> Option<&Ident> {
    match ty {
        Type::Path(path) => path.path.segments.last().map(|s| &s.ident),
        _ => None,
    }
}

/// The outer type's name and its generic arguments, for a signature's
/// return type: `Result<A, B>` is `("Result", [A, B])`.
fn returned(sig: &Signature) -> Option<(String, Vec<Type>)> {
    let ReturnType::Type(_, ty) = &sig.output else {
        return None;
    };
    let Type::Path(path) = &**ty else {
        return None;
    };
    let last = path.path.segments.last()?;
    let args = match &last.arguments {
        PathArguments::AngleBracketed(args) => args
            .args
            .iter()
            .filter_map(|a| match a {
                GenericArgument::Type(t) => Some(t.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    Some((last.ident.to_string(), args))
}

/// The receiver a method takes: `Some(true)` for `&mut self`, `Some(false)`
/// for `&self`, `None` for anything else.
fn receiver(sig: &Signature) -> Option<bool> {
    match sig.inputs.first()? {
        FnArg::Receiver(r) if r.reference.is_some() => Some(r.mutability.is_some()),
        _ => None,
    }
}

/// Hold each marked method's signature to its marker (see the crate docs).
fn check_signature(x: &Marked<'_>) -> syn::Result<()> {
    let sig = &x.method.sig;
    let fail = |message: &str| Err(Error::new_spanned(sig, message));
    match x.marker {
        "applies" => {
            if receiver(sig) != Some(true) {
                return fail("an applier takes `&mut self`: it assigns the event's state");
            }
            return Ok(());
        }
        _ => {
            if receiver(sig) != Some(false) {
                return fail(
                    "a handler takes `&self`: it decides, and only the router's appliers \
                     change the instance",
                );
            }
        }
    }
    let output = returned(sig);
    let outer = output.as_ref().map(|(name, _)| name.as_str());
    match x.marker {
        "handles_cmd" => {
            let Some((_, args)) = output.as_ref().filter(|(name, _)| name == "Result") else {
                return fail(
                    "a command handler returns `Result<_, Rejection>`: a command may be refused",
                );
            };
            if !x.args.refuses.is_empty()
                && args
                    .get(1)
                    .and_then(last_ident)
                    .is_none_or(|e| e != "Refuses")
            {
                return fail(
                    "a command handler with `refuses(..)` returns `Result<_, Refuses<Self, Cmd>>`",
                );
            }
            Ok(())
        }
        _ if outer == Some("Result") => fail(
            "a fact handler returns no `Result`: a fact cannot be refused \
             (`#[on_refused]` on a domain is a fact handler)",
        ),
        _ if outer == Some("Emit") => {
            fail("a fact handler returns a `Record`, not an `Emit`, whose steps may refuse")
        }
        _ => Ok(()),
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

/// `sig` with its arguments named `arg0`, `arg1`, ..: the handler trait's
/// signature, and the arguments to forward.
fn renamed(sig: &Signature) -> (Signature, Vec<Ident>) {
    let mut sig = sig.clone();
    let mut names = Vec::new();
    for (n, input) in sig.inputs.iter_mut().enumerate() {
        if let FnArg::Typed(typed) = input {
            let name = format_ident!("arg{}", n);
            *typed.pat = Pat::Verbatim(quote!(#name));
            names.push(name);
        }
    }
    (sig, names)
}

/// A payload enum: one variant per type, with its `Payload` impl. `extra`
/// are further variants (a refusal handler's), each with its type, how it
/// is decoded and how it is packed.
fn payload_set(
    name: &Ident,
    variants: &[(Ident, Type)],
    refused: &[(Ident, Type)],
    vis: &Visibility,
) -> Tokens {
    let m = markup();
    let (idents, types): (Vec<_>, Vec<_>) = variants.iter().cloned().unzip();
    let (refused_idents, refused_types): (Vec<_>, Vec<_>) = refused.iter().cloned().unzip();
    let packing = if variants.is_empty() && refused.is_empty() {
        quote!(match *self {})
    } else {
        quote! {
            match self {
                #(#name::#idents(value) => #m::__pack(value),)*
                #(#name::#refused_idents(_, any) => ::core::clone::Clone::clone(any),)*
            }
        }
    };
    let refusal_url = (!refused.is_empty()).then(|| quote!(#m::__command_refused_url(),));
    quote! {
        #[doc(hidden)]
        #[derive(Clone, Debug)]
        #[allow(non_camel_case_types)]
        #vis enum #name {
            #(#idents(#types),)*
            #(#refused_idents(#m::CommandRefused<#refused_types>, ::annals::next::Any),)*
        }

        impl ::annals::next::Payload for #name {
            fn to_any(&self) -> ::annals::next::Any {
                #packing
            }

            fn from_any(
                any: &::annals::next::Any,
            ) -> ::core::result::Result<Self, ::annals::next::PayloadError> {
                #(
                    if let ::core::option::Option::Some(value) = #m::__decode::<#types>(any)? {
                        return ::core::result::Result::Ok(#name::#idents(value));
                    }
                )*
                #(
                    if let ::core::option::Option::Some(refused) =
                        #m::__refused::<#refused_types>(any)?
                    {
                        return ::core::result::Result::Ok(
                            #name::#refused_idents(refused, ::core::clone::Clone::clone(any)),
                        );
                    }
                )*
                ::core::result::Result::Err(::annals::next::PayloadError::UnknownType {
                    type_url: #m::__refused_command_url(any)
                        .unwrap_or_else(|| ::core::clone::Clone::clone(&any.type_url)),
                })
            }

            fn type_urls() -> ::std::vec::Vec<::std::string::String> {
                ::std::vec![#(#m::__type_url::<#types>(),)* #refusal_url]
            }
        }
    }
}

/// A variant's name for `ty`: its last segment, or `prefix` with it.
fn variant(ty: &Type, prefix: &str) -> syn::Result<Ident> {
    last_ident(ty)
        .map(|ident| format_ident!("{}{}", prefix, ident))
        .ok_or_else(|| Error::new_spanned(ty, "names no type"))
}

/// The expansion of an aggregate or a process manager (see the crate
/// docs).
fn domain(attr: Tokens, item: Tokens, kind: Kind) -> syn::Result<Tokens> {
    let args = Punctuated::<Arg, Token![,]>::parse_terminated.parse2(attr)?;
    let (mut domain, mut rejection, mut snapshot) = (None, None, None);
    let mut vis: Visibility = syn::parse_quote!(pub);
    for arg in args {
        match arg {
            Arg::Domain(value) => domain = Some(value),
            Arg::Rejection(value) => rejection = Some(value),
            Arg::Snapshot(value) => snapshot = Some(value),
            Arg::Vis(value) => vis = value,
        }
    }
    let domain = domain.ok_or_else(|| Error::new(Span::call_site(), "needs `domain = \"..\"`"))?;
    let imp: ItemImpl = syn::parse2(item)?;
    let self_ty = &imp.self_ty;
    let name = last_ident(self_ty)
        .ok_or_else(|| Error::new_spanned(self_ty, "goes on `impl <Type>`"))?
        .clone();
    let m = markup();
    let methods = marked(
        &imp,
        &["handles_cmd", "handles_fact", "on_refused", "applies"],
    )?;

    // Each signature held to its marker; one handler per type.
    let mut seen: Vec<(&'static str, String)> = Vec::new();
    for x in &methods {
        refuse_extras(x, kind == Kind::ProcessManager)?;
        check_signature(x)?;
        let set = match x.marker {
            "handles_cmd" | "handles_fact" => "input",
            other => other,
        };
        let key = (set, x.args.ty.to_token_stream().to_string());
        if seen.contains(&key) {
            return Err(Error::new_spanned(
                &x.args.ty,
                "a second handler for this type: which one decides is not a question the \
                 markup leaves open",
            ));
        }
        seen.push(key);
    }
    let of = |marker: &'static str| methods.iter().filter(move |x| x.marker == marker);

    // The domain's own rejection: given, or the message-only command
    // handlers', which must agree.
    let rejection = match rejection {
        Some(rejection) => rejection,
        None => {
            let mut found: Option<Type> = None;
            for x in of("handles_cmd").filter(|x| x.args.refuses.is_empty()) {
                let Some(error) =
                    returned(&x.method.sig).and_then(|(_, args)| args.get(1).cloned())
                else {
                    continue;
                };
                match &found {
                    None => found = Some(error),
                    Some(first)
                        if first.to_token_stream().to_string()
                            != error.to_token_stream().to_string() =>
                    {
                        return Err(Error::new_spanned(
                            &x.method.sig.output,
                            format!(
                                "the command handlers disagree on the rejection: `{}` here, \
                                 `{}` before; the domain has one (or give `rejection = ..`)",
                                error.to_token_stream(),
                                first.to_token_stream()
                            ),
                        ));
                    }
                    Some(_) => {}
                }
            }
            found.unwrap_or_else(|| syn::parse_quote!(::core::convert::Infallible))
        }
    };
    let rejection_ty = quote!(#m::Rejection<#rejection>);

    let command_set = format_ident!("__{}Command", name);
    let fact_set = format_ident!("__{}Fact", name);
    let event_set = format_ident!("__{}Event", name);
    let typed = |marker: &'static str, prefix: &str| -> syn::Result<Vec<(Ident, Type)>> {
        of(marker)
            .map(|x| Ok((variant(&x.args.ty, prefix)?, x.args.ty.clone())))
            .collect()
    };
    let commands = typed("handles_cmd", "")?;
    let facts = typed("handles_fact", "")?;
    let refusals = typed("on_refused", "Refused")?;
    let events = typed("applies", "")?;
    let sets = [
        payload_set(&command_set, &commands, &[], &vis),
        payload_set(&fact_set, &facts, &refusals, &vis),
        payload_set(&event_set, &events, &[], &vis),
    ];

    let call = |x: &Marked<'_>| x.method.sig.ident.clone();
    let command_arms = of("handles_cmd").map(|x| {
        let (v, method) = (variant(&x.args.ty, "").expect("checked"), call(x));
        quote! {
            #command_set::#v(input) => {
                let handled = <#self_ty>::#method(tx.state(), ::core::clone::Clone::clone(input), &ctx);
                #m::__decide(tx, &ctx, handled)
            }
        }
    });
    let fact_arms = of("handles_fact").map(|x| {
        let (v, method) = (variant(&x.args.ty, "").expect("checked"), call(x));
        quote! {
            #fact_set::#v(input) => #m::__ingest(state, &ctx, <#self_ty>::#method(state, ::core::clone::Clone::clone(input), &ctx)),
        }
    });
    let refusal_arms = of("on_refused").map(|x| {
        let (v, method) = (variant(&x.args.ty, "Refused").expect("checked"), call(x));
        quote! {
            #fact_set::#v(refused, _) => #m::__ingest(state, &ctx, <#self_ty>::#method(state, ::core::clone::Clone::clone(refused), &ctx)),
        }
    });
    let apply_arms = of("applies").map(|x| {
        let (v, method) = (variant(&x.args.ty, "").expect("checked"), call(x));
        quote! {
            #event_set::#v(event) => <#self_ty>::#method(state, ::core::clone::Clone::clone(event)),
        }
    });
    let decide_body = if commands.is_empty() {
        quote!(match *command {})
    } else {
        quote!(match command { #(#command_arms)* })
    };
    let ingest_body = if facts.is_empty() && refusals.is_empty() {
        quote!(match *fact {})
    } else {
        quote!(match fact { #(#fact_arms)* #(#refusal_arms)* })
    };
    let apply_body = if events.is_empty() {
        quote!(match *event {})
    } else {
        quote!(match event { #(#apply_arms)* })
    };

    // A process manager's start rule, before any handler.
    let (decide_start, ingest_start, process_manager) = match kind {
        Kind::Aggregate => (quote!(), quote!(), quote!()),
        Kind::ProcessManager => {
            let starting = |marker: &'static str, set: &Ident, refused: bool| {
                let arms = of(marker).map(|x| {
                    let v = variant(&x.args.ty, "").expect("checked");
                    let starts = x.args.starts.is_some();
                    quote!(#set::#v(..) => #starts,)
                });
                let refused = refused.then(|| {
                    let arms = of("on_refused").map(|x| {
                        let v = variant(&x.args.ty, "Refused").expect("checked");
                        quote!(#set::#v(..) => false,)
                    });
                    quote!(#(#arms)*)
                });
                quote!(#(#arms)* #refused)
            };
            let command_starts = starting("handles_cmd", &command_set, false);
            let fact_starts = starting("handles_fact", &fact_set, true);
            let starts = methods
                .iter()
                .filter(|x| x.args.starts.is_some())
                .map(|x| &x.args.ty);
            (
                if commands.is_empty() {
                    quote!()
                } else {
                    quote! {
                        if !#m::__decided_instance(tx) && !(match command { #command_starts }) {
                            return ::core::result::Result::Err(#m::__no_instance(tx));
                        }
                    }
                },
                if facts.is_empty() && refusals.is_empty() {
                    quote!()
                } else {
                    quote! {
                        if !#m::__ingested_instance(ctx) && !(match fact { #fact_starts }) {
                            return ::std::vec::Vec::new();
                        }
                    }
                },
                quote! {
                    impl #m::ProcessManager for #self_ty {
                        const STARTS: &'static [#m::TypeName] = &[
                            #(#m::TypeName::of::<<#starts as #m::DomainMessage>::Wire>(),)*
                        ];
                    }
                },
            )
        }
    };

    let from_events = events.iter().map(|(v, ty)| {
        quote! {
            impl ::core::convert::From<#ty> for #event_set {
                fn from(event: #ty) -> Self {
                    #event_set::#v(event)
                }
            }
        }
    });
    let decides = commands
        .iter()
        .map(|(_, ty)| quote!(impl #m::Decides<#ty> for #self_ty {}));
    let ingests = facts
        .iter()
        .map(|(_, ty)| quote!(impl #m::Ingests<#ty> for #self_ty {}));
    let mut refusing = Vec::new();
    for x in of("handles_cmd").filter(|x| !x.args.refuses.is_empty()) {
        let command = &x.args.ty;
        let choice = format_ident!("__{}{}Refuses", name, variant(command, "")?);
        let variants = x
            .args
            .refuses
            .iter()
            .map(|ty| variant(ty, ""))
            .collect::<syn::Result<Vec<_>>>()?;
        let types = &x.args.refuses;
        refusing.push(quote! {
            #[doc(hidden)]
            #[allow(non_camel_case_types)]
            #vis enum #choice {
                #(#variants(#types),)*
            }

            #(
                impl ::core::convert::From<#types> for #choice {
                    fn from(object: #types) -> Self {
                        #choice::#variants(object)
                    }
                }
            )*

            impl ::core::convert::From<#choice> for #m::TypedRefusal {
                fn from(choice: #choice) -> Self {
                    match choice {
                        #(#choice::#variants(object) => #m::TypedRefusal::of(&object),)*
                    }
                }
            }

            impl ::core::convert::From<#choice> for #rejection_ty {
                fn from(choice: #choice) -> Self {
                    #m::Rejection::Typed(::core::convert::Into::into(choice))
                }
            }

            impl #m::Refusing<#command> for #self_ty {
                type Refuses = #choice;
                const REFUSES: &'static [#m::TypeName] = &[
                    #(#m::TypeName::of::<<#types as #m::DomainMessage>::Wire>(),)*
                ];
            }
        });
    }

    // The snapshot form: the user's conversions, or none.
    let state = match snapshot {
        Some(snapshot) => quote! {
            impl ::annals::next::State for #self_ty {
                type Snapshot = #snapshot;

                fn to_snapshot(&self) -> #snapshot {
                    <#snapshot as ::core::convert::From<&#self_ty>>::from(self)
                }

                fn from_snapshot(
                    snapshot: #snapshot,
                ) -> ::core::result::Result<Self, #m::Report> {
                    let mut state = <#self_ty as ::core::default::Default>::default();
                    ::annals::next::State::restore(&mut state, snapshot)?;
                    ::core::result::Result::Ok(state)
                }

                fn restore(
                    &mut self,
                    snapshot: #snapshot,
                ) -> ::core::result::Result<(), #m::Report> {
                    <#self_ty>::restore(self, snapshot).map_err(|error| #m::Report::of(&error))
                }

                fn descriptor() -> ::annals::next::v1::StateDescriptor {
                    #m::__descriptor::<#snapshot>()
                }
            }
        },
        None => quote! {
            impl ::annals::next::State for #self_ty {
                type Snapshot = #m::NoSnapshot;

                const SNAPSHOTS: bool = false;

                fn to_snapshot(&self) -> #m::NoSnapshot {
                    #m::NoSnapshot {}
                }

                fn from_snapshot(
                    _snapshot: #m::NoSnapshot,
                ) -> ::core::result::Result<Self, #m::Report> {
                    ::core::result::Result::Ok(<#self_ty as ::core::default::Default>::default())
                }

                fn restore(
                    &mut self,
                    _snapshot: #m::NoSnapshot,
                ) -> ::core::result::Result<(), #m::Report> {
                    ::core::result::Result::Ok(())
                }

                fn descriptor() -> ::annals::next::v1::StateDescriptor {
                    #m::__descriptor::<#m::NoSnapshot>()
                }
            }
        },
    };

    // The handler trait: every marked method, forwarded.
    let handler = format_ident!("{}Handler", name);
    let (declared, forwarded): (Vec<_>, Vec<_>) = methods
        .iter()
        .map(|x| {
            let (sig, names) = renamed(&x.method.sig);
            let method = &sig.ident;
            let declared = own_type(quote!(#sig), self_ty);
            let forwarded = quote!(#declared { <#self_ty>::#method(self, #(#names),*) });
            (declared, forwarded)
        })
        .unzip();
    let constant = |marker: &'static str| {
        let types: Vec<_> = of(marker).map(|x| &x.args.ty).collect();
        quote!(&[#(#m::TypeName::of::<<#types as #m::DomainMessage>::Wire>(),)*])
    };
    let handled: Vec<_> = methods
        .iter()
        .filter(|x| x.marker == "handles_cmd" || x.marker == "handles_fact")
        .map(|x| &x.args.ty)
        .collect();
    let refusal_constant = constant("on_refused");
    let applies_constant = constant("applies");

    Ok(quote! {
        #imp

        #(#sets)*

        #state

        #(#from_events)*
        #(#decides)*
        #(#ingests)*
        #(#refusing)*

        impl ::annals::next::Domain for #self_ty {
            type State = Self;
            type Command = #command_set;
            type Fact = #fact_set;
            type Event = #event_set;
            type Rejection = #rejection_ty;
            type Rules = ();

            const NAME: &'static str = #domain;

            fn decide(
                command: &#command_set,
                tx: &mut ::annals::next::Tx<'_, Self>,
            ) -> ::core::result::Result<(), #rejection_ty> {
                #decide_start
                let ctx = #m::__context(tx);
                #decide_body
            }

            fn ingest(
                state: &Self,
                fact: &#fact_set,
                ctx: &::annals::next::IngestCtx<'_, Self>,
            ) -> ::std::vec::Vec<#event_set> {
                #ingest_start
                let ctx = #m::__fact_context(ctx);
                #ingest_body
            }

            fn apply(state: &mut Self, event: &#event_set, _rules: &()) {
                #apply_body
            }

            fn refusal(rejection: &#rejection_ty) -> ::core::option::Option<::annals::next::Any> {
                rejection.refusal().cloned()
            }

        }

        #process_manager

        /// The domain's handler set: every marked method.
        #vis trait #handler {
            #(#declared;)*
        }

        impl #handler for #self_ty {
            #(#forwarded)*
        }

        impl #self_ty {
            /// The handled commands and facts, in marker order.
            pub const HANDLES: &'static [#m::TypeName] =
                &[#(#m::TypeName::of::<<#handled as #m::DomainMessage>::Wire>(),)*];
            /// The commands whose refusals it handles (`#[on_refused]`),
            /// in marker order.
            pub const REFUSALS: &'static [#m::TypeName] = #refusal_constant;
            /// The applied events, in marker order.
            pub const APPLIES: &'static [#m::TypeName] = #applies_constant;
        }
    })
}

#[cfg(test)]
#[path = "lib.test.rs"]
mod tests;
