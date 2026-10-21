//! The two halves of a domain: deciding and applying.

use annals_diagnostic::Diagnostic;

use crate::stream::{Envelope, Seq};

/// One kind of event stream: its state, its commands, its events and its
/// rules for each.
///
/// Implement it on a marker type. A rejection is a
/// [`Diagnostic`](annals_diagnostic::Diagnostic), so a player can read it in
/// their own language:
///
/// ```
/// use std::fmt;
///
/// use annals::{Domain, Tx};
/// use annals_diagnostic::{Arg, Diagnostic};
///
/// struct Counter;
///
/// #[derive(Debug)]
/// struct NothingToAdd;
///
/// impl fmt::Display for NothingToAdd {
///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
///         f.write_str("nothing to add")
///     }
/// }
///
/// impl Diagnostic for NothingToAdd {
///     fn code(&self) -> &'static str {
///         "counter-nothing-to-add"
///     }
///     fn args(&self) -> Vec<(&'static str, Arg)> {
///         vec![]
///     }
/// }
///
/// impl Domain for Counter {
///     type State = i64;
///     type Command = i64;
///     type Event = i64;
///     type Rejection = NothingToAdd;
///     type Rules = ();
///     type Ctx = ();
///
///     fn decide(add: &i64, tx: &mut Tx<Self>) -> Result<(), NothingToAdd> {
///         if *add == 0 {
///             return Err(NothingToAdd);
///         }
///         tx.emit(*add);
///         Ok(())
///     }
///
///     fn apply(total: &mut i64, added: &i64, _: &()) {
///         *total += added;
///     }
/// }
/// ```
pub trait Domain: Sized {
    /// Folded from the events. Never stored.
    type State: Default;
    /// A request to change the stream. Decided, never stored.
    type Command;
    /// A fact. The only thing a stream stores.
    type Event: Clone;
    /// Why a command was refused: a [`Diagnostic`], with a code and args a
    /// game client renders in the player's language. A domain that never
    /// rejects uses [`std::convert::Infallible`].
    type Rejection: Diagnostic;
    /// Immutable reference data both halves may read: content, tables.
    type Rules;
    /// What a decide may use beyond the state: dice, a clock. Apply never
    /// sees it.
    type Ctx;

    /// Decide a command against the current state. Validate first; once an
    /// event is emitted the command has committed and must not reject.
    fn decide(cmd: &Self::Command, tx: &mut Tx<Self>) -> Result<(), Self::Rejection>;

    /// Fold one event into the state. Pure: the same state, event and rules
    /// always give the same result.
    fn apply(state: &mut Self::State, event: &Self::Event, rules: &Self::Rules);
}

/// A decide in progress. Reads go through it; writes are events emitted
/// through it, applied the moment they are emitted.
pub struct Tx<'a, D: Domain> {
    state: &'a mut D::State,
    rules: &'a D::Rules,
    ctx: &'a mut D::Ctx,
    log: &'a mut Vec<Envelope<D::Event>>,
    cause: Option<Seq>,
    emitted: usize,
}

impl<'a, D: Domain> Tx<'a, D> {
    pub(crate) fn new(
        state: &'a mut D::State,
        rules: &'a D::Rules,
        ctx: &'a mut D::Ctx,
        log: &'a mut Vec<Envelope<D::Event>>,
        cause: Option<Seq>,
    ) -> Tx<'a, D> {
        Tx {
            state,
            rules,
            ctx,
            log,
            cause,
            emitted: 0,
        }
    }

    pub fn state(&self) -> &D::State {
        self.state
    }

    /// The rules outlive the transaction, so a decide can keep hold of them
    /// while it goes on emitting.
    pub fn rules(&self) -> &'a D::Rules {
        self.rules
    }

    pub fn ctx(&mut self) -> &mut D::Ctx {
        self.ctx
    }

    /// State, rules and context at once — for a decide that must read the
    /// state while it consults the context (rolling against a target the
    /// state sets, say).
    pub fn parts(&mut self) -> (&D::State, &D::Rules, &mut D::Ctx) {
        (self.state, self.rules, self.ctx)
    }

    /// Apply the event and append it to the log, caused by whatever caused
    /// this command.
    pub fn emit(&mut self, event: D::Event) -> Seq {
        let cause = self.cause;
        self.emit_caused(event, cause)
    }

    /// Apply the event and append it to the log with an explicit cause — the
    /// earlier event that led to it.
    pub fn emit_caused(&mut self, event: D::Event, cause: Option<Seq>) -> Seq {
        D::apply(self.state, &event, self.rules);
        let seq = self.log.len() as Seq;
        self.log.push(Envelope { seq, cause, event });
        self.emitted += 1;
        seq
    }

    /// The sequence number the next emitted event will take.
    pub fn next_seq(&self) -> Seq {
        self.log.len() as Seq
    }

    /// Events emitted so far by this command.
    pub fn emitted(&self) -> usize {
        self.emitted
    }
}

#[cfg(test)]
#[path = "domain.test.rs"]
mod tests;
