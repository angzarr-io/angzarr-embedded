//! Books: a command is a one-page `CommandBook`; what a chain recorded is
//! a set of `EventBook`s, one per stream, each page carrying `EventMeta`.

use angzarr_proto::v1::{CommandBook, EventBook, EventPage};
use annals::next::{CommandMeta, Domain, Envelope, Recorded, StreamKey, Timestamp};

use crate::error::BookError;
use crate::payload::Payload;

/// One command addressed to one stream, with its metadata: what a
/// `CommandBook` carries, in annals' terms. The input to
/// [`handle`](annals::next::handle): `handle::<D, ..>(.., &cmd.key,
/// &cmd.command, &cmd.meta, ..)`.
#[derive(Clone, PartialEq, Debug)]
pub struct Command<C> {
    pub key: StreamKey,
    pub meta: CommandMeta,
    pub command: C,
}

/// A command as a one-page `CommandBook`:
///
/// - the cover is the key ([`wire::cover`](crate::wire::cover)), with the
///   correlation if the command has one
///   ([`wire::correlation_to_wire`](crate::wire::correlation_to_wire));
/// - the page's payload is the command packed as `Any`
///   ([`Payload::to_any`]);
/// - the page's `CommandMeta` has the actor (empty for the system), the
///   request id, and the command's identity and cause
///   ([`wire::command_cause_to_wire`](crate::wire::command_cause_to_wire));
/// - `merge_strategy` is left unspecified: annals checks every stream a
///   chain touches against the length it loaded, whatever a book asks.
///
/// `recorded_at` is not on the wire: angzarr stamps it where the command
/// is handled, and so does annals ([`command_from_book`]).
pub fn command_book<C: Payload>(command: &Command<C>) -> CommandBook {
    todo!("proposal: cover, one page with payload, meta and header")
}

/// A one-page `CommandBook` as a command for `D`, recorded at
/// `recorded_at` (sampled by the caller, as every instant is). Refuses a
/// book for another domain, of no page or several pages, with no request
/// id, with a claim-check payload or an external deferred sequence, and a
/// payload of a type `D::Command` does not decode. A book with no
/// correlation gives a command with none, and the kernel generates one
/// from the request ([`Correlation::generated`]).
///
/// [`Correlation::generated`]: annals::next::Correlation::generated
pub fn command_from_book<D: Domain>(
    book: &CommandBook,
    recorded_at: Timestamp,
) -> Result<Command<D::Command>, BookError>
where
    D::Command: Payload,
{
    todo!("proposal: the inverse of command_book")
}

/// One recorded event as an `EventPage`: the header's explicit sequence
/// (the seq, 64-bit), `created_at` (annals keeps business time in the
/// payload, so this is the recorded instant), the payload packed as `Any`,
/// and the metadata as `EventMeta` ([`wire::event_meta`]).
///
/// [`wire::event_meta`]: crate::wire::event_meta
pub fn event_page<E: Payload>(envelope: &Envelope<E>) -> EventPage {
    todo!("proposal: header sequence, created_at, payload, meta")
}

/// What a chain recorded in the `D` stream `key`, as an `EventBook`: the
/// cover ([`wire::cover`](crate::wire::cover), with the chain's
/// correlation), a page per event ([`event_page`]), `next_sequence` the
/// stream's new length, and no snapshot. `None` if the chain did not touch
/// `key` or it is not a `D` stream.
pub fn event_book<D: Domain>(recorded: &Recorded, key: &StreamKey) -> Option<EventBook>
where
    D::Event: Payload,
{
    todo!("proposal: recorded.events::<D>(key), then cover and pages")
}
