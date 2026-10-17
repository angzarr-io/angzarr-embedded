//! Payloads are `google.protobuf.Any`, packed from a domain's prost
//! messages by their [`prost::Name`]. The type URL is the version: a new
//! version of an event is a new message in a new package (`pkg.v1.Foo`,
//! then `pkg.v2.Foo`), and the old one is upcast at read time
//! ([`Upcaster`](crate::Upcaster)).
//!
//! A domain's events (and, for command books, its commands) are one Rust
//! type, usually an enum with one variant per message; [`Payload`] packs it
//! and unpacks it, and [`Decoder`] is the table of type URLs that unpacking
//! reads from.

use std::collections::BTreeMap;

use prost::{Message, Name};
use prost_types::Any;

use crate::error::PayloadError;

/// A domain's events or commands, as `Any`.
pub trait Payload: Sized {
    /// Pack this value as the message for its current type: [`pack`].
    fn to_any(&self) -> Any;

    /// Unpack a payload of one of the current types. Any other type URL is
    /// [`PayloadError::UnknownType`], never skipped: an older type must be
    /// upcast first.
    fn from_any(any: &Any) -> Result<Self, PayloadError>;

    /// The type URLs [`Payload::from_any`] reads: the current types. What
    /// an upcast chain must end at
    /// ([`Registry::check`](crate::Registry::check)).
    fn type_urls() -> Vec<String>;
}

/// A domain that takes no facts (`type Fact = Infallible`): nothing to pack,
/// and every payload refused as [`PayloadError::UnknownType`].
impl Payload for std::convert::Infallible {
    fn to_any(&self) -> Any {
        match *self {}
    }

    fn from_any(any: &Any) -> Result<Self, PayloadError> {
        Err(PayloadError::UnknownType {
            type_url: any.type_url.clone(),
        })
    }

    fn type_urls() -> Vec<String> {
        Vec::new()
    }
}

/// `message` as an `Any` whose type URL is its [`Name::type_url`]
/// (`/pkg.v1.Foo`, as angzarr writes them). Encoding to a vector cannot
/// run out of room, so this cannot fail.
pub fn pack<M: Message + Name>(message: &M) -> Any {
    todo!("proposal: Any::from_msg, which only fails on a full buffer")
}

/// The full name a type URL ends with: everything after its last `/`
/// (`type.googleapis.com/pkg.v1.Foo` and `/pkg.v1.Foo` both name
/// `pkg.v1.Foo`). Type URLs are compared by this.
pub fn type_name(type_url: &str) -> &str {
    todo!("proposal: after the last '/', or all of it")
}

/// One message type's decoder.
type Decode<E> = Box<dyn Fn(&[u8]) -> Result<E, prost::DecodeError> + Send + Sync>;

/// The type URLs a domain's payload decodes from, each to its variant.
/// Built once:
///
/// ```ignore
/// Decoder::new()
///     .on::<v3::Opened>(|_| AccountEvent::Opened)
///     .on::<v3::Deposited>(|m| AccountEvent::Deposited(m.cents))
/// ```
pub struct Decoder<E> {
    by_name: BTreeMap<String, Decode<E>>,
}

impl<E> Default for Decoder<E> {
    fn default() -> Self {
        Decoder {
            by_name: BTreeMap::new(),
        }
    }
}

impl<E: 'static> Decoder<E> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Decode `M`'s type URL as `M`, then `wrap` it.
    pub fn on<M: Message + Name + Default + 'static>(self, wrap: fn(M) -> E) -> Self {
        todo!("proposal: insert M::full_name() -> decode M, wrap")
    }

    /// `any` as the variant its type URL names.
    pub fn decode(&self, any: &Any) -> Result<E, PayloadError> {
        todo!("proposal: look up type_name(type_url); UnknownType if absent")
    }

    /// Every type URL this decoder reads, in order.
    pub fn type_urls(&self) -> Vec<String> {
        todo!("proposal: \"/\" + each name")
    }
}
