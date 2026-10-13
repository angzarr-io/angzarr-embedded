//! The toy test domains of `proto/annals/log/toy/v1/toy.proto`, written by
//! hand to match it field for field. `tests/codegen.rs` checks them against
//! the generated types.

/// A counter: a domain that is not ticked.
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct CounterEvent {
    #[prost(oneof = "counter_event::Kind", tags = "1, 2")]
    pub kind: Option<counter_event::Kind>,
}

/// The oneof in `CounterEvent`.
pub mod counter_event {
    #[derive(Clone, PartialEq, Eq, Hash, ::prost::Oneof)]
    pub enum Kind {
        /// The counter went up by this much.
        #[prost(int64, tag = "1")]
        Added(i64),
        /// The counter was reset, with a reason.
        #[prost(string, tag = "2")]
        Reset(String),
    }
}

/// The counter's typed envelope: field for field the generic
/// `annals.log.v1.Envelope`, with the event as a message.
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct CounterEnvelope {
    #[prost(uint64, tag = "1")]
    pub seq: u64,
    #[prost(uint64, optional, tag = "2")]
    pub cause: Option<u64>,
    #[prost(message, optional, tag = "3")]
    pub event: Option<CounterEvent>,
}

/// A ticked domain's event: the tick is field 1.
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct TimedEvent {
    #[prost(uint64, tag = "1")]
    pub tick: u64,
    #[prost(string, tag = "2")]
    pub what: String,
}
