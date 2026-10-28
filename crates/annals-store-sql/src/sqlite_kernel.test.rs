//! The kernel over the SQLite store: what commands do to streams on the
//! real store, beside the contract's checks of the store alone. One toy
//! domain, a tally, whose every command adds to a count.

use std::fmt;

use annals::next::contract::{cover, cover_in};
use annals::next::v1::{
    command_page, Command, CommandBook, CommandPage, ConflictResolution, Cover, Snapshot,
    StateDescriptor, StateOptions, Uuid,
};
use annals::next::{
    pack, Any, AsyncEventStore, AsyncStore, Domain, Failure, Handled, IngestCtx, Kernel,
    KernelError, MemoryDeadLetters, Payload, PayloadError, Registry, Routes, Rulebook, Sampled,
    State, StoreError, Stream, Timestamp, Tx, Upcaster,
};
use annals_diagnostic::{Arg, Diagnostic, Report};

use super::SqliteStore;

/// A message in `tally.v1`.
macro_rules! tally_message {
    ($name:ident) => {
        #[derive(Clone, PartialEq, prost::Message)]
        struct $name {
            #[prost(int64, tag = "1")]
            n: i64,
        }

        impl prost::Name for $name {
            const NAME: &'static str = stringify!($name);
            const PACKAGE: &'static str = "tally.v1";
        }
    };
}

tally_message!(Count);
tally_message!(Add);
tally_message!(Added);

impl State for Count {
    type Snapshot = Count;

    fn to_snapshot(&self) -> Count {
        self.clone()
    }

    fn from_snapshot(snapshot: Count) -> Result<Count, Report> {
        Ok(snapshot)
    }

    fn descriptor() -> StateDescriptor {
        StateDescriptor {
            type_url: "/tally.v1.Count".to_string(),
            options: Some(StateOptions {
                conflict_resolutions: vec![
                    ConflictResolution::Strict as i32,
                    ConflictResolution::AggregateHandles as i32,
                ],
            }),
        }
    }
}

/// `any` as `M`, by its type URL.
fn unpack<M: prost::Message + prost::Name + Default>(any: &Any) -> Result<M, PayloadError> {
    if any.type_url != M::type_url() {
        return Err(PayloadError::UnknownType {
            type_url: any.type_url.clone(),
        });
    }
    M::decode(&any.value[..]).map_err(|error| PayloadError::Decode {
        type_url: any.type_url.clone(),
        error,
    })
}

macro_rules! payload {
    ($name:ident) => {
        impl Payload for $name {
            fn to_any(&self) -> Any {
                pack(self)
            }

            fn from_any(any: &Any) -> Result<Self, PayloadError> {
                unpack(any)
            }

            fn type_urls() -> Vec<String> {
                vec![<$name as prost::Name>::type_url()]
            }
        }
    };
}

payload!(Add);
payload!(Added);

/// A tally refuses nothing.
#[derive(Debug)]
struct Never;

impl fmt::Display for Never {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("never")
    }
}

impl Diagnostic for Never {
    fn code(&self) -> &'static str {
        "tally-never"
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        Vec::new()
    }
}

struct Tally;

impl Domain for Tally {
    type State = Count;
    type Command = Add;
    type Fact = std::convert::Infallible;
    type Event = Added;
    type Rejection = Never;
    type Rules = ();
    const NAME: &'static str = "tally";

    fn decide(cmd: &Add, tx: &mut Tx<'_, Self>) -> Result<(), Never> {
        tx.emit(Added { n: cmd.n });
        Ok(())
    }

    fn ingest(_: &Count, fact: &std::convert::Infallible, _: &IngestCtx<'_, Self>) -> Vec<Added> {
        match *fact {}
    }

    fn apply(state: &mut Count, event: &Added, _: &()) {
        state.n += event.n;
    }
}

fn registry() -> Registry {
    let mut registry = Registry::new();
    registry
        .register::<Tally>(Rulebook::new("v1", ()), Upcaster::new("tally-up", "tally"))
        .unwrap();
    registry
}

/// Command `id`: add `n` to `stream`.
fn add(stream: Cover, id: u8, n: i64) -> CommandBook {
    CommandBook {
        cover: Some(stream),
        page: Some(CommandPage {
            command_id: Some(Uuid::from_bytes([id; 16])),
            body: Some(command_page::Body::Command(Command {
                payload: Some(Add { n }.to_any()),
            })),
            ..CommandPage::default()
        }),
        handling: None,
    }
}

/// `book` through the kernel, in a unit of work of its own.
async fn handle(
    store: &mut SqliteStore,
    registry: &Registry,
    book: CommandBook,
) -> Result<Handled, Failure<Never>> {
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(registry, &log);
    let sampled = Sampled {
        now: Timestamp {
            seconds: 10,
            nanos: 0,
        },
        command_id: Uuid::from_bytes([0x07; 16]),
    };
    let unit = store.begin().await;
    kernel
        .handle_async::<Tally, _, _>(unit, &Routes::new(), book, &sampled, &mut ())
        .await
}

/// The count `stream` holds, folded from its events.
async fn count(store: &mut SqliteStore, registry: &Registry, stream: &Cover) -> i64 {
    let slice = store.read_from(stream, 0).await.unwrap().unwrap();
    Stream::<Tally>::replay(stream.clone(), &slice.pages, registry)
        .unwrap()
        .state()
        .n
}

/// A snapshot at or past the head claims events the stream does not hold:
/// not used, and the stream is read from its events, so the commands after
/// it land. (`annals`' own `a_snapshot_at_or_past_the_head_is_not_used`
/// holds the in-memory store to the same.)
#[tokio::test]
async fn a_snapshot_at_or_past_the_head_is_not_used() {
    let registry = registry();
    let stream = cover("tally", 1);
    for sequence in [2, 5] {
        let mut store = SqliteStore::open_in_memory().await.unwrap();
        for id in [1, 2] {
            handle(&mut store, &registry, add(stream.clone(), id, 1))
                .await
                .unwrap();
        }
        let ahead = Snapshot {
            sequence,
            state: Some(pack(&Count { n: 999 })),
            rules_version: "v1".to_string(),
        };
        store.write_snapshot(&stream, &ahead).await.unwrap();
        for (id, head) in [(3, 3), (4, 4)] {
            let handled = handle(&mut store, &registry, add(stream.clone(), id, 10))
                .await
                .unwrap_or_else(|e| panic!("a snapshot at {sequence}, command {id}: {e}"));
            assert_eq!(handled.books()[0].next_sequence, head);
        }
        assert_eq!(count(&mut store, &registry, &stream).await, 22);
    }
}

/// No edition registry exists yet, so the store's units have created no
/// edition: a command naming one is refused, as a read naming one is,
/// rather than given a stream of its own with no main history. (`annals`'
/// own `a_command_to_an_edition_is_refused_and_writes_nothing` holds the
/// in-memory store to the same.)
#[tokio::test]
async fn a_command_to_an_edition_is_refused_and_writes_nothing() {
    let registry = registry();
    let mut store = SqliteStore::open_in_memory().await.unwrap();
    let stream = cover_in("tally", 1, "what-if");
    let failure = handle(&mut store, &registry, add(stream.clone(), 1, 1))
        .await
        .err()
        .unwrap();
    assert!(
        matches!(
            &failure,
            Failure::Kernel(KernelError::Store(StoreError::UnknownEdition { stream: s }))
                if *s == stream
        ),
        "{failure:?}"
    );
    assert_eq!(store.covers("tally").await, Ok(vec![]), "nothing written");
}
