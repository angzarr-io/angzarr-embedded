//! processed_state.feature: the record a projector's or a policy's driver
//! keeps of what its handler processed (angzarr-ports' `Processed`, its
//! positions), which a purge never touches (decision 14.68). A projector
//! here is a recording one, caught up stored; a policy is caught up by the
//! kernel's resume, which first tells it of the streams gone.

use std::sync::{Arc, Mutex};

use angzarr_embedded::angzarr_ports::{ListStreams as _, Processed as _};
use angzarr_embedded::v1::{Cover, EventBook, Projection};
use angzarr_embedded::{CatchUp, EventStore as _, PositionKey, Purge as _, StreamRef, Write};
use cucumber::{given, then, when};

use crate::tests::policy::locked;
use crate::tests::world::Coordinator;

/// A catch-up projector a scenario declares: its source domain, and every
/// book it was handed.
#[derive(Clone, Default)]
pub(crate) struct Projected {
    pub(crate) domain: String,
    pub(crate) books: Arc<Mutex<Vec<EventBook>>>,
}

/// The projector handler that records what it is handed.
struct Recording {
    name: String,
    books: Arc<Mutex<Vec<EventBook>>>,
}

impl angzarr_embedded::ProjectorHandler for Recording {
    fn name(&self) -> &str {
        &self.name
    }

    async fn dispatch(
        &self,
        book: &EventBook,
    ) -> Result<Projection, angzarr_embedded::angzarr_router::error::CodedError> {
        self.books.lock().unwrap().push(book.clone());
        Ok(Projection::default())
    }
}

/// Order `name`'s stream, `n` events long, with `handler`'s position at its
/// end: what the handler has processed.
fn processed_to(w: &mut Coordinator, handler: &str, name: &str, n: usize) -> Cover {
    let cover = w.cover("order", name);
    let names = vec!["Happened"; n];
    w.write(&cover, &names);
    let stream = StreamRef::try_from(&cover).unwrap();
    let key = PositionKey { handler, stream };
    futures_ready(w.store.write(Write::positions(&[(key, n as u64)])));
    cover
}

/// A future the in-memory store answers at once.
fn futures_ready<T>(
    future: impl std::future::Future<Output = Result<T, angzarr_embedded::StoreError>>,
) -> T {
    angzarr_embedded::testing::block_on_ready(future).expect("the in-memory store")
}

#[given(
    regex = r#"^a projector "(\w+)" of domain "([\w-]+)" that has processed order "([\w-]+)" to sequence (\d+)$"#
)]
fn projector_processed(w: &mut Coordinator, name: String, domain: String, order: String, n: usize) {
    w.projectors.insert(
        name.clone(),
        Projected {
            domain,
            books: Arc::default(),
        },
    );
    processed_to(w, &name, &order, n);
}

#[given(
    regex = r#"^a policy "(\w+)" translating from "([\w-]+)" to "([\w-]+)" that has processed order "([\w-]+)" to sequence (\d+)$"#
)]
fn policy_processed(
    w: &mut Coordinator,
    name: String,
    input: String,
    output: String,
    order: String,
    n: usize,
) {
    w.policy(&name, &input, &output);
    processed_to(w, &name, &order, n);
}

#[when(regex = r#"^order "([\w-]+)" is purged(?: while "\w+" is not running)?$"#)]
fn purged(w: &mut Coordinator, order: String) {
    let cover = w.cover("order", &order);
    let stream = StreamRef::try_from(&cover).unwrap();
    let head = futures_ready(w.store.head(stream)).next_sequence;
    futures_ready(w.store.purge(stream, head));
}

#[given(regex = r#"^order "([\w-]+)" is purged while "\w+" is not running$"#)]
fn purged_given(w: &mut Coordinator, order: String) {
    purged(w, order);
}

#[when(regex = r#"^"(\w+)" catches up$"#)]
async fn catches_up(w: &mut Coordinator, name: String) {
    match w.projectors.get(&name).cloned() {
        Some(projected) => {
            let recording = Recording {
                name: name.clone(),
                books: Arc::clone(&projected.books),
            };
            let registry = w.registry();
            let mut catch_up = CatchUp::stored(&[projected.domain.as_str()], recording).unwrap();
            let store = w.store.clone();
            catch_up
                .step(&store, registry.catalog())
                .await
                .expect("the catch-up steps");
        }
        None => {
            let resumed = w.resume().await;
            w.resumed.push(resumed);
        }
    }
}

/// The books `name`, a projector or a policy, was handed of `cover`.
fn handed(w: &Coordinator, name: &str, cover: &Cover) -> Vec<EventBook> {
    let books = match w.projectors.get(name) {
        Some(projected) => projected.books.lock().unwrap().clone(),
        None => {
            let def = w
                .policies
                .iter()
                .find(|p| p.name == name)
                .expect("a policy");
            locked(&def.script).sources.clone()
        }
    };
    books
        .into_iter()
        .filter(|b| b.cover.as_ref() == Some(cover))
        .collect()
}

#[then(regex = r#"^"(\w+)" is handed order "([\w-]+)" as gone(, and answers nothing)?$"#)]
fn handed_gone(w: &mut Coordinator, name: String, order: String, nothing: String) {
    let cover = w.cover("order", &order);
    let books = handed(w, &name, &cover);
    assert_eq!(
        books,
        [EventBook {
            cover: Some(cover),
            ..EventBook::default()
        }],
        "its gone book alone"
    );
    if !nothing.is_empty() {
        let others: Vec<String> = (w.domains.keys())
            .filter(|d| d.as_str() != "order")
            .cloned()
            .collect();
        for domain in others {
            assert!(w.business(&domain).delivered().is_empty(), "{domain}");
        }
    }
}

#[then(regex = r#"^"(\w+)" is handed nothing of order "([\w-]+)"$"#)]
fn handed_nothing(w: &mut Coordinator, name: String, order: String) {
    let cover = w.cover("order", &order);
    assert_eq!(handed(w, &name, &cover), []);
}

#[then(regex = r#"^the record of "(\w+)" still holds order "([\w-]+)" at sequence (\d+)$"#)]
fn still_holds(w: &mut Coordinator, name: String, order: String, n: u64) {
    let cover = w.cover("order", &order);
    let record = futures_ready(w.store.processed(&name));
    assert!(record.contains(&(cover, n)), "{record:?}");
}

#[then(regex = r#"^the record of "(\w+)" no longer holds order "([\w-]+)"$"#)]
fn no_longer_holds(w: &mut Coordinator, name: String, order: String) {
    let cover = w.cover("order", &order);
    let record = futures_ready(w.store.processed(&name));
    assert!(record.iter().all(|(c, _)| *c != cover), "{record:?}");
    assert!(!futures_ready(w.store.list_streams()).contains(&cover));
}
