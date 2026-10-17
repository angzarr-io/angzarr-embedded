//! The proposed audited kernel, `annals::next`, pinned before it is built.
//! Tests of what is implemented (the parse step, identification, the
//! registry, payloads, the dead-letter line) run; every other test is
//! ignored until its behaviour is implemented, each the contract
//! `docs/design/audit.md` proposes.

// `Failure` is large by design (see `annals::next`); tests hold it in
// closures' results.
#![allow(clippy::result_large_err)]

mod chain;
mod conflict;
mod dead_letter;
mod dedup;
mod handlers;
mod identify;
mod ingest;
mod parse;
mod payload;
mod pipeline;
mod registry;
mod snapshot;
mod store;
mod toys;
mod upcast;

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

/// Poll a future to completion with a waker that does nothing: enough for
/// futures that are ready at once.
pub fn ready<F: Future>(f: F) -> F::Output {
    fn noop(_: *const ()) {}
    fn clone(p: *const ()) -> RawWaker {
        RawWaker::new(p, &VTABLE)
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    // SAFETY: every vtable function ignores the data pointer.
    let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
    let mut f = pin!(f);
    match f.as_mut().poll(&mut Context::from_waker(&waker)) {
        Poll::Ready(out) => out,
        Poll::Pending => panic!("not ready at once"),
    }
}
