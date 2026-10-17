//! annals on angzarr's types, pinned before it is built. Every test here
//! that needs a `todo!()` body is ignored until its behaviour is
//! implemented; each is the contract `docs/design/audit.md` proposes.

mod books;
mod ids;
mod payload;
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
