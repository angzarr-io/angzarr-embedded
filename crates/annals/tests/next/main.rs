//! The proposed audited kernel, `annals::next`, pinned before it is built.
//! Every test here is ignored until its behaviour is implemented; each is
//! the contract `docs/design/audit.md` proposes.

// `Failure` is large by design (see `annals::next`); tests hold it in
// closures' results.
#![allow(clippy::result_large_err)]

mod chain;
mod handlers;
mod kernel;
mod reactions;
mod snapshot;
mod store;
mod toys;
