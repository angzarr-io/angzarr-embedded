//! A store's re-fold digest: every stream of a domain loaded as the
//! kernel loads it, its head and its state, what two builds compare after
//! a store has crossed a proto generation (the store export/import plan,
//! section 5.3.2). The lines are written elsewhere, in proto3 JSON
//! (angzarr-store-transfer's `write_digest`): here is only the fold.

use angzarr_ports::Store;
use prost_types::Any;

use crate::domain::Domain;
use crate::error::KernelError;
use crate::handle::Kernel;
use crate::v1::Cover;

/// One stream folded: its cover, its head, and its state in its snapshot
/// form, packed.
#[derive(Clone, PartialEq, Debug)]
pub struct Folded {
    pub cover: Cover,
    pub head: u64,
    pub state: Any,
}

impl Kernel<'_> {
    /// Every stream of `D` in `store` holding events of its own, in every
    /// edition, in the store's order ([`EventStore::covers`]), each loaded
    /// as [`Kernel::load`] loads it (its newest usable snapshot, then its
    /// events, each checked and upcast), with its head and its state
    /// packed as its snapshot form.
    pub async fn fold_digest<D, S>(&self, store: &S) -> Result<Vec<Folded>, KernelError>
    where
        D: Domain,
        S: Store,
    {
        let _ = store;
        todo!("the fold of every {} stream", D::NAME)
    }
}

#[cfg(test)]
#[path = "digest.test.rs"]
mod tests;
