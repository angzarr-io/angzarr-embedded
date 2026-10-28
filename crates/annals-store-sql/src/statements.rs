//! Every statement the store runs, backend-neutral: a backend renders them
//! with its own query builder and runs them on its own driver.

use annals::next::v1::event_meta::Causation;
use annals::next::v1::{Cover, EventPage, Snapshot, Uuid};
use annals::next::StoreError;
use prost::Message;
use sea_query::{
    Cond, Condition, DeleteStatement, Expr, Func, InsertStatement, OnConflict, Order, Query,
    SelectStatement, SimpleExpr, UpdateStatement,
};

use crate::error::unavailable;
use crate::schema::{CommitPosition, Events, Snapshots};
use crate::Dialect;

/// The root's bytes: the key column every statement matches on.
fn root(cover: &Cover) -> Vec<u8> {
    cover
        .root
        .as_ref()
        .map(|r| r.value.to_vec())
        .unwrap_or_default()
}

/// A sequence as SQL's 64-bit signed integer. Sequences count events in one
/// stream, so they never reach the sign bit.
pub(crate) fn sequence_column(sequence: u64) -> i64 {
    i64::try_from(sequence).unwrap_or(i64::MAX)
}

/// Whether a row is `cover`'s, by its table's key columns.
fn of_stream<D: Dialect>(
    cover: &Cover,
    domain: impl sea_query::IntoColumnRef,
    edition: impl sea_query::IntoColumnRef,
    root_column: impl sea_query::IntoColumnRef,
) -> Condition {
    Cond::all()
        .add(Expr::col(domain).eq(cover.domain.clone()))
        .add(Expr::col(edition).eq(D::edition(cover)))
        .add(Expr::col(root_column).eq(root(cover)))
}

fn event_of<D: Dialect>(cover: &Cover) -> Condition {
    of_stream::<D>(cover, Events::Domain, Events::Edition, Events::Root)
}

fn snapshot_of_stream<D: Dialect>(cover: &Cover) -> Condition {
    of_stream::<D>(
        cover,
        Snapshots::Domain,
        Snapshots::Edition,
        Snapshots::Root,
    )
}

fn events_of<D: Dialect>(cover: &Cover) -> SelectStatement {
    Query::select()
        .from(Events::Table)
        .cond_where(event_of::<D>(cover))
        .to_owned()
}

/// Row `(MAX(sequence))`: the stream's last sequence, NULL for no stream.
/// Its pages run from 0 without a gap, so its length is one past it. A
/// seek to the end of the stream in the primary key, not a count of its
/// rows.
pub(crate) fn stream_last<D: Dialect>(cover: &Cover) -> SelectStatement {
    events_of::<D>(cover)
        .expr(Func::max(Expr::col(Events::Sequence)))
        .to_owned()
}

/// Row `(sequence, page)` for each of the stream's pages `filter` keeps, in
/// sequence order.
fn pages<D: Dialect>(cover: &Cover, filter: SimpleExpr) -> SelectStatement {
    events_of::<D>(cover)
        .columns([Events::Sequence, Events::Page])
        .and_where(filter)
        .order_by(Events::Sequence, Order::Asc)
        .to_owned()
}

/// The stream's pages from `from` on.
pub(crate) fn pages_from<D: Dialect>(cover: &Cover, from: u64) -> SelectStatement {
    pages::<D>(
        cover,
        Expr::col(Events::Sequence).gte(sequence_column(from)),
    )
}

/// The stream's page at `sequence`.
pub(crate) fn page_at<D: Dialect>(cover: &Cover, sequence: u64) -> SelectStatement {
    pages::<D>(
        cover,
        Expr::col(Events::Sequence).eq(sequence_column(sequence)),
    )
}

/// The stream's pages `command_id` produced: the deduplication lookup.
pub(crate) fn pages_of_command<D: Dialect>(cover: &Cover, command_id: &Uuid) -> SelectStatement {
    pages::<D>(
        cover,
        Expr::col(Events::CommandId).eq(command_id.value.to_vec()),
    )
}

/// The store's commit position: the one row of `commit_position`.
pub(crate) fn commit_position() -> SelectStatement {
    Query::select()
        .column(CommitPosition::Next)
        .from(CommitPosition::Table)
        .to_owned()
}

/// Raise the commit position by one, past an append's events.
pub(crate) fn raise_commit_position() -> UpdateStatement {
    Query::update()
        .table(CommitPosition::Table)
        .value(CommitPosition::Next, Expr::col(CommitPosition::Next).add(1))
        .to_owned()
}

/// One page, as the row it is stored as, stamped with the commit `position`
/// it lands at.
pub(crate) fn insert_page<D: Dialect>(
    cover: &Cover,
    page: &EventPage,
    position: i64,
) -> Result<InsertStatement, StoreError> {
    let command_id = match page.meta.as_ref().and_then(|m| m.causation.as_ref()) {
        Some(Causation::CommandId(id)) => Some(id.value.to_vec()),
        _ => None,
    };
    Ok(Query::insert()
        .into_table(Events::Table)
        .columns([
            Events::Domain,
            Events::Edition,
            Events::Root,
            Events::Sequence,
            Events::CommandId,
            Events::Page,
            Events::Position,
        ])
        .values([
            cover.domain.clone().into(),
            D::edition(cover).into(),
            root(cover).into(),
            sequence_column(page.sequence).into(),
            command_id.into(),
            page.encode_to_vec().into(),
            position.into(),
        ])
        .map_err(unavailable)?
        .to_owned())
}

/// Row `(edition, root)` for each of `domain`'s streams: main timeline first
/// (the dialect's main edition sorts first), then by edition, each by root.
pub(crate) fn domain_covers(domain: &str) -> SelectStatement {
    Query::select()
        .distinct()
        .columns([Events::Edition, Events::Root])
        .from(Events::Table)
        .and_where(Expr::col(Events::Domain).eq(domain))
        .order_by(Events::Edition, Order::Asc)
        .order_by(Events::Root, Order::Asc)
        .to_owned()
}

/// Row `(snapshot)`: the stream's newest snapshot.
pub(crate) fn snapshot_of<D: Dialect>(cover: &Cover) -> SelectStatement {
    Query::select()
        .column(Snapshots::Snapshot)
        .from(Snapshots::Table)
        .cond_where(snapshot_of_stream::<D>(cover))
        .to_owned()
}

/// Delete the stream's events, and with them the command ids recorded in
/// their rows.
pub(crate) fn delete_events<D: Dialect>(cover: &Cover) -> DeleteStatement {
    Query::delete()
        .from_table(Events::Table)
        .cond_where(event_of::<D>(cover))
        .to_owned()
}

/// Delete the stream's snapshot.
pub(crate) fn delete_snapshot<D: Dialect>(cover: &Cover) -> DeleteStatement {
    Query::delete()
        .from_table(Snapshots::Table)
        .cond_where(snapshot_of_stream::<D>(cover))
        .to_owned()
}

/// Keep `snapshot` as the stream's newest, replacing the one before.
pub(crate) fn upsert_snapshot<D: Dialect>(
    cover: &Cover,
    snapshot: &Snapshot,
) -> Result<InsertStatement, StoreError> {
    Ok(Query::insert()
        .into_table(Snapshots::Table)
        .columns([
            Snapshots::Domain,
            Snapshots::Edition,
            Snapshots::Root,
            Snapshots::Snapshot,
        ])
        .values([
            cover.domain.clone().into(),
            D::edition(cover).into(),
            root(cover).into(),
            snapshot.encode_to_vec().into(),
        ])
        .map_err(unavailable)?
        .on_conflict(
            OnConflict::columns([Snapshots::Domain, Snapshots::Edition, Snapshots::Root])
                .update_column(Snapshots::Snapshot)
                .to_owned(),
        )
        .to_owned())
}
