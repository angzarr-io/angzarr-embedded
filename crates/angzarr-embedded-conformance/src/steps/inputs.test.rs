//! The inputs a scenario hands the coordinator: commands and facts, as a
//! caller sends them, and the whens that have them handled. Every caller
//! waits for its answer (angzarr-embedded's one mode), so a command "sent
//! through Handle by a caller waiting for the answer" is any command.

use cucumber::{given, when};

use crate::tests::world::{resolution, Coordinator};

#[given(
    regex = r"^an? (\w+) command with conflict resolution (\w+) targeting sequence (\d+)(?:, sent through Handle by a caller waiting for the answer)?$"
)]
fn command_resolved(w: &mut Coordinator, name: String, resolved_by: String, expected: u64) {
    let cover = w.order();
    let book = w.command(&cover, &name);
    let book = Coordinator::handled_as(book, Some(expected), Some(resolution(&resolved_by)));
    w.pending.push(book);
}

#[given(regex = r"^an? (\w+) command with no conflict resolution targeting sequence (\d+)$")]
fn command_unresolved(w: &mut Coordinator, name: String, expected: u64) {
    let cover = w.order();
    let book = w.command(&cover, &name);
    w.pending
        .push(Coordinator::handled_as(book, Some(expected), None));
}

#[given(
    regex = r"^an? (\w+) command with no expected sequence and no conflict resolution(?:, sent through Handle by a caller waiting for the answer)?$"
)]
fn command_unhandled(w: &mut Coordinator, name: String) {
    let cover = w.order();
    let book = w.command(&cover, &name);
    w.pending.push(book);
}

#[given(
    regex = r"^two (\w+) commands with conflict resolution (\w+) both targeting sequence (\d+)$"
)]
fn two_commands(w: &mut Coordinator, name: String, resolved_by: String, expected: u64) {
    for _ in 0..2 {
        command_resolved(w, name.clone(), resolved_by.clone(), expected);
    }
}

#[given(
    regex = r#"^an AddItem command with command_id "([^"]+)" was applied, its ItemAdded event persisted at sequence (\d+)$"#
)]
async fn applied_with_id(w: &mut Coordinator, command_id: String, sequence: u64) {
    let cover = w.order();
    let mut book = w.command(&cover, "AddItem");
    if let Some(page) = book.page.as_mut() {
        page.command_id = Some(command_id.parse().expect("a UUID"));
    }
    w.handle(book).await;
    let pages = w.pages(&cover);
    let page = pages.get(sequence as usize).expect("persisted");
    assert!(page
        .payload
        .as_ref()
        .is_some_and(|p| p.type_url.ends_with(".ItemAdded")));
}

#[given(
    regex = r"^a retry of that AddItem command, with the same command_id, conflict resolution (\w+) targeting sequence (\d+)$"
)]
fn retry_of_applied(w: &mut Coordinator, resolved_by: String, expected: u64) {
    let book = Coordinator::handled_as(
        w.last_book(),
        Some(expected),
        Some(resolution(&resolved_by)),
    );
    w.pending.push(book);
}

#[given(
    regex = r#"^a (\w+) fact for the "order" stream, sent through Handle by a client waiting for the answer$"#
)]
fn fact_for_order(w: &mut Coordinator, name: String) {
    let cover = w.order();
    let book = w.fact(&cover, &name, "shipment-S1");
    w.pending.push(book);
}

#[when(regex = r"^the coordinator processes (?:the command|the fact|both commands)$")]
async fn processes(w: &mut Coordinator) {
    w.handle_pending().await;
}

#[when("the caller sends the same command twice")]
async fn sends_twice(w: &mut Coordinator) {
    w.begin();
    let book = w.pending.pop().expect("a command");
    w.handle(book.clone()).await;
    w.handle(book).await;
}

#[when("the coordinator processes the same command again")]
async fn processes_again(w: &mut Coordinator) {
    let book = w.last_book();
    w.handle(book).await;
}
