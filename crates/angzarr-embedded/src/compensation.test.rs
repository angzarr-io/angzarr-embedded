//! The compensation tooling's edges, beside the handler-contract tier's
//! `compensation.feature` (the conformance crate's): a reply of no step's,
//! an undo no step becomes, several warnings, and the warning as a
//! diagnostic. A toy order process manager with two steps, each undone by
//! a fact of its destination's or, for `Ship`, a command.

use angzarr_diagnostic::Diagnostic as _;

use super::*;
use crate::markup::{CommandTarget, DomainMessage, TypeName};
use crate::WireError;

macro_rules! toy {
    ($($name:ident),*) => {$(
        #[derive(Clone, PartialEq, ::prost::Message)]
        struct $name {}

        impl ::prost::Name for $name {
            const NAME: &'static str = stringify!($name);
            const PACKAGE: &'static str = "toy.v1";
        }

        impl DomainMessage for $name {
            type Wire = $name;

            fn to_wire(&self) -> $name {
                self.clone()
            }

            fn from_wire(wire: $name) -> Result<Self, WireError> {
                Ok(wire)
            }
        }
    )*};
}

toy!(Reserved, Charged, Unrelated, Release, Refund, Ship);

/// A destination deciding `Ship`, taking every other type as a fact.
struct Warehouse;

impl CommandTarget for Warehouse {
    const DOMAIN: &'static str = "warehouse";

    fn command_types() -> Vec<String> {
        vec![TypeName::of::<Ship>().full_name()]
    }
}

#[derive(Default)]
struct State {
    reserved: bool,
    charged: bool,
    failed: bool,
}

#[derive(Clone, PartialEq, Debug)]
enum Event {
    Reserved,
    Charged,
    Unrelated,
    Failed(&'static str),
    ReleaseDue,
    RefundDue,
    CompensationFailed(String),
}

impl From<Charged> for Event {
    fn from(_: Charged) -> Event {
        Event::Charged
    }
}

impl From<Unrelated> for Event {
    fn from(_: Unrelated) -> Event {
        Event::Unrelated
    }
}

impl From<Reserved> for Event {
    fn from(_: Reserved) -> Event {
        Event::Reserved
    }
}

fn order() -> Compensation<State, Event, &'static str> {
    Compensation::new(
        |_: &State, reason| Event::Failed(reason),
        |state: &State| state.failed,
        |_: &State, step: &str| Event::CompensationFailed(step.to_string()),
    )
    .step::<Warehouse, Release, _>(
        "reserve",
        |s: &State| s.reserved.then_some(Reserved {}),
        |_| Event::ReleaseDue,
    )
    .step::<Warehouse, Refund, _>(
        "charge",
        |s: &State| s.charged.then_some(Charged {}),
        |_| Event::RefundDue,
    )
}

#[test]
fn a_rollback_undoes_the_recorded_steps_newest_first_after_the_failure() {
    let state = State {
        reserved: true,
        charged: true,
        failed: false,
    };
    assert_eq!(
        order().rollback(&state, "no"),
        [Event::Failed("no"), Event::RefundDue, Event::ReleaseDue]
    );
    let only_charged = State {
        charged: true,
        ..State::default()
    };
    assert_eq!(
        order().rollback(&only_charged, "no"),
        [Event::Failed("no"), Event::RefundDue]
    );
}

#[test]
fn a_reply_is_undone_only_once_rolled_back_and_only_for_its_step() {
    let live = State::default();
    assert_eq!(order().reply(&live, Charged {}), [Event::Charged]);
    let failed = State {
        failed: true,
        ..State::default()
    };
    assert_eq!(
        order().reply(&failed, Charged {}),
        [Event::Charged, Event::RefundDue]
    );
    assert_eq!(
        order().reply(&failed, Reserved {}),
        [Event::Reserved, Event::ReleaseDue]
    );
    // No step is recorded by it: the reply alone.
    assert_eq!(order().reply(&failed, Unrelated {}), [Event::Unrelated]);
}

#[test]
fn a_refused_undo_is_its_steps_compensation_failure() {
    let state = State::default();
    assert_eq!(
        order().undo_refused::<Refund>(&state),
        Some(Event::CompensationFailed("charge".to_string()))
    );
    assert_eq!(
        order().undo_refused::<Release>(&state),
        Some(Event::CompensationFailed("reserve".to_string()))
    );
    assert_eq!(order().undo_refused::<Ship>(&state), None);
}

#[test]
fn registering_warns_of_each_undo_that_becomes_a_command_and_no_other() {
    assert!(order().warnings().is_empty());
    let shipping = order()
        .step::<Warehouse, Ship, _>("ship", |_: &State| None::<Unrelated>, |_| Event::ReleaseDue)
        .step::<Warehouse, Ship, _>(
            "ship again",
            |_: &State| None::<Unrelated>,
            |_| Event::ReleaseDue,
        );
    let warned: Vec<(&str, &str)> = shipping
        .warnings()
        .iter()
        .map(|w| (w.step, w.command.as_str()))
        .collect();
    assert_eq!(
        warned,
        [("ship", "toy.v1.Ship"), ("ship again", "toy.v1.Ship")]
    );
}

#[test]
fn the_warning_is_a_diagnostic_naming_its_step_and_command() {
    let warning = UndoTargetsCommand {
        step: "ship",
        command: "toy.v1.Ship".to_string(),
    };
    assert_eq!(warning.code(), "angzarr-embedded-undo-targets-command");
    let args: Vec<(&str, String)> = warning
        .args()
        .into_iter()
        .map(|(name, arg)| match arg {
            angzarr_diagnostic::Arg::Str(text) => (name, text),
            other => panic!("{name}: {other:?}"),
        })
        .collect();
    assert_eq!(
        args,
        [
            ("step", "ship".to_string()),
            ("command", "toy.v1.Ship".to_string())
        ]
    );
    assert!(warning.to_string().contains("toy.v1.Ship"), "{warning}");
}

#[test]
fn a_compensation_shows_its_steps_and_warnings() {
    let shown = format!("{:?}", order());
    assert!(shown.contains("[\"reserve\", \"charge\"]"), "{shown}");
    assert!(shown.contains("warnings: []"), "{shown}");
}
