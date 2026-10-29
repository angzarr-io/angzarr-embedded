//! angzarr-embedded's dead-letter queue is a logger whose every line warns that it
//! almost certainly indicates a problem.
use crate::next::toys::prelude::*;
use crate::next::v1::{angzarr_dead_letter, AngzarrDeadLetter, ComponentKind};
use crate::next::{
    check, dead_letter_line, pack, DeadLetterLog, Failure, Kernel, KernelError, MemoryDeadLetters,
    DEAD_LETTER_PREFIX,
};

use crate::next::toys::{account, at, command, cover, fact, registry, Account, Edit};

#[test]
fn a_dead_letter_line_starts_with_the_warning_and_names_the_command() {
    let book = check(
        command(
            cover("account", 1),
            pack(&account::cmd::Deposit { cents: 1 }),
        )
        .id(5),
        &registry(),
    )
    .unwrap()
    .identify(&at(30));
    let letter = AngzarrDeadLetter {
        payload: Some(angzarr_dead_letter::Payload::RejectedCommand(
            book.to_book(),
        )),
        rejection_reason: "ingest panicked".to_string(),
        source_component: "account".to_string(),
        source_component_kind: ComponentKind::Aggregate as i32,
        ..AngzarrDeadLetter::default()
    };

    let line = dead_letter_line(&letter);
    assert!(
        line.starts_with(DEAD_LETTER_PREFIX),
        "an operator alerts on the prefix: {line}"
    );
    assert!(DEAD_LETTER_PREFIX.contains("almost certainly"));
    assert!(line.contains("account: ingest panicked"), "{line}");
    assert!(
        line.contains(&crate::next::toys::command_id(5).to_string()),
        "the command id, as display text: {line}"
    );

    let log = MemoryDeadLetters::new();
    log.write(&line);
    assert_eq!(log.lines(), [line]);
}

#[test]
fn a_panicking_ingest_aborts_the_chain_and_is_logged() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();

    let bad = fact(
        cover("account", 1),
        pack(&account::fact::PaymentReceived { cents: -1 }),
        "pi_bad",
    );
    let failure = kernel
        .handle::<Account, _, _>(&store, bad, &at(30), &mut ())
        .ready()
        .err()
        .expect("a bug, never a business rejection");
    assert!(matches!(
        failure,
        Failure::Kernel(KernelError::IngestPanicked { .. })
    ));
    assert_eq!(failure.recovery(), crate::next::Recovery::Bug);
    assert!(streams(&store).is_empty(), "nothing written");
    let lines = log.lines();
    assert_eq!(lines.len(), 1);
    assert!(lines[0].starts_with(DEAD_LETTER_PREFIX));
    assert!(lines[0].contains(&crate::next::ids::fact_command_id("pi_bad").to_string()));
}

/// A dead letter carries its error's code and args, never rendered
/// English: the operator's text is produced where the line is displayed.
#[test]
fn a_panicking_ingests_dead_letter_carries_its_code_not_english() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    let bad = fact(
        cover("account", 1),
        pack(&account::fact::PaymentReceived { cents: -1 }),
        "pi_bad",
    );
    let _ = kernel
        .handle::<Account, _, _>(&store, bad, &at(30), &mut ())
        .ready();
    let lines = log.lines();
    assert_eq!(lines.len(), 1);
    assert!(
        lines[0].contains("angzarr-embedded-next-ingest-panicked"),
        "{}",
        lines[0]
    );
    assert!(!lines[0].contains("ingest panicked"), "{}", lines[0]);
}
