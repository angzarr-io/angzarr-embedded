//! angzarr-embedded's dead-letter queue is a logger whose every line warns that it
//! almost certainly indicates a problem.
use crate::next::toys::prelude::*;
use crate::next::v1::{angzarr_dead_letter, AngzarrDeadLetter, ComponentKind, Diagnostic};
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
        reason: Some(Diagnostic {
            code: "angzarr-embedded-next-unhandled-handler-error".to_string(),
            args: [(
                "cause".to_string(),
                "ROUTER_REASON_HANDLER_PANICKED".to_string(),
            )]
            .into(),
        }),
        source_component: "account".to_string(),
        source_component_kind: ComponentKind::Aggregate as i32,
        ..AngzarrDeadLetter::default()
    };

    let line = dead_letter_line(&letter);
    assert!(
        line.starts_with(DEAD_LETTER_PREFIX),
        "an operator alerts on the prefix: {line}"
    );
    // A key to alert on, never English (decision 14.37).
    assert_eq!(DEAD_LETTER_PREFIX, "DEAD_LETTER");
    // The reason is the letter's Diagnostic: its code, then its args
    // (decision 14.37).
    assert!(
        line.contains(
            "account: angzarr-embedded-next-unhandled-handler-error cause=ROUTER_REASON_HANDLER_PANICKED"
        ),
        "{line}"
    );
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
    // The router catches the panic: a handler's bug, its cause
    // ROUTER_REASON_HANDLER_PANICKED (decision 14.37).
    assert!(
        matches!(
            &failure,
            Failure::Kernel(KernelError::Router(e))
                if e.extras.get("cause").map(String::as_str) == Some("ROUTER_REASON_HANDLER_PANICKED")
        ),
        "{failure:?}"
    );
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
        lines[0].contains(
            "angzarr-embedded-next-unhandled-handler-error cause=ROUTER_REASON_HANDLER_PANICKED"
        ),
        "{}",
        lines[0]
    );
    assert!(!lines[0].contains("panicked:"), "{}", lines[0]);
    assert!(
        !lines[0].contains("negative"),
        "the panic's text stays out: {}",
        lines[0]
    );
}

/// A dead letter's value that would not parse back is quoted (decision
/// 14.37's display rule), its reason's args and its metadata alike: one
/// empty, or holding whitespace, `=`, `"` or `\`, double-quoted with its
/// `"` and `\` escaped; any other value bare.
#[test]
fn a_dead_letters_ambiguous_value_is_quoted() {
    let pairs = |pairs: &[(&str, &str)]| {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    };
    let letter = AngzarrDeadLetter {
        reason: Some(Diagnostic {
            code: "toy-store-down".to_string(),
            args: pairs(&[
                ("ext_err_txt", "database is locked"),
                ("empty", ""),
                ("path", "C:\\dir"),
                ("said", "say \"hi\""),
                ("seat", "3"),
            ]),
        }),
        metadata: pairs(&[("note", "k=v")]),
        source_component: "account".to_string(),
        ..AngzarrDeadLetter::default()
    };
    assert_eq!(
        dead_letter_line(&letter),
        "DEAD_LETTER account: toy-store-down empty=\"\" ext_err_txt=\"database is locked\" \
         path=\"C:\\\\dir\" said=\"say \\\"hi\\\"\" seat=3 note=\"k=v\""
    );
}

/// A step whose retryable failure outlasted its retries is dead-lettered
/// with its command, its failure's code and args as the reason and as the
/// processing failure's, transient, with how many retries it had; its line
/// shows the count.
#[test]
fn a_retried_steps_dead_letter_holds_its_command_failure_and_retry_count() {
    use crate::next::v1::angzarr_dead_letter::RejectionDetails;
    use crate::next::v1::ProcessingFailedDetails;
    use angzarr_embedded_diagnostic::Report;
    let book = check(
        command(
            cover("account", 1),
            pack(&account::cmd::Deposit { cents: 1 }),
        )
        .id(5),
        &registry(),
    )
    .unwrap()
    .identify(&at(30))
    .to_book();
    let failure = Report::new("toy-down").arg("seat", 3u32);
    let letter = super::retries_exhausted(
        angzarr_dead_letter::Payload::RejectedCommand(book.clone()),
        "account",
        &failure,
        3,
    );
    let reason = Diagnostic {
        code: "toy-down".to_string(),
        args: [("seat".to_string(), "3".to_string())].into(),
    };
    assert_eq!(
        letter.payload,
        Some(angzarr_dead_letter::Payload::RejectedCommand(book))
    );
    assert_eq!(letter.reason, Some(reason.clone()));
    assert_eq!(
        letter.rejection_details,
        Some(RejectionDetails::ProcessingFailed(
            ProcessingFailedDetails {
                failure: Some(reason),
                retry_count: 3,
                is_transient: true,
                ..ProcessingFailedDetails::default()
            }
        ))
    );
    assert_eq!(letter.source_component, "account");
    let line = dead_letter_line(&letter);
    assert!(
        line.contains("account: toy-down seat=3 retry_count=3 command_id="),
        "{line}"
    );
}
