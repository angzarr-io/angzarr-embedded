use std::sync::Arc;

use angzarr_router::PayloadUpcaster;
use prost_types::Any;

use super::{is_nothing, CommandUpcaster, NOTHING};
use crate::next::toys::account_upcaster;
use crate::next::v1::EventPage;

/// A fact's record of nothing is known by its type URL alone.
#[test]
fn a_record_of_nothing_is_known_by_its_type_url() {
    let page = |type_url: &str| EventPage {
        payload: Some(Any {
            type_url: type_url.to_string(),
            value: Vec::new(),
        }),
        ..EventPage::default()
    };
    assert!(is_nothing(&page(NOTHING)));
    assert!(!is_nothing(&page("/toy.account.v1.Opened")));
    assert!(!is_nothing(&EventPage::default()));
}

/// The router reads an upcaster's sources: the types it upcasts from.
#[test]
fn the_router_reads_an_upcasters_sources() {
    let upcaster = CommandUpcaster(Arc::new(account_upcaster()));
    let mut sources = upcaster.source_types();
    sources.sort();
    assert_eq!(
        sources,
        ["toy.account.cmd.v0.Credit", "toy.account.v0.Credited"]
    );
}

/// A fold that fails on a persisted event says why as its cause, a key and
/// args (decision 14.37: `FoldFailure.cause`, flattened as `cause` and
/// `cause.<arg>`), never as text: here the kernel's own payload error for
/// bytes that do not decode as the event's type.
#[test]
fn a_fold_that_fails_carries_its_cause_as_a_key_and_args() {
    use crate::next::toys::{account, command, cover, registry};
    use angzarr_router::error::codes;
    use angzarr_router::pb::ContextualCommand;

    let registry = registry();
    let stream = cover("account", 1);
    let mut corrupt = crate::next::pack(&account::Deposited { cents: 1 });
    corrupt.value = vec![0xff, 0xff, 0xff];
    let type_url = corrupt.type_url.clone();
    let request = ContextualCommand {
        events: Some(crate::next::v1::EventBook {
            cover: Some(stream.clone()),
            pages: vec![EventPage {
                sequence: 0,
                payload: Some(corrupt),
                meta: None,
            }],
            snapshot: None,
            next_sequence: 1,
        }),
        command: Some(command(
            stream,
            crate::next::pack(&account::cmd::Deposit { cents: 1 }),
        )),
        ..ContextualCommand::default()
    };
    let error = registry
        .handler("account")
        .expect("registered")
        .dispatch(&request)
        .expect_err("the persisted event does not fold");
    assert_eq!(error.code, codes::FOLD_FAILED);
    let extra = |key: &str| error.extras.get(key).map(String::as_str);
    assert_eq!(
        extra("cause"),
        Some("angzarr-embedded-next-payload-decode"),
        "{error:?}"
    );
    assert_eq!(
        extra("cause.type_url"),
        Some(type_url.as_str()),
        "{error:?}"
    );
}
