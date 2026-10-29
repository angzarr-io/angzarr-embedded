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

/// A handler's refusal goes out under whichever of the six business
/// statuses it gave, typed or not, and `Rejected::status` reads that same
/// status back: an untyped refusal under its own code and args, a typed
/// one keyed COMMAND_REFUSED with its object beside.
#[test]
fn a_refusal_goes_out_and_reads_back_under_each_business_status() {
    use angzarr_embedded_diagnostic::{Diagnostic, Report};
    use angzarr_router::error::{BusinessStatus, GrpcCode, HandlerError};

    use super::{refused, Rejected};

    let coded = |error: HandlerError| match error {
        HandlerError::Coded(coded) => coded,
        other => panic!("coded, not {other:?}"),
    };
    let object = Any {
        type_url: "type.googleapis.com/toy.meter.v1.MeterSealed".to_string(),
        value: Vec::new(),
    };
    for status in [
        BusinessStatus::FailedPrecondition,
        BusinessStatus::InvalidArgument,
        BusinessStatus::NotFound,
        BusinessStatus::AlreadyExists,
        BusinessStatus::OutOfRange,
        BusinessStatus::PermissionDenied,
    ] {
        let report = Report::new("toy-meter-low").arg("reading", 3i64);

        let untyped = coded(refused(&report, None, status));
        assert_eq!(untyped.grpc, GrpcCode::from(status), "{status:?}");
        assert_eq!(untyped.code, "toy-meter-low", "{status:?}");
        assert_eq!(
            untyped.extras.get("reading").map(String::as_str),
            Some("3"),
            "{status:?}"
        );
        let read = Rejected::of(&untyped);
        assert_eq!(read.status(), status);
        assert_eq!(read.code(), "toy-meter-low");
        assert_eq!(read.refusal_object(), None);

        let typed = coded(refused(&report, Some(object.clone()), status));
        assert_eq!(typed.grpc, GrpcCode::from(status), "{status:?}");
        assert_eq!(typed.code, "COMMAND_REFUSED", "{status:?}");
        let read = Rejected::of(&typed);
        assert_eq!(read.status(), status);
        assert_eq!(read.code(), "angzarr-embedded-next-command-refused");
        assert_eq!(read.refusal_object(), Some(&object));
    }
}
