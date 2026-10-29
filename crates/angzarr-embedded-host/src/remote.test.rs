use std::collections::BTreeMap;

use crate::rpc::google_rpc::{ErrorInfo, Status as RpcStatus, ERROR_INFO_TYPE_URL};
use angzarr_router::error::{codes as router_codes, extras, GrpcCode};
use prost::Message;
use prost_types::Any;
use tonic::Code;

use super::*;

/// A status as a child answers it: code and message, and its details the
/// encoded `google.rpc.Status` carrying them again with the details.
pub(crate) fn status_with_details(code: Code, message: &str, details: Vec<Any>) -> tonic::Status {
    let rpc = RpcStatus {
        code: code as i32,
        message: message.to_string(),
        details,
    };
    tonic::Status::with_details(code, message, rpc.encode_to_vec().into())
}

pub(crate) fn error_info(reason: &str, metadata: &[(&str, &str)]) -> Any {
    let info = ErrorInfo {
        reason: reason.to_string(),
        domain: angzarr_router::error::ERROR_INFO_DOMAIN.to_string(),
        metadata: metadata
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    };
    Any {
        type_url: ERROR_INFO_TYPE_URL.to_string(),
        value: info.encode_to_vec(),
    }
}

fn refusal() -> Any {
    Any {
        type_url: "/examples.blackjack.TableFull".to_string(),
        value: vec![8, 6],
    }
}

/// The typed refusal travels as the status's other detail; the router gets
/// it as `CodedError::refusal`, a FAILED_PRECONDITION, so its
/// `refusal_response` is what the refused policy is given.
#[test]
fn a_failed_precondition_is_the_routers_refusal() {
    let status = status_with_details(
        Code::FailedPrecondition,
        "table is full",
        vec![error_info("TABLE_FULL", &[("seats", "6")]), refusal()],
    );
    let coded = status_to_coded("player", &status);
    assert_eq!(
        coded,
        CodedError {
            code: "TABLE_FULL".to_string(),
            message: "table is full".to_string(),
            extras: BTreeMap::from([("seats".to_string(), "6".to_string())]),
            grpc: GrpcCode::FailedPrecondition,
            refusal: Some(Box::new(refusal())),
        }
    );
    let response = coded.refusal_response().expect("a refusal");
    assert_eq!(response.refusal, Some(refusal()));
    assert!(!is_retryable(&coded));
}

/// The detail order is not the contract: the refusal is the detail that is
/// not the ErrorInfo.
#[test]
fn the_refusal_is_found_before_the_error_info_too() {
    let status = status_with_details(
        Code::FailedPrecondition,
        "table is full",
        vec![refusal(), error_info("TABLE_FULL", &[])],
    );
    let coded = status_to_coded("player", &status);
    assert_eq!(coded.code, "TABLE_FULL");
    assert_eq!(coded.refusal, Some(Box::new(refusal())));
}

#[test]
fn an_unavailable_status_is_retryable() {
    let coded = status_to_coded("player", &tonic::Status::unavailable("connection refused"));
    assert_eq!(coded.code, codes::REMOTE_HANDLER_UNAVAILABLE);
    assert_eq!(
        coded.extras,
        BTreeMap::from([
            (extras::DOMAIN.to_string(), "player".to_string()),
            (CAUSE.to_string(), causes::TRANSPORT.to_string()),
        ])
    );
    assert!(is_retryable(&coded));
}

#[test]
fn a_child_not_running_is_retryable() {
    let coded = unavailable("player", causes::NOT_RUNNING);
    assert_eq!(
        coded,
        CodedError {
            code: codes::REMOTE_HANDLER_UNAVAILABLE.to_string(),
            message: REMOTE_HANDLER_UNAVAILABLE_MESSAGE.to_string(),
            extras: BTreeMap::from([
                (extras::DOMAIN.to_string(), "player".to_string()),
                (CAUSE.to_string(), causes::NOT_RUNNING.to_string()),
            ]),
            grpc: GrpcCode::Aborted,
            refusal: None,
        }
    );
    assert!(is_retryable(&coded));
}

/// Another coded failure keeps its code and its gRPC code, and is not
/// retried: it would fail the same way again.
#[test]
fn another_status_keeps_its_code() {
    let status = status_with_details(
        Code::InvalidArgument,
        "Unknown command type",
        vec![error_info(
            router_codes::NO_HANDLER_REGISTERED,
            &[("type_url", "/x.Y")],
        )],
    );
    let coded = status_to_coded("player", &status);
    assert_eq!(coded.code, router_codes::NO_HANDLER_REGISTERED);
    assert_eq!(coded.message, "Unknown command type");
    assert_eq!(
        coded.extras,
        BTreeMap::from([("type_url".to_string(), "/x.Y".to_string())])
    );
    assert_eq!(coded.grpc, GrpcCode::InvalidArgument);
    assert_eq!(coded.refusal, None);
    assert!(!is_retryable(&coded));
}

/// A status with no ErrorInfo is a failure no one classified.
#[test]
fn a_status_without_error_info_is_unhandled() {
    let coded = status_to_coded("player", &tonic::Status::internal("boom"));
    assert_eq!(coded.code, router_codes::UNHANDLED_HANDLER_ERROR);
    assert_eq!(coded.message, "boom");
    assert_eq!(coded.grpc, GrpcCode::Internal);
    assert!(!is_retryable(&coded));
}

/// Every gRPC code the router knows is kept; a business refusal's status
/// it has no variant for (ALREADY_EXISTS, OUT_OF_RANGE, PERMISSION_DENIED)
/// is FAILED_PRECONDITION; any other is INTERNAL.
#[test]
fn each_status_code_maps_to_the_routers() {
    for (code, grpc) in [
        (Code::InvalidArgument, GrpcCode::InvalidArgument),
        (Code::NotFound, GrpcCode::NotFound),
        (Code::FailedPrecondition, GrpcCode::FailedPrecondition),
        (Code::AlreadyExists, GrpcCode::FailedPrecondition),
        (Code::OutOfRange, GrpcCode::FailedPrecondition),
        (Code::PermissionDenied, GrpcCode::FailedPrecondition),
        (Code::Aborted, GrpcCode::Aborted),
        (Code::Unimplemented, GrpcCode::Unimplemented),
        (Code::DataLoss, GrpcCode::DataLoss),
        (Code::Internal, GrpcCode::Internal),
        (Code::Unknown, GrpcCode::Internal),
        (Code::Unavailable, GrpcCode::Internal),
        (Code::DeadlineExceeded, GrpcCode::Internal),
        (Code::ResourceExhausted, GrpcCode::Internal),
    ] {
        let status = status_with_details(code, "m", vec![error_info("SOME_REASON", &[])]);
        assert_eq!(status_to_coded("player", &status).grpc, grpc, "{code:?}");
    }
}

/// A child's refusal under `code`, a business status (the owner's ruling,
/// 2026-10-29): its reason, metadata and typed refusal kept, under `grpc`,
/// and not retried.
fn refused_under(code: Code, grpc: GrpcCode) {
    let status = status_with_details(
        code,
        "The seat is taken",
        vec![error_info("SEAT_TAKEN", &[("seat", "3")]), refusal()],
    );
    let coded = status_to_coded("player", &status);
    assert_eq!(coded.code, "SEAT_TAKEN", "{code:?}");
    assert_eq!(
        coded.extras,
        BTreeMap::from([("seat".to_string(), "3".to_string())]),
        "{code:?}"
    );
    assert_eq!(coded.grpc, grpc, "{code:?}");
    assert_eq!(coded.refusal.as_deref(), Some(&refusal()), "{code:?}");
    assert!(!is_retryable(&coded), "{code:?}");
}

#[test]
fn a_childs_invalid_argument_is_a_refusal_with_its_object() {
    refused_under(Code::InvalidArgument, GrpcCode::InvalidArgument);
}

#[test]
fn a_childs_not_found_is_a_refusal_with_its_object() {
    refused_under(Code::NotFound, GrpcCode::NotFound);
}

/// angzarr-router's `GrpcCode` has no ALREADY_EXISTS: the refusal arrives
/// FAILED_PRECONDITION.
#[test]
fn a_childs_already_exists_is_a_refusal_with_its_object() {
    refused_under(Code::AlreadyExists, GrpcCode::FailedPrecondition);
}

/// angzarr-router's `GrpcCode` has no OUT_OF_RANGE: the refusal arrives
/// FAILED_PRECONDITION.
#[test]
fn a_childs_out_of_range_is_a_refusal_with_its_object() {
    refused_under(Code::OutOfRange, GrpcCode::FailedPrecondition);
}

/// angzarr-router's `GrpcCode` has no PERMISSION_DENIED: the refusal
/// arrives FAILED_PRECONDITION.
#[test]
fn a_childs_permission_denied_is_a_refusal_with_its_object() {
    refused_under(Code::PermissionDenied, GrpcCode::FailedPrecondition);
}

/// A child's bug (INTERNAL, UNKNOWN, DATA_LOSS) keeps its reason, carries
/// no typed refusal, and is not retried; UNKNOWN arrives INTERNAL.
#[test]
fn a_childs_bug_status_is_no_refusal() {
    for (code, grpc) in [
        (Code::Internal, GrpcCode::Internal),
        (Code::Unknown, GrpcCode::Internal),
        (Code::DataLoss, GrpcCode::DataLoss),
    ] {
        let status = status_with_details(
            code,
            "broke",
            vec![error_info("PLAYER_BROKE", &[]), refusal()],
        );
        let coded = status_to_coded("player", &status);
        assert_eq!(coded.code, "PLAYER_BROKE", "{code:?}");
        assert_eq!(coded.grpc, grpc, "{code:?}");
        assert_eq!(coded.refusal, None, "{code:?}");
        assert!(!is_retryable(&coded), "{code:?}");
    }
}

/// The retryable and other statuses keep their classification: a child's
/// UNAVAILABLE with no details is unavailability, retried; one it sent
/// with its reason, and a DEADLINE_EXCEEDED or RESOURCE_EXHAUSTED, keep
/// the reason, INTERNAL; an ABORTED or UNIMPLEMENTED keeps its status.
/// None carries a typed refusal.
#[test]
fn a_childs_retryable_or_other_status_keeps_its_classification() {
    let bare = status_to_coded("player", &tonic::Status::unavailable("gone"));
    assert_eq!(bare.code, codes::REMOTE_HANDLER_UNAVAILABLE);
    assert!(is_retryable(&bare));
    for (code, grpc) in [
        (Code::Unavailable, GrpcCode::Internal),
        (Code::DeadlineExceeded, GrpcCode::Internal),
        (Code::ResourceExhausted, GrpcCode::Internal),
        (Code::Aborted, GrpcCode::Aborted),
        (Code::Unimplemented, GrpcCode::Unimplemented),
    ] {
        let status =
            status_with_details(code, "m", vec![error_info("PLAYER_BUSY", &[]), refusal()]);
        let coded = status_to_coded("player", &status);
        assert_eq!(coded.code, "PLAYER_BUSY", "{code:?}");
        assert_eq!(coded.grpc, grpc, "{code:?}");
        assert_eq!(coded.refusal, None, "{code:?}");
        assert!(!is_retryable(&coded), "{code:?}");
    }
}

/// UNKNOWN is the transport's only when the transport is its source; a
/// child's own UNKNOWN is a failure no one classified.
#[test]
fn a_childs_own_unknown_is_unhandled() {
    let coded = status_to_coded("player", &tonic::Status::unknown("what"));
    assert_eq!(coded.code, router_codes::UNHANDLED_HANDLER_ERROR);
    assert!(!is_retryable(&coded));
}

/// A typed detail on a status outside the business set is no refusal.
#[test]
fn only_a_business_status_carries_a_refusal() {
    let status = status_with_details(
        Code::Internal,
        "bad",
        vec![error_info("BAD", &[]), refusal()],
    );
    assert_eq!(status_to_coded("player", &status).refusal, None);
}

#[test]
fn only_unavailability_is_retryable() {
    let other = CodedError::invalid_argument("REMOTE_HANDLER_UNAVAILABLE_NOT", "m", []);
    assert!(!is_retryable(&other));
    let stale = CodedError::stale(router_codes::CONFLICT_OVERLAP, "m", []);
    assert!(!is_retryable(&stale));
}
