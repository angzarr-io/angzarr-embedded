use std::collections::BTreeMap;

use angzarr_router::error::{codes as router_codes, extras, GrpcCode};
use prost::Message;
use prost_types::Any;
use tonic::Code;
use tonic_types::pb::{ErrorInfo, Status as RpcStatus};

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
        type_url: "type.googleapis.com/google.rpc.ErrorInfo".to_string(),
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

#[test]
fn only_unavailability_is_retryable() {
    let other = CodedError::invalid_argument("REMOTE_HANDLER_UNAVAILABLE_NOT", "m", []);
    assert!(!is_retryable(&other));
    let stale = CodedError::stale(router_codes::CONFLICT_OVERLAP, "m", []);
    assert!(!is_retryable(&stale));
}
