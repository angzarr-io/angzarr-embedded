//! The kernel's own classification of a status (decisions 14.37, 14.42).

use super::{class, Class};

#[test]
fn the_kernel_classifies_every_status_itself() {
    // Every gRPC status by its wire number, OK (0) to UNAUTHENTICATED (16).
    let expected = [
        (0, Class::BugRetryable),
        (1, Class::BugRetryable),       // CANCELLED
        (2, Class::BugFatal),           // UNKNOWN
        (3, Class::BusinessFatal),      // INVALID_ARGUMENT
        (4, Class::BugRetryable),       // DEADLINE_EXCEEDED
        (5, Class::BusinessFatal),      // NOT_FOUND
        (6, Class::BusinessFatal),      // ALREADY_EXISTS
        (7, Class::BusinessFatal),      // PERMISSION_DENIED
        (8, Class::BugRetryable),       // RESOURCE_EXHAUSTED
        (9, Class::BusinessFatal),      // FAILED_PRECONDITION
        (10, Class::BusinessImmediate), // ABORTED
        (11, Class::BusinessFatal),     // OUT_OF_RANGE
        (12, Class::BugRetryable),      // UNIMPLEMENTED
        (13, Class::BugFatal),          // INTERNAL
        (14, Class::BugRetryable),      // UNAVAILABLE
        (15, Class::BugFatal),          // DATA_LOSS
        (16, Class::BugRetryable),      // UNAUTHENTICATED
        (99, Class::BugRetryable),      // none at all
    ];
    for (code, class_of) in expected {
        assert_eq!(class(code), class_of, "status {code}");
    }
}

/// The coordinator's conflicts on the wire (decisions 14.42, 14.45):
/// each ABORTED under its key, its args as metadata; an error that is
/// none of them has no wire answer of the coordinator's.
#[test]
fn the_coordinators_conflicts_are_coded_under_their_keys() {
    use std::collections::BTreeMap;

    use angzarr_router::error::GrpcCode;

    use super::super::reasons::CoordinatorReason;
    use super::KernelError;
    let stream = || angzarr_proto::v1::Cover {
        domain: "shelf".to_string(),
        ..Default::default()
    };
    let coded = |error: KernelError| {
        error
            .as_coded()
            .map(|c| (c.code, c.extras, c.grpc))
            .expect("one of the coordinator's answers")
    };
    let extras = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    };
    assert_eq!(
        coded(KernelError::Overlap {
            stream: stream(),
            expected: 1,
            actual: 3,
            window_units: "items[a]".to_string(),
            command_units: "items[a].count".to_string(),
        }),
        (
            CoordinatorReason::ConflictOverlap.as_str_name().to_string(),
            extras(&[
                ("window_units", "items[a]"),
                ("command_units", "items[a].count")
            ]),
            GrpcCode::Aborted
        )
    );
    assert_eq!(
        coded(KernelError::Unresolved {
            stream: stream(),
            expected: 1,
            actual: 3,
        }),
        (
            CoordinatorReason::ConflictUnresolved
                .as_str_name()
                .to_string(),
            BTreeMap::new(),
            GrpcCode::Aborted
        )
    );
    assert!(KernelError::UnregisteredDomain {
        domain: "shelf".to_string()
    }
    .as_coded()
    .is_none());
}

/// The limits and an unreachable handler on the wire, under decision
/// 14.49's keys: the two limits INTERNAL (fatal bugs) with their `limit`,
/// the unreachable handler UNAVAILABLE (a retryable bug) with its domain
/// and cause.
#[test]
fn the_limits_and_an_unreachable_handler_are_coded_under_their_keys() {
    use std::collections::BTreeMap;

    use angzarr_router::error::GrpcCode;

    use super::super::reasons::{CoordinatorReason, RouterReason};
    use super::KernelError;
    let stream = || angzarr_proto::v1::Cover {
        domain: "shelf".to_string(),
        ..Default::default()
    };
    let coded = |error: KernelError| {
        error
            .as_coded()
            .map(|c| (c.code, c.extras, c.grpc))
            .expect("one of the coordinator's answers")
    };
    let extras = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    };
    assert_eq!(
        coded(KernelError::ChainTooDeep { limit: 8 }),
        (
            CoordinatorReason::ChainTooDeep.as_str_name().to_string(),
            extras(&[("limit", "8")]),
            GrpcCode::Internal
        )
    );
    assert_eq!(
        coded(KernelError::TooManySteps {
            stream: stream(),
            limit: 16
        }),
        (
            RouterReason::TooManySteps.as_str_name().to_string(),
            extras(&[("limit", "16")]),
            GrpcCode::Internal
        )
    );
    assert_eq!(
        coded(KernelError::HandlerUnavailable {
            stream: stream(),
            cause: "timeout".to_string()
        }),
        (
            CoordinatorReason::HandlerUnavailable
                .as_str_name()
                .to_string(),
            extras(&[("domain", "shelf"), ("cause", "timeout")]),
            GrpcCode::Unavailable
        )
    );
}
