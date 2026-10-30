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
