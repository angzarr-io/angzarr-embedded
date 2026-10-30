//! The coordinator's reason keys (decisions 14.37 and 14.42): the keys
//! angzarr-project's catalog (`catalog/reasons.toml`) lists as answered by
//! the coordinator, which the kernel is, and the names of their args. The
//! router's keys are angzarr-router's own (`error::codes`); none is defined
//! twice. Each constant is its catalog entry: who answers it, its status and
//! its args.

/// Answered by the coordinator, with status ABORTED; no args.
pub const CONFLICT_UNRESOLVED: &str = "CONFLICT_UNRESOLVED";

/// Answered by the coordinator, with status ABORTED; args: `window_units`,
/// `command_units`.
pub const CONFLICT_OVERLAP: &str = "CONFLICT_OVERLAP";

/// Answered by the coordinator, with status FAILED_PRECONDITION; args:
/// `type_url`.
pub const PM_NO_INSTANCE: &str = "PM_NO_INSTANCE";

/// Answered by the coordinator, with status INTERNAL; args: `domain`.
pub const REFUSAL_FACT_WRONG_STREAM: &str = "REFUSAL_FACT_WRONG_STREAM";

/// Answered by the coordinator, with status INTERNAL; args:
/// `expected_domain`, `actual_domain`.
pub const POLICY_COMMAND_OUTSIDE_DESTINATION: &str = "POLICY_COMMAND_OUTSIDE_DESTINATION";

/// Answered by the coordinator, with status INTERNAL; args:
/// `expected_edition`, `actual_edition`.
pub const POLICY_COMMAND_OUTSIDE_EDITION: &str = "POLICY_COMMAND_OUTSIDE_EDITION";

/// Answered by the coordinator, with status INTERNAL; args: `domain`,
/// `command_type`.
pub const POLICY_COMMAND_NO_ROOT: &str = "POLICY_COMMAND_NO_ROOT";

/// Answered by the coordinator, with status INTERNAL; args: `domain`,
/// `command_type`.
pub const POLICY_COMMAND_OTHER_CORRELATION: &str = "POLICY_COMMAND_OTHER_CORRELATION";

/// Answered by the coordinator, with status INTERNAL; no args.
pub const REFUSAL_FACT_ADDRESSED: &str = "REFUSAL_FACT_ADDRESSED";

/// Answered by the coordinator, with status INTERNAL; no args.
pub const REFUSAL_FACT_NO_PAGE: &str = "REFUSAL_FACT_NO_PAGE";

/// Answered by the coordinator, with status INTERNAL; no args.
pub const REFUSAL_FACT_NOT_A_FACT: &str = "REFUSAL_FACT_NOT_A_FACT";

/// Answered by the coordinator, with status INTERNAL; no args.
pub const REFUSAL_FACT_IDENTIFIED: &str = "REFUSAL_FACT_IDENTIFIED";

/// Answered by the coordinator, as a readiness refusal; args: `component`.
pub const MISSING_RULES_VERSION: &str = "MISSING_RULES_VERSION";

/// Answered by the coordinator, as a readiness refusal; args: `component`,
/// `component_kind`.
pub const MISSING_STATE_TYPE: &str = "MISSING_STATE_TYPE";

/// Answered by the coordinator, as a readiness refusal; args: `component`,
/// `component_kind`.
pub const STARTING_TYPES_ON_NON_PM: &str = "STARTING_TYPES_ON_NON_PM";

/// Answered by the coordinator, as a readiness refusal; args: `component`,
/// `component_kind`.
pub const STATE_ON_STATELESS_KIND: &str = "STATE_ON_STATELESS_KIND";

/// Answered by the coordinator, as a readiness refusal; args: `component`,
/// `input_count`.
pub const POLICY_INPUT_COUNT: &str = "POLICY_INPUT_COUNT";

/// Answered by the coordinator, as a readiness refusal; args: `component`.
pub const POLICY_NO_OUTPUT_DOMAIN: &str = "POLICY_NO_OUTPUT_DOMAIN";

/// Answered by the coordinator, as a readiness refusal; args: `component`,
/// `component_kind`.
pub const OUTPUT_DOMAIN_ON_NON_POLICY: &str = "OUTPUT_DOMAIN_ON_NON_POLICY";

/// The coordinator's keys an input is answered with, in the router's shape
/// (a code, its args as metadata, a status): what the kernel forwards as
/// [`KernelError::Router`](super::KernelError::Router) or a
/// [`Rejected`](super::Rejected), each under an angzarr-embedded code of its
/// own. The readiness keys are a component's, refused before any input.
pub const ANSWERED: &[&str] = &[
    CONFLICT_UNRESOLVED,
    CONFLICT_OVERLAP,
    PM_NO_INSTANCE,
    REFUSAL_FACT_WRONG_STREAM,
    POLICY_COMMAND_OUTSIDE_DESTINATION,
    POLICY_COMMAND_OUTSIDE_EDITION,
    POLICY_COMMAND_NO_ROOT,
    POLICY_COMMAND_OTHER_CORRELATION,
    REFUSAL_FACT_ADDRESSED,
    REFUSAL_FACT_NO_PAGE,
    REFUSAL_FACT_NOT_A_FACT,
    REFUSAL_FACT_IDENTIFIED,
];

/// The catalog's arg names for the coordinator's keys (the `ErrorInfo`
/// metadata keys) that angzarr-router's `error::extras` does not name.
pub mod args {
    /// The domain a refusal fact or a policy's command was addressed to.
    pub const DOMAIN: &str = "domain";
    /// The window's conflict units, comma-separated.
    pub const WINDOW_UNITS: &str = "window_units";
    /// The command's conflict units, comma-separated.
    pub const COMMAND_UNITS: &str = "command_units";
    /// A policy's one destination domain.
    pub const EXPECTED_DOMAIN: &str = "expected_domain";
    /// The domain a policy's command named instead.
    pub const ACTUAL_DOMAIN: &str = "actual_domain";
    /// The trigger's edition, which a policy's commands go into.
    pub const EXPECTED_EDITION: &str = "expected_edition";
    /// The edition a policy's command named instead.
    pub const ACTUAL_EDITION: &str = "actual_edition";
}
