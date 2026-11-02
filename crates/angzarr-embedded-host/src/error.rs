//! The host's errors: why a remote component could not be started. Each is a
//! code and its args (`angzarr_diagnostic::Diagnostic`), and its
//! `Display` is those, never English. A failure once a domain is running is not
//! one of these: it is the router's [`CodedError`], from
//! [`RemoteCommandHandler`]'s dispatch.
//!
//! [`CodedError`]: angzarr_router::error::CodedError
//! [`RemoteCommandHandler`]: crate::RemoteCommandHandler

use std::fmt;

use angzarr_diagnostic::{Arg, Diagnostic, Report};
// A child's failures are the local-process transport's keys; a projector
// with no input domain is core's (decision 14.64).
use angzarr_proto::v1::HostReason as CoreHostReason;
use angzarr_proto_transport_embedded::v1::{reason_args, HostReason};

/// What was wrong with a child's handshake: the one stdout line,
/// `angzarr-handshake v1 unix:<absolute-socket-path>`, it prints once it is
/// serving.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandshakeError {
    /// A line that announces itself as a handshake (it starts
    /// `angzarr-handshake`) but is not a v1 one naming an absolute UTF-8
    /// Unix socket path: the spec's `HOST_REASON_HANDSHAKE_MALFORMED`.
    /// `line` is the line as read, its line ending removed; one that is
    /// not UTF-8 has each byte outside printable ASCII, and each
    /// backslash, written `\xhh` (`\xe9`), so nothing is decoded lossily.
    Malformed { line: String },
    /// No handshake within the configured time: the spec's
    /// `HOST_REASON_HANDSHAKE_TIMEOUT`.
    Timeout { timeout_ms: u64 },
    /// The child's stdout ended (it exited, or closed it) before a
    /// handshake: the spec's `HOST_REASON_HANDSHAKE_CLOSED`.
    Closed,
    /// Reading the child's stdout failed. `kind` is the I/O error's
    /// `std::io::ErrorKind`, by name.
    Unreadable { kind: String },
}

impl fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for HandshakeError {
    fn code(&self) -> &str {
        match self {
            HandshakeError::Malformed { .. } => HostReason::HandshakeMalformed.as_str_name(),
            HandshakeError::Timeout { .. } => HostReason::HandshakeTimeout.as_str_name(),
            HandshakeError::Closed => HostReason::HandshakeClosed.as_str_name(),
            HandshakeError::Unreadable { .. } => "angzarr-embedded-host-handshake-unreadable",
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            HandshakeError::Malformed { line } => {
                vec![(reason_args::LINE, line.as_str().into())]
            }
            HandshakeError::Timeout { timeout_ms } => {
                vec![(reason_args::TIMEOUT_MS, (*timeout_ms).into())]
            }
            HandshakeError::Closed => vec![],
            HandshakeError::Unreadable { kind } => vec![("kind", kind.as_str().into())],
        }
    }
}

/// Why the host could not start: a config it refuses, or a remote
/// component that did not come up. Every variant about one component names
/// it as `domain`: its `component` entry name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostError {
    /// Two `component` entries with one name.
    DuplicateDomain { domain: String },
    /// A remote component whose language has no default launch command, and
    /// whose config gives none.
    NoLaunch { domain: String, language: String },
    /// A `launch` with no program in it.
    EmptyLaunch { domain: String },
    /// A component whose `language` is not one of the languages loaded
    /// (`config::Languages`): `key` its key path
    /// (`component[1].language`), `loaded` the loaded languages' names, in
    /// order, comma-separated.
    UnknownLanguage {
        key: String,
        language: String,
        loaded: String,
    },
    /// The launch command's program could not be started. `kind` is the I/O
    /// error's `std::io::ErrorKind`, by name.
    Spawn {
        domain: String,
        program: String,
        kind: String,
    },
    /// The child started but its handshake failed.
    Handshake {
        domain: String,
        cause: HandshakeError,
    },
    /// No gRPC connection to the socket the handshake named.
    Connect { domain: String, socket: String },
    /// `ComponentService.Describe` failed; `grpc_code` is the status's wire
    /// code.
    Describe { domain: String, grpc_code: i32 },
    /// A child's `grpc.health.v1.Health.Watch` failed (the child protocol:
    /// every child serves gRPC health); `grpc_code` is the status's wire
    /// code.
    Health { domain: String, grpc_code: i32 },
    /// A child that is described reported its server (service `""`) other
    /// than SERVING once it had handshaken, when the child protocol says
    /// it handshakes only once SERVING: the spec's
    /// `HOST_REASON_CHILD_NOT_SERVING`. `status` is the health status, by
    /// its name (`NOT_SERVING`).
    NotServing { component: String, status: String },
    /// The child described itself as another kind than its `kind` says:
    /// `described` is its descriptor's `ComponentKind`, by name.
    KindMismatch {
        domain: String,
        configured: String,
        described: String,
    },
    /// A policy whose descriptor names no input domain: it has no source.
    NoInput { domain: String },
    /// A projector whose descriptor names no input domain: there is no
    /// wildcard domain, so it would fold nothing. The spec's
    /// `HOST_REASON_PROJECTOR_NO_INPUT_DOMAIN` (decision 14.55), its
    /// `component` the component entry's name.
    ProjectorNoInputDomain { component: String },
    /// A `kind = "upcaster"` component that names no domain to upcast
    /// (`upcasts`).
    UpcasterNoDomain { domain: String },
    /// A component given an `expose` other than `"none"` that is not an
    /// aggregate: `kind` is its `kind`, by its name in the config. Only an aggregate
    /// is served through the host's listener.
    NotExposable { domain: String, kind: String },
    /// The child's descriptor fails the coordinator's validation
    /// (decision 14.42, C-0687; `angzarr_embedded::validate_descriptor`).
    Descriptor {
        domain: String,
        cause: angzarr_embedded::Misconfigured,
    },
    /// A restarted child described another component than its first
    /// child did: another name or kind, or a policy or projector of other
    /// inputs or another output domain. It is not published; the host
    /// takes such a change only when it starts.
    DescriptorChanged { domain: String },
    /// A probed child exited on its stdin's end with a status other than
    /// 0 ([`crate::probe`]): the spec's `HOST_REASON_CHILD_EXIT_STATUS`.
    ExitStatus { component: String, exit_code: i32 },
    /// A probed child did not exit by itself on its stdin's end within the
    /// shutdown grace ([`crate::probe`]): the spec's
    /// `HOST_REASON_CHILD_NOT_EXITED`. `ended` is how it ended
    /// ([`ChildExit::ended`](crate::ChildExit::ended)).
    NotExited { component: String, ended: String },
    /// The probe was given an input a child of `kind` does not handle (a
    /// command for a policy, a book of events for a domain): the spec's
    /// `HOST_REASON_PROBE_INPUT`.
    ProbeInput { component: String, kind: String },
    /// The configured `socket_dir` is too long for a Unix socket path to
    /// hold the host's socket names in it: at most `max_bytes` bytes.
    SocketDirTooLong { dir: String, max_bytes: u64 },
    /// The config file could not be read. `kind` is the I/O error's
    /// `std::io::ErrorKind`, by name.
    ConfigUnreadable { path: String, kind: String },
    /// The config file does not load: YAML the reader does not take, a
    /// `schema_version` newer than this build reads or older than the
    /// oldest it reads (none is version 0), an unknown key, a missing one,
    /// a value outside its closed set or of the wrong type. `cause` is the
    /// reader's error, angzarr-yaml's `LoadError`, naming for a decode the
    /// key path where it goes wrong (`component[1].kind`).
    ConfigInvalid { path: String, cause: Report },
    /// An in-process component its `component` entry lists (`kind` its kind
    /// in the config) that the application's `register` did not register: no
    /// domain, policy or projector of that name, or, for an upcaster, no
    /// domain here of its `upcasts`.
    NotRegistered { domain: String, kind: String },
    /// A component the application's `register` registered in process
    /// (`kind` its kind in the config) that no `component` entry lists as an
    /// in-process component of that kind: every component is listed.
    NotListed { domain: String, kind: String },
    /// The registry refused a domain (the kernel's `Misconfigured`).
    Register { cause: Report },
    /// The store could not be opened (the kernel's store error).
    Store { cause: Report },
    /// The host's own runtime could not be built. `kind` is the I/O error's
    /// `std::io::ErrorKind`, by name.
    Runtime { kind: String },
}

impl fmt::Display for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for HostError {
    fn code(&self) -> &str {
        match self {
            HostError::DuplicateDomain { .. } => "angzarr-embedded-host-duplicate-domain",
            HostError::NoLaunch { .. } => "angzarr-embedded-host-no-launch",
            HostError::EmptyLaunch { .. } => "angzarr-embedded-host-empty-launch",
            HostError::UnknownLanguage { .. } => "angzarr-embedded-host-unknown-language",
            HostError::Spawn { .. } => "angzarr-embedded-host-spawn",
            HostError::Handshake { .. } => "angzarr-embedded-host-handshake",
            HostError::Connect { .. } => "angzarr-embedded-host-connect",
            HostError::Describe { .. } => "angzarr-embedded-host-describe",
            HostError::Health { .. } => "angzarr-embedded-host-health",
            HostError::NotServing { .. } => HostReason::ChildNotServing.as_str_name(),
            HostError::KindMismatch { .. } => "angzarr-embedded-host-kind-mismatch",
            HostError::NoInput { .. } => "angzarr-embedded-host-no-input",
            HostError::ProjectorNoInputDomain { .. } => {
                CoreHostReason::ProjectorNoInputDomain.as_str_name()
            }
            HostError::Descriptor { .. } => "angzarr-embedded-host-descriptor",
            HostError::DescriptorChanged { .. } => "angzarr-embedded-host-descriptor-changed",
            HostError::UpcasterNoDomain { .. } => "angzarr-embedded-host-upcaster-no-domain",
            HostError::NotExposable { .. } => "angzarr-embedded-host-not-exposable",
            HostError::ExitStatus { .. } => HostReason::ChildExitStatus.as_str_name(),
            HostError::NotExited { .. } => HostReason::ChildNotExited.as_str_name(),
            HostError::ProbeInput { .. } => HostReason::ProbeInput.as_str_name(),
            HostError::SocketDirTooLong { .. } => "angzarr-embedded-host-socket-dir-too-long",
            HostError::ConfigUnreadable { .. } => "angzarr-embedded-host-config-unreadable",
            HostError::ConfigInvalid { .. } => "angzarr-embedded-host-config-invalid",
            HostError::NotRegistered { .. } => "angzarr-embedded-host-not-registered",
            HostError::NotListed { .. } => "angzarr-embedded-host-not-listed",
            HostError::Register { .. } => "angzarr-embedded-host-register",
            HostError::Store { .. } => "angzarr-embedded-host-store",
            HostError::Runtime { .. } => "angzarr-embedded-host-runtime",
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        let domain = |d: &str| ("domain", Arg::from(d));
        let component = |c: &str| (reason_args::COMPONENT, Arg::from(c));
        match self {
            HostError::DuplicateDomain { domain: d }
            | HostError::EmptyLaunch { domain: d }
            | HostError::NoInput { domain: d }
            | HostError::DescriptorChanged { domain: d }
            | HostError::UpcasterNoDomain { domain: d } => {
                vec![domain(d)]
            }
            HostError::ProjectorNoInputDomain { component: c } => vec![component(c)],
            HostError::NoLaunch {
                domain: d,
                language,
            } => vec![domain(d), ("language", language.as_str().into())],
            HostError::Spawn {
                domain: d,
                program,
                kind,
            } => vec![
                domain(d),
                ("program", program.as_str().into()),
                ("kind", kind.as_str().into()),
            ],
            HostError::Handshake { domain: d, cause } => {
                vec![domain(d), ("cause", Arg::nested(cause))]
            }
            HostError::ExitStatus {
                component: c,
                exit_code,
            } => vec![component(c), (reason_args::EXIT_CODE, (*exit_code).into())],
            HostError::NotExited {
                component: c,
                ended,
            } => {
                vec![component(c), (reason_args::ENDED, ended.as_str().into())]
            }
            HostError::ProbeInput { component: c, kind } => {
                vec![component(c), (reason_args::KIND, kind.as_str().into())]
            }
            HostError::NotExposable { domain: d, kind }
            | HostError::NotRegistered { domain: d, kind }
            | HostError::NotListed { domain: d, kind } => {
                vec![domain(d), ("kind", kind.as_str().into())]
            }
            HostError::Connect { domain: d, socket } => {
                vec![domain(d), ("socket", socket.as_str().into())]
            }
            HostError::NotServing {
                component: c,
                status,
            } => {
                vec![component(c), (reason_args::STATUS, status.as_str().into())]
            }
            HostError::Describe {
                domain: d,
                grpc_code,
            }
            | HostError::Health {
                domain: d,
                grpc_code,
            } => vec![domain(d), ("grpc_code", (*grpc_code).into())],
            HostError::KindMismatch {
                domain: d,
                configured,
                described,
            } => vec![
                domain(d),
                ("configured", configured.as_str().into()),
                ("described", described.as_str().into()),
            ],
            HostError::Descriptor { domain: d, cause } => {
                vec![domain(d), ("cause", Arg::nested(cause))]
            }
            HostError::SocketDirTooLong { dir, max_bytes } => vec![
                ("dir", dir.as_str().into()),
                ("max_bytes", (*max_bytes).into()),
            ],
            HostError::UnknownLanguage {
                key,
                language,
                loaded,
            } => vec![
                ("key", key.as_str().into()),
                ("language", language.as_str().into()),
                ("loaded", loaded.as_str().into()),
            ],
            HostError::ConfigUnreadable { path, kind } => vec![
                ("path", path.as_str().into()),
                ("kind", kind.as_str().into()),
            ],
            HostError::ConfigInvalid { path, cause } => vec![
                ("path", path.as_str().into()),
                ("cause", Arg::nested(cause)),
            ],
            HostError::Register { cause } | HostError::Store { cause } => {
                vec![("cause", Arg::nested(cause))]
            }
            HostError::Runtime { kind } => vec![("kind", kind.as_str().into())],
        }
    }
}

impl std::error::Error for HandshakeError {}
impl std::error::Error for HostError {}

/// One value of every error this module defines, each variant once.
pub(crate) fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    let handshake = handshake_samples()
        .into_iter()
        .map(|e| Box::new(e) as Box<dyn Diagnostic>);
    let host = component_samples().into_iter().chain(host_samples());
    handshake.chain(host.map(|e| Box::new(e) as _)).collect()
}

/// Each [`HandshakeError`] variant.
fn handshake_samples() -> Vec<HandshakeError> {
    vec![
        HandshakeError::Malformed {
            line: "angzarr-handshake v2".to_string(),
        },
        HandshakeError::Timeout { timeout_ms: 30_000 },
        HandshakeError::Closed,
        HandshakeError::Unreadable {
            kind: "BrokenPipe".to_string(),
        },
    ]
}

/// Each [`HostError`] variant about one component, which it names.
fn component_samples() -> Vec<HostError> {
    let d = || "player".to_string();
    vec![
        HostError::DuplicateDomain { domain: d() },
        HostError::NoLaunch {
            domain: d(),
            language: "go".to_string(),
        },
        HostError::EmptyLaunch { domain: d() },
        HostError::UnknownLanguage {
            key: "component[1].language".to_string(),
            language: "go".to_string(),
            loaded: "python,rust".to_string(),
        },
        HostError::Spawn {
            domain: d(),
            program: "uv".to_string(),
            kind: "NotFound".to_string(),
        },
        HostError::Handshake {
            domain: d(),
            cause: HandshakeError::Closed,
        },
        HostError::Connect {
            domain: d(),
            socket: "/run/angzarr/player.sock".to_string(),
        },
        HostError::Describe {
            domain: d(),
            grpc_code: 14,
        },
        HostError::Health {
            domain: d(),
            grpc_code: 12,
        },
        HostError::NotServing {
            component: d(),
            status: "NOT_SERVING".to_string(),
        },
        HostError::KindMismatch {
            domain: d(),
            configured: "policy".to_string(),
            described: "AGGREGATE".to_string(),
        },
        HostError::NoInput { domain: d() },
        HostError::ProjectorNoInputDomain { component: d() },
        HostError::UpcasterNoDomain { domain: d() },
        HostError::NotExposable {
            domain: d(),
            kind: "policy".to_string(),
        },
        HostError::Descriptor {
            domain: d(),
            cause: angzarr_embedded::Misconfigured::PolicyNoOutputDomain {
                component: "TableWalletPolicy".to_string(),
            },
        },
        HostError::DescriptorChanged { domain: d() },
        HostError::ExitStatus {
            component: d(),
            exit_code: 3,
        },
        HostError::NotExited {
            component: d(),
            ended: "terminated".to_string(),
        },
        HostError::ProbeInput {
            component: d(),
            kind: "policy".to_string(),
        },
        HostError::NotRegistered {
            domain: d(),
            kind: "policy".to_string(),
        },
        HostError::NotListed {
            domain: d(),
            kind: "projector".to_string(),
        },
    ]
}

/// Each [`HostError`] variant of the host as a whole: its config, its
/// socket directory, its registry, its store and its runtime.
fn host_samples() -> Vec<HostError> {
    vec![
        HostError::SocketDirTooLong {
            dir: "/very/deep/directory".to_string(),
            max_bytes: 65,
        },
        HostError::ConfigUnreadable {
            path: "angzarr.yaml".to_string(),
            kind: "NotFound".to_string(),
        },
        HostError::ConfigInvalid {
            path: "angzarr.yaml".to_string(),
            cause: Report::of(
                &crate::config::HostConfig::from_yaml("schema_version: 99\n", "angzarr.yaml")
                    .expect_err("a newer config is refused"),
            ),
        },
        HostError::Register {
            cause: Report::new("angzarr-embedded-host-duplicate-domain").arg("domain", "player"),
        },
        HostError::Store {
            cause: Report::new("angzarr-embedded-host-runtime").arg("kind", "Other"),
        },
        HostError::Runtime {
            kind: "Other".to_string(),
        },
    ]
}
