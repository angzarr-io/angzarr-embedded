//! The host's errors: why a remote domain could not be started. Each is a
//! code and its args (`angzarr_embedded_diagnostic::Diagnostic`), the English
//! `Display` only the fallback. A failure once a domain is running is not
//! one of these: it is the router's [`CodedError`], from
//! [`RemoteCommandHandler`]'s dispatch.
//!
//! [`CodedError`]: angzarr_router::error::CodedError
//! [`RemoteCommandHandler`]: crate::RemoteCommandHandler

use std::fmt;

use angzarr_embedded_diagnostic::{Arg, Diagnostic, Report};

/// What was wrong with a child's handshake: the one stdout line,
/// `angzarr-handshake v1 unix:<absolute-socket-path>`, it prints once it is
/// serving.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandshakeError {
    /// A line that announces itself as a handshake (it starts
    /// `angzarr-handshake`) but is not a v1 one naming an absolute Unix
    /// socket path. `line` is the line as read, its line ending removed.
    Malformed { line: String },
    /// No handshake within the configured time.
    Timeout { timeout_ms: u64 },
    /// The child's stdout ended (it exited, or closed it) before a
    /// handshake.
    Closed,
    /// Reading the child's stdout failed. `kind` is the I/O error's
    /// `std::io::ErrorKind`, by name.
    Unreadable { kind: String },
}

impl fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HandshakeError::Malformed { line } => write!(
                f,
                "malformed handshake \"{line}\": expected angzarr-handshake v1 unix:<absolute path>"
            ),
            HandshakeError::Timeout { timeout_ms } => {
                write!(f, "no handshake within {timeout_ms} ms")
            }
            HandshakeError::Closed => f.write_str("stdout closed before a handshake"),
            HandshakeError::Unreadable { kind } => {
                write!(f, "stdout could not be read ({kind})")
            }
        }
    }
}

impl Diagnostic for HandshakeError {
    fn code(&self) -> &'static str {
        match self {
            HandshakeError::Malformed { .. } => "angzarr-embedded-host-handshake-malformed",
            HandshakeError::Timeout { .. } => "angzarr-embedded-host-handshake-timeout",
            HandshakeError::Closed => "angzarr-embedded-host-handshake-closed",
            HandshakeError::Unreadable { .. } => "angzarr-embedded-host-handshake-unreadable",
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match self {
            HandshakeError::Malformed { line } => vec![("line", line.as_str().into())],
            HandshakeError::Timeout { timeout_ms } => vec![("timeout_ms", (*timeout_ms).into())],
            HandshakeError::Closed => vec![],
            HandshakeError::Unreadable { kind } => vec![("kind", kind.as_str().into())],
        }
    }
}

/// Why the host could not start: a config it refuses, or a remote domain
/// that did not come up. Every variant names its domain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostError {
    /// Two `[[domain]]` tables with one name.
    DuplicateDomain { domain: String },
    /// A remote domain whose language has no default launch command, and
    /// whose config gives none.
    NoLaunch { domain: String, language: String },
    /// A `launch` with no program in it.
    EmptyLaunch { domain: String },
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
    /// The configured `socket_dir` is too long for a Unix socket path to
    /// hold the host's socket names in it: at most `max_bytes` bytes.
    SocketDirTooLong { dir: String, max_bytes: u64 },
    /// The config file could not be read. `kind` is the I/O error's
    /// `std::io::ErrorKind`, by name.
    ConfigUnreadable { path: String, kind: String },
    /// The config file is not a valid config: malformed TOML, an unknown
    /// key, a missing one. `offset` is the byte where the problem starts.
    ConfigInvalid { path: String, offset: u64 },
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
        match self {
            HostError::DuplicateDomain { domain } => {
                write!(f, "domain \"{domain}\" is configured twice")
            }
            HostError::NoLaunch { domain, language } => write!(
                f,
                "domain \"{domain}\": language \"{language}\" has no default launch command; give one"
            ),
            HostError::EmptyLaunch { domain } => {
                write!(f, "domain \"{domain}\": the launch command is empty")
            }
            HostError::Spawn {
                domain,
                program,
                kind,
            } => write!(f, "domain \"{domain}\": could not start \"{program}\" ({kind})"),
            HostError::Handshake { domain, cause } => {
                write!(f, "domain \"{domain}\": {cause}")
            }
            HostError::Connect { domain, socket } => {
                write!(f, "domain \"{domain}\": could not connect to {socket}")
            }
            HostError::Describe { domain, grpc_code } => write!(
                f,
                "domain \"{domain}\": Describe failed with gRPC code {grpc_code}"
            ),
            HostError::SocketDirTooLong { dir, max_bytes } => write!(
                f,
                "socket_dir \"{dir}\" is too long for a Unix socket path: at most {max_bytes} bytes"
            ),
            HostError::ConfigUnreadable { path, kind } => {
                write!(f, "the config {path} could not be read ({kind})")
            }
            HostError::ConfigInvalid { path, offset } => {
                write!(f, "the config {path} is not valid, at byte {offset}")
            }
            HostError::Register { cause } => write!(f, "a domain could not be registered: {cause}"),
            HostError::Store { cause } => write!(f, "the store could not be opened: {cause}"),
            HostError::Runtime { kind } => {
                write!(f, "the host's runtime could not be built ({kind})")
            }
        }
    }
}

impl Diagnostic for HostError {
    fn code(&self) -> &'static str {
        match self {
            HostError::DuplicateDomain { .. } => "angzarr-embedded-host-duplicate-domain",
            HostError::NoLaunch { .. } => "angzarr-embedded-host-no-launch",
            HostError::EmptyLaunch { .. } => "angzarr-embedded-host-empty-launch",
            HostError::Spawn { .. } => "angzarr-embedded-host-spawn",
            HostError::Handshake { .. } => "angzarr-embedded-host-handshake",
            HostError::Connect { .. } => "angzarr-embedded-host-connect",
            HostError::Describe { .. } => "angzarr-embedded-host-describe",
            HostError::SocketDirTooLong { .. } => "angzarr-embedded-host-socket-dir-too-long",
            HostError::ConfigUnreadable { .. } => "angzarr-embedded-host-config-unreadable",
            HostError::ConfigInvalid { .. } => "angzarr-embedded-host-config-invalid",
            HostError::Register { .. } => "angzarr-embedded-host-register",
            HostError::Store { .. } => "angzarr-embedded-host-store",
            HostError::Runtime { .. } => "angzarr-embedded-host-runtime",
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        let domain = |d: &str| ("domain", Arg::from(d));
        match self {
            HostError::DuplicateDomain { domain: d } | HostError::EmptyLaunch { domain: d } => {
                vec![domain(d)]
            }
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
            HostError::Connect { domain: d, socket } => {
                vec![domain(d), ("socket", socket.as_str().into())]
            }
            HostError::Describe {
                domain: d,
                grpc_code,
            } => vec![domain(d), ("grpc_code", (*grpc_code).into())],
            HostError::SocketDirTooLong { dir, max_bytes } => vec![
                ("dir", dir.as_str().into()),
                ("max_bytes", (*max_bytes).into()),
            ],
            HostError::ConfigUnreadable { path, kind } => vec![
                ("path", path.as_str().into()),
                ("kind", kind.as_str().into()),
            ],
            HostError::ConfigInvalid { path, offset } => {
                vec![("path", path.as_str().into()), ("offset", (*offset).into())]
            }
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
    let d = || "player".to_string();
    vec![
        Box::new(HandshakeError::Malformed {
            line: "angzarr-handshake v2".to_string(),
        }),
        Box::new(HandshakeError::Timeout { timeout_ms: 30_000 }),
        Box::new(HandshakeError::Closed),
        Box::new(HandshakeError::Unreadable {
            kind: "BrokenPipe".to_string(),
        }),
        Box::new(HostError::DuplicateDomain { domain: d() }),
        Box::new(HostError::NoLaunch {
            domain: d(),
            language: "go".to_string(),
        }),
        Box::new(HostError::EmptyLaunch { domain: d() }),
        Box::new(HostError::Spawn {
            domain: d(),
            program: "uv".to_string(),
            kind: "NotFound".to_string(),
        }),
        Box::new(HostError::Handshake {
            domain: d(),
            cause: HandshakeError::Closed,
        }),
        Box::new(HostError::Connect {
            domain: d(),
            socket: "/run/angzarr/player.sock".to_string(),
        }),
        Box::new(HostError::Describe {
            domain: d(),
            grpc_code: 14,
        }),
        Box::new(HostError::SocketDirTooLong {
            dir: "/very/deep/directory".to_string(),
            max_bytes: 65,
        }),
        Box::new(HostError::ConfigUnreadable {
            path: "angzarr.toml".to_string(),
            kind: "NotFound".to_string(),
        }),
        Box::new(HostError::ConfigInvalid {
            path: "angzarr.toml".to_string(),
            offset: 12,
        }),
        Box::new(HostError::Register {
            cause: Report::new(
                "angzarr-embedded-host-duplicate-domain",
                "domain \"player\" is configured twice",
            )
            .arg("domain", "player"),
        }),
        Box::new(HostError::Store {
            cause: Report::new(
                "angzarr-embedded-host-handshake-closed",
                "stdout closed before a handshake",
            ),
        }),
        Box::new(HostError::Runtime {
            kind: "Other".to_string(),
        }),
    ]
}
