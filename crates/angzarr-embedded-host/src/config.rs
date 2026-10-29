//! The host's config: the application's domains, the language each is
//! written in, and how each remote one is launched. The CLI writes it beside
//! the thin `main.rs` it generates; its TOML form is
//!
//! ```toml
//! [[domain]]
//! name = "table"
//! language = "rust"            # in-process: registered by main.rs, not launched
//!
//! [[domain]]
//! name = "player"
//! language = "python"          # launched as `uv run --locked -m player.serve`
//! entry = ["-m", "player.serve"]
//! dir = "domains/player"
//!
//! [[domain]]
//! name = "dealer"
//! language = "go"              # no default: the launch command is given
//! launch = ["./bin/dealer"]
//!
//! [supervision]
//! handshake_timeout_ms = 10000
//! ```
//!
//! A domain's language picks its default launch command and nothing else:
//! the host speaks one protocol to every child (the handshake, then gRPC on
//! a Unix socket), so a language with no default here needs only a `launch`.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::HostError;

/// The language whose domains run in the application's own process, as
/// angzarr-embedded domains: the host launches none of them unless the
/// config gives one a `launch`.
pub const IN_PROCESS_LANGUAGE: &str = "rust";

/// The default launch command of each language that has one, before the
/// domain's `entry`. Python's runs the domain's project with exactly its
/// lock file's dependencies.
pub fn default_launch(language: &str) -> Option<&'static [&'static str]> {
    match language {
        "python" => Some(&["uv", "run", "--locked"]),
        _ => None,
    }
}

/// The host's whole config.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostConfig {
    /// Every domain of the application, in-process and remote: the
    /// `[[domain]]` tables.
    #[serde(rename = "domain", default)]
    pub domains: Vec<DomainConfig>,
    /// How the host waits for, restarts and stops its children.
    #[serde(default)]
    pub supervision: Supervision,
}

/// One `[[domain]]` table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DomainConfig {
    /// The domain's name: the component name its child must describe.
    pub name: String,
    /// The language it is written in (`"rust"`, `"python"`, `"go"`, ...),
    /// lowercase.
    pub language: String,
    /// The command that starts its child, program first. Overrides the
    /// language's default (and its `entry`) entirely.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch: Option<Vec<String>>,
    /// The arguments after the language's default launch command: the
    /// module or entry point (`["-m", "player.serve"]`, `["player-serve"]`).
    /// Unused when `launch` is given.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entry: Vec<String>,
    /// The child's working directory; the application's when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dir: Option<PathBuf>,
    /// Environment variables set for the child, beside the host's own and
    /// `ANGZARR_SOCKET`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

/// How the host supervises its children. Every field has a default, so the
/// `[supervision]` table, and any key in it, may be left out.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Supervision {
    /// How long a child has, from its start, to print its handshake.
    pub handshake_timeout_ms: u64,
    /// The wait before the first restart of a child that exited; each
    /// restart that fails doubles it, up to `max_backoff_ms`, and a child
    /// that comes up resets it.
    pub initial_backoff_ms: u64,
    /// The longest wait between restarts.
    pub max_backoff_ms: u64,
    /// How long a child has, once its stdin is closed at shutdown, to exit
    /// before it is sent SIGTERM.
    pub shutdown_grace_ms: u64,
    /// How long a child has, once sent SIGTERM, to exit before it is
    /// killed (SIGKILL).
    pub terminate_grace_ms: u64,
    /// How long a dispatch waits for the child's answer; past it, the call
    /// fails retryably (`cause = timeout`).
    pub call_timeout_ms: u64,
    /// The directory the host puts the children's sockets in. When absent,
    /// `$XDG_RUNTIME_DIR` if it is set, absolute and short enough, else
    /// `/tmp`: never `$TMPDIR`, which may be deep. A Unix socket's path is
    /// at most 107 bytes, so a directory too long to hold the host's socket
    /// names is refused at start ([`HostError::SocketDirTooLong`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub socket_dir: Option<PathBuf>,
}

impl Default for Supervision {
    fn default() -> Self {
        Supervision {
            handshake_timeout_ms: 30_000,
            initial_backoff_ms: 100,
            max_backoff_ms: 5_000,
            shutdown_grace_ms: 5_000,
            terminate_grace_ms: 2_000,
            call_timeout_ms: 30_000,
            socket_dir: None,
        }
    }
}

/// Where a domain runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Placement {
    /// In the application's process: main.rs registers it; the host does
    /// nothing with it.
    InProcess,
    /// In a child process the host launches.
    Remote(Launch),
}

/// A child's resolved launch: what [`DomainConfig::placement`] makes of the
/// config.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Launch {
    pub program: String,
    pub args: Vec<String>,
    pub dir: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
}

impl DomainConfig {
    /// Where this domain runs and, when remote, how it is launched: its
    /// `launch`, else its language's default followed by its `entry`. A
    /// `"rust"` domain with no `launch` is in-process. Refused when the
    /// launch is empty, or when no launch is given for a language with no
    /// default.
    pub fn placement(&self) -> Result<Placement, HostError> {
        let words: Vec<String> = match (&self.launch, default_launch(&self.language)) {
            (Some(launch), _) => launch.clone(),
            (None, _) if self.language == IN_PROCESS_LANGUAGE => return Ok(Placement::InProcess),
            (None, Some(default)) => default
                .iter()
                .map(|w| w.to_string())
                .chain(self.entry.iter().cloned())
                .collect(),
            (None, None) => {
                return Err(HostError::NoLaunch {
                    domain: self.name.clone(),
                    language: self.language.clone(),
                })
            }
        };
        let mut words = words.into_iter();
        let Some(program) = words.next() else {
            return Err(HostError::EmptyLaunch {
                domain: self.name.clone(),
            });
        };
        Ok(Placement::Remote(Launch {
            program,
            args: words.collect(),
            dir: self.dir.clone(),
            env: self.env.clone(),
        }))
    }
}

#[cfg(test)]
#[path = "config.test.rs"]
mod tests;
