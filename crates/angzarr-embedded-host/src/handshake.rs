//! The handshake: how a child says where it serves. Once it is serving, a
//! child prints exactly one line on stdout,
//!
//! ```text
//! angzarr-handshake v1 unix:/absolute/path/to/the.sock
//! ```
//!
//! and the host connects to that socket. The host chooses the path, and
//! passes it in `ANGZARR_SOCKET`; the child listens there and names it back.
//! A child exits when its stdin reaches end of file, or on SIGTERM.
//!
//! The protocol is the same for every language. Lines before the handshake
//! that do not start `angzarr-handshake` (a library's banner, a test
//! harness's) are skipped; a line that does start so and is not a v1
//! handshake naming an absolute Unix socket is refused, never skipped.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::io::{AsyncBufRead, AsyncBufReadExt};

use crate::error::HandshakeError;

/// The first word of a handshake line.
pub const HANDSHAKE_PREFIX: &str = "angzarr-handshake";

/// The protocol version this host speaks.
pub const PROTOCOL_VERSION: &str = "v1";

/// The environment variable the host sets to the socket path it chose.
pub const SOCKET_ENV: &str = "ANGZARR_SOCKET";

/// A parsed handshake: where the child serves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Handshake {
    /// The Unix socket's absolute path.
    pub socket: PathBuf,
}

/// One stdout line, its line ending (`\n` or `\r\n`) included or not:
/// `Ok(Some)` for a handshake, `Ok(None)` for a line that is not one, and
/// an error for a line that starts `angzarr-handshake` but is not a v1
/// handshake naming an absolute Unix socket path.
pub fn parse_line(line: &str) -> Result<Option<Handshake>, HandshakeError> {
    let line = line.strip_suffix('\n').unwrap_or(line);
    let line = line.strip_suffix('\r').unwrap_or(line);
    let Some(rest) = line.strip_prefix(HANDSHAKE_PREFIX) else {
        return Ok(None);
    };
    if !rest.is_empty() && !rest.starts_with(' ') {
        // A longer first word (`angzarr-handshaker`): not a handshake.
        return Ok(None);
    }
    let malformed = || HandshakeError::Malformed {
        line: line.to_string(),
    };
    let (version, address) = rest
        .strip_prefix(' ')
        .and_then(|rest| rest.split_once(' '))
        .ok_or_else(malformed)?;
    let path = address.strip_prefix("unix:").ok_or_else(malformed)?;
    if version != PROTOCOL_VERSION || !Path::new(path).is_absolute() {
        return Err(malformed());
    }
    Ok(Some(Handshake {
        socket: PathBuf::from(path),
    }))
}

/// Reads `stdout` line by line to its handshake, skipping the lines before
/// it, within `timeout`.
pub async fn read_handshake<R: AsyncBufRead + Unpin>(
    stdout: &mut R,
    timeout: Duration,
) -> Result<Handshake, HandshakeError> {
    let reading = async {
        let mut line = Vec::new();
        loop {
            line.clear();
            stdout
                .read_until(b'\n', &mut line)
                .await
                .map_err(|e| HandshakeError::Unreadable {
                    kind: format!("{:?}", e.kind()),
                })?;
            if line.last() != Some(&b'\n') {
                return Err(HandshakeError::Closed);
            }
            if let Some(handshake) = parse_line(&String::from_utf8_lossy(&line))? {
                return Ok(handshake);
            }
        }
    };
    tokio::time::timeout(timeout, reading)
        .await
        .unwrap_or(Err(HandshakeError::Timeout {
            timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
        }))
}

#[cfg(test)]
#[path = "handshake.test.rs"]
mod tests;
