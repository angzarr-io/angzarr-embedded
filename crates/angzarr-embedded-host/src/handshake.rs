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
//! The protocol is the same for every language: angzarr-project's
//! `docs/design/child-protocol.md` (decision 14.59). A line is classified
//! by its first word, as bytes: lines before the handshake whose first
//! word is not `angzarr-handshake` (a library's banner, a test harness's)
//! are skipped; a line whose first word is and that is not a v1 handshake
//! naming an absolute Unix socket is refused, never skipped
//! (`HOST_REASON_HANDSHAKE_MALFORMED`). Nothing read is decoded lossily: a
//! handshake naming a path that is not UTF-8 is refused under its own key,
//! and a refused line that is not UTF-8 is given back with its bytes
//! escaped.

use std::path::PathBuf;
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

/// One stdout line, as read, its line ending (`\n` or `\r\n`) included
/// or not: `Ok(Some)` for a handshake, `Ok(None)` for a line that is not
/// one, and an error for a line whose first word is `angzarr-handshake`
/// but is not a v1 handshake naming an absolute Unix socket path, or one
/// whose path is not UTF-8.
pub fn parse_line(line: impl AsRef<[u8]>) -> Result<Option<Handshake>, HandshakeError> {
    let line = line.as_ref();
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let Some(rest) = line.strip_prefix(HANDSHAKE_PREFIX.as_bytes()) else {
        return Ok(None);
    };
    if !rest.is_empty() && !rest.starts_with(b" ") {
        // A longer first word (`angzarr-handshaker`): not a handshake.
        return Ok(None);
    }
    let malformed = || HandshakeError::Malformed { line: text(line) };
    let rest = rest.strip_prefix(b" ").ok_or_else(malformed)?;
    let space = rest.iter().position(|b| *b == b' ').ok_or_else(malformed)?;
    let (version, address) = (&rest[..space], &rest[space + 1..]);
    let path = address.strip_prefix(b"unix:").ok_or_else(malformed)?;
    if version != PROTOCOL_VERSION.as_bytes() || !path.starts_with(b"/") {
        return Err(malformed());
    }
    let path = std::str::from_utf8(path).map_err(|_| HandshakeError::PathNotUtf8 {
        path: path.escape_ascii().to_string(),
    })?;
    Ok(Some(Handshake {
        socket: PathBuf::from(path),
    }))
}

/// `bytes` as text: as they are when they are UTF-8, else every byte
/// outside printable ASCII escaped, so nothing is lost.
fn text(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| bytes.escape_ascii().to_string())
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
            if let Some(handshake) = parse_line(&line)? {
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
