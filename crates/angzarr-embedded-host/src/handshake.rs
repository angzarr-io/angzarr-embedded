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

use std::path::PathBuf;
use std::time::Duration;

use tokio::io::AsyncBufRead;

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
    let _ = line;
    todo!("red: parse one stdout line")
}

/// Reads `stdout` line by line to its handshake, skipping the lines before
/// it, within `timeout`.
pub async fn read_handshake<R: AsyncBufRead + Unpin>(
    stdout: &mut R,
    timeout: Duration,
) -> Result<Handshake, HandshakeError> {
    let _ = (stdout, timeout);
    todo!("red: read the handshake within the timeout")
}

#[cfg(test)]
#[path = "handshake.test.rs"]
mod tests;
