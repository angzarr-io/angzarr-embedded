use std::path::PathBuf;
use std::time::Duration;

use tokio::io::{AsyncWriteExt, BufReader};

use super::*;

fn socket(path: &str) -> Option<Handshake> {
    Some(Handshake {
        socket: PathBuf::from(path),
    })
}

fn malformed(line: &str) -> HandshakeError {
    HandshakeError::Malformed {
        line: line.to_string(),
    }
}

#[test]
fn a_good_line_names_its_socket() {
    assert_eq!(
        parse_line("angzarr-handshake v1 unix:/run/angzarr/player.sock"),
        Ok(socket("/run/angzarr/player.sock"))
    );
}

/// The line ending is not part of the path; a space inside the path is.
#[test]
fn the_line_ending_is_not_part_of_the_path() {
    assert_eq!(
        parse_line("angzarr-handshake v1 unix:/tmp/a.sock\n"),
        Ok(socket("/tmp/a.sock"))
    );
    assert_eq!(
        parse_line("angzarr-handshake v1 unix:/tmp/a.sock\r\n"),
        Ok(socket("/tmp/a.sock"))
    );
    assert_eq!(
        parse_line("angzarr-handshake v1 unix:/tmp/my dir/a.sock"),
        Ok(socket("/tmp/my dir/a.sock"))
    );
}

/// A line that starts `angzarr-handshake` is a handshake or an error: a
/// child speaking another version, network or a relative path is refused,
/// the line (without its ending) given back.
#[test]
fn a_malformed_handshake_is_refused() {
    for line in [
        "angzarr-handshake",
        "angzarr-handshake v1",
        "angzarr-handshake v2 unix:/tmp/a.sock",
        "angzarr-handshake v1 tcp:127.0.0.1:5000",
        "angzarr-handshake v1 /tmp/a.sock",
        "angzarr-handshake v1 unix:",
        "angzarr-handshake v1 unix:relative/a.sock",
        "angzarr-handshake  v1 unix:/tmp/a.sock",
    ] {
        assert_eq!(parse_line(line), Err(malformed(line)), "{line:?}");
    }
    assert_eq!(
        parse_line("angzarr-handshake v2 unix:/tmp/a.sock\n"),
        Err(malformed("angzarr-handshake v2 unix:/tmp/a.sock"))
    );
}

/// Anything else is not a handshake at all: skipped, not refused.
#[test]
fn a_line_that_is_not_a_handshake_is_none() {
    for line in [
        "",
        "\n",
        "running 1 test",
        "angzarr-handshaker v1 unix:/tmp/a.sock",
        " angzarr-handshake v1 unix:/tmp/a.sock",
        "INFO starting player",
    ] {
        assert_eq!(parse_line(line), Ok(None), "{line:?}");
    }
}

#[tokio::test]
async fn reading_skips_stdout_before_the_handshake() {
    let (mut child, host) = tokio::io::duplex(1024);
    child
        .write_all(b"Loading player...\n\nrunning 1 test\nangzarr-handshake v1 unix:/tmp/p.sock\n")
        .await
        .unwrap();
    let mut host = BufReader::new(host);
    assert_eq!(
        read_handshake(&mut host, Duration::from_secs(5)).await,
        Ok(Handshake {
            socket: PathBuf::from("/tmp/p.sock"),
        })
    );
}

/// Reading stops at the handshake: what follows is left for the host to
/// drain.
#[tokio::test]
async fn reading_stops_at_the_handshake() {
    let (mut child, host) = tokio::io::duplex(1024);
    child
        .write_all(b"angzarr-handshake v1 unix:/tmp/p.sock\nafter\n")
        .await
        .unwrap();
    let mut host = BufReader::new(host);
    read_handshake(&mut host, Duration::from_secs(5))
        .await
        .unwrap();
    let mut rest = String::new();
    tokio::io::AsyncBufReadExt::read_line(&mut host, &mut rest)
        .await
        .unwrap();
    assert_eq!(rest, "after\n");
}

#[tokio::test]
async fn reading_refuses_a_malformed_handshake_after_noise() {
    let (mut child, host) = tokio::io::duplex(1024);
    child
        .write_all(b"hello\nangzarr-handshake v2 unix:/tmp/p.sock\n")
        .await
        .unwrap();
    let mut host = BufReader::new(host);
    assert_eq!(
        read_handshake(&mut host, Duration::from_secs(5)).await,
        Err(malformed("angzarr-handshake v2 unix:/tmp/p.sock"))
    );
}

/// A child that writes but never handshakes, or writes nothing, times out;
/// its stdout held open (the writer kept alive) so it is the clock, not the
/// end of the stream, that ends the wait.
#[tokio::test]
async fn reading_times_out_without_a_handshake() {
    let (mut child, host) = tokio::io::duplex(1024);
    child.write_all(b"still starting\n").await.unwrap();
    let mut host = BufReader::new(host);
    let started = std::time::Instant::now();
    assert_eq!(
        read_handshake(&mut host, Duration::from_millis(200)).await,
        Err(HandshakeError::Timeout { timeout_ms: 200 })
    );
    assert!(started.elapsed() >= Duration::from_millis(200));
    drop(child);
}

/// A half line is not a handshake: the line must end before stdout does.
#[tokio::test]
async fn stdout_closing_first_is_closed() {
    let (mut child, host) = tokio::io::duplex(1024);
    child
        .write_all(b"booting\nangzarr-handshake v1 unix:/tmp/p.so")
        .await
        .unwrap();
    drop(child);
    let mut host = BufReader::new(host);
    assert_eq!(
        read_handshake(&mut host, Duration::from_secs(5)).await,
        Err(HandshakeError::Closed)
    );
}
