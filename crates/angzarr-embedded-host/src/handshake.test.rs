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

/// A v1 handshake whose path is not UTF-8 is malformed (C-0760; decision
/// 14.62), the line given back without loss (its bytes escaped): it is
/// never decoded into another path.
#[tokio::test]
async fn a_handshake_naming_a_path_that_is_not_utf8_is_malformed() {
    let (mut child, host) = tokio::io::duplex(1024);
    child
        .write_all(b"angzarr-handshake v1 unix:/tmp/caf\xe9.sock\n")
        .await
        .unwrap();
    let mut host = BufReader::new(host);
    assert_eq!(
        read_handshake(&mut host, Duration::from_secs(5)).await,
        Err(malformed("angzarr-handshake v1 unix:/tmp/caf\\xe9.sock"))
    );
}

/// A path of any UTF-8 characters is named as it is (C-0759).
#[test]
fn a_path_of_any_utf8_characters_is_named() {
    assert_eq!(
        parse_line(b"angzarr-handshake v1 unix:/run/caf\xc3\xa9/order.sock"),
        Ok(socket("/run/caf\u{e9}/order.sock"))
    );
    assert_eq!(
        parse_line(b"angzarr-handshake v1 unix:/run/\xe2\x82\xac/order.sock"),
        Ok(socket("/run/\u{20ac}/order.sock"))
    );
}

/// A path that is not UTF-8, a truncated sequence or an encoded surrogate
/// included, is malformed, the line escaped: each byte outside printable
/// ASCII, and each backslash, written `\xhh` (C-0760). A line that is
/// UTF-8 is given as it is, its backslashes too.
#[test]
fn a_line_that_is_not_utf8_is_given_back_escaped() {
    for (bytes, line) in [
        (
            &b"angzarr-handshake v1 unix:/run/\xc3/order.sock"[..],
            "angzarr-handshake v1 unix:/run/\\xc3/order.sock",
        ),
        (
            b"angzarr-handshake v1 unix:/run/\xed\xa0\x80.sock",
            "angzarr-handshake v1 unix:/run/\\xed\\xa0\\x80.sock",
        ),
        (
            b"angzarr-handshake v1 unix:/run/a\\b\t\xff.sock",
            "angzarr-handshake v1 unix:/run/a\\x5cb\\x09\\xff.sock",
        ),
        (
            b"angzarr-handshake v1 unix:/run/~ \x7f\xff",
            "angzarr-handshake v1 unix:/run/~ \\x7f\\xff",
        ),
    ] {
        assert_eq!(parse_line(bytes), Err(malformed(line)), "{line}");
    }
    assert_eq!(
        parse_line(b"angzarr-handshake v2 unix:/run/a\\b\t.sock"),
        Err(malformed("angzarr-handshake v2 unix:/run/a\\b\t.sock"))
    );
}

/// A line that is not UTF-8 whose first word is not `angzarr-handshake`
/// is not a handshake, a longer first word included (C-0761).
#[test]
fn a_line_that_is_not_utf8_and_not_a_handshake_is_skipped() {
    for bytes in [
        &b"listening on /run/caf\xe9/order.sock"[..],
        b"angzarr-handshake\xe9 v1 unix:/run/order.sock",
        b"\xff",
    ] {
        assert_eq!(parse_line(bytes), Ok(None), "{bytes:?}");
    }
}

/// The handshake's failures are the spec's keys (decision 14.62).
#[test]
fn the_handshakes_failures_are_spec_keys() {
    use angzarr_diagnostic::Diagnostic;
    let timeout = HandshakeError::Timeout { timeout_ms: 200 };
    assert_eq!(timeout.code(), "HOST_REASON_HANDSHAKE_TIMEOUT");
    assert_eq!(
        timeout.to_string(),
        "HOST_REASON_HANDSHAKE_TIMEOUT timeout_ms=200"
    );
    assert_eq!(
        HandshakeError::Closed.code(),
        "HOST_REASON_HANDSHAKE_CLOSED"
    );
    assert_eq!(
        malformed("angzarr-handshake v2").code(),
        "HOST_REASON_HANDSHAKE_MALFORMED"
    );
}

/// A line that is not UTF-8 is classified by its first word all the same:
/// one that is not a handshake is skipped, and a malformed one is refused
/// with its bytes escaped, never decoded lossily.
#[tokio::test]
async fn a_line_that_is_not_utf8_is_classified_by_its_first_word() {
    let (mut child, host) = tokio::io::duplex(1024);
    child
        .write_all(b"caf\xe9 starting\nangzarr-handshake v1 unix:/tmp/p.sock\n")
        .await
        .unwrap();
    let mut host = BufReader::new(host);
    assert_eq!(
        read_handshake(&mut host, Duration::from_secs(5)).await,
        Ok(Handshake {
            socket: PathBuf::from("/tmp/p.sock"),
        })
    );
    let (mut child, host) = tokio::io::duplex(1024);
    child
        .write_all(b"angzarr-handshake v2 unix:/tmp/caf\xe9.sock\n")
        .await
        .unwrap();
    let mut host = BufReader::new(host);
    assert_eq!(
        read_handshake(&mut host, Duration::from_secs(5)).await,
        Err(malformed("angzarr-handshake v2 unix:/tmp/caf\\xe9.sock"))
    );
}
