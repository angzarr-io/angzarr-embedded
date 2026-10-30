//! A fake remote domain for the host's tests: this very test binary, run
//! again as the child. The host launches it as
//! `<test binary> --exact fake_child::entry --nocapture` with
//! `ANGZARR_FAKE_CHILD` set to a mode; [`entry`] is a no-op in every other
//! run. libtest's own "running 1 test" line comes before the handshake on
//! stdout, as a chatty child's would.
//!
//! The child serves stub `CommandHandlerService` and `ComponentService`s
//! on `ANGZARR_SOCKET`, describing itself as `ANGZARR_FAKE_NAME` (an
//! aggregate whose rules version is `pid-<its pid>`, so a test can tell one
//! child from its restart), and handles a command by its type URL's last
//! segment:
//!
//! - `...Refuse`: FAILED_PRECONDITION, with an ErrorInfo (reason
//!   `TABLE_FULL`, metadata `seats = 6`) and the typed refusal
//!   [`refusal`] in its details;
//! - `...Taken`: ALREADY_EXISTS, with an ErrorInfo (reason `SEAT_TAKEN`,
//!   metadata `seat = 3`) and the typed refusal [`refusal`]: a business
//!   refusal under a status angzarr-router's `GrpcCode` has no variant for;
//! - `...Break`: INTERNAL, with an ErrorInfo (reason `PLAYER_BROKE`, a
//!   code the router does not know, metadata `seat = 3`): a bug;
//! - `...Die`: the process exits with code 7, mid-call;
//! - `...Hang`: never answers;
//! - anything else: one event, [`noted`], on the command's cover, rules
//!   version `pid-<its pid>`.
//!
//! It exits with code 0 when its stdin reaches end of file.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;

use crate::rpc::google_rpc::{ErrorInfo, Status as RpcStatus, ERROR_INFO_TYPE_URL};
use angzarr_router::pb;
use prost::Message;
use prost_types::Any;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};

use crate::config::{DomainConfig, HostConfig, Supervision};
use crate::handshake::SOCKET_ENV;
use crate::rpc::command_handler::command_handler_service_server::{
    CommandHandlerService, CommandHandlerServiceServer,
};
use crate::rpc::component::component_service_server::{ComponentService, ComponentServiceServer};
use crate::rpc::DescribeRequest;

/// The variable naming the fake's mode.
pub(crate) const MODE_ENV: &str = "ANGZARR_FAKE_CHILD";
/// The variable naming a file the fake writes when it exits on stdin's end,
/// and only then: a kill leaves no mark.
pub(crate) const EOF_MARK_ENV: &str = "ANGZARR_FAKE_EOF_MARK";
/// The variable naming the component it describes.
pub(crate) const NAME_ENV: &str = "ANGZARR_FAKE_NAME";

/// The fake's behaviours.
pub(crate) mod mode {
    /// Handshake, then serve.
    pub(crate) const SERVE: &str = "serve";
    /// Lines of chatter on stdout, then the handshake, then serve.
    pub(crate) const NOISY: &str = "noisy";
    /// Never handshake; wait for stdin's end.
    pub(crate) const SILENT: &str = "silent";
    /// A v2 handshake; then wait for stdin's end.
    pub(crate) const MALFORMED: &str = "malformed";
    /// Serve, and ignore stdin's end: SIGTERM (its default action) stops
    /// it.
    pub(crate) const STUBBORN: &str = "stubborn";
    /// Serve, and ignore stdin's end and SIGTERM: only SIGKILL stops it.
    pub(crate) const DEAF: &str = "deaf";
    /// Handshake, close stdout, then serve: the host sees stdout's end
    /// while the child lives on.
    pub(crate) const CLOSES_STDOUT: &str = "closes-stdout";
}

/// The v2 line the malformed fake prints.
pub(crate) const MALFORMED_LINE: &str = "angzarr-handshake v2 unix:/tmp/angzarr-fake.sock";

/// The typed refusal a `...Refuse` command is answered with.
pub(crate) fn refusal() -> Any {
    Any {
        type_url: "/examples.blackjack.TableFull".to_string(),
        value: vec![8, 6],
    }
}

/// The launch command that runs this binary as the fake child.
pub(crate) fn launch() -> Vec<String> {
    vec![
        std::env::current_exe()
            .unwrap()
            .to_string_lossy()
            .into_owned(),
        "--exact".to_string(),
        "fake_child::entry".to_string(),
        "--nocapture".to_string(),
    ]
}

/// A domain whose child is the fake in `mode`, describing itself as
/// `name`. Its language is Python, whose default launch (uv) the config's
/// `launch` overrides: the fake is what runs.
pub(crate) fn domain(name: &str, described: &str, mode: &str) -> DomainConfig {
    DomainConfig {
        name: name.to_string(),
        language: "python".to_string(),
        launch: Some(launch()),
        entry: vec!["-m".to_string(), "never.used".to_string()],
        dir: None,
        env: BTreeMap::from([
            (MODE_ENV.to_string(), mode.to_string()),
            (NAME_ENV.to_string(), described.to_string()),
        ]),
    }
}

/// A config of `domains`, with short waits so the tests are quick.
pub(crate) fn config(domains: Vec<DomainConfig>) -> HostConfig {
    HostConfig {
        store: Default::default(),
        policy: Default::default(),
        retry: Default::default(),
        domains,
        supervision: Supervision {
            handshake_timeout_ms: 20_000,
            initial_backoff_ms: 50,
            max_backoff_ms: 200,
            shutdown_grace_ms: 3_000,
            terminate_grace_ms: 3_000,
            call_timeout_ms: 20_000,
            socket_dir: None,
        },
    }
}

/// The child's entry: a no-op unless this run is the fake.
#[test]
fn entry() {
    let Ok(mode) = std::env::var(MODE_ENV) else {
        return;
    };
    let name = std::env::var(NAME_ENV).unwrap_or_default();
    if mode == mode::DEAF {
        // SAFETY: setting a signal's disposition to SIG_IGN installs no
        // handler code; nothing else here touches SIGTERM.
        unsafe {
            libc::signal(libc::SIGTERM, libc::SIG_IGN);
        }
    }
    if mode != mode::STUBBORN && mode != mode::DEAF {
        std::thread::spawn(|| {
            let _ = std::io::copy(&mut std::io::stdin(), &mut std::io::sink());
            if let Some(mark) = std::env::var_os(EOF_MARK_ENV) {
                std::fs::write(mark, b"eof").unwrap();
            }
            std::process::exit(0);
        });
    }
    match mode.as_str() {
        mode::SILENT => park(),
        mode::MALFORMED => {
            say(MALFORMED_LINE);
            park()
        }
        mode::NOISY => {
            say("Loading player...");
            say("");
            serve(name, false)
        }
        mode::CLOSES_STDOUT => serve(name, true),
        _ => serve(name, false),
    }
}

fn say(line: &str) {
    let mut out = std::io::stdout().lock();
    writeln!(out, "{line}").unwrap();
    out.flush().unwrap();
}

fn park() -> ! {
    loop {
        std::thread::park();
    }
}

fn serve(name: String, close_stdout: bool) -> ! {
    let socket = PathBuf::from(std::env::var(SOCKET_ENV).expect("the host sets ANGZARR_SOCKET"));
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async move {
        let _ = std::fs::remove_file(&socket);
        let listener = UnixListener::bind(&socket).unwrap();
        say(&format!("angzarr-handshake v1 unix:{}", socket.display()));
        if close_stdout {
            close_stdout_for_good();
        }
        tonic::transport::Server::builder()
            .add_service(CommandHandlerServiceServer::new(Fake {
                name: name.clone(),
            }))
            .add_service(ComponentServiceServer::new(Fake { name }))
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await
            .unwrap();
    });
    // Reached only if the server stopped on its own.
    std::process::exit(3)
}

/// Closes the stdout pipe, so its reader sees end of file, and parks
/// /dev/null on descriptor 1 (the lowest free one, just closed), so a later
/// write to stdout goes nowhere rather than into a socket.
fn close_stdout_for_good() {
    use std::os::fd::{FromRawFd, OwnedFd};
    // SAFETY: descriptor 1 is this process's stdout, which nothing else
    // here owns; it is flushed, and closed exactly once.
    drop(unsafe { OwnedFd::from_raw_fd(1) });
    let null = std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/null")
        .unwrap();
    std::mem::forget(null);
}

struct Fake {
    name: String,
}

fn rules_version() -> String {
    format!("pid-{}", std::process::id())
}

#[tonic::async_trait]
impl ComponentService for Fake {
    async fn describe(
        &self,
        _: Request<DescribeRequest>,
    ) -> Result<Response<pb::ComponentDescriptor>, Status> {
        Ok(Response::new(pb::ComponentDescriptor {
            name: self.name.clone(),
            kind: pb::ComponentKind::Aggregate as i32,
            rules_version: rules_version(),
            state: Some(pb::StateDescriptor {
                type_url: "/test.player.v1.State".to_string(),
                options: Some(pb::StateOptions {
                    conflict_resolutions: vec![
                        pb::ConflictResolution::Strict as i32,
                        pb::ConflictResolution::AggregateHandles as i32,
                    ],
                }),
            }),
            ..Default::default()
        }))
    }
}

#[tonic::async_trait]
impl CommandHandlerService for Fake {
    async fn handle(
        &self,
        request: Request<pb::ContextualCommand>,
    ) -> Result<Response<pb::CommandHandlerResponse>, Status> {
        let request = request.into_inner();
        let next = request.events.as_ref().map_or(0, |b| b.next_sequence);
        let command = request.command.unwrap_or_default();
        let type_url = command
            .page
            .as_ref()
            .and_then(angzarr_router::command_payload)
            .map(|any| any.type_url.clone())
            .unwrap_or_default();
        if type_url.ends_with("Refuse") {
            return Err(refused());
        }
        if type_url.ends_with("Taken") {
            return Err(taken());
        }
        if type_url.ends_with("Break") {
            return Err(broke());
        }
        if type_url.ends_with("Hang") {
            std::future::pending::<()>().await;
        }
        if type_url.ends_with("Die") {
            std::process::exit(7);
        }
        Ok(Response::new(pb::CommandHandlerResponse {
            conflict: None,
            events: Some(pb::EventBook {
                cover: command.cover,
                pages: vec![pb::EventPage {
                    payload: Some(noted(next)),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            rules_version: rules_version(),
        }))
    }
}

/// The one event a command is answered with: `Noted`, its value the
/// stream's next sequence when the command arrived.
pub(crate) fn noted(next: u64) -> Any {
    Any {
        type_url: "/test.player.v1.Noted".to_string(),
        value: next.to_le_bytes().to_vec(),
    }
}

fn refused() -> Status {
    status(
        tonic::Code::FailedPrecondition,
        "table is full",
        "TABLE_FULL",
        ("seats", "6"),
        Some(refusal()),
    )
}

fn taken() -> Status {
    status(
        tonic::Code::AlreadyExists,
        "the seat is taken",
        "SEAT_TAKEN",
        ("seat", "3"),
        Some(refusal()),
    )
}

fn broke() -> Status {
    status(
        tonic::Code::Internal,
        "the player's handler broke",
        "PLAYER_BROKE",
        ("seat", "3"),
        None,
    )
}

/// A status as angzarr's client libraries answer one: `code` and
/// `message`, and in its details an ErrorInfo of `reason` and the one
/// `metadata` pair, then `typed`, if any.
fn status(
    code: tonic::Code,
    message: &str,
    reason: &str,
    metadata: (&str, &str),
    typed: Option<Any>,
) -> Status {
    let info = ErrorInfo {
        reason: reason.to_string(),
        domain: angzarr_router::error::ERROR_INFO_DOMAIN.to_string(),
        metadata: [(metadata.0.to_string(), metadata.1.to_string())].into(),
    };
    let info = Any {
        type_url: ERROR_INFO_TYPE_URL.to_string(),
        value: info.encode_to_vec(),
    };
    let rpc = RpcStatus {
        code: code as i32,
        message: message.to_string(),
        details: std::iter::once(info).chain(typed).collect(),
    };
    Status::with_details(code, message, rpc.encode_to_vec().into())
}
