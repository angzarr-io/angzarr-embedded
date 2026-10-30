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
//! `ANGZARR_FAKE_KIND` makes it another kind (unset: an aggregate):
//!
//! - `process_manager`: a process manager whose starting types are
//!   `ANGZARR_FAKE_STARTING`'s, comma-separated, handling as above;
//! - `policy`: a `PolicyService` from `ANGZARR_FAKE_INPUT` to
//!   `ANGZARR_FAKE_TARGET`, answering a `...Seated` event with
//!   [`HOLD`] and a `...SeatedBig` one with [`HOLD_TOO_MUCH`], each to the
//!   event's root; and a refusal with the fact [`SEAT_RELEASED`], its value
//!   the refused command's type URL;
//! - `projector`: a `ProjectorService` of `ANGZARR_FAKE_INPUT`'s events,
//!   failing an event by its type URL's last segment: `...Break` INTERNAL
//!   (`PROJECTOR_BROKE`), `...Refuse` FAILED_PRECONDITION
//!   (`VIEW_REFUSED`), and the first time only, `...Flaky` UNAVAILABLE
//!   (`VIEW_DB_DOWN`) and `...Stale` ABORTED (`VIEW_STALE`).
//!
//! A policy or projector appends a line to `ANGZARR_FAKE_LOG` for each
//! call: `handle <type>` per event, `refused <typed refusal type URL>`.
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
use crate::rpc::policy::policy_service_server::{PolicyService, PolicyServiceServer};
use crate::rpc::projector::projector_service_server::{ProjectorService, ProjectorServiceServer};
use crate::rpc::DescribeRequest;

/// The variable naming the fake's mode.
pub(crate) const MODE_ENV: &str = "ANGZARR_FAKE_CHILD";
/// The variable naming a file the fake writes when it exits on stdin's end,
/// and only then: a kill leaves no mark.
pub(crate) const EOF_MARK_ENV: &str = "ANGZARR_FAKE_EOF_MARK";
/// The variable naming the component it describes.
pub(crate) const NAME_ENV: &str = "ANGZARR_FAKE_NAME";
/// The variable naming its kind: [`kind`]'s; an aggregate when unset.
pub(crate) const KIND_ENV: &str = "ANGZARR_FAKE_KIND";
/// A process manager's starting types, comma-separated, as its descriptor
/// lists them.
pub(crate) const STARTING_ENV: &str = "ANGZARR_FAKE_STARTING";
/// A policy's or projector's input domain.
pub(crate) const INPUT_ENV: &str = "ANGZARR_FAKE_INPUT";
/// A policy's target domain.
pub(crate) const TARGET_ENV: &str = "ANGZARR_FAKE_TARGET";
/// The file a policy or projector logs its calls to.
pub(crate) const LOG_ENV: &str = "ANGZARR_FAKE_LOG";

/// The kinds besides an aggregate.
pub(crate) mod kind {
    pub(crate) const PROCESS_MANAGER: &str = "process_manager";
    pub(crate) const POLICY: &str = "policy";
    pub(crate) const PROJECTOR: &str = "projector";
}

/// The command a policy answers `...Seated` with.
pub(crate) const HOLD: &str = "/test.wallet.v1.Hold";
/// The command a policy answers `...SeatedBig` with.
pub(crate) const HOLD_TOO_MUCH: &str = "/test.wallet.v1.HoldTooMuch";
/// The fact a policy answers a refusal with.
pub(crate) const SEAT_RELEASED: &str = "/test.table.v1.SeatReleased";

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
        kind: Default::default(),
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
        let fake = Fake { name };
        let kind = std::env::var(KIND_ENV).unwrap_or_default();
        let server = tonic::transport::Server::builder()
            .add_service(ComponentServiceServer::new(fake.clone()));
        let server = match kind.as_str() {
            kind::POLICY => server.add_service(PolicyServiceServer::new(fake)),
            kind::PROJECTOR => server.add_service(ProjectorServiceServer::new(fake)),
            _ => server.add_service(CommandHandlerServiceServer::new(fake)),
        };
        server
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

#[derive(Clone)]
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
        let env = |name| std::env::var(name).unwrap_or_default();
        let kind = env(KIND_ENV);
        if kind == kind::POLICY || kind == kind::PROJECTOR {
            let kind = match kind.as_str() {
                kind::POLICY => pb::ComponentKind::Policy,
                _ => pb::ComponentKind::Projector,
            };
            return Ok(Response::new(pb::ComponentDescriptor {
                name: self.name.clone(),
                kind: kind as i32,
                inputs: vec![pb::Target {
                    domain: env(INPUT_ENV),
                }],
                rules_version: rules_version(),
                ..Default::default()
            }));
        }
        let (kind, starting_types) = match kind.as_str() {
            kind::PROCESS_MANAGER => (
                pb::ComponentKind::ProcessManager,
                env(STARTING_ENV)
                    .split(',')
                    .filter(|t| !t.is_empty())
                    .map(str::to_string)
                    .collect(),
            ),
            _ => (pb::ComponentKind::Aggregate, Vec::new()),
        };
        Ok(Response::new(pb::ComponentDescriptor {
            name: self.name.clone(),
            kind: kind as i32,
            rules_version: rules_version(),
            starting_types,
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

/// Appends `line` to the fake's call log, if it has one.
fn log(line: &str) {
    if let Some(path) = std::env::var_os(LOG_ENV) {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        writeln!(file, "{line}").unwrap();
    }
}

/// How many lines of the call log are `line`.
fn logged(line: &str) -> usize {
    std::env::var_os(LOG_ENV)
        .and_then(|path| std::fs::read_to_string(path).ok())
        .map_or(0, |text| text.lines().filter(|l| *l == line).count())
}

/// An `Any` of `type_url` carrying `value`.
fn packed(type_url: &str, value: Vec<u8>) -> Any {
    Any {
        type_url: type_url.to_string(),
        value,
    }
}

#[tonic::async_trait]
impl PolicyService for Fake {
    async fn handle(
        &self,
        request: Request<pb::EventBook>,
    ) -> Result<Response<pb::PolicyResponse>, Status> {
        let book = request.into_inner();
        let target = std::env::var(TARGET_ENV).unwrap_or_default();
        let root = book.cover.as_ref().and_then(|c| c.root.clone());
        let mut commands = Vec::new();
        for page in &book.pages {
            let type_url = page.payload.as_ref().map_or("", |a| a.type_url.as_str());
            log(&format!("handle {type_url}"));
            let command = if type_url.ends_with("SeatedBig") {
                HOLD_TOO_MUCH
            } else if type_url.ends_with("Seated") {
                HOLD
            } else {
                continue;
            };
            commands.push(pb::CommandBook {
                cover: Some(pb::Cover {
                    domain: target.clone(),
                    root: root.clone(),
                    edition: None,
                }),
                page: Some(pb::CommandPage {
                    body: Some(pb::command_page::Body::Command(pb::Command {
                        payload: Some(packed(command, Vec::new())),
                    })),
                    ..Default::default()
                }),
                handling: None,
            });
        }
        Ok(Response::new(pb::PolicyResponse { commands }))
    }

    async fn on_refused(
        &self,
        request: Request<pb::Refusal>,
    ) -> Result<Response<pb::OnRefusedResponse>, Status> {
        let refusal = request.into_inner();
        let typed = refusal
            .response
            .as_ref()
            .and_then(|r| match &r.reason {
                Some(pb::refusal_response::Reason::Refusal(any)) => Some(any.type_url.clone()),
                _ => None,
            })
            .unwrap_or_default();
        log(&format!("refused {typed}"));
        let refused = refusal
            .command
            .as_ref()
            .and_then(|c| c.page.as_ref())
            .and_then(angzarr_router::command_payload)
            .map_or(String::new(), |a| a.type_url.clone());
        Ok(Response::new(pb::OnRefusedResponse {
            fact: Some(pb::CommandBook {
                cover: None,
                page: Some(pb::CommandPage {
                    body: Some(pb::command_page::Body::Fact(pb::Fact {
                        payload: Some(packed(SEAT_RELEASED, refused.into_bytes())),
                    })),
                    ..Default::default()
                }),
                handling: None,
            }),
        }))
    }
}

#[tonic::async_trait]
impl ProjectorService for Fake {
    async fn handle(
        &self,
        request: Request<pb::EventBook>,
    ) -> Result<Response<pb::Projection>, Status> {
        let book = request.into_inner();
        for page in &book.pages {
            let type_url = page.payload.as_ref().map_or("", |a| a.type_url.as_str());
            let line = format!("handle {type_url}");
            let first = logged(&line) == 0;
            log(&line);
            let fail =
                |code, reason: &str| status(code, reason, reason, ("type_url", type_url), None);
            if type_url.ends_with("Break") {
                return Err(fail(tonic::Code::Internal, "PROJECTOR_BROKE"));
            }
            if type_url.ends_with("Refuse") {
                return Err(fail(tonic::Code::FailedPrecondition, "VIEW_REFUSED"));
            }
            if first && type_url.ends_with("Flaky") {
                return Err(fail(tonic::Code::Unavailable, "VIEW_DB_DOWN"));
            }
            if first && type_url.ends_with("Stale") {
                return Err(fail(tonic::Code::Aborted, "VIEW_STALE"));
            }
        }
        Ok(Response::new(pb::Projection {
            cover: book.cover.clone(),
            projector: self.name.clone(),
            next_sequence: book.pages.last().map_or(0, |p| p.sequence + 1),
            projection: None,
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
