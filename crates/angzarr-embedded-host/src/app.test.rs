use std::path::{Path, PathBuf};

use angzarr_embedded::next::v1::{command_page, Command, CommandBook, CommandPage, Cover, Uuid};
use angzarr_embedded::next::{Any, Handled, Misconfigured, Registry, Sampled, Timestamp};
use angzarr_embedded_diagnostic::Report;

use super::*;
use crate::config::HostConfig;
use crate::fake_child::{config, domain, mode, noted};

/// A directory of this test's own, emptied, and removed with what is in it
/// when the guard drops, the test passing or not.
fn scratch(name: &str) -> Scratch {
    let dir = std::env::temp_dir().join(format!("angzarr-host-app-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    Scratch(dir)
}

/// A scratch directory, removed on drop.
struct Scratch(PathBuf);

impl std::ops::Deref for Scratch {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

impl AsRef<Path> for Scratch {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn a_scratch_directory_is_gone_once_dropped() {
    let dir = scratch("dropped");
    std::fs::write(dir.join("angzarr.toml"), "").unwrap();
    let path = dir.to_path_buf();
    assert!(path.is_dir());
    drop(dir);
    assert!(!path.exists(), "{} was left behind", path.display());
}

/// `config` written as the TOML the CLI writes, into `dir`.
fn written(dir: &Path, config: &HostConfig) -> PathBuf {
    let path = dir.join("angzarr.toml");
    std::fs::write(&path, toml::to_string(config).unwrap()).unwrap();
    path
}

/// A player domain served by the fake child.
fn player_config() -> HostConfig {
    config(vec![domain("player", "PlayerAggregate", mode::SERVE)])
}

fn player() -> Cover {
    Cover {
        domain: "player".to_string(),
        root: Some(Uuid::from_bytes([0x0a; 16])),
        edition: None,
    }
}

fn sit_down(n: u8) -> CommandBook {
    CommandBook {
        cover: Some(player()),
        page: Some(CommandPage {
            body: Some(command_page::Body::Command(Command {
                payload: Some(Any {
                    type_url: "/test.player.v1.SitDown".to_string(),
                    value: Vec::new(),
                }),
            })),
            command_id: Some(Uuid::from_bytes([n; 16])),
            ..CommandPage::default()
        }),
        handling: None,
    }
}

fn sampled(n: u8) -> Sampled {
    Sampled {
        now: Timestamp {
            seconds: i64::from(n),
            nanos: 0,
        },
        command_id: Uuid::from_bytes([0x70 | n; 16]),
    }
}

fn nothing(_: &mut Registry) {}

/// The whole application from its config: a command for the remote domain
/// goes through the kernel and its event is in the store.
#[tokio::test(flavor = "multi_thread")]
async fn run_handles_a_command_for_a_remote_domain() {
    let dir = scratch("memory");
    let mut config = player_config();
    config.store.in_memory = true;
    let app = run(written(&dir, &config), nothing)
        .await
        .expect("the app runs");
    assert!(matches!(
        app.handle(sit_down(1), &sampled(1)).await,
        Ok(Handled::Applied(_))
    ));
    let events: Vec<_> = app
        .events(&player())
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.payload)
        .collect();
    assert_eq!(events, [Some(noted(0))]);
    assert_eq!(
        app.shutdown().await,
        vec![("player".to_string(), ChildExit::Exited(Some(0)))]
    );
}

/// `register` is called with the remote domains already registered, and
/// what it registers is the kernel's too.
#[tokio::test(flavor = "multi_thread")]
async fn run_calls_register_for_the_in_process_domains() {
    let dir = scratch("register");
    let mut config = player_config();
    config.store.in_memory = true;
    let app = run(written(&dir, &config), |registry: &mut Registry| {
        assert_eq!(
            registry.command_handler("player", NoHandler, Default::default()),
            Err(Misconfigured::AlreadyRegistered {
                domain: "player".to_string(),
            }),
            "the remote domain is registered before register runs"
        );
    })
    .await
    .unwrap();
    assert_eq!(
        app.registry().domains().collect::<Vec<_>>(),
        ["player"],
        "the app's registry is the one run filled"
    );
    drop(app);
}

/// The SQLite store is at the config's path: what one run wrote, the next
/// reads, and the next command reaches the child with it.
#[tokio::test(flavor = "multi_thread")]
async fn the_store_is_the_configured_sqlite_file() {
    let dir = scratch("sqlite");
    let mut config = player_config();
    config.store.path = dir.join("app.db");
    let path = written(&dir, &config);
    let app = run(&path, nothing).await.unwrap();
    app.handle(sit_down(1), &sampled(1)).await.unwrap();
    drop(app);
    assert!(dir.join("app.db").exists());
    let app = run(&path, nothing).await.unwrap();
    app.handle(sit_down(2), &sampled(2)).await.unwrap();
    let events: Vec<_> = app
        .events(&player())
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.payload)
        .collect();
    assert_eq!(events, [Some(noted(0)), Some(noted(1))]);
    drop(app);
}

/// The kernel the app hands out does with a policy's bug what the
/// config's `[policy] on_bug` says; the default when it is left out.
#[tokio::test(flavor = "multi_thread")]
async fn the_kernel_follows_the_configs_policy_setting() {
    use angzarr_embedded::next::OnPolicyBug;
    let dir = scratch("policy");
    let path = dir.join("angzarr.toml");
    std::fs::write(&path, "[store]\nin_memory = true\n").unwrap();
    let app = run(&path, nothing).await.unwrap();
    assert_eq!(app.kernel().policy_bug(), OnPolicyBug::MovePast);
    drop(app);
    std::fs::write(
        &path,
        "[store]\nin_memory = true\n\n[policy]\non_bug = \"stay\"\n",
    )
    .unwrap();
    let app = run(&path, nothing).await.unwrap();
    assert_eq!(app.kernel().policy_bug(), OnPolicyBug::Stay);
    drop(app);
}

/// The kernel the app hands out retries a step's retryable failure as the
/// config's `[retry]` says, its defaults (the kernel's) when it is left
/// out: retrying is on with no config.
#[tokio::test(flavor = "multi_thread")]
async fn the_kernel_follows_the_configs_retry_setting() {
    use angzarr_embedded::next::OnRetryable;
    use std::time::Duration;
    let dir = scratch("retry");
    let path = dir.join("angzarr.toml");
    std::fs::write(&path, "[store]\nin_memory = true\n").unwrap();
    let app = run(&path, nothing).await.unwrap();
    assert_eq!(app.kernel().retryable(), OnRetryable::default());
    drop(app);
    std::fs::write(
        &path,
        "[store]\nin_memory = true\n\n[retry]\nmax_retries = 5\ninitial_backoff_ms = 50\n\
         multiplier = 3.0\nmax_backoff_ms = 1000\njitter = 0.5\n",
    )
    .unwrap();
    let app = run(&path, nothing).await.unwrap();
    assert_eq!(
        app.kernel().retryable(),
        OnRetryable {
            max_retries: 5,
            initial_backoff: Duration::from_millis(50),
            multiplier: 3.0,
            max_backoff: Duration::from_millis(1_000),
            jitter: 0.5,
        }
    );
    drop(app);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_config_is_unreadable() {
    let dir = scratch("missing");
    let path = dir.join("angzarr.toml");
    assert_eq!(
        run(&path, nothing).await.err(),
        Some(HostError::ConfigUnreadable {
            path: path.display().to_string(),
            kind: "NotFound".to_string(),
        })
    );
}

/// Malformed TOML and an unknown key are both invalid, at the byte the
/// problem starts.
#[tokio::test(flavor = "multi_thread")]
async fn an_invalid_config_is_refused_where_it_goes_wrong() {
    let dir = scratch("invalid");
    let path = dir.join("angzarr.toml");
    std::fs::write(&path, "[store]\nin_memory = true\nlanuch = 1\n").unwrap();
    assert_eq!(
        run(&path, nothing).await.err(),
        Some(HostError::ConfigInvalid {
            path: path.display().to_string(),
            offset: 25,
        })
    );
    std::fs::write(&path, "[[domain]\n").unwrap();
    assert!(matches!(
        run(&path, nothing).await.err(),
        Some(HostError::ConfigInvalid { .. })
    ));
}

/// A store that cannot be opened fails the run, its cause the kernel's
/// store error; the children started are stopped.
#[tokio::test(flavor = "multi_thread")]
async fn a_store_that_cannot_open_fails_the_run() {
    let dir = scratch("nostore");
    let mut config = player_config();
    config.store.path = dir.join("no/such/dir/app.db");
    let failed = run(written(&dir, &config), nothing).await.err();
    assert!(
        matches!(&failed, Some(HostError::Store { cause }) if cause != &Report::new("")),
        "{failed:?}"
    );
}

/// A handler registering nothing useful: only its name is asked for.
struct NoHandler;

impl angzarr_embedded::next::angzarr_router::router::CommandHandler for NoHandler {
    fn name(&self) -> &str {
        "none"
    }

    fn command_types(&self) -> Vec<String> {
        Vec::new()
    }

    fn dispatch(
        &self,
        _: &angzarr_embedded::next::v1::ContextualCommand,
    ) -> Result<
        angzarr_router::pb::CommandHandlerResponse,
        angzarr_embedded::next::angzarr_router::error::CodedError,
    > {
        unreachable!("never registered")
    }
}

/// The App is shared by every task that sends it commands, and by the
/// host's listener: `Send + Sync`, and `'static`.
#[test]
fn the_app_is_shareable() {
    fn shareable<T: Send + Sync + 'static>() {}
    shareable::<App>();
}

/// In-process aggregates for the concurrency and exposure tests,
/// hand-written as the router's `CommandHandler`, and the books they take.
mod fixtures {
    use std::sync::{Condvar, Mutex};
    use std::time::Duration;

    use angzarr_embedded::next::angzarr_router::error::CodedError;
    use angzarr_embedded::next::angzarr_router::router::CommandHandler;
    use angzarr_embedded::next::v1::{
        command_page, Command, CommandBook, CommandPage, ComponentDescriptor, ComponentKind,
        ConflictResolution, ContextualCommand, Cover, EventBook, EventPage, StateDescriptor,
        StateOptions, Uuid,
    };
    use angzarr_embedded::next::Any;
    use angzarr_router::pb::CommandHandlerResponse;

    /// How long a dispatch waits for what a test is forcing: past it, the
    /// test fails rather than hangs.
    pub const WITHIN: Duration = Duration::from_secs(10);

    pub fn any(type_url: &str) -> Any {
        Any {
            type_url: type_url.to_string(),
            value: Vec::new(),
        }
    }

    pub fn cover(domain: &str, root: u8) -> Cover {
        Cover {
            domain: domain.to_string(),
            root: Some(Uuid::from_bytes([root; 16])),
            edition: None,
        }
    }

    /// Command `n`, of type `type_url`, to `domain`'s stream `root`.
    pub fn book(domain: &str, root: u8, n: u8, type_url: &str) -> CommandBook {
        CommandBook {
            cover: Some(cover(domain, root)),
            page: Some(CommandPage {
                body: Some(command_page::Body::Command(Command {
                    payload: Some(any(type_url)),
                })),
                command_id: Some(Uuid::from_bytes([n; 16])),
                ..CommandPage::default()
            }),
            handling: None,
        }
    }

    /// An aggregate's descriptor, as angzarr-build generates it.
    pub fn descriptor(name: &str) -> ComponentDescriptor {
        ComponentDescriptor {
            name: name.to_string(),
            kind: ComponentKind::Aggregate as i32,
            rules_version: format!("{name}-1"),
            state: Some(StateDescriptor {
                type_url: format!("/test.{name}.v1.State"),
                options: Some(StateOptions {
                    conflict_resolutions: vec![
                        ConflictResolution::Strict as i32,
                        ConflictResolution::AggregateHandles as i32,
                    ],
                }),
            }),
            ..ComponentDescriptor::default()
        }
    }

    /// The command's type URL.
    fn command_type(command: &ContextualCommand) -> String {
        match command
            .command
            .as_ref()
            .and_then(|b| b.page.as_ref())
            .and_then(|p| p.body.as_ref())
        {
            Some(command_page::Body::Command(Command {
                payload: Some(payload),
            })) => payload.type_url.clone(),
            _ => String::new(),
        }
    }

    /// One event, `type_url`.
    fn decided(type_url: &str, rules_version: &str) -> CommandHandlerResponse {
        CommandHandlerResponse {
            conflict: None,
            events: Some(EventBook {
                pages: vec![EventPage {
                    payload: Some(any(type_url)),
                    ..EventPage::default()
                }],
                ..EventBook::default()
            }),
            rules_version: rules_version.to_string(),
        }
    }

    pub const MET: &str = "/test.meet.v1.Met";
    pub const ALONE: &str = "/test.meet.v1.Alone";

    /// Where `parties` dispatches meet: each waits, up to [`WITHIN`], for
    /// all of them to be inside their handlers at once.
    pub struct Rendezvous {
        inside: Mutex<usize>,
        all: Condvar,
        parties: usize,
    }

    impl Rendezvous {
        pub fn of(parties: usize) -> Rendezvous {
            Rendezvous {
                inside: Mutex::new(0),
                all: Condvar::new(),
                parties,
            }
        }

        /// Whether every party arrived within [`WITHIN`].
        fn meet(&self) -> bool {
            let mut inside = self.inside.lock().unwrap();
            *inside += 1;
            self.all.notify_all();
            let (inside, waited) = self
                .all
                .wait_timeout_while(inside, WITHIN, |n| *n < self.parties)
                .unwrap();
            drop(inside);
            !waited.timed_out()
        }
    }

    /// An aggregate whose every command waits at the rendezvous: it
    /// decides [`MET`] when every party arrived while it waited, [`ALONE`]
    /// when it waited out [`WITHIN`] alone.
    pub struct Meeting(pub std::sync::Arc<Rendezvous>);

    impl CommandHandler for Meeting {
        fn name(&self) -> &str {
            "Meeting"
        }

        fn command_types(&self) -> Vec<String> {
            vec!["test.meet.v1.Meet".to_string()]
        }

        fn dispatch(&self, _: &ContextualCommand) -> Result<CommandHandlerResponse, CodedError> {
            let met = self.0.meet();
            Ok(decided(if met { MET } else { ALONE }, "meeting-1"))
        }
    }

    pub const FIRST: &str = "/test.gate.v1.First";
    pub const SECOND: &str = "/test.gate.v1.Second";

    /// How long the first command holds its stream's handler open for a
    /// second dispatch on it to show up: a second dispatch while the first
    /// is open is the fault the test detects, so the hold bounds the
    /// detection, and a correct host is never seen to overlap however long
    /// it is.
    pub const HOLD: Duration = Duration::from_millis(500);

    /// What the gate saw.
    #[derive(Default)]
    pub struct Seen {
        /// Dispatches inside the handler now.
        inside: usize,
        /// Whether two were ever inside at once.
        pub overlapped: bool,
        /// Each dispatch's command type, in the order they entered.
        pub entered: Vec<String>,
    }

    /// An aggregate whose [`FIRST`] command tells the test it is inside
    /// (`entered`), then holds the handler open for [`HOLD`], or until a
    /// second dispatch enters beside it; each command decides its own
    /// type's event (`.../First` decides `.../FirstDone`).
    pub struct Gate {
        pub seen: Mutex<Seen>,
        pub changed: Condvar,
        pub entered: Mutex<std::sync::mpsc::Sender<()>>,
    }

    impl CommandHandler for Gate {
        fn name(&self) -> &str {
            "Gate"
        }

        fn command_types(&self) -> Vec<String> {
            vec![FIRST[1..].to_string(), SECOND[1..].to_string()]
        }

        fn dispatch(
            &self,
            command: &ContextualCommand,
        ) -> Result<CommandHandlerResponse, CodedError> {
            let kind = command_type(command);
            let mut seen = self.seen.lock().unwrap();
            seen.inside += 1;
            seen.overlapped |= seen.inside > 1;
            seen.entered.push(kind.clone());
            self.changed.notify_all();
            if kind == FIRST {
                let _ = self.entered.lock().unwrap().send(());
                seen = self
                    .changed
                    .wait_timeout_while(seen, HOLD, |s| !s.overlapped)
                    .unwrap()
                    .0;
            }
            seen.inside -= 1;
            Ok(decided(&format!("{kind}Done"), "gate-1"))
        }
    }
}

/// The App's async API, shared (red, design-by-test, 2026-10-30): commands
/// for different streams run at once; one stream's run one at a time, in
/// the order they arrive. Each test forces what it claims: handlers that
/// can only finish together, or a handler held open where a second
/// dispatch would show.
mod concurrency {
    use std::sync::{Arc, Condvar, Mutex};

    use super::fixtures::*;
    use super::*;

    /// A config with no component of its own (every aggregate here is
    /// in-process) and an in-memory store.
    fn in_memory(dir: &Path) -> PathBuf {
        let path = dir.join("angzarr.toml");
        std::fs::write(&path, "[store]\nin_memory = true\n").unwrap();
        path
    }

    /// The payloads of `cover`'s stream.
    async fn payloads(app: &App, cover: &Cover) -> Vec<String> {
        app.events(cover)
            .await
            .unwrap()
            .into_iter()
            .map(|p| p.payload.map(|a| a.type_url).unwrap_or_default())
            .collect()
    }

    /// Two aggregates, two domains: a command to each, sent from two tasks
    /// at once. Their handlers can finish only once both are inside at
    /// the same time, so both deciding [`MET`] is the overlap; a host that
    /// handled one after the other would leave the first [`ALONE`].
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_aggregates_handle_commands_concurrently() {
        let dir = scratch("concurrent-domains");
        let rendezvous = Arc::new(Rendezvous::of(2));
        let meeting = rendezvous.clone();
        let app = run(in_memory(&dir), move |registry: &mut Registry| {
            for domain in ["left", "right"] {
                registry
                    .command_handler(domain, Meeting(meeting.clone()), descriptor(domain))
                    .unwrap();
            }
        })
        .await
        .expect("the app runs");
        let app = Arc::new(app);
        let send = |domain: &'static str, n: u8| {
            let app = app.clone();
            tokio::spawn(async move {
                app.handle(book(domain, 1, n, "/test.meet.v1.Meet"), &sampled(n))
                    .await
            })
        };
        let (left, right) = (send("left", 1), send("right", 2));
        let (left, right) = (left.await.unwrap(), right.await.unwrap());
        assert!(matches!(left, Ok(Handled::Applied(_))), "{:?}", left.err());
        assert!(
            matches!(right, Ok(Handled::Applied(_))),
            "{:?}",
            right.err()
        );
        assert_eq!(payloads(&app, &fixtures::cover("left", 1)).await, [MET]);
        assert_eq!(payloads(&app, &fixtures::cover("right", 1)).await, [MET]);
    }

    /// Two streams of ONE aggregate are as independent as two aggregates'
    /// streams: nothing orders streams against each other, a domain's
    /// included.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_streams_of_one_aggregate_handle_commands_concurrently() {
        let dir = scratch("concurrent-streams");
        let rendezvous = Arc::new(Rendezvous::of(2));
        let meeting = rendezvous.clone();
        let app = run(in_memory(&dir), move |registry: &mut Registry| {
            registry
                .command_handler("left", Meeting(meeting), descriptor("left"))
                .unwrap();
        })
        .await
        .expect("the app runs");
        let app = Arc::new(app);
        let send = |root: u8| {
            let app = app.clone();
            tokio::spawn(async move {
                app.handle(
                    book("left", root, root, "/test.meet.v1.Meet"),
                    &sampled(root),
                )
                .await
            })
        };
        let (one, two) = (send(1), send(2));
        let (one, two) = (one.await.unwrap(), two.await.unwrap());
        assert!(matches!(one, Ok(Handled::Applied(_))), "{:?}", one.err());
        assert!(matches!(two, Ok(Handled::Applied(_))), "{:?}", two.err());
        assert_eq!(payloads(&app, &fixtures::cover("left", 1)).await, [MET]);
        assert_eq!(payloads(&app, &fixtures::cover("left", 2)).await, [MET]);
    }

    /// Two commands to ONE stream: the second is sent once the first is
    /// inside its handler, which holds it open for [`HOLD`] or until a
    /// second dispatch enters beside it. The second waits its turn: it is
    /// never dispatched beside the first, both are applied (neither loses
    /// a race at the write), and the stream holds their events in the
    /// order they arrived.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn commands_to_one_stream_are_handled_one_at_a_time_in_arrival_order() {
        let dir = scratch("one-stream");
        let (entered, inside) = std::sync::mpsc::channel();
        let gate = Arc::new(Gate {
            seen: Mutex::new(Seen::default()),
            changed: Condvar::new(),
            entered: Mutex::new(entered),
        });
        let handler = gate.clone();
        let app = run(in_memory(&dir), move |registry: &mut Registry| {
            registry
                .command_handler("left", ArcHandler(handler), descriptor("left"))
                .unwrap();
        })
        .await
        .expect("the app runs");
        let app = Arc::new(app);
        let send = |n: u8, type_url: &'static str| {
            let app = app.clone();
            tokio::spawn(async move { app.handle(book("left", 1, n, type_url), &sampled(n)).await })
        };
        let first = send(1, FIRST);
        tokio::task::spawn_blocking(move || inside.recv_timeout(WITHIN))
            .await
            .unwrap()
            .expect("the first command reaches its handler");
        let second = send(2, SECOND);
        let (first, second) = (first.await.unwrap(), second.await.unwrap());

        let (overlapped, entered) = {
            let seen = gate.seen.lock().unwrap();
            (seen.overlapped, seen.entered.clone())
        };
        assert!(
            !overlapped,
            "the second command was dispatched while the first was inside its handler"
        );
        assert_eq!(entered, [FIRST, SECOND]);
        assert!(
            matches!(first, Ok(Handled::Applied(_))),
            "{:?}",
            first.err()
        );
        assert!(
            matches!(second, Ok(Handled::Applied(_))),
            "{:?}",
            second.err()
        );
        let events = app.events(&fixtures::cover("left", 1)).await.unwrap();
        let order: Vec<_> = events
            .iter()
            .map(|p| (p.sequence, p.payload.clone().unwrap_or_default().type_url))
            .collect();
        assert_eq!(
            order,
            [(0, format!("{FIRST}Done")), (1, format!("{SECOND}Done"))]
        );
    }

    /// A handler shared with the test, which reads what it saw.
    struct ArcHandler(Arc<Gate>);

    impl angzarr_embedded::next::angzarr_router::router::CommandHandler for ArcHandler {
        fn name(&self) -> &str {
            self.0.name()
        }

        fn command_types(&self) -> Vec<String> {
            self.0.command_types()
        }

        fn dispatch(
            &self,
            command: &angzarr_embedded::next::v1::ContextualCommand,
        ) -> Result<
            angzarr_router::pb::CommandHandlerResponse,
            angzarr_embedded::next::angzarr_router::error::CodedError,
        > {
            self.0.dispatch(command)
        }
    }
}

/// The exposure seam (red, design-by-test, 2026-10-30): what the host's
/// listener calls for each request, [`App::serve`]. A domain is served
/// only to an arrival its `[[component]]`'s `expose` admits; the answer is
/// `CommandHandlerCoordinatorService.Handle`'s.
mod exposure {
    use angzarr_embedded::next::v1::DeliveryOutcome;
    use angzarr_embedded::next::{KernelError, Malformed};

    use super::fixtures::*;
    use super::*;
    use crate::config::{ComponentConfig, Expose};
    use crate::exposure::{Arrival, Refused};

    /// An in-process (Rust) aggregate's `[[component]]`, exposed `expose`.
    fn aggregate(name: &str, expose: Expose) -> ComponentConfig {
        ComponentConfig {
            name: name.to_string(),
            language: "rust".to_string(),
            kind: Default::default(),
            upcasts: None,
            launch: None,
            entry: Vec::new(),
            dir: None,
            env: Default::default(),
            expose,
        }
    }

    /// `hidden` (exposed none), `local` (loopback) and `open` (ip), all
    /// in-process, and `unlisted`, registered but named by no
    /// `[[component]]`; each answers a command with one `Gated` event.
    async fn app(dir: &Path) -> App {
        let mut config = HostConfig {
            components: vec![
                aggregate("hidden", Expose::None),
                aggregate("local", Expose::Loopback),
                aggregate("open", Expose::Ip),
            ],
            ..HostConfig::default()
        };
        config.store.in_memory = true;
        run(written(dir, &config), |registry: &mut Registry| {
            for domain in ["hidden", "local", "open", "unlisted"] {
                registry
                    .command_handler(
                        domain,
                        Meeting(std::sync::Arc::new(Rendezvous::of(1))),
                        descriptor(domain),
                    )
                    .unwrap();
            }
        })
        .await
        .expect("the app runs")
    }

    fn meet(domain: &str) -> CommandBook {
        book(domain, 1, 1, "/test.meet.v1.Meet")
    }

    fn not_exposed(domain: &str, exposed: Expose, arrived: Arrival) -> Refused {
        Refused::NotExposed {
            domain: domain.to_string(),
            exposed,
            arrived,
        }
    }

    /// A request is served when its domain is exposed to where it arrived
    /// from: loopback reaches `"loopback"` and `"ip"`, the network only
    /// `"ip"`. The answer is the stream's events, APPLIED.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_exposed_domain_is_served_to_the_arrivals_its_exposure_admits() {
        let dir = scratch("exposed");
        let app = app(&dir).await;
        for (domain, arrived) in [
            ("local", Arrival::Loopback),
            ("open", Arrival::Loopback),
            ("open", Arrival::Ip),
        ] {
            let mut book = meet(domain);
            if arrived == Arrival::Ip {
                book.cover = Some(fixtures::cover(domain, 2));
            }
            let response = app
                .serve(book.clone(), arrived, &sampled(1))
                .await
                .unwrap_or_else(|e| panic!("{domain} from {arrived:?}: {e:?}"));
            let events = response.events.expect("the stream's events");
            assert_eq!(events.cover, book.cover, "{domain} from {arrived:?}");
            let payloads: Vec<_> = events
                .pages
                .iter()
                .map(|p| p.payload.clone().unwrap_or_default().type_url)
                .collect();
            assert_eq!(payloads, [MET], "{domain} from {arrived:?}");
            assert_eq!(
                response.outcome.map(|o| o.delivery),
                Some(DeliveryOutcome::Applied as i32),
                "{domain} from {arrived:?}"
            );
        }
    }

    /// A request for a domain not exposed to where it arrived from is
    /// refused, and reaches no handler: nothing is written. A domain the
    /// config does not name is exposed `"none"`.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_domain_not_exposed_to_the_arrival_is_refused() {
        let dir = scratch("not-exposed");
        let app = app(&dir).await;
        for (domain, exposed, arrived) in [
            ("hidden", Expose::None, Arrival::Loopback),
            ("hidden", Expose::None, Arrival::Ip),
            ("local", Expose::Loopback, Arrival::Ip),
            ("unlisted", Expose::None, Arrival::Loopback),
        ] {
            assert_eq!(
                app.serve(meet(domain), arrived, &sampled(1)).await.err(),
                Some(not_exposed(domain, exposed, arrived)),
                "{domain} from {arrived:?}"
            );
            assert!(
                app.events(&fixtures::cover(domain, 1))
                    .await
                    .unwrap()
                    .is_empty(),
                "{domain}: nothing written"
            );
        }
    }

    /// A domain that is not registered at all is refused as the kernel
    /// refuses it for a local caller: the unknown domain, whatever the
    /// arrival.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unknown_domain_is_the_kernels_unknown_domain() {
        let dir = scratch("unknown");
        let app = app(&dir).await;
        for arrived in [Arrival::Loopback, Arrival::Ip] {
            assert_eq!(
                app.serve(meet("nowhere"), arrived, &sampled(1)).await.err(),
                Some(Refused::Failed(Failure::Kernel(KernelError::Malformed(
                    Malformed::UnknownDomain {
                        domain: "nowhere".to_string(),
                    }
                )))),
                "from {arrived:?}"
            );
        }
    }

    /// A refusal is a code and args, as every error here is.
    #[test]
    fn not_exposed_is_a_code_and_args() {
        let refused = not_exposed("hidden", Expose::None, Arrival::Ip);
        assert_eq!(
            refused.to_string(),
            "angzarr-embedded-host-not-exposed domain=hidden exposed=none arrived=ip"
        );
    }
}
