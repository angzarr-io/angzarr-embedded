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
        "[store]\nin_memory = true\n\n[retry]\nmax_retries = 5\nmax_race_runs = 4\n\
         initial_backoff_ms = 50\nmultiplier = 3.0\nmax_backoff_ms = 1000\njitter = 0.5\n",
    )
    .unwrap();
    let app = run(&path, nothing).await.unwrap();
    assert_eq!(
        app.kernel().retryable(),
        OnRetryable {
            max_retries: 5,
            max_race_runs: 4,
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

/// Malformed TOML and an unknown key are both invalid, at the key path
/// and the byte the problem starts, with the TOML reader's text.
#[tokio::test(flavor = "multi_thread")]
async fn an_invalid_config_is_refused_where_it_goes_wrong() {
    let dir = scratch("invalid");
    let path = dir.join("angzarr.toml");
    std::fs::write(&path, "[store]\nin_memory = true\nlanuch = 1\n").unwrap();
    let refused = run(&path, nothing).await.err();
    assert!(
        matches!(
            &refused,
            Some(HostError::ConfigInvalid { path: at, key, offset: 25, ext_err_txt })
                if *at == path.display().to_string()
                    && key == "store.lanuch"
                    && ext_err_txt.contains("lanuch")
        ),
        "{refused:?}"
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

    async fn dispatch(
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

        async fn dispatch(
            &self,
            _: &ContextualCommand,
        ) -> Result<CommandHandlerResponse, CodedError> {
            let met = self.0.meet();
            Ok(decided(if met { MET } else { ALONE }, "meeting-1"))
        }
    }
}

/// The App's async API, shared (red, design-by-test, 2026-10-30): commands
/// for different streams run at once, forced by handlers that can only
/// finish together. One stream's commands race optimistically, as the
/// kernel's tests show (`next/concurrency.test.rs` in angzarr-embedded).
mod concurrency {
    use std::sync::Arc;

    use super::fixtures::*;
    use super::*;

    /// A config listing the in-process aggregates `domains`, and an
    /// in-memory store.
    fn in_memory(dir: &Path, domains: &[&str]) -> PathBuf {
        let path = dir.join("angzarr.toml");
        let mut text = String::new();
        for domain in domains {
            text.push_str(&format!(
                "[[component]]\nname = \"{domain}\"\nkind = \"domain\"\nlanguage = \"rust\"\n\n"
            ));
        }
        text.push_str("[store]\nin_memory = true\n");
        std::fs::write(&path, text).unwrap();
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
        let app = run(
            in_memory(&dir, &["left", "right"]),
            move |registry: &mut Registry| {
                for domain in ["left", "right"] {
                    registry
                        .command_handler(domain, Meeting(meeting.clone()), descriptor(domain))
                        .unwrap();
                }
            },
        )
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
        let app = run(
            in_memory(&dir, &["left"]),
            move |registry: &mut Registry| {
                registry
                    .command_handler("left", Meeting(meeting), descriptor("left"))
                    .unwrap();
            },
        )
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
    use crate::exposure::Arrival;

    /// An in-process (Rust) aggregate's `[[component]]`, exposed `expose`.
    fn aggregate(name: &str, expose: Expose) -> ComponentConfig {
        ComponentConfig {
            name: name.to_string(),
            language: "rust".to_string(),
            kind: crate::config::ComponentKindConfig::Domain,
            upcasts: None,
            launch: None,
            entry: Vec::new(),
            dir: None,
            env: Default::default(),
            expose,
        }
    }

    /// `hidden` (exposed none), `local` (loopback) and `open` (ip), all
    /// in-process; each answers a command with one `Gated` event.
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
            for domain in ["hidden", "local", "open"] {
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

    /// The kernel's refusal of `domain` as unknown: what a caller gets for
    /// a domain that is not registered, and for one not exposed to it.
    fn unknown(domain: &str) -> Failure<Rejected> {
        Failure::Kernel(KernelError::Malformed(Malformed::UnknownDomain {
            domain: domain.to_string(),
        }))
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
    /// refused exactly as an unknown domain is: the caller cannot tell a
    /// hidden domain from a missing one (the owner's ruling, 2026-10-30;
    /// the host logs the real reason locally). It reaches no handler:
    /// nothing is written.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_domain_not_exposed_to_the_arrival_is_refused_as_unknown() {
        let dir = scratch("not-exposed");
        let app = app(&dir).await;
        for (domain, arrived) in [
            ("hidden", Arrival::Loopback),
            ("hidden", Arrival::Ip),
            ("local", Arrival::Ip),
        ] {
            assert_eq!(
                app.serve(meet(domain), arrived, &sampled(1)).await.err(),
                Some(unknown(domain)),
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
                Some(unknown("nowhere")),
                "from {arrived:?}"
            );
        }
    }
}

/// What the exposure seam answers beyond the owner's cases: an exposed
/// process manager is refused at run, and a decision with no events is
/// answered with the addressed stream, empty.
mod exposure_edges {
    use angzarr_embedded::next::angzarr_router::error::CodedError;
    use angzarr_embedded::next::angzarr_router::router::CommandHandler;
    use angzarr_embedded::next::v1::{
        ComponentKind, ContextualCommand, DeliveryOutcome, EventBook,
    };
    use angzarr_router::pb::CommandHandlerResponse;

    use super::fixtures::*;
    use super::*;
    use crate::config::{ComponentConfig, Expose};
    use crate::exposure::Arrival;

    /// An in-process component's `[[component]]`, exposed `expose`.
    fn in_process(name: &str, expose: Expose) -> ComponentConfig {
        ComponentConfig {
            name: name.to_string(),
            language: "rust".to_string(),
            kind: crate::config::ComponentKindConfig::Domain,
            upcasts: None,
            launch: None,
            entry: Vec::new(),
            dir: None,
            env: Default::default(),
            expose,
        }
    }

    /// A domain answering every input with no events, of `kind`.
    struct Quiet(ComponentKind);

    impl CommandHandler for Quiet {
        fn name(&self) -> &str {
            "Quiet"
        }

        fn kind(&self) -> ComponentKind {
            self.0
        }

        fn command_types(&self) -> Vec<String> {
            vec!["test.meet.v1.Meet".to_string()]
        }

        async fn dispatch(
            &self,
            _: &ContextualCommand,
        ) -> Result<CommandHandlerResponse, CodedError> {
            Ok(CommandHandlerResponse {
                rules_version: "quiet-1".to_string(),
                ..CommandHandlerResponse::default()
            })
        }
    }

    /// `quiet`, registered as `kind`, its component exposed `expose`.
    async fn quiet(dir: &Path, kind: ComponentKind, expose: Expose) -> Result<App, HostError> {
        let mut config = HostConfig {
            components: vec![in_process("quiet", expose)],
            ..HostConfig::default()
        };
        config.store.in_memory = true;
        run(written(dir, &config), move |registry: &mut Registry| {
            let mut described = descriptor("quiet");
            described.kind = kind as i32;
            if kind == ComponentKind::ProcessManager {
                described.starting_types = vec!["test.meet.v1.Meet".to_string()];
            }
            registry
                .command_handler("quiet", Quiet(kind), described)
                .unwrap();
        })
        .await
    }

    /// Only an aggregate is served through the listener: a domain exposed
    /// but registered as a process manager is refused at run, by its
    /// descriptor's kind; one not exposed runs.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_exposed_process_manager_is_refused_at_run() {
        let dir = scratch("exposed-pm");
        assert_eq!(
            quiet(&dir, ComponentKind::ProcessManager, Expose::Loopback)
                .await
                .err(),
            Some(HostError::NotExposable {
                domain: "quiet".to_string(),
                kind: "process_manager".to_string(),
            })
        );
        let hidden = quiet(&dir, ComponentKind::ProcessManager, Expose::None).await;
        assert!(hidden.is_ok(), "{:?}", hidden.err());
        let aggregate = quiet(&dir, ComponentKind::Aggregate, Expose::Loopback).await;
        assert!(aggregate.is_ok(), "{:?}", aggregate.err());
    }

    /// A decision with no events is answered APPLIED, with the addressed
    /// stream's book, empty.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_decision_with_no_events_answers_the_addressed_stream_empty() {
        let dir = scratch("quiet");
        let app = quiet(&dir, ComponentKind::Aggregate, Expose::Ip)
            .await
            .expect("the app runs");
        let book = book("quiet", 1, 1, "/test.meet.v1.Meet");
        let response = app
            .serve(book.clone(), Arrival::Ip, &sampled(1))
            .await
            .expect("served");
        assert_eq!(
            response.events,
            Some(EventBook {
                cover: book.cover,
                ..EventBook::default()
            })
        );
        assert_eq!(
            response.outcome.map(|o| o.delivery),
            Some(DeliveryOutcome::Applied as i32)
        );
    }
}

/// Every component is listed (owner ruling, 2026-10-31): an in-process
/// component's `[[component]]` names what the application's `register`
/// registers, and `run` holds the two to each other, by name and kind. A
/// listed one nobody registered is refused, and so is one registered and
/// listed nowhere; the children started are stopped.
mod listing {
    use angzarr_embedded::next::angzarr_router::error::CodedError;
    use angzarr_embedded::next::angzarr_router::policy::PolicyDispatch;
    use angzarr_embedded::next::angzarr_router::router::ProjectorHandler;
    use angzarr_embedded::next::v1::{EventBook, Projection};

    use super::fixtures::*;
    use super::*;
    use crate::config::{ComponentConfig, ComponentKindConfig, Expose};

    /// An in-process `[[component]]` of `kind`.
    fn listed(name: &str, kind: ComponentKindConfig) -> ComponentConfig {
        ComponentConfig {
            name: name.to_string(),
            language: "rust".to_string(),
            kind,
            upcasts: None,
            launch: None,
            entry: Vec::new(),
            dir: None,
            env: Default::default(),
            expose: Expose::None,
        }
    }

    /// A projector that projects nothing.
    struct Still;

    impl ProjectorHandler for Still {
        fn name(&self) -> &str {
            "left-view"
        }

        async fn dispatch(&self, _: &EventBook) -> Result<Projection, CodedError> {
            Ok(Projection::default())
        }
    }

    /// What `register` registers: each of `left` and `right` as a domain,
    /// `left-right` as a policy, `left-view` as a projector, as `with`
    /// says.
    #[derive(Clone, Copy, Default)]
    struct With {
        right: bool,
        policy: bool,
        projector: bool,
    }

    /// `components` and an in-memory store, run with `left` registered
    /// and what `with` adds.
    async fn ran(
        dir: &Path,
        components: Vec<ComponentConfig>,
        with: With,
    ) -> Result<App, HostError> {
        let mut config = HostConfig {
            components,
            ..HostConfig::default()
        };
        config.store.in_memory = true;
        run(written(dir, &config), move |registry: &mut Registry| {
            let rendezvous = || Meeting(std::sync::Arc::new(Rendezvous::of(1)));
            registry
                .command_handler("left", rendezvous(), descriptor("left"))
                .unwrap();
            if with.right {
                registry
                    .command_handler("right", rendezvous(), descriptor("right"))
                    .unwrap();
            }
            if with.policy {
                registry.policy_dispatch(PolicyDispatch::new("left-right", "left", "right"));
            }
            if with.projector {
                registry.projector_handler(&["left"], Still);
            }
        })
        .await
    }

    /// Every kind listed and registered in process runs.
    #[tokio::test(flavor = "multi_thread")]
    async fn every_listed_in_process_component_registered_runs() {
        let dir = scratch("listed");
        let mut upcaster = listed("left-upcaster", ComponentKindConfig::Upcaster);
        upcaster.upcasts = Some("left".to_string());
        let app = ran(
            &dir,
            vec![
                listed("left", ComponentKindConfig::Domain),
                listed("right", ComponentKindConfig::Domain),
                listed("left-right", ComponentKindConfig::Policy),
                listed("left-view", ComponentKindConfig::Projector),
                upcaster,
            ],
            With {
                right: true,
                policy: true,
                projector: true,
            },
        )
        .await;
        assert!(app.is_ok(), "{:?}", app.err());
    }

    /// A listed in-process component nobody registered, of each kind, is
    /// refused by its name and kind.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_listed_component_nobody_registered_is_refused() {
        let dir = scratch("unregistered");
        let left = || listed("left", ComponentKindConfig::Domain);
        for (component, kind) in [
            (listed("right", ComponentKindConfig::Domain), "domain"),
            (listed("left-right", ComponentKindConfig::Policy), "policy"),
            (
                listed("left-view", ComponentKindConfig::Projector),
                "projector",
            ),
        ] {
            let name = component.name.clone();
            assert_eq!(
                ran(&dir, vec![left(), component], With::default())
                    .await
                    .err(),
                Some(HostError::NotRegistered {
                    domain: name,
                    kind: kind.to_string(),
                }),
                "{kind}"
            );
        }
    }

    /// An in-process upcaster's domain is one registered here; one naming
    /// a domain nobody registered is refused, and one naming none is
    /// refused as an upcaster with no domain.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_in_process_upcaster_names_a_registered_domain() {
        let dir = scratch("upcaster");
        let left = || listed("left", ComponentKindConfig::Domain);
        let mut nowhere = listed("upcaster", ComponentKindConfig::Upcaster);
        nowhere.upcasts = Some("nowhere".to_string());
        assert_eq!(
            ran(&dir, vec![left(), nowhere], With::default())
                .await
                .err(),
            Some(HostError::NotRegistered {
                domain: "upcaster".to_string(),
                kind: "upcaster".to_string(),
            })
        );
        let none = listed("upcaster", ComponentKindConfig::Upcaster);
        assert_eq!(
            ran(&dir, vec![left(), none], With::default()).await.err(),
            Some(HostError::UpcasterNoDomain {
                domain: "upcaster".to_string(),
            })
        );
    }

    /// A component registered in process that no `[[component]]` lists,
    /// of each kind, is refused by its name and kind.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_registered_component_listed_nowhere_is_refused() {
        let dir = scratch("unlisted");
        assert_eq!(
            ran(&dir, Vec::new(), With::default()).await.err(),
            Some(HostError::NotListed {
                domain: "left".to_string(),
                kind: "domain".to_string(),
            })
        );
        let domains = || {
            vec![
                listed("left", ComponentKindConfig::Domain),
                listed("right", ComponentKindConfig::Domain),
            ]
        };
        assert_eq!(
            ran(
                &dir,
                domains(),
                With {
                    right: true,
                    policy: true,
                    ..With::default()
                }
            )
            .await
            .err(),
            Some(HostError::NotListed {
                domain: "left-right".to_string(),
                kind: "policy".to_string(),
            })
        );
        assert_eq!(
            ran(
                &dir,
                domains(),
                With {
                    right: true,
                    projector: true,
                    ..With::default()
                }
            )
            .await
            .err(),
            Some(HostError::NotListed {
                domain: "left-view".to_string(),
                kind: "projector".to_string(),
            })
        );
    }

    /// A policy whose name a listed domain has is listed nowhere as a
    /// policy: names are held to their kind.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_listing_of_another_kind_does_not_list_a_component() {
        let dir = scratch("same-name");
        let mut config = HostConfig {
            components: vec![
                listed("left", ComponentKindConfig::Domain),
                listed("right", ComponentKindConfig::Domain),
            ],
            ..HostConfig::default()
        };
        config.store.in_memory = true;
        let refused = run(written(&dir, &config), |registry: &mut Registry| {
            for domain in ["left", "right"] {
                registry
                    .command_handler(
                        domain,
                        Meeting(std::sync::Arc::new(Rendezvous::of(1))),
                        descriptor(domain),
                    )
                    .unwrap();
            }
            registry.policy_dispatch(PolicyDispatch::new("right", "left", "right"));
        })
        .await
        .err();
        assert_eq!(
            refused,
            Some(HostError::NotListed {
                domain: "right".to_string(),
                kind: "policy".to_string(),
            })
        );
    }

    /// A component listed as one kind and registered as another is both:
    /// refused as the listed kind nobody registered.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_component_registered_as_another_kind_is_refused() {
        let dir = scratch("other-kind");
        assert_eq!(
            ran(
                &dir,
                vec![listed("left", ComponentKindConfig::Policy)],
                With::default()
            )
            .await
            .err(),
            Some(HostError::NotRegistered {
                domain: "left".to_string(),
                kind: "policy".to_string(),
            })
        );
    }
}
