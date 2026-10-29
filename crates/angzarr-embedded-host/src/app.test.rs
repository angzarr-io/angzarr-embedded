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
#[test]
fn run_handles_a_command_for_a_remote_domain() {
    let dir = scratch("memory");
    let mut config = player_config();
    config.store.in_memory = true;
    let app = run(written(&dir, &config), nothing).expect("the app runs");
    assert!(matches!(
        app.handle(sit_down(1), &sampled(1)),
        Ok(Handled::Applied(_))
    ));
    let events: Vec<_> = app
        .events(&player())
        .unwrap()
        .into_iter()
        .map(|p| p.payload)
        .collect();
    assert_eq!(events, [Some(noted(0))]);
    assert_eq!(
        app.shutdown(),
        vec![("player".to_string(), ChildExit::Exited(Some(0)))]
    );
}

/// `register` is called with the remote domains already registered, and
/// what it registers is the kernel's too.
#[test]
fn run_calls_register_for_the_in_process_domains() {
    let dir = scratch("register");
    let mut config = player_config();
    config.store.in_memory = true;
    let app = run(written(&dir, &config), |registry: &mut Registry| {
        assert_eq!(
            registry.command_handler("player", NoHandler),
            Err(Misconfigured::AlreadyRegistered {
                domain: "player".to_string(),
            }),
            "the remote domain is registered before register runs"
        );
    })
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
#[test]
fn the_store_is_the_configured_sqlite_file() {
    let dir = scratch("sqlite");
    let mut config = player_config();
    config.store.path = dir.join("app.db");
    let path = written(&dir, &config);
    let app = run(&path, nothing).unwrap();
    app.handle(sit_down(1), &sampled(1)).unwrap();
    drop(app);
    assert!(dir.join("app.db").exists());
    let app = run(&path, nothing).unwrap();
    app.handle(sit_down(2), &sampled(2)).unwrap();
    let events: Vec<_> = app
        .events(&player())
        .unwrap()
        .into_iter()
        .map(|p| p.payload)
        .collect();
    assert_eq!(events, [Some(noted(0)), Some(noted(1))]);
    drop(app);
}

/// The kernel the app hands out does with a policy's bug what the
/// config's `[policy] on_bug` says; the default when it is left out.
#[test]
fn the_kernel_follows_the_configs_policy_setting() {
    use angzarr_embedded::next::OnPolicyBug;
    let dir = scratch("policy");
    let path = dir.join("angzarr.toml");
    std::fs::write(&path, "[store]\nin_memory = true\n").unwrap();
    let app = run(&path, nothing).unwrap();
    assert_eq!(app.kernel().policy_bug(), OnPolicyBug::MovePast);
    drop(app);
    std::fs::write(
        &path,
        "[store]\nin_memory = true\n\n[policy]\non_bug = \"stay\"\n",
    )
    .unwrap();
    let app = run(&path, nothing).unwrap();
    assert_eq!(app.kernel().policy_bug(), OnPolicyBug::Stay);
    drop(app);
}

/// The kernel the app hands out retries a step's retryable failure as the
/// config's `[retry]` says, its defaults (the kernel's) when it is left
/// out: retrying is on with no config.
#[test]
fn the_kernel_follows_the_configs_retry_setting() {
    use angzarr_embedded::next::OnRetryable;
    use std::time::Duration;
    let dir = scratch("retry");
    let path = dir.join("angzarr.toml");
    std::fs::write(&path, "[store]\nin_memory = true\n").unwrap();
    let app = run(&path, nothing).unwrap();
    assert_eq!(app.kernel().retryable(), OnRetryable::default());
    drop(app);
    std::fs::write(
        &path,
        "[store]\nin_memory = true\n\n[retry]\nmax_retries = 5\ninitial_backoff_ms = 50\n\
         multiplier = 3.0\nmax_backoff_ms = 1000\njitter = 0.5\n",
    )
    .unwrap();
    let app = run(&path, nothing).unwrap();
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

#[test]
fn a_missing_config_is_unreadable() {
    let dir = scratch("missing");
    let path = dir.join("angzarr.toml");
    assert_eq!(
        run(&path, nothing).err(),
        Some(HostError::ConfigUnreadable {
            path: path.display().to_string(),
            kind: "NotFound".to_string(),
        })
    );
}

/// Malformed TOML and an unknown key are both invalid, at the byte the
/// problem starts.
#[test]
fn an_invalid_config_is_refused_where_it_goes_wrong() {
    let dir = scratch("invalid");
    let path = dir.join("angzarr.toml");
    std::fs::write(&path, "[store]\nin_memory = true\nlanuch = 1\n").unwrap();
    assert_eq!(
        run(&path, nothing).err(),
        Some(HostError::ConfigInvalid {
            path: path.display().to_string(),
            offset: 25,
        })
    );
    std::fs::write(&path, "[[domain]\n").unwrap();
    assert!(matches!(
        run(&path, nothing).err(),
        Some(HostError::ConfigInvalid { .. })
    ));
}

/// A store that cannot be opened fails the run, its cause the kernel's
/// store error; the children started are stopped.
#[test]
fn a_store_that_cannot_open_fails_the_run() {
    let dir = scratch("nostore");
    let mut config = player_config();
    config.store.path = dir.join("no/such/dir/app.db");
    let failed = run(written(&dir, &config), nothing).err();
    assert!(
        matches!(&failed, Some(HostError::Store { cause }) if cause != &Report::new("", "")),
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

    fn component_descriptor(
        &self,
    ) -> Result<
        angzarr_embedded::next::v1::ComponentDescriptor,
        angzarr_embedded::next::angzarr_router::error::CodedError,
    > {
        unreachable!("never registered")
    }
}
