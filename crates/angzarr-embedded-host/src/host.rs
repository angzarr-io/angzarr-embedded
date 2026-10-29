//! The host: launches every remote domain of a [`HostConfig`], waits for its
//! handshake, connects, describes it, and hands back a
//! [`RemoteCommandHandler`] per domain to register with the application's
//! registry beside its in-process domains. Then it supervises: a child that
//! exits is restarted, after a backoff that grows while restarts fail; while
//! it is down its handler's dispatch is retryable
//! ([`crate::is_retryable`]).
//!
//! The generated `main.rs` is, in outline:
//!
//! ```ignore
//! let config: HostConfig = toml::from_str(include_str!("angzarr.toml"))?;
//! let host = Host::start(config)?;
//! let mut registry = Registry::new().domain(table::Table);
//! for handler in host.handlers() {
//!     registry = registry.handler(Box::new(handler));
//! }
//! // ... run the application; dropping `host` stops the children.
//! ```
//!
//! The host runs its own tokio runtime, on its own threads, so it works the
//! same under any caller, async or not.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use angzarr_router::pb;
use tokio::io::BufReader;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::oneshot;

use crate::config::{HostConfig, Launch, Placement, Supervision};
use crate::error::HostError;
use crate::handshake::{read_handshake, SOCKET_ENV};
use crate::remote::{Link, RemoteCommandHandler};
use crate::rpc::command_handler::command_handler_service_client::CommandHandlerServiceClient;
use crate::rpc::component::component_service_client::ComponentServiceClient;
use crate::rpc::connect_unix;

/// How a child ended at shutdown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChildExit {
    /// It exited by itself once its stdin was closed: its exit code, `None`
    /// when a signal ended it.
    Exited(Option<i32>),
    /// It was still running when the grace ran out, and was killed.
    Killed,
    /// No child was running (it had exited and was waiting to be
    /// restarted).
    NotRunning,
}

/// The running host: its children and their handlers. Dropping it shuts
/// them down as [`Host::shutdown`] does.
pub struct Host {
    runtime: Option<tokio::runtime::Runtime>,
    domains: Vec<Supervised>,
}

/// One remote domain under supervision.
struct Supervised {
    handler: RemoteCommandHandler,
    /// Tells the supervisor to stop the child; taken when sent.
    stop: Option<oneshot::Sender<()>>,
    /// How the child ended, once the supervisor has stopped it.
    ended: mpsc::Receiver<ChildExit>,
}

impl Host {
    /// Starts every remote domain of `config` and waits for each to come
    /// up: launched, handshaken, connected, and described. In-process domains are left to the caller. Refused, with every
    /// child already started stopped again, when any domain's config is
    /// refused or any child fails to come up the first time.
    pub fn start(config: HostConfig) -> Result<Host, HostError> {
        let mut names = BTreeSet::new();
        let mut remote = Vec::new();
        for domain in &config.domains {
            if !names.insert(domain.name.as_str()) {
                return Err(HostError::DuplicateDomain {
                    domain: domain.name.clone(),
                });
            }
            if let Placement::Remote(launch) = domain.placement()? {
                remote.push((domain.name.clone(), launch));
            }
        }
        let socket_dir = socket_dir(
            config.supervision.socket_dir.as_deref(),
            std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from),
        )?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("angzarr-host")
            .enable_all()
            .build()
            .map_err(|e| HostError::Runtime {
                kind: format!("{:?}", e.kind()),
            })?;
        let mut host = Host {
            runtime: None,
            domains: Vec::new(),
        };
        for (domain, launch) in remote {
            let link = Arc::new(Link::default());
            let (stop, stopped) = oneshot::channel();
            let (up, came_up) = mpsc::channel();
            let (end, ended) = mpsc::channel();
            let supervisor = Supervisor {
                domain: domain.clone(),
                launch,
                supervision: config.supervision.clone(),
                socket_dir: socket_dir.clone(),
                link: link.clone(),
            };
            runtime.spawn(async move {
                let _ = end.send(supervisor.run(stopped, up).await);
            });
            let error = match came_up.recv() {
                Ok(Ok(descriptor)) => {
                    host.domains.push(Supervised {
                        handler: RemoteCommandHandler::new(
                            domain,
                            descriptor,
                            link,
                            runtime.handle().clone(),
                        ),
                        stop: Some(stop),
                        ended,
                    });
                    continue;
                }
                Ok(Err(error)) => error,
                // The supervisor ended without a word: only the runtime
                // going away does that.
                Err(_) => HostError::Runtime {
                    kind: "Interrupted".to_string(),
                },
            };
            // Its supervisor has stopped its child already; stop the rest.
            let _ = ended.recv();
            host.runtime = Some(runtime);
            host.stop_all();
            return Err(error);
        }
        host.runtime = Some(runtime);
        Ok(host)
    }

    /// One handler per remote domain, in the config's order.
    pub fn handlers(&self) -> Vec<RemoteCommandHandler> {
        self.domains.iter().map(|d| d.handler.clone()).collect()
    }

    /// The handler of the remote domain named `domain`.
    pub fn handler(&self, domain: &str) -> Option<RemoteCommandHandler> {
        self.domains
            .iter()
            .find(|d| d.handler.domain() == domain)
            .map(|d| d.handler.clone())
    }

    /// Stops supervising and stops every child: closes its stdin, waits up
    /// to the shutdown grace for it to exit, and kills it if it has not.
    /// How each ended, by domain, in the config's order.
    pub fn shutdown(mut self) -> Vec<(String, ChildExit)> {
        self.stop_all()
    }

    /// Stops every child, all at once, and then the runtime.
    fn stop_all(&mut self) -> Vec<(String, ChildExit)> {
        for domain in &mut self.domains {
            if let Some(stop) = domain.stop.take() {
                let _ = stop.send(());
            }
        }
        let exits = self
            .domains
            .drain(..)
            .map(|d| {
                let exit = d.ended.recv().unwrap_or(ChildExit::NotRunning);
                (d.handler.domain().to_string(), exit)
            })
            .collect();
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
        exits
    }
}

impl Drop for Host {
    /// Stops the children, as [`Host::shutdown`] does.
    fn drop(&mut self) {
        self.stop_all();
    }
}

/// The wait after `backoff` when a restart fails again: twice as long, up
/// to `max`.
fn next_backoff(backoff: Duration, max: Duration) -> Duration {
    (backoff * 2).min(max)
}

/// The longest Unix socket path, in bytes: `sun_path`'s 108, less the NUL.
pub const MAX_SOCKET_PATH: usize = 107;

/// The longest name [`socket_path`] gives, with its leading `/`: a pid
/// (at most 7 digits on Linux) and a counter (at most 20).
const MAX_SOCKET_NAME: usize = "/angzarr--.sock".len() + 7 + 20;

/// Where the host puts its sockets: `configured`, refused when too long to
/// hold the socket names; else `xdg_runtime_dir` when it is absolute and
/// short enough; else `/tmp`.
pub(crate) fn socket_dir(
    configured: Option<&Path>,
    xdg_runtime_dir: Option<PathBuf>,
) -> Result<PathBuf, HostError> {
    let max = MAX_SOCKET_PATH - MAX_SOCKET_NAME;
    let fits = |dir: &Path| dir.as_os_str().len() <= max;
    match configured {
        Some(dir) if fits(dir) => Ok(dir.to_path_buf()),
        Some(dir) => Err(HostError::SocketDirTooLong {
            dir: dir.display().to_string(),
            max_bytes: max as u64,
        }),
        None => Ok(xdg_runtime_dir
            .filter(|dir| dir.is_absolute() && fits(dir))
            .unwrap_or_else(|| PathBuf::from("/tmp"))),
    }
}

/// A fresh socket path in `dir`: the host's pid and a counter, kept short
/// (at most [`MAX_SOCKET_NAME`] bytes after `dir`).
fn socket_path(dir: &Path) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    dir.join(format!("angzarr-{}-{n}.sock", std::process::id()))
}

/// A child that came up: handshaken, connected and described.
struct Up {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: Option<BufReader<ChildStdout>>,
    client: CommandHandlerServiceClient<tonic::transport::Channel>,
    descriptor: pb::ComponentDescriptor,
    socket: PathBuf,
}

/// One remote domain's supervisor: it starts the child, restarts it when it
/// exits, and stops it when told.
struct Supervisor {
    domain: String,
    launch: Launch,
    supervision: Supervision,
    socket_dir: PathBuf,
    link: Arc<Link>,
}

impl Supervisor {
    /// Supervises until `stop`; how the child ended. The first start's
    /// outcome goes to `up`; a first start that fails ends supervision.
    async fn run(
        self,
        mut stop: oneshot::Receiver<()>,
        up: mpsc::Sender<Result<pb::ComponentDescriptor, HostError>>,
    ) -> ChildExit {
        let mut up = Some(up);
        let initial = Duration::from_millis(self.supervision.initial_backoff_ms);
        let max = Duration::from_millis(self.supervision.max_backoff_ms);
        let mut backoff = initial;
        loop {
            let started = tokio::select! {
                started = self.come_up() => started,
                _ = &mut stop => return ChildExit::NotRunning,
            };
            match started {
                Ok(mut child) => {
                    if let Some(up) = up.take() {
                        let _ = up.send(Ok(child.descriptor.clone()));
                    }
                    backoff = initial;
                    self.set_client(Some(child.client.clone()));
                    // The child's stdout after its handshake is its own
                    // output: passed on to the host's stderr, and drained so
                    // the child never blocks on a full pipe.
                    if let Some(mut stdout) = child.stdout.take() {
                        tokio::spawn(async move {
                            let _ = tokio::io::copy(&mut stdout, &mut tokio::io::stderr()).await;
                        });
                    }
                    tokio::select! {
                        _ = child.child.wait() => {
                            self.set_client(None);
                            let _ = std::fs::remove_file(&child.socket);
                        }
                        _ = &mut stop => {
                            self.set_client(None);
                            let exit = self.stop_child(&mut child).await;
                            let _ = std::fs::remove_file(&child.socket);
                            return exit;
                        }
                    }
                }
                Err(error) => {
                    if let Some(up) = up.take() {
                        let _ = up.send(Err(error));
                        return ChildExit::NotRunning;
                    }
                    backoff = next_backoff(backoff, max);
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = &mut stop => return ChildExit::NotRunning,
            }
        }
    }

    fn set_client(&self, client: Option<CommandHandlerServiceClient<tonic::transport::Channel>>) {
        *self
            .link
            .client
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = client;
    }

    /// Closes the child's stdin, gives it the grace to exit, and kills it
    /// if it has not.
    async fn stop_child(&self, up: &mut Up) -> ChildExit {
        drop(up.stdin.take());
        let grace = Duration::from_millis(self.supervision.shutdown_grace_ms);
        match tokio::time::timeout(grace, up.child.wait()).await {
            Ok(Ok(status)) => ChildExit::Exited(status.code()),
            _ => {
                let _ = up.child.kill().await;
                ChildExit::Killed
            }
        }
    }

    /// Launches the child and brings it up. On a failure the child, if one
    /// started, is killed and reaped before the error is returned.
    async fn come_up(&self) -> Result<Up, HostError> {
        let domain = || self.domain.clone();
        let socket = socket_path(&self.socket_dir);
        let _ = std::fs::remove_file(&socket);
        let mut command = Command::new(&self.launch.program);
        command
            .args(&self.launch.args)
            .envs(&self.launch.env)
            .env(SOCKET_ENV, &socket)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        if let Some(dir) = &self.launch.dir {
            command.current_dir(dir);
        }
        let mut child = command.spawn().map_err(|e| HostError::Spawn {
            domain: domain(),
            program: self.launch.program.clone(),
            kind: format!("{:?}", e.kind()),
        })?;
        let stdin = child.stdin.take();
        let mut stdout = BufReader::new(child.stdout.take().expect("stdout is piped"));
        let timeout = Duration::from_millis(self.supervision.handshake_timeout_ms);
        let reached = async {
            let handshake = read_handshake(&mut stdout, timeout)
                .await
                .map_err(|cause| HostError::Handshake {
                    domain: domain(),
                    cause,
                })?;
            let channel =
                connect_unix(handshake.socket.clone())
                    .await
                    .map_err(|_| HostError::Connect {
                        domain: domain(),
                        socket: handshake.socket.display().to_string(),
                    })?;
            let descriptor = ComponentServiceClient::new(channel.clone())
                .describe(())
                .await
                .map_err(|status| HostError::Describe {
                    domain: domain(),
                    grpc_code: status.code() as i32,
                })?
                .into_inner();
            Ok((channel, descriptor, handshake.socket))
        }
        .await;
        match reached {
            Ok((channel, descriptor, socket)) => Ok(Up {
                child,
                stdin,
                stdout: Some(stdout),
                client: CommandHandlerServiceClient::new(channel),
                descriptor,
                socket,
            }),
            Err(error) => {
                let _ = child.kill().await;
                let _ = std::fs::remove_file(&socket);
                Err(error)
            }
        }
    }
}

#[cfg(test)]
#[path = "host.test.rs"]
mod tests;
