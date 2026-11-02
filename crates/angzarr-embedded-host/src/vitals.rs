//! A child's health as its supervisor judges it: the child protocol's
//! supervision (angzarr-project's `docs/design/child-protocol.md`, section
//! 8; decision 14.63).
//!
//! Every check interval the supervisor sends the child a grpc.health.v1
//! `Check` of its overall server, within a deadline ([`check`]), beside the
//! health watch it holds from the handshake on: a child wedged with its
//! watch still SERVING answers no check. Each time it sees the child's
//! health, by a check or its watch, it asks [`Vitals::seen`] what to do:
//! publish it (up), withdraw it (down: unhealthy, its calls failing
//! retryably), restart it (unhealthy for its whole grace, unbroken), or
//! give it up (so, past its component's restart limit: it stays down).
//!
//! The checks run on a [`Clock`]: tokio's, or in a test one the test moves
//! itself.

use std::time::Duration;

use tonic::transport::Channel;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;

use crate::config::Supervision;

/// What the supervisor does with a child whose health it has just seen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Healthy: its calls are sent to it.
    Up,
    /// Unhealthy, within its grace: it is sent no call.
    Down,
    /// Unhealthy for its whole grace: stop it and launch another.
    Restart,
    /// Unhealthy for its whole grace once its component has been
    /// restarted as often as its limit: stop it, and launch none.
    GiveUp,
}

/// One component's health over its children's lives: how long its
/// current child has been unhealthy, and how many children it has
/// restarted for their health.
pub(crate) struct Vitals {
    grace: Duration,
    limit: u32,
    restarts: u32,
    /// When the current child was first seen unhealthy, unbroken since;
    /// `None` while it is healthy.
    unhealthy_since: Option<Duration>,
}

impl Vitals {
    /// A component's, under `supervision`'s grace and restart limit, none
    /// of its children restarted yet.
    pub(crate) fn new(supervision: &Supervision) -> Vitals {
        Vitals {
            grace: Duration::from_millis(supervision.unhealthy_grace_ms),
            limit: supervision.unhealthy_restart_limit,
            restarts: 0,
            unhealthy_since: None,
        }
    }

    /// A child launched: healthy until it is seen otherwise. Its
    /// component's restarts are counted on.
    pub(crate) fn launched(&mut self) {
        self.unhealthy_since = None;
    }

    /// The current child's health seen `serving` (or not) at `now`: up,
    /// down, or, unhealthy since at least the grace before `now`, restarted
    /// (each restart counted) or, past the restart limit, given up.
    pub(crate) fn seen(&mut self, serving: bool, now: Duration) -> Verdict {
        if serving {
            self.unhealthy_since = None;
            return Verdict::Up;
        }
        let since = *self.unhealthy_since.get_or_insert(now);
        if now.saturating_sub(since) < self.grace {
            return Verdict::Down;
        }
        if self.restarts >= self.limit {
            return Verdict::GiveUp;
        }
        self.restarts += 1;
        Verdict::Restart
    }
}

/// What a supervisor's checks run on: tokio's clock, or in a test a clock
/// the test moves itself.
#[derive(Clone)]
pub(crate) struct Clock {
    started: tokio::time::Instant,
    #[cfg(test)]
    manual: Option<std::sync::Arc<crate::clock::ManualClock>>,
}

impl Clock {
    /// Tokio's clock, from now.
    pub(crate) fn tokio() -> Clock {
        Clock {
            started: tokio::time::Instant::now(),
            #[cfg(test)]
            manual: None,
        }
    }

    /// The clock a test moves itself.
    #[cfg(test)]
    pub(crate) fn manual(clock: std::sync::Arc<crate::clock::ManualClock>) -> Clock {
        Clock {
            manual: Some(clock),
            ..Clock::tokio()
        }
    }

    /// The time since the clock started.
    pub(crate) fn now(&self) -> Duration {
        #[cfg(test)]
        if let Some(manual) = &self.manual {
            return manual.now();
        }
        self.started.elapsed()
    }

    /// Ends once `duration` has passed on the clock.
    pub(crate) async fn sleep(&self, duration: Duration) {
        #[cfg(test)]
        if let Some(manual) = &self.manual {
            return manual.sleep(duration).await;
        }
        tokio::time::sleep(duration).await
    }
}

/// One `Check` of the overall server (service `""`) on `channel`, given
/// `deadline` on `clock`: whether it was answered SERVING. A check that
/// fails, or is not answered within its deadline, is not.
pub(crate) async fn check(channel: &Channel, deadline: Duration, clock: &Clock) -> bool {
    let mut health = HealthClient::new(channel.clone());
    tokio::select! {
        answered = health.check(HealthCheckRequest::default()) => answered
            .is_ok_and(|answer| answer.into_inner().status == ServingStatus::Serving as i32),
        _ = clock.sleep(deadline) => false,
    }
}

#[cfg(test)]
#[path = "vitals.test.rs"]
mod tests;
