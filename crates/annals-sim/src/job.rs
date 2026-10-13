//! Scheduled work as data.
//!
//! Whatever a simulation does later — a shot landing, a unit arriving — is a
//! **job**: a mechanic's own type with its own logic, recorded in events as
//! plain data, `(kind, Value)`. A [`Registry`] knows every kind and finds a
//! job's type by its kind when it comes due. There is no central list of
//! jobs: each mechanic registers its own.
//!
//! Jobs run against a context the domain chooses through [`JobContext`].

use std::fmt;

use crate::record::Value;

/// What one domain's jobs run against.
///
/// `Cx` takes a lifetime so it can borrow. A domain's jobs usually run on its
/// transaction — `type Cx<'a> = annals::Tx<'a, D>` — and a domain with its own
/// wrapper around the `Tx` builds it inside [`Job::run`].
pub trait JobContext: 'static {
    /// What a job acts through.
    type Cx<'a>;
    /// Where and as whom a job runs, kept beside it on the clock.
    type At;
}

/// A mechanic's scheduled work.
pub trait Job<K: JobContext>: Sized + 'static {
    /// Stable, unique, never reused: the job's id in every log.
    const KIND: &'static str;

    fn encode(&self) -> Value;
    fn decode(v: &Value) -> Option<Self>;
    fn run(self, cx: &mut K::Cx<'_>, at: K::At);

    /// A representative value, for the round-trip check every kind must pass.
    fn sample() -> Self;
}

/// A job as an event carries it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobRecord {
    pub kind: String,
    pub data: Value,
}

impl JobRecord {
    pub fn of<K: JobContext, J: Job<K>>(job: &J) -> JobRecord {
        JobRecord {
            kind: J::KIND.to_string(),
            data: job.encode(),
        }
    }
}

type RunFn<K> =
    for<'a, 'b> fn(&Value, &'b mut <K as JobContext>::Cx<'a>, <K as JobContext>::At) -> bool;

struct Kind<K: JobContext> {
    name: &'static str,
    run: RunFn<K>,
    round_trips: fn() -> bool,
}

/// Every job kind of one domain, by id. Usually held in the domain's `Rules`.
pub struct Registry<K: JobContext> {
    kinds: Vec<Kind<K>>,
}

impl<K: JobContext> Default for Registry<K> {
    fn default() -> Self {
        Registry { kinds: Vec::new() }
    }
}

fn run_as<K: JobContext, J: Job<K>>(data: &Value, cx: &mut K::Cx<'_>, at: K::At) -> bool {
    match J::decode(data) {
        Some(job) => {
            job.run(cx, at);
            true
        }
        None => false,
    }
}

fn round_trips<K: JobContext, J: Job<K> + PartialEq>() -> bool {
    let sample = J::sample();
    J::decode(&sample.encode()).as_ref() == Some(&sample)
}

impl<K: JobContext> Registry<K> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a job kind. A kind registered twice is a bug, and panics.
    pub fn register<J: Job<K> + PartialEq>(mut self) -> Self {
        assert!(
            self.kinds.iter().all(|k| k.name != J::KIND),
            "job kind {:?} registered twice",
            J::KIND
        );
        self.kinds.push(Kind {
            name: J::KIND,
            run: run_as::<K, J>,
            round_trips: round_trips::<K, J>,
        });
        self
    }

    /// Run a job that has come due. An unknown kind or undecodable data
    /// panics, naming the kind: a log that cannot be run must say why.
    pub fn run(&self, job: &JobRecord, cx: &mut K::Cx<'_>, at: K::At) {
        let kind = self
            .kinds
            .iter()
            .find(|k| k.name == job.kind)
            .unwrap_or_else(|| panic!("unknown job kind {:?}", job.kind));
        if !(kind.run)(&job.data, cx, at) {
            panic!("job {:?} could not decode {:?}", job.kind, job.data);
        }
    }

    /// Every registered kind, in registration order.
    pub fn kinds(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.kinds.iter().map(|k| k.name)
    }

    /// Every registered kind whose sample does not survive encode, then
    /// decode. A test asserts this is empty.
    pub fn failing_round_trips(&self) -> Vec<&'static str> {
        self.kinds
            .iter()
            .filter(|k| !(k.round_trips)())
            .map(|k| k.name)
            .collect()
    }
}

impl<K: JobContext> fmt::Debug for Registry<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.kinds()).finish()
    }
}
