//! The coordinator-contract tier, run: every scenario of `features/` that
//! is not tagged ([`tags`]) runs against angzarr-embedded's kernel, through
//! the [`world::Coordinator`] world and the step modules; one with a step
//! that matches no binding, or a step that fails, fails the run. A tagged
//! scenario is not run, and the run lists it with its tag; with
//! `SPEC_RUN_TAGGED=1` in the environment every scenario runs, tagged or
//! not, to see which tags no longer hold.

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use cucumber::writer::Stats as _;
use cucumber::World as _;

#[path = "answer.test.rs"]
mod answer;
#[path = "business.test.rs"]
mod business;
#[path = "tags.test.rs"]
mod tags;
#[path = "world.test.rs"]
mod world;

#[path = "steps/deliveries.test.rs"]
mod deliveries;
#[path = "steps/history.test.rs"]
mod history;
#[path = "steps/inputs.test.rs"]
mod inputs;
#[path = "steps/outcomes.test.rs"]
mod outcomes;
#[path = "steps/script.test.rs"]
mod script;
#[path = "steps/versions.test.rs"]
mod versions;

/// Where the vendored tier is.
const FEATURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/features");

/// Wakes the thread blocked in [`block_on`].
struct Unpark(Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Run `future` to its end on this thread, parking it while the future
/// waits: cucumber's runner needs no runtime of its own, and the kernel
/// over the in-memory store never waits on I/O.
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        thread::park();
    }
}

/// The tier, run: every untagged scenario passes.
#[test]
fn coordinator_contract() {
    let run_tagged = std::env::var_os("SPEC_RUN_TAGGED").is_some();
    let writer = block_on(
        world::Coordinator::cucumber()
            .with_default_cli()
            .before(|_, _, scenario, world| {
                world.row = tags::values(scenario);
                Box::pin(std::future::ready(()))
            })
            .fail_on_skipped()
            .filter_run(FEATURES, move |_, _, scenario| {
                run_tagged || tags::of(scenario).is_none()
            }),
    );
    print!("{}", tags::listing(FEATURES));
    assert!(
        !writer.execution_has_failed(),
        "{} scenarios failed ({} steps failed, {} skipped), {} parsing errors: see the output above",
        writer.scenarios_stats().failed,
        writer.steps_stats().failed,
        writer.steps_stats().skipped,
        writer.parsing_errors(),
    );
}
