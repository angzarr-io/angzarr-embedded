//! The tags on the scenarios the runner does not run, each naming why: the
//! one record of where angzarr-embedded and the coordinator contract part.
//!
//! - `divergence_*`: angzarr-embedded is not angzarr. The wire meaning is
//!   shared, the execution model is not, and here the difference is
//!   deliberate: the tag's `why` is the design's reason.
//! - `unsupported_*`: angzarr-embedded has no surface of the kind the
//!   scenario drives.
//! - `unbound_*`: angzarr-embedded may well satisfy it, but no step binding
//!   drives it yet.
//!
//! A tag names a scenario by its ID (`C-0558`) or one row of an outline's
//! examples, counted from 1 across its tables (`C-0558#4`).

use std::collections::BTreeMap;

use cucumber::feature::Ext as _;
use cucumber::gherkin::{Feature, GherkinEnv, Scenario};

/// Why a scenario is not run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Tag {
    /// The tag, `divergence_`, `unsupported_` or `unbound_` and the reason.
    pub(crate) name: &'static str,
    pub(crate) why: &'static str,
}

/// The chain runs in process and waits to settle. A command commits on
/// its own stream; each policy step after it is its own atomic write to
/// one stream, with the policy's position on its source, run before the
/// call returns. There is no bus, so nothing is delivered asynchronously
/// (no HandleAsync, no queued command, no command waiting for its
/// component to register), and no sync mode chooses how much of the chain a
/// caller waits for: every caller waits for all of it. A step left undone
/// (a crash, a failed store) is resumed by position (`Kernel::resume`) and
/// deduplicated by its derived command id. angzarr runs the same steps
/// asynchronously, from a bus.
const IN_PROCESS_CHAIN: Tag = Tag {
    name: "divergence_in_process_chain",
    why: "the chain runs in process and the caller waits for all of it: no bus, no asynchronous delivery, no sync modes",
};

/// No transient outbox. angzarr-embedded delivers each step in process, so
/// it writes no outbox entries; its positions say what is still owed. Its
/// command ids are the derived ones, so the audit record is identical.
const NO_OUTBOX: Tag = Tag {
    name: "divergence_no_outbox",
    why: "each step is delivered in process, so no outbox entry is written; positions say what is owed",
};

/// Causation is never accepted from outside. A book from the application,
/// or through the host's listener (`App::serve`), is a client's, and
/// angzarr-embedded has no framework component outside its process to take
/// one from; angzarr needs mTLS-identified framework components for the
/// same guarantee. (This is the command's `causation_id`; it is never
/// persisted on events, in either runtime.)
const NO_OUTSIDE_CAUSATION: Tag = Tag {
    name: "divergence_no_outside_causation",
    why: "a book carrying a causation is refused from outside: only the kernel's own policies send one",
};

/// What a caller waits for and the kernel gives up on (a command at a
/// retry or race re-run cap, a chain past its depth limit) is handed back
/// to that caller, never dead-lettered: only work with no caller left is
/// (owner ruling, 2026-10-31; `OnRetryable`).
const WAITING_CALLER_ANSWERED: Tag = Tag {
    name: "divergence_waiting_caller_answered",
    why: "what a waiting caller's call gives up on is answered to it and not dead-lettered (owner ruling 2026-10-31)",
};

/// A fact whose handler fails with a bug fails the call that sent it: it
/// is dead-lettered and its sender answered the failure, where the spec
/// answers it OK, PARKED. The kernel parks only a fact given up at the race
/// re-run cap (`Handled::Parked`).
const FAILING_FACT_FAILS: Tag = Tag {
    name: "divergence_failing_fact_answered_failed",
    why: "a fact whose handler fails is answered the failure, not OK PARKED",
};

/// No edition lifecycle. Editions are angzarr's, created by its system
/// layer (ForkStream, a fork on arrival, ListEditions, DeleteEdition);
/// angzarr-embedded creates none. An outside input to an edition stream
/// never forked is refused before its handler runs
/// (`COORDINATOR_REASON_EDITION_NOT_FORKED`), and a policy's command to one
/// by the store at the write, with nothing written.
const NO_EDITION_LIFECYCLE: Tag = Tag {
    name: "divergence_no_edition_lifecycle",
    why: "editions are angzarr's system layer's: angzarr-embedded forks, lists and deletes none",
};

/// angzarr-embedded serves no query interface (EventQueryService, a
/// TemporalQuery, a standalone query server): an application reads its
/// streams through its store, and the kernel loads them for itself.
const NO_QUERY: Tag = Tag {
    name: "unsupported_query",
    why: "no query interface: streams are read through the store",
};

/// No payload is stored by reference (a claim check), so none can fail to
/// be retrieved.
const NO_CLAIM_CHECK: Tag = Tag {
    name: "unsupported_claim_check",
    why: "no payload is stored by reference",
};

/// The host's component discovery (a child described on connect, its
/// readiness, its descriptor validated) and a component's serving status
/// drive a child process: no binding runs one here yet. The host's own
/// tests pin C-0563, C-0681 and C-0687.
const UNBOUND_HOST: Tag = Tag {
    name: "unbound_host",
    why: "the host's: a child's discovery and serving status; no binding runs a child process yet",
};

/// An in-process policy's translating callback answers a payload alone
/// (angzarr-router's `PolicyComponent::dispatch_refusal`), which the kernel
/// addresses and identifies; only a remote policy's `OnRefused` answer can
/// carry more, and no binding runs one here yet.
const UNBOUND_REMOTE_POLICY: Tag = Tag {
    name: "unbound_remote_policy",
    why: "an in-process callback answers a payload alone: only a remote policy's answer can be more, and no binding runs one",
};

/// A policy answers commands alone (`PolicyResponse.reactions`): it emits
/// no event.
const POLICY_EMITS_NO_EVENT: Tag = Tag {
    name: "unsupported_policy_event",
    why: "a policy answers commands alone (PolicyResponse): it emits no event",
};

/// A failing translating callback fails its policy step, answered to the
/// call the chain runs in, and is not dead-lettered; a policy's own
/// failure on an event is dead-lettered as well.
const FAILING_CALLBACK_ANSWERED: Tag = Tag {
    name: "divergence_failing_callback_answered",
    why: "a failing translating callback is answered to the caller and not dead-lettered",
};

/// The fold is business code's: angzarr-router's, which runs the
/// handler-contract tier for it. A binding here needs a typed domain of
/// its own; the kernel's part, the history it delivers, is C-0287's.
const UNBOUND_FOLD: Tag = Tag {
    name: "unbound_router_fold",
    why: "the fold is business code's (angzarr-router's); no typed domain binds it here",
};

/// A policy's handler that fails is dead-lettered on its first failure,
/// whatever its status: the kernel retries a step's delivery, never the
/// policy's own call. The spec retries a retryable failure up to the retry
/// cap first.
const POLICY_FAILURE_NOT_RETRIED: Tag = Tag {
    name: "divergence_policy_failure_not_retried",
    why: "a failing policy handler is dead-lettered at once, never retried, whatever its status",
};

/// No step binding drives the scenario yet.
const UNBOUND: Tag = Tag {
    name: "unbound",
    why: "no step binding drives it yet",
};

/// Every tagged scenario, by ID or outline row, in the tier's file order.
const TAGGED: &[(&str, Tag)] = &[
    // component_discovery.feature
    ("C-0561", UNBOUND_HOST),
    ("C-0562", UNBOUND_HOST),
    ("C-0563", UNBOUND_HOST),
    ("C-0564", UNBOUND_HOST),
    ("C-0687", UNBOUND_HOST),
    ("C-0566", UNBOUND_HOST),
    ("C-0567", UNBOUND_HOST),
    // conflict_resolution.feature
    ("C-0729", IN_PROCESS_CHAIN),
    ("C-0724", WAITING_CALLER_ANSWERED),
    ("C-0725", WAITING_CALLER_ANSWERED),
    ("C-0459", UNBOUND),
    ("C-0460", UNBOUND),
    // dead_letter_queue.feature
    ("C-0443", IN_PROCESS_CHAIN),
    ("C-0738", IN_PROCESS_CHAIN),
    ("C-0744", IN_PROCESS_CHAIN),
    ("C-0739", IN_PROCESS_CHAIN),
    ("C-0444", POLICY_FAILURE_NOT_RETRIED),
    ("C-0445", UNBOUND),
    ("C-0447", NO_CLAIM_CHECK),
    ("C-0730", IN_PROCESS_CHAIN),
    ("C-0731", IN_PROCESS_CHAIN),
    ("C-0732", IN_PROCESS_CHAIN),
    ("C-0733", IN_PROCESS_CHAIN),
    // edition_forks.feature
    ("C-0628", NO_EDITION_LIFECYCLE),
    ("C-0629", NO_EDITION_LIFECYCLE),
    ("C-0630", NO_EDITION_LIFECYCLE),
    ("C-0631", NO_EDITION_LIFECYCLE),
    ("C-0634", NO_QUERY),
    ("C-0635", NO_EDITION_LIFECYCLE),
    ("C-0636", NO_EDITION_LIFECYCLE),
    ("C-0637", NO_EDITION_LIFECYCLE),
    ("C-0640", NO_EDITION_LIFECYCLE),
    ("C-0641", NO_EDITION_LIFECYCLE),
    ("C-0642", NO_EDITION_LIFECYCLE),
    ("C-0643", NO_EDITION_LIFECYCLE),
    ("C-0639", NO_EDITION_LIFECYCLE),
    ("C-0647", NO_EDITION_LIFECYCLE),
    ("C-0644", NO_EDITION_LIFECYCLE),
    ("C-0645", NO_EDITION_LIFECYCLE),
    ("C-0646", NO_EDITION_LIFECYCLE),
    ("C-0652", NO_EDITION_LIFECYCLE),
    ("C-0653", NO_EDITION_LIFECYCLE),
    ("C-0654", NO_EDITION_LIFECYCLE),
    ("C-0655", NO_EDITION_LIFECYCLE),
    ("C-0661", NO_EDITION_LIFECYCLE),
    ("C-0662", NO_EDITION_LIFECYCLE),
    ("C-0663", NO_EDITION_LIFECYCLE),
    // edition_propagation.feature
    ("C-0139", POLICY_EMITS_NO_EVENT),
    ("C-0410", NO_EDITION_LIFECYCLE),
    // fact_flow.feature
    ("C-0609", NO_OUTSIDE_CAUSATION),
    ("C-0610", NO_OUTSIDE_CAUSATION),
    // policy_addressing.feature
    ("C-0735", NO_OUTSIDE_CAUSATION),
    ("C-0737", WAITING_CALLER_ANSWERED),
    // policy_refusal.feature
    ("C-0616", NO_OUTBOX),
    ("C-0619", FAILING_CALLBACK_ANSWERED),
    ("C-0621", UNBOUND_REMOTE_POLICY),
    ("C-0685", UNBOUND_REMOTE_POLICY),
    // process_manager_delivery.feature
    ("C-0599", FAILING_FACT_FAILS),
    // query_server.feature
    ("C-0588", NO_QUERY),
    ("C-0589", NO_QUERY),
    // rules_version.feature
    ("C-0554", NO_QUERY),
    ("C-0555", NO_QUERY),
    ("C-0556", NO_QUERY),
    ("C-0560", NO_QUERY),
    ("C-0558#4", IN_PROCESS_CHAIN),
    // state_building.feature
    ("C-0281", UNBOUND_FOLD),
    ("C-0282", UNBOUND_FOLD),
    ("C-0283", UNBOUND_FOLD),
    ("C-0284", UNBOUND_FOLD),
    ("C-0285", UNBOUND_FOLD),
    ("C-0286", UNBOUND_FOLD),
    ("C-0288", UNBOUND_FOLD),
    ("C-0289", UNBOUND_FOLD),
    ("C-0290", UNBOUND_FOLD),
    ("C-0291", UNBOUND_FOLD),
    ("C-0292", UNBOUND_FOLD),
    ("C-0293", UNBOUND_FOLD),
    ("C-0294", UNBOUND_FOLD),
    ("C-0295", UNBOUND_FOLD),
    ("C-0296", UNBOUND_FOLD),
    ("C-0297", UNBOUND_FOLD),
    ("C-0298", UNBOUND_FOLD),
    ("C-0299", UNBOUND_FOLD),
    ("C-0300", UNBOUND_FOLD),
    ("C-0301", UNBOUND_FOLD),
    ("C-0302", UNBOUND_FOLD),
    // sync_modes.feature
    ("C-0425", IN_PROCESS_CHAIN),
    ("C-0426", IN_PROCESS_CHAIN),
    ("C-0427", IN_PROCESS_CHAIN),
    ("C-0428", IN_PROCESS_CHAIN),
    ("C-0429", IN_PROCESS_CHAIN),
    ("C-0676", IN_PROCESS_CHAIN),
    ("C-0677", IN_PROCESS_CHAIN),
    // temporal_query.feature
    ("C-0402", NO_QUERY),
    ("C-0403", NO_QUERY),
    ("C-0404", NO_QUERY),
    ("C-0405", NO_QUERY),
    ("C-0406", NO_QUERY),
    ("C-0407", NO_QUERY),
    ("C-0408", NO_QUERY),
    // upcasting.feature
    ("C-0585", NO_QUERY),
    ("C-0586", UNBOUND),
    ("C-0681", UNBOUND_HOST),
];

/// The scenario's ID, its one `@C-NNNN` tag.
pub(crate) fn id(scenario: &Scenario) -> Option<&str> {
    let mut ids = scenario
        .tags
        .iter()
        .filter(|tag| tag.starts_with("C-"))
        .map(String::as_str);
    let id = ids.next();
    match ids.next() {
        None => id,
        Some(_) => None,
    }
}

/// An expanded outline row: its number, counted from 1 across the
/// outline's examples' tables, and its values by their header.
struct Row {
    number: usize,
    values: BTreeMap<String, String>,
}

/// The outline row `scenario` was expanded from, if it was.
fn row(scenario: &Scenario) -> Option<Row> {
    let mut first = 1;
    for examples in &scenario.examples {
        let rows = examples.table.as_ref().map_or(&[][..], |t| &t.rows[..]);
        let Some((header, values)) = rows.split_first() else {
            continue;
        };
        // An expanded row sits at its table's line plus its index plus two.
        let offset = scenario
            .position
            .line
            .checked_sub(examples.position.line + 2);
        if let Some(index) = offset.filter(|i| *i < values.len()) {
            let values = header
                .iter()
                .cloned()
                .zip(values[index].iter().cloned())
                .collect();
            return Some(Row {
                number: first + index,
                values,
            });
        }
        first += values.len();
    }
    None
}

/// The scenario's outline row's values by their header: empty for a
/// scenario that is no outline's row. cucumber fills in an outline's
/// placeholders, but not one with a space in its name (`<snapshot
/// version>`): a step resolves those from these.
pub(crate) fn values(scenario: &Scenario) -> BTreeMap<String, String> {
    row(scenario).map(|row| row.values).unwrap_or_default()
}

/// The scenario's key: its ID, then, for an outline's row, `#` and the row
/// counted from 1 across its examples' tables.
pub(crate) fn key(scenario: &Scenario) -> Option<String> {
    let id = id(scenario)?;
    Some(match row(scenario) {
        Some(row) => format!("{id}#{}", row.number),
        None => id.to_string(),
    })
}

/// The tag on `scenario`, by its row's key, else its ID.
pub(crate) fn of(scenario: &Scenario) -> Option<Tag> {
    let key = key(scenario)?;
    let id = id(scenario)?;
    let tagged = |wanted: &str| {
        TAGGED
            .iter()
            .find(|(k, _)| *k == wanted)
            .map(|(_, tag)| *tag)
    };
    tagged(&key).or_else(|| tagged(id))
}

/// Every scenario of the tier, its outlines expanded, in file order.
fn scenarios(features: &str) -> Vec<Scenario> {
    let mut paths: Vec<_> = std::fs::read_dir(features)
        .expect("the vendored tier")
        .map(|entry| entry.expect("a directory entry").path())
        .filter(|path| path.extension().is_some_and(|e| e == "feature"))
        .collect();
    paths.sort();
    paths
        .iter()
        .map(|path| {
            Feature::parse_path(path, GherkinEnv::default())
                .and_then(|f| f.expand_examples().map_err(|e| panic!("{e}")))
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()))
        })
        .flat_map(|feature| {
            let rules = feature.rules.into_iter().flat_map(|r| r.scenarios);
            feature
                .scenarios
                .into_iter()
                .chain(rules)
                .collect::<Vec<_>>()
        })
        .collect()
}

/// What the run left out, by tag, and how many it ran.
pub(crate) fn listing(features: &str) -> String {
    let all = scenarios(features);
    let mut by_tag: BTreeMap<&str, (Tag, Vec<String>)> = BTreeMap::new();
    for scenario in &all {
        if let Some(tag) = of(scenario) {
            let key = key(scenario).unwrap_or_default();
            by_tag
                .entry(tag.name)
                .or_insert((tag, Vec::new()))
                .1
                .push(key);
        }
    }
    let tagged: usize = by_tag.values().map(|(_, keys)| keys.len()).sum();
    let mut out = format!(
        "coordinator-contract: {} scenarios (outline rows counted), {} bound, {} tagged\n",
        all.len(),
        all.len() - tagged,
        tagged
    );
    for (name, (tag, keys)) in by_tag {
        out += &format!(
            "  @{name} ({}): {}\n    {}\n",
            keys.len(),
            tag.why,
            keys.join(" ")
        );
    }
    out
}

/// Every scenario carries one ID.
#[test]
fn every_scenario_has_one_id() {
    for scenario in scenarios(super::FEATURES) {
        assert!(
            id(&scenario).is_some(),
            "{}: {:?}",
            scenario.name,
            scenario.tags
        );
    }
}

/// Every tag names a scenario, or one of its rows, of the tier, once.
#[test]
fn every_tag_names_a_scenario_once() {
    let all = scenarios(super::FEATURES);
    let keys: Vec<String> = all.iter().filter_map(key).collect();
    let ids: Vec<&str> = all.iter().filter_map(id).collect();
    for (n, (wanted, _)) in TAGGED.iter().enumerate() {
        assert!(
            ids.contains(wanted) || keys.iter().any(|k| k == wanted),
            "{wanted} is no scenario of the tier"
        );
        assert!(
            !TAGGED[..n].iter().any(|(k, _)| k == wanted),
            "{wanted} is tagged twice"
        );
    }
}

/// Every tag is a divergence, an unsupported surface or unbound.
#[test]
fn every_tag_is_of_a_kind() {
    for (key, tag) in TAGGED {
        assert!(
            ["divergence_", "unsupported_", "unbound"]
                .iter()
                .any(|kind| tag.name.starts_with(kind)),
            "{key}: {}",
            tag.name
        );
    }
}
