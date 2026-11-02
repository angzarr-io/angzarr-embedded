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

/// A command whose caller waits for its answer, given up at a cap, is
/// handed back to that caller, never dead-lettered: only work with no caller
/// left is (owner ruling, 2026-10-31; `OnRetryable`).
const WAITING_CALLER_ANSWERED: Tag = Tag {
    name: "divergence_waiting_caller_answered",
    why: "a command given up at a cap is answered to its waiting caller and not dead-lettered (owner ruling 2026-10-31)",
};

/// A COMMUTATIVE command at the head has no window, so the kernel hands it
/// to business code as STRICT, with no nonce: there is nothing for the
/// resolver to compare (`coordinator::contextual_command`). The spec
/// delivers it COMMUTATIVE with a nonce, so the handler's path is uniform.
const COMMUTATIVE_AT_HEAD: Tag = Tag {
    name: "divergence_commutative_at_head_as_strict",
    why: "a COMMUTATIVE command at the head is delivered as STRICT, with no nonce",
};

/// No edition lifecycle. Editions are angzarr's, created by its system
/// layer (ForkStream, a fork on arrival, ListEditions, DeleteEdition);
/// angzarr-embedded creates none. A command to an edition stream never
/// forked is refused by the store at the write (`EDITION_NOT_FORKED`),
/// with nothing written.
const NO_EDITION_LIFECYCLE: Tag = Tag {
    name: "divergence_no_edition_lifecycle",
    why: "editions are angzarr's system layer's: angzarr-embedded forks, lists and deletes none",
};

/// Configuration lives on the domain (`CONFLICT_RESOLUTION`,
/// `CONFLICT_RESOLUTION_BY_COMMAND`, the descriptor), checked at
/// registration: there is no coordinator configuration to load.
const CONFIGURATION_ON_DOMAIN: Tag = Tag {
    name: "divergence_configuration_on_domain",
    why: "configuration is the domain's, checked at registration: there is no coordinator configuration to load",
};

/// angzarr-embedded serves no query interface (EventQueryService, a
/// TemporalQuery, a standalone query server): an application reads its
/// streams through its store, and the kernel loads them for itself.
const NO_QUERY: Tag = Tag {
    name: "unsupported_query",
    why: "no query interface: streams are read through the store",
};

/// The snapshot compatibility rule is the default one only: no domain sets
/// an accept or reject pattern.
const NO_SNAPSHOT_PATTERNS: Tag = Tag {
    name: "unsupported_snapshot_version_patterns",
    why: "only the default major.minor rule judges a snapshot: no accept or reject pattern",
};

/// No payload is stored by reference (a claim check), so none can fail to
/// be retrieved.
const NO_CLAIM_CHECK: Tag = Tag {
    name: "unsupported_claim_check",
    why: "no payload is stored by reference",
};

/// The host's component discovery (a child described on connect, its
/// readiness, its descriptor validated) drives a child process: no binding
/// runs one here yet. The host's own tests pin C-0563 and C-0687.
const UNBOUND_HOST: Tag = Tag {
    name: "unbound_host",
    why: "the host's child discovery: no binding runs a child process yet",
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
    ("C-0149", COMMUTATIVE_AT_HEAD),
    ("C-0724", WAITING_CALLER_ANSWERED),
    ("C-0725", WAITING_CALLER_ANSWERED),
    ("C-0742", UNBOUND),
    ("C-0458", UNBOUND),
    ("C-0552", UNBOUND),
    ("C-0459", UNBOUND),
    ("C-0460", UNBOUND),
    // dead_letter_queue.feature
    ("C-0443", IN_PROCESS_CHAIN),
    ("C-0738", IN_PROCESS_CHAIN),
    ("C-0744", IN_PROCESS_CHAIN),
    ("C-0739", IN_PROCESS_CHAIN),
    ("C-0444", UNBOUND),
    ("C-0445", UNBOUND),
    ("C-0446", UNBOUND),
    ("C-0447", NO_CLAIM_CHECK),
    ("C-0449", UNBOUND),
    ("C-0673", UNBOUND),
    ("C-0730", IN_PROCESS_CHAIN),
    ("C-0731", IN_PROCESS_CHAIN),
    ("C-0732", IN_PROCESS_CHAIN),
    ("C-0733", IN_PROCESS_CHAIN),
    // edition_forks.feature
    ("C-0628", NO_EDITION_LIFECYCLE),
    ("C-0629", NO_EDITION_LIFECYCLE),
    ("C-0630", NO_EDITION_LIFECYCLE),
    ("C-0631", NO_EDITION_LIFECYCLE),
    ("C-0633", UNBOUND),
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
    ("C-0138", UNBOUND),
    ("C-0139", UNBOUND),
    ("C-0140", UNBOUND),
    ("C-0141", UNBOUND),
    ("C-0144", UNBOUND),
    ("C-0590", UNBOUND),
    ("C-0608", UNBOUND),
    ("C-0409", UNBOUND),
    ("C-0410", NO_EDITION_LIFECYCLE),
    // fact_flow.feature
    ("C-0185", UNBOUND),
    ("C-0186", UNBOUND),
    ("C-0591", UNBOUND),
    ("C-0609", NO_OUTSIDE_CAUSATION),
    ("C-0610", NO_OUTSIDE_CAUSATION),
    ("C-0492", UNBOUND),
    ("C-0190", UNBOUND),
    // policy_addressing.feature
    ("C-0656", UNBOUND),
    ("C-0657", UNBOUND),
    ("C-0658", UNBOUND),
    ("C-0659", UNBOUND),
    ("C-0660", UNBOUND),
    ("C-0666", UNBOUND),
    ("C-0583", UNBOUND),
    ("C-0734", UNBOUND),
    ("C-0735", NO_OUTSIDE_CAUSATION),
    ("C-0736", UNBOUND),
    ("C-0737", UNBOUND),
    // policy_refusal.feature
    ("C-0613", UNBOUND),
    ("C-0614", UNBOUND),
    ("C-0615", UNBOUND),
    ("C-0616", NO_OUTBOX),
    ("C-0617", UNBOUND),
    ("C-0618", UNBOUND),
    ("C-0619", UNBOUND),
    ("C-0621", UNBOUND),
    ("C-0622", UNBOUND),
    ("C-0624", UNBOUND),
    ("C-0674", UNBOUND),
    ("C-0685", UNBOUND),
    // process_manager_delivery.feature
    ("C-0578", UNBOUND),
    ("C-0592", UNBOUND),
    ("C-0593", UNBOUND),
    ("C-0594", UNBOUND),
    ("C-0595", UNBOUND),
    ("C-0596", UNBOUND),
    ("C-0597", UNBOUND),
    ("C-0599", UNBOUND),
    ("C-0600", UNBOUND),
    ("C-0601", UNBOUND),
    ("C-0602", UNBOUND),
    ("C-0603", UNBOUND),
    // query_server.feature
    ("C-0588", NO_QUERY),
    ("C-0589", NO_QUERY),
    // rules_version.feature
    ("C-0527", NO_SNAPSHOT_PATTERNS),
    ("C-0528", NO_SNAPSHOT_PATTERNS),
    ("C-0529", NO_SNAPSHOT_PATTERNS),
    ("C-0530", CONFIGURATION_ON_DOMAIN),
    ("C-0554", NO_QUERY),
    ("C-0555", NO_QUERY),
    ("C-0556", NO_QUERY),
    ("C-0560", NO_QUERY),
    ("C-0558#4", IN_PROCESS_CHAIN),
    ("C-0558#5", UNBOUND),
    ("C-0559", UNBOUND),
    // state_building.feature
    ("C-0281", UNBOUND),
    ("C-0282", UNBOUND),
    ("C-0283", UNBOUND),
    ("C-0284", UNBOUND),
    ("C-0285", UNBOUND),
    ("C-0286", UNBOUND),
    ("C-0287", UNBOUND),
    ("C-0288", UNBOUND),
    ("C-0289", UNBOUND),
    ("C-0290", UNBOUND),
    ("C-0291", UNBOUND),
    ("C-0292", UNBOUND),
    ("C-0293", UNBOUND),
    ("C-0294", UNBOUND),
    ("C-0295", UNBOUND),
    ("C-0296", UNBOUND),
    ("C-0297", UNBOUND),
    ("C-0298", UNBOUND),
    ("C-0299", UNBOUND),
    ("C-0300", UNBOUND),
    ("C-0301", UNBOUND),
    ("C-0302", UNBOUND),
    ("C-0675", UNBOUND),
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
    ("C-0584", UNBOUND),
    ("C-0585", NO_QUERY),
    ("C-0586", UNBOUND),
    ("C-0587", UNBOUND),
    ("C-0537", UNBOUND),
    ("C-0538", UNBOUND),
    ("C-0539", UNBOUND),
    ("C-0540", UNBOUND),
    ("C-0680", UNBOUND),
    ("C-0681", UNBOUND),
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
