use std::collections::BTreeMap;
use std::path::PathBuf;

use super::*;

fn domain(name: &str, language: &str) -> DomainConfig {
    DomainConfig {
        name: name.to_string(),
        language: language.to_string(),
        kind: Default::default(),
        launch: None,
        entry: Vec::new(),
        dir: None,
        env: BTreeMap::new(),
    }
}

fn strings(words: &[&str]) -> Vec<String> {
    words.iter().map(|w| w.to_string()).collect()
}

/// The CLI writes `[[domain]]` tables (the `domain` key holds the list);
/// every key but `name` and `language` may be left out, and
/// `[supervision]` keeps the defaults of the keys it leaves out. Read here
/// through serde_json, the serde format in the graph: the shape is serde's,
/// whatever the format.
#[test]
fn the_domain_tables_deserialize() {
    let text = r#"{
        "domain": [
            { "name": "table", "language": "rust" },
            {
                "name": "player",
                "language": "python",
                "entry": ["-m", "player.serve"],
                "dir": "domains/player",
                "env": { "PLAYER_SEATS": "6" }
            },
            { "name": "dealer", "language": "go", "launch": ["./bin/dealer", "--serve"] }
        ],
        "supervision": { "handshake_timeout_ms": 10000 }
    }"#;
    let config: HostConfig = serde_json::from_str(text).unwrap();
    let mut player = domain("player", "python");
    player.entry = strings(&["-m", "player.serve"]);
    player.dir = Some(PathBuf::from("domains/player"));
    player.env = BTreeMap::from([("PLAYER_SEATS".to_string(), "6".to_string())]);
    let mut dealer = domain("dealer", "go");
    dealer.launch = Some(strings(&["./bin/dealer", "--serve"]));
    assert_eq!(
        config,
        HostConfig {
            domains: vec![domain("table", "rust"), player, dealer],
            supervision: Supervision {
                handshake_timeout_ms: 10_000,
                ..Supervision::default()
            },
            store: StoreConfig::default(),
            policy: PolicyConfig::default(),
            retry: RetryConfig::default(),
        }
    );
}

/// The module docs' TOML, as the CLI writes it, reads as their comments
/// say.
#[test]
fn the_documented_toml_reads() {
    let text = r#"
        [[domain]]
        name = "table"
        language = "rust"

        [[domain]]
        name = "player"
        language = "python"
        entry = ["-m", "player.serve"]
        dir = "domains/player"

        [[domain]]
        name = "dealer"
        language = "go"
        launch = ["./bin/dealer"]

        [supervision]
        handshake_timeout_ms = 10000

        [store]
        path = "angzarr.db"

        [policy]
        on_bug = "move_past"

        [retry]
        max_retries = 3
        initial_backoff_ms = 100
        multiplier = 2.0
        max_backoff_ms = 5000
        jitter = 0.2
    "#;
    let config: HostConfig = toml::from_str(text).unwrap();
    assert_eq!(config.domains.len(), 3);
    assert_eq!(config.domains[1].placement(), {
        let mut player = domain("player", "python");
        player.entry = strings(&["-m", "player.serve"]);
        player.dir = Some(PathBuf::from("domains/player"));
        player.placement()
    });
    assert_eq!(config.supervision.handshake_timeout_ms, 10_000);
    assert_eq!(config.supervision.call_timeout_ms, 30_000);
    assert_eq!(config.store, StoreConfig::default());
    assert_eq!(config.policy.on_bug, OnPolicyBug::MovePast);
    assert_eq!(config.retry, RetryConfig::default());
    let memory: HostConfig = toml::from_str("[store]\nin_memory = true\n").unwrap();
    assert!(memory.store.in_memory);
    assert!(memory.domains.is_empty());
}

/// A misspelt key is refused, not ignored: `lanuch` would otherwise run the
/// default launch silently.
#[test]
fn an_unknown_key_is_refused() {
    let text = r#"{
        "domain": [
            { "name": "player", "language": "python", "lanuch": ["python3", "serve.py"] }
        ]
    }"#;
    assert!(serde_json::from_str::<HostConfig>(text).is_err());
    assert!(serde_json::from_str::<HostConfig>(r#"{ "domains": [] }"#).is_err());
}

/// `[policy] on_bug` is what a policy step that hits a bug does with the
/// policy's position: `"move_past"` the trigger (the default, when the key
/// or the table is left out) or `"stay"` at it, so a resume runs it again.
#[test]
fn the_policy_table_sets_what_a_policy_bug_does_with_the_position() {
    let on_bug = |text: &str| toml::from_str::<HostConfig>(text).map(|c| c.policy.on_bug);
    assert_eq!(
        on_bug("[policy]\non_bug = \"stay\"\n"),
        Ok(OnPolicyBug::Stay)
    );
    assert_eq!(
        on_bug("[policy]\non_bug = \"move_past\"\n"),
        Ok(OnPolicyBug::MovePast)
    );
    assert_eq!(on_bug("[policy]\n"), Ok(OnPolicyBug::MovePast));
    assert_eq!(on_bug(""), Ok(OnPolicyBug::MovePast));
    assert_eq!(PolicyConfig::default().on_bug, OnPolicyBug::MovePast);
}

/// An `on_bug` that is neither `"move_past"` nor `"stay"` is refused, and
/// so is an unknown key in `[policy]`.
#[test]
fn a_bad_policy_setting_is_refused() {
    assert!(toml::from_str::<HostConfig>("[policy]\non_bug = \"sometimes\"\n").is_err());
    assert!(toml::from_str::<HostConfig>("[policy]\non_bug = \"MovePast\"\n").is_err());
    assert!(toml::from_str::<HostConfig>("[policy]\non_bugs = \"stay\"\n").is_err());
}

/// `[retry]` is how the kernel retries a step's retryable failure before
/// it dead-letters it: `max_retries` (0 turns it off),
/// `initial_backoff_ms`, `multiplier`, `max_backoff_ms` and `jitter`, each
/// with a default (3, 100, 2.0, 5000 and 0.2) when left out, as the whole
/// table may be; an unknown key is refused.
#[test]
fn the_retry_table_sets_the_cap_the_backoff_and_the_jitter() {
    let retry = |text: &str| toml::from_str::<HostConfig>(text).map(|c| c.retry);
    assert_eq!(
        retry(
            "[retry]\nmax_retries = 5\ninitial_backoff_ms = 50\nmultiplier = 3.0\n\
             max_backoff_ms = 1000\njitter = 0.5\n"
        ),
        Ok(RetryConfig {
            max_retries: 5,
            initial_backoff_ms: 50,
            multiplier: 3.0,
            max_backoff_ms: 1_000,
            jitter: 0.5,
        })
    );
    assert_eq!(
        retry("[retry]\nmax_retries = 0\n"),
        Ok(RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        })
    );
    let defaults = RetryConfig {
        max_retries: 3,
        initial_backoff_ms: 100,
        multiplier: 2.0,
        max_backoff_ms: 5_000,
        jitter: 0.2,
    };
    assert_eq!(retry(""), Ok(defaults.clone()));
    assert_eq!(RetryConfig::default(), defaults);
    assert!(retry("[retry]\nretries = 5\n").is_err());
    assert!(retry("[retry]\nmax_retries = -1\n").is_err());
}

/// The setting writes back as it reads.
#[test]
fn the_policy_setting_round_trips() {
    let config = HostConfig {
        policy: PolicyConfig {
            on_bug: OnPolicyBug::Stay,
        },
        ..HostConfig::default()
    };
    let text = toml::to_string(&config).unwrap();
    assert!(text.contains("on_bug = \"stay\""), "{text}");
    assert_eq!(toml::from_str::<HostConfig>(&text).unwrap(), config);
}

#[test]
fn python_defaults_to_uv_run_locked_then_its_entry() {
    assert_eq!(
        default_launch("python"),
        Some(&["uv", "run", "--locked"][..])
    );
    let mut player = domain("player", "python");
    player.entry = strings(&["-m", "player.serve"]);
    player.dir = Some(PathBuf::from("domains/player"));
    player.env = BTreeMap::from([("K".to_string(), "v".to_string())]);
    assert_eq!(
        player.placement(),
        Ok(Placement::Remote(Launch {
            program: "uv".to_string(),
            args: strings(&["run", "--locked", "-m", "player.serve"]),
            dir: Some(PathBuf::from("domains/player")),
            env: BTreeMap::from([("K".to_string(), "v".to_string())]),
        }))
    );
}

/// `launch` replaces the default and the entry entirely.
#[test]
fn a_configured_launch_overrides_the_default() {
    let mut player = domain("player", "python");
    player.entry = strings(&["-m", "player.serve"]);
    player.launch = Some(strings(&["python3", "serve.py", "--fast"]));
    assert_eq!(
        player.placement(),
        Ok(Placement::Remote(Launch {
            program: "python3".to_string(),
            args: strings(&["serve.py", "--fast"]),
            dir: None,
            env: BTreeMap::new(),
        }))
    );
}

#[test]
fn a_rust_domain_without_a_launch_is_in_process() {
    assert_eq!(default_launch(IN_PROCESS_LANGUAGE), None);
    assert_eq!(
        domain("table", "rust").placement(),
        Ok(Placement::InProcess)
    );
}

/// A Rust domain may be remote too: a `launch` makes it a child.
#[test]
fn a_rust_domain_with_a_launch_is_remote() {
    let mut table = domain("table", "rust");
    table.launch = Some(strings(&["./target/release/table-server"]));
    assert_eq!(
        table.placement(),
        Ok(Placement::Remote(Launch {
            program: "./target/release/table-server".to_string(),
            args: Vec::new(),
            dir: None,
            env: BTreeMap::new(),
        }))
    );
}

/// Nothing here knows Go: it needs only its `launch`, and without one it
/// is refused by name, not guessed at.
#[test]
fn a_language_with_no_default_needs_a_launch() {
    assert_eq!(default_launch("go"), None);
    assert_eq!(
        domain("dealer", "go").placement(),
        Err(HostError::NoLaunch {
            domain: "dealer".to_string(),
            language: "go".to_string(),
        })
    );
    let mut dealer = domain("dealer", "go");
    dealer.launch = Some(strings(&["./bin/dealer"]));
    assert!(matches!(dealer.placement(), Ok(Placement::Remote(_))));
}

#[test]
fn an_empty_launch_is_refused() {
    let mut player = domain("player", "python");
    player.launch = Some(Vec::new());
    assert_eq!(
        player.placement(),
        Err(HostError::EmptyLaunch {
            domain: "player".to_string(),
        })
    );
}

/// A `[[domain]]`'s `kind` names the service its child serves:
/// `"domain"` by default, `"policy"` or `"projector"`; anything else is
/// refused.
#[test]
fn a_domains_kind_is_domain_policy_or_projector() {
    let kind = |text: &str| {
        toml::from_str::<HostConfig>(&format!(
            "[[domain]]\nname = \"x\"\nlanguage = \"python\"\n{text}"
        ))
        .map(|c| c.domains[0].kind)
    };
    assert_eq!(kind("").unwrap(), ComponentKindConfig::Domain);
    assert_eq!(
        kind("kind = \"domain\"").unwrap(),
        ComponentKindConfig::Domain
    );
    assert_eq!(
        kind("kind = \"policy\"").unwrap(),
        ComponentKindConfig::Policy
    );
    assert_eq!(
        kind("kind = \"projector\"").unwrap(),
        ComponentKindConfig::Projector
    );
    assert!(kind("kind = \"saga\"").is_err());
    assert!(kind("kind = \"Policy\"").is_err());
}
