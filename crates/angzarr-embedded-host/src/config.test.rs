use std::collections::BTreeMap;
use std::path::PathBuf;

use super::*;

fn domain(name: &str, language: &str) -> DomainConfig {
    DomainConfig {
        name: name.to_string(),
        language: language.to_string(),
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
        }
    );
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
