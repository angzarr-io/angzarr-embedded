//! Versioned loading: chain checks, every load error, a v0 to v3 migration
//! with the helpers, pass-through, write-back and determinism.

use annals_yaml::versioned::{
    map_seq, move_key, remove_key, rename_key, set_default, version_of, ChainError, ChainProblem, Fixture, LoadProblem, Loader,
    MigrationError, Migrations,
};
use annals_yaml::{parse, write, Value};

fn ok(_: &mut Value) -> Result<(), MigrationError> {
    Ok(())
}

fn chain_problem(m: Migrations) -> ChainProblem {
    match m.build() {
        Err(ChainError { kind, problem }) => {
            assert_eq!(kind, "toy");
            problem
        }
        Ok(_) => panic!("the chain should not build"),
    }
}

#[test]
fn chains_must_be_whole() {
    assert!(Migrations::new("toy", 0).build().is_ok());
    assert!(Migrations::new("toy", 3).build().is_ok());
    assert!(Migrations::new("toy", 3).step(1, ok).step(2, ok).build().is_ok());
    assert!(Migrations::new("toy", 3).step(2, ok).step(0, ok).step(1, ok).build().is_ok());

    assert_eq!(chain_problem(Migrations::new("toy", 3).step(0, ok).step(2, ok)), ChainProblem::Gap { from: 1 });
    assert_eq!(chain_problem(Migrations::new("toy", 3).step(0, ok).step(1, ok)), ChainProblem::Gap { from: 2 });
    assert_eq!(
        chain_problem(Migrations::new("toy", 3).step(1, ok).step(1, ok).step(2, ok)),
        ChainProblem::Duplicate { from: 1 }
    );
    assert_eq!(
        chain_problem(Migrations::new("toy", 2).step(1, ok).step(2, ok)),
        ChainProblem::BeyondCurrent { from: 2, current: 2 }
    );
    let e = Migrations::new("toy", 3).step(0, ok).step(2, ok).build().unwrap_err();
    assert_eq!(e.to_string(), "toy migrations: no step from version 1 to 2");
}

// The toy schema's history:
//   v0: `name`, `hp`, `units: [{kind, hp}]`
//   v1: `hp` renamed `health`, in the root and in each unit
//   v2: `speed` defaults to 1; `legacy` dropped
//   v3: `name` moves under `meta`, as `meta.title`

fn v0_to_v1(doc: &mut Value) -> Result<(), MigrationError> {
    rename_key(doc, "hp", "health")?;
    map_seq(doc, "units", |u| rename_key(u, "hp", "health"))
}

fn v1_to_v2(doc: &mut Value) -> Result<(), MigrationError> {
    set_default(doc, "speed", Value::scalar("1"))?;
    remove_key(doc, "legacy")?;
    Ok(())
}

fn v2_to_v3(doc: &mut Value) -> Result<(), MigrationError> {
    move_key(doc, &["name"], &["meta", "title"])
}

fn toy() -> Loader {
    Migrations::new("toy", 3).step(2, v2_to_v3).step(0, v0_to_v1).step(1, v1_to_v2).build().unwrap()
}

const V0: &str = "# an old file\nversion: 0\nname: Ogre\nhp: 12\nlegacy: yes\nunits:\n  - {kind: club, hp: 3}\n  - {kind: rock, hp: 1}\n";
const V3: &str = "version: 3\nhealth: 12\nunits:\n  - {kind: club, health: 3}\n  - {kind: rock, health: 1}\nspeed: 1\nmeta:\n  title: Ogre\n";

#[test]
fn migrates_v0_to_v3_with_the_helpers() {
    let got = toy().load(Some("ogre.yaml"), V0).unwrap();
    assert_eq!(got, parse(V3).unwrap());
    // Keys keep their places; new ones go last.
    let keys: Vec<&str> = got.as_map().unwrap().iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, ["version", "health", "units", "speed", "meta"]);
    // Each intermediate version migrates from where it is.
    let v2 = "version: 2\nname: Ogre\nhealth: 12\nunits:\n  - {kind: club, health: 3}\n  - {kind: rock, health: 1}\nspeed: 1\n";
    assert_eq!(toy().load(None, v2).unwrap(), parse(V3).unwrap());
}

#[test]
fn a_current_document_passes_through_untouched() {
    let doc = parse(V3).unwrap();
    assert_eq!(toy().migrate(None, doc.clone()).unwrap(), doc);
    assert_eq!(toy().upgrade(None, V3, 1).unwrap(), None);
    // No steps at all: the identity on current documents.
    let none = Migrations::new("toy", 1).build().unwrap();
    let text = "version: 1\nb: 2\na: [1, 2]\n";
    assert_eq!(none.load(None, text).unwrap(), parse(text).unwrap());
}

#[test]
fn writes_back_the_migrated_tree() {
    let written = toy().upgrade(Some("ogre.yaml"), V0, 1).unwrap().expect("an old file upgrades");
    assert_eq!(written, write(&parse(V3).unwrap(), 1));
    // The written file reads back as the current version, and is current.
    assert_eq!(parse(&written).unwrap(), parse(V3).unwrap());
    assert_eq!(toy().upgrade(None, &written, 1).unwrap(), None);
    assert_eq!(toy().load(None, &written).unwrap(), parse(V3).unwrap());
}

#[test]
fn the_same_input_gives_the_same_bytes() {
    let first = toy().upgrade(None, V0, 2).unwrap().unwrap();
    for _ in 0..10 {
        assert_eq!(toy().upgrade(None, V0, 2).unwrap().unwrap().as_bytes(), first.as_bytes());
    }
}

fn problem(text: &str) -> LoadProblem {
    let e = toy().load(Some("bad.yaml"), text).unwrap_err();
    assert_eq!(e.kind, "toy");
    assert_eq!(e.source.as_deref(), Some("bad.yaml"));
    e.problem
}

#[test]
fn every_failure_is_a_typed_error() {
    assert!(matches!(problem("version: 3\na: [1, 2\n"), LoadProblem::Yaml(_)));
    assert_eq!(problem("- version: 3\n"), LoadProblem::NotAMapping);
    assert_eq!(problem(""), LoadProblem::NotAMapping);
    assert_eq!(problem("version: two\n"), LoadProblem::BadVersion("\"two\"".into()));
    assert_eq!(problem("version: -1\n"), LoadProblem::BadVersion("\"-1\"".into()));
    assert_eq!(problem("version: 1.0\n"), LoadProblem::BadVersion("\"1.0\"".into()));
    assert_eq!(problem("version:\n"), LoadProblem::BadVersion("an empty value".into()));
    assert_eq!(problem("version: [1]\n"), LoadProblem::BadVersion("a sequence".into()));
    assert!(matches!(problem("version: 99999999999\n"), LoadProblem::BadVersion(_)));
    assert_eq!(problem("version: 4\n"), LoadProblem::TooNew { found: 4, current: 3 });

    let trimmed = Migrations::new("toy", 3).step(1, v1_to_v2).step(2, v2_to_v3).build().unwrap();
    let e = trimmed.load(Some("ancient.yaml"), V0).unwrap_err();
    assert_eq!(e.problem, LoadProblem::TooOld { found: 0, oldest: 1 });

    // A step that fails: which step, and what went wrong.
    let e = toy().load(Some("bad.yaml"), "version: 0\nhp: 1\nhealth: 2\n").unwrap_err();
    assert_eq!(
        e.problem,
        LoadProblem::Step { from: 0, error: MigrationError::new("rename \"hp\" to \"health\": \"health\" is already there") }
    );
    assert_eq!(
        e.to_string(),
        "toy document bad.yaml: migrating version 0 to 1: rename \"hp\" to \"health\": \"health\" is already there"
    );
    let e = toy().load(None, "version: 0\nunits: [{hp: 1}, {hp: 2, health: 2}]\n").unwrap_err();
    assert_eq!(e.to_string(), "toy document: migrating version 0 to 1: units[1]: rename \"hp\" to \"health\": \"health\" is already there");

    // A step that breaks the root.
    fn clobber(doc: &mut Value) -> Result<(), MigrationError> {
        *doc = Value::Null;
        Ok(())
    }
    let broken = Migrations::new("toy", 1).step(0, clobber).build().unwrap();
    assert!(matches!(broken.load(None, "version: 0\n").unwrap_err().problem, LoadProblem::Step { from: 0, .. }));

    // The decoder's error.
    let e = toy()
        .load_as(Some("ogre.yaml"), V0, |doc| doc.get("missing").ok_or("`missing` is missing").cloned())
        .unwrap_err();
    assert_eq!(e.problem, LoadProblem::Decode("`missing` is missing".into()));
    assert_eq!(e.to_string(), "toy document ogre.yaml: `missing` is missing");

    // The error messages name the problem.
    assert_eq!(
        toy().load(Some("new.yaml"), "version: 7\n").unwrap_err().to_string(),
        "toy document new.yaml: version 7 was written by a newer build; this build reads up to version 3"
    );
}

#[test]
fn load_as_decodes_the_current_tree() {
    let health: u32 =
        toy().load_as(None, V0, |doc| doc.get("health").and_then(Value::as_str).unwrap().parse::<u32>()).unwrap();
    assert_eq!(health, 12);
}

#[test]
fn helpers_handle_the_edges() {
    let mut doc = parse("a: 1\nb: 2\nn:\n").unwrap();
    rename_key(&mut doc, "missing", "c").unwrap();
    rename_key(&mut doc, "a", "a").unwrap();
    assert!(rename_key(&mut Value::scalar("x"), "a", "b").is_err());
    set_default(&mut doc, "a", Value::scalar("9")).unwrap();
    set_default(&mut doc, "n", Value::scalar("0")).unwrap();
    assert_eq!(doc, parse("a: 1\nb: 2\nn: 0\n").unwrap());
    assert_eq!(remove_key(&mut doc, "b").unwrap(), Some(Value::scalar("2")));
    assert_eq!(remove_key(&mut doc, "b").unwrap(), None);
    // Moves make the mappings they need, and refuse to overwrite.
    move_key(&mut doc, &["a"], &["x", "y", "z"]).unwrap();
    assert_eq!(doc, parse("n: 0\nx: {y: {z: 1}}\n").unwrap());
    move_key(&mut doc, &["x", "y", "z"], &["a"]).unwrap();
    move_key(&mut doc, &["nowhere", "deep"], &["a"]).unwrap();
    assert!(move_key(&mut doc, &["n"], &["a"]).is_err());
    let before = doc.clone();
    assert!(move_key(&mut doc, &["n"], &["a", "b"]).is_err());
    assert_eq!(doc, before, "a failed move changes nothing");
    assert!(move_key(&mut doc, &[], &["a"]).is_err());
    assert!(map_seq(&mut doc, "n", |_| Ok(())).is_err());
    map_seq(&mut doc, "missing", |_| Err("never".into())).unwrap();
}

#[test]
fn fixtures_must_migrate_to_their_expected_trees() {
    let loader = toy();
    loader.assert_fixtures(&[
        Fixture { name: "v0.yaml", input: V0, expected: Some(V3) },
        Fixture { name: "v3.yaml", input: V3, expected: None },
    ]);

    let failures = loader
        .check_fixtures(&[
            // At an old version with no expected output: stale.
            Fixture { name: "stale.yaml", input: V0, expected: None },
            // Migrates, but not to what it should.
            Fixture { name: "wrong.yaml", input: V0, expected: Some("version: 3\n") },
            // Expected output not at the current version.
            Fixture { name: "old-expected.yaml", input: V0, expected: Some(V0) },
            // Does not load.
            Fixture { name: "broken.yaml", input: "version: 9\n", expected: None },
        ])
        .unwrap_err();
    let named = |n: &str| failures.iter().any(|f| f.contains(n));
    assert!(named("stale.yaml: at version 0"), "{failures:?}");
    assert!(named("wrong.yaml: migrated to"), "{failures:?}");
    assert!(named("old-expected.yaml: the expected output must be at the current version"), "{failures:?}");
    assert!(named("broken.yaml: version 9 was written by a newer build"), "{failures:?}");
    assert!(loader.check_fixtures(&[]).is_err());
}

#[test]
#[should_panic(expected = "toy fixtures do not migrate")]
fn assert_fixtures_panics_on_failure() {
    toy().assert_fixtures(&[Fixture { name: "v0.yaml", input: V0, expected: None }]);
}

#[test]
fn a_document_without_a_version_is_version_0() {
    let unstamped = V0.replace("version: 0\n", "");
    assert_eq!(version_of(&annals_yaml::parse(&unstamped).unwrap()), Ok(0));
    assert_eq!(toy().load(None, &unstamped).unwrap(), toy().load(None, V0).unwrap());
    let from_1 = Migrations::new("toy", 3).step(1, v1_to_v2).step(2, v2_to_v3).build().unwrap();
    assert_eq!(
        from_1.load(None, "name: Ogre\n").unwrap_err().problem,
        LoadProblem::TooOld { found: 0, oldest: 1 }
    );
}
