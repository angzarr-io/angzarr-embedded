use super::*;

const SRC: &str = r#"
# a comment
Feature: Counting
  Prose about the feature.

  Background:
    Given a counter at 2

  Scenario: Adding
    When I add 3
    And I add "4" quoted and 5
    Then the total is 10
    But it is not 11

  Scenario: A table
    Given these rows
      | name | n |
      | a    | 1 |
"#;

#[test]
fn parses_background_keywords_and_tables() {
    let f = parse_feature(SRC);
    assert_eq!(f.name, "Counting");
    assert_eq!(f.scenarios.len(), 2);
    let adding = &f.scenarios[0];
    let kw: Vec<&str> = adding.steps.iter().map(|s| s.keyword.as_str()).collect();
    assert_eq!(kw, ["Given", "When", "When", "Then", "Then"]);
    assert_eq!(adding.steps[2].numbers(), [5]);
    assert_eq!(adding.steps[2].quoted(), ["4"]);
    let table = &f.scenarios[1].steps[1].table;
    assert_eq!(table, &[vec!["name", "n"], vec!["a", "1"]]);
}

#[test]
fn a_leading_bom_does_not_hide_the_feature() {
    let f = parse_feature(
        "\u{feff}Feature: \u{c9}p\u{e9}es\r\n  Scenario: \u{5263}\r\n    Given a blade\r\n",
    );
    assert_eq!(f.name, "\u{c9}p\u{e9}es");
    assert_eq!(f.scenarios[0].name, "\u{5263}");
    assert_eq!(f.scenarios[0].steps[0].text, "a blade");
}

#[test]
fn table_cells_take_escapes() {
    let f = parse_feature(
        "Feature: F\n  Scenario: S\n    Given rows\n      | a \\| b | c\\\\d | e\\nf | \\x |\n",
    );
    assert_eq!(
        f.scenarios[0].steps[0].table,
        [vec!["a | b", "c\\d", "e\nf", "\\x"]]
    );
}

#[test]
fn numbers_skip_quotes() {
    assert_eq!(numbers(r#"12 "34" 5x6"#), [12, 5, 6]);
    assert_eq!(numbers(r#""7"8"#), [8]);
}

#[test]
fn only_then_is_an_assertion() {
    let f = parse_feature(SRC);
    let asserts: Vec<bool> = f.scenarios[0]
        .steps
        .iter()
        .map(Step::is_assertion)
        .collect();
    assert_eq!(asserts, [false, false, false, true, true]);
}

/// Two scenarios, the second of which fails, under `name`.
fn half_failing(name: &str) -> String {
    format!(
        "Feature: {name}\n  Scenario: holds\n    Then it holds\n  Scenario: breaks\n    Given it fails\n    Then it holds\n"
    )
}

/// A step that fails on any step saying so, counting what ran.
fn failing_step(ran: &mut Vec<String>, s: &Step) {
    ran.push(s.text.clone());
    assert!(!s.text.contains("fails"), "the step fails");
}

#[test]
fn a_feature_counts_its_passes_and_names_its_failures() {
    let mut report = Report {
        passed: 2,
        failed: vec![],
    };
    run_feature(
        &parse_feature(&half_failing("F")),
        Vec::new,
        failing_step,
        &mut report,
    );
    assert_eq!(
        report,
        Report {
            passed: 3,
            failed: vec!["F: breaks".to_string()],
        }
    );
}

/// A fresh, empty directory of this process's for `name`.
fn scratch(name: &str) -> std::path::PathBuf {
    let dir =
        std::env::temp_dir().join(format!("annals-sim-gherkin-{}-{name}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("make the scratch directory");
    dir
}

#[test]
fn a_directory_runs_its_feature_files_in_name_order() {
    let dir = scratch("run-dir");
    fs::write(dir.join("b.feature"), half_failing("B")).unwrap();
    fs::write(dir.join("a.feature"), half_failing("A")).unwrap();
    fs::write(dir.join("notes.txt"), half_failing("Notes")).unwrap();
    let report = run_dir(&dir, Vec::new, failing_step);
    fs::remove_dir_all(&dir).unwrap();
    assert_eq!(
        report,
        Report {
            passed: 2,
            failed: vec!["A: breaks".to_string(), "B: breaks".to_string()],
        }
    );
}

/// Set by [`main_exits_non_zero_only_on_a_failure`] for the child it
/// starts: the features directory [`main_in_a_child`] runs.
const CHILD_DIR: &str = "ANNALS_SIM_GHERKIN_MAIN_DIR";

/// Runs [`main`] when started as a child, since `main` exits the process;
/// otherwise does nothing.
#[test]
fn main_in_a_child() {
    if let Some(dir) = std::env::var_os(CHILD_DIR) {
        main(Path::new(&dir), Vec::new, failing_step);
    }
}

/// This test binary, rerun on [`main_in_a_child`] alone over `dir`.
fn main_child(dir: &Path) -> std::process::Output {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "gherkin::tests::main_in_a_child", "--nocapture"])
        .env(CHILD_DIR, dir)
        .output()
        .expect("rerun this test binary")
}

#[test]
fn main_exits_non_zero_only_on_a_failure() {
    let dir = scratch("main");
    fs::write(
        dir.join("a.feature"),
        "Feature: A\n  Scenario: holds\n    Then it holds\n",
    )
    .unwrap();
    let passing = main_child(&dir);
    fs::write(dir.join("b.feature"), half_failing("B")).unwrap();
    let failing = main_child(&dir);
    fs::remove_dir_all(&dir).unwrap();

    let stdout = |o: &std::process::Output| String::from_utf8_lossy(&o.stdout).into_owned();
    assert!(passing.status.success(), "{}", stdout(&passing));
    assert!(stdout(&passing).contains("\n1 passed, 0 failed\n"));
    assert_eq!(failing.status.code(), Some(1), "{}", stdout(&failing));
    assert!(stdout(&failing).contains("\n2 passed, 1 failed\n  failed: B: breaks\n"));
}

mod brawl {
    //! The Gherkin runner, driving the toy domain through a feature file.

    use std::path::Path;

    use crate::brawl::*;
    use crate::gherkin::{self, Step};

    #[derive(Default)]
    struct World {
        seed: u64,
        fighters: Vec<(i64, u8)>,
        arena: Option<Arena>,
    }

    impl World {
        fn arena(&self) -> &Arena {
            self.arena.as_ref().expect("the brawl has not run")
        }
    }

    fn step(w: &mut World, s: &Step) {
        let t = s.text.as_str();
        let n = s.numbers();
        if t.starts_with("a seed of") {
            w.seed = n[0] as u64;
        } else if t.starts_with("these fighters") {
            let header = &s.table[0];
            let col = |name: &str| header.iter().position(|h| h == name).expect(name);
            for row in &s.table[1..] {
                let hp = row[col("hp")].parse().expect("hp");
                let side = row[col("side")].parse().expect("side");
                w.fighters.push((hp, side));
            }
        } else if t == "the brawl runs to its end" {
            let (stream, _) = brawl(w.seed, w.fighters.clone());
            w.arena = Some(stream.state().clone());
        } else if t.starts_with("side") && t.ends_with("wins") {
            assert_eq!(w.arena().ended, Some(Outcome::Wins(n[0] as u8)));
        } else if t.starts_with("fighter") && t.ends_with("is down") {
            assert!(w.arena().fighters[n[0] as usize].hp <= 0);
        } else if t.starts_with("fighter") && t.contains("swung at least") {
            assert!(w.arena().fighters[n[0] as usize].swings >= n[1] as u64);
        } else if t == "nothing is left on the clock" || t == "something is left on the clock" {
            assert_eq!(w.arena().clock.is_empty(), t.starts_with("nothing"));
        } else {
            panic!("unknown step: {} {t}", s.keyword);
        }
    }

    #[test]
    fn every_feature_passes() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("features");
        let report = gherkin::run_dir(&dir, World::default, step);
        assert!(report.failed.is_empty(), "failed: {:?}", report.failed);
    }
}
