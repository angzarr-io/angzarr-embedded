use annals_diagnostic::{Arg, Diagnostic};

use super::{Damage, SqlError};

#[test]
fn an_error_renders_its_english_fallback_with_its_detail() {
    assert_eq!(
        SqlError::Unavailable {
            detail: "disk I/O error".to_string()
        }
        .to_string(),
        "the database failed: disk I/O error"
    );
    assert_eq!(
        SqlError::Undecodable {
            detail: "buffer underflow".to_string()
        }
        .to_string(),
        "a stored value does not decode: buffer underflow"
    );
    assert_eq!(
        SqlError::Unmigrated { pending: 1 }.to_string(),
        "the database, opened to read, has a migration not yet applied"
    );
    assert_eq!(
        SqlError::Unmigrated { pending: 2 }.to_string(),
        "the database, opened to read, has 2 migrations not yet applied"
    );
}

#[test]
fn damage_shows_the_first_problem_and_counts_the_rest() {
    let damage = |problems: &[&str]| Damage {
        problems: problems.iter().map(|p| p.to_string()).collect(),
    };
    let one = damage(&["row 1 missing from index i"]);
    assert_eq!(
        one.to_string(),
        "the database file is damaged: row 1 missing from index i"
    );
    assert_eq!(one.code(), "annals-store-sql-damaged");
    let args = one.args();
    assert_eq!(args.len(), 2);
    assert!(matches!(&args[0], ("first", Arg::Str(s)) if s == "row 1 missing from index i"));
    assert!(matches!(args[1], ("more", Arg::Uint(0))));

    let two = damage(&["a", "b"]);
    assert_eq!(
        two.to_string(),
        "the database file is damaged: a (and 1 more problem)"
    );
    assert!(matches!(two.args()[1], ("more", Arg::Uint(1))));
    let three = damage(&["a", "b", "c"]);
    assert_eq!(
        three.to_string(),
        "the database file is damaged: a (and 2 more problems)"
    );
    assert_eq!(three.problems(), ["a", "b", "c"]);
}
