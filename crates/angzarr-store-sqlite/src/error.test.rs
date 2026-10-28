use angzarr_ports::{Arg, StoreError};

use super::{causes, damaged, unavailable, Damage, SqlError};

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
    assert_eq!(one.code(), "angzarr-store-sqlite-damaged");
    let args = one.args();
    assert_eq!(args.len(), 2);
    assert_eq!(
        args[0],
        ("first", Arg::Str("row 1 missing from index i".to_string()))
    );
    assert_eq!(args[1], ("more", Arg::Uint(0)));

    let two = damage(&["a", "b"]);
    assert_eq!(
        two.to_string(),
        "the database file is damaged: a (and 1 more problem)"
    );
    assert_eq!(two.args()[1], ("more", Arg::Uint(1)));
    let three = damage(&["a", "b", "c"]);
    assert_eq!(
        three.to_string(),
        "the database file is damaged: a (and 2 more problems)"
    );
    assert_eq!(three.problems(), ["a", "b", "c"]);
}

/// A failure reaches the ports as angzarr-ports' `StoreError`, this crate's
/// code and English its `Cause`.
#[test]
fn a_failure_reaches_the_ports_with_this_crates_code_as_its_cause() {
    let StoreError::Unavailable(cause) = unavailable("disk I/O error") else {
        panic!("unavailable");
    };
    assert_eq!(cause.code, "angzarr-store-sqlite-unavailable");
    assert_eq!(cause.message, "the database failed: disk I/O error");
    let StoreError::Unavailable(cause) = damaged("file is not a database".to_string()) else {
        panic!("unavailable");
    };
    assert_eq!(cause.code, "angzarr-store-sqlite-damaged-file");
    assert_eq!(
        cause.message,
        "the database file is too damaged to open: file is not a database"
    );
}

#[test]
fn every_cause_has_this_crates_code_once() {
    let codes: Vec<&str> = causes().iter().map(|c| c.code).collect();
    assert_eq!(
        codes,
        [
            "angzarr-store-sqlite-unavailable",
            "angzarr-store-sqlite-undecodable",
            "angzarr-store-sqlite-unmigrated",
            "angzarr-store-sqlite-damaged-file",
        ]
    );
    assert_eq!(
        SqlError::Unmigrated { pending: 2 }.cause().message,
        "the database, opened to read, has 2 migrations not yet applied"
    );
}
