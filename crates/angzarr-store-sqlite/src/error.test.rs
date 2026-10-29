use angzarr_ports::{Arg, StoreError};

use super::{causes, damaged, unavailable, Damage, SqlError};

/// An error's `Display` is its code, then its args (` name=value` each),
/// never English (decision 14.37): text is rendered only where it is
/// shown, from the catalog. The driver's own text is not an arg.
#[test]
fn an_error_displays_its_code_and_args_never_english() {
    assert_eq!(
        SqlError::Unmigrated { pending: 2 }.to_string(),
        "angzarr-store-sqlite-unmigrated pending=2"
    );
    let unavailable = SqlError::Unavailable {
        detail: "disk I/O error".to_string(),
    }
    .to_string();
    assert!(
        unavailable.starts_with("angzarr-store-sqlite-unavailable"),
        "{unavailable}"
    );
    assert!(!unavailable.contains("disk I/O error"), "{unavailable}");
    assert!(
        !unavailable.contains("the database failed"),
        "{unavailable}"
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
/// code its `Cause`; the driver's text stays in the adapter's own log and
/// never travels, in an arg or anywhere else (decision 14.37).
#[test]
fn a_failure_reaches_the_ports_with_this_crates_code_and_no_driver_text() {
    let StoreError::Unavailable(cause) = unavailable("disk I/O error") else {
        panic!("unavailable");
    };
    assert_eq!(cause.code, "angzarr-store-sqlite-unavailable");
    assert!(
        !format!("{cause:?}").contains("disk I/O error"),
        "{cause:?}"
    );
    let StoreError::Unavailable(cause) = damaged("file is not a database".to_string()) else {
        panic!("unavailable");
    };
    assert_eq!(cause.code, "angzarr-store-sqlite-damaged-file");
    assert!(
        !format!("{cause:?}").contains("file is not a database"),
        "{cause:?}"
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
        SqlError::Unmigrated { pending: 2 }.cause().args,
        [("pending", Arg::Uint(2))],
        "a cause carries its args (decision 14.37)"
    );
}
