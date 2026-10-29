use angzarr_ports::{Arg, StoreError};

use super::{causes, damaged, unavailable, Damage, SqlError};

/// An error's `Display` is its code, then its args (` name=value` each),
/// never English of angzarr's (decision 14.37): text is rendered only
/// where it is shown, from the catalog. A third party's text (the
/// driver's) is carried verbatim as `ext_err_txt`.
#[test]
fn an_error_displays_its_code_and_args_never_english() {
    assert_eq!(
        SqlError::Unmigrated { pending: 2 }.to_string(),
        "angzarr-store-sqlite-unmigrated pending=2"
    );
    let unavailable = SqlError::Unavailable {
        errno: None,
        detail: "disk I/O error".to_string(),
    }
    .to_string();
    assert_eq!(
        unavailable,
        "angzarr-store-sqlite-unavailable ext_err_txt=disk I/O error"
    );
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
/// code its `Cause`; SQLite's primary result code is its `errno` and its
/// text, verbatim, its `ext_err_txt`, never wrapped in English of
/// angzarr's (the owner's ruling, 2026-10-29).
#[test]
fn a_failure_reaches_the_ports_with_the_drivers_code_and_text() {
    let locked = SqlError::Unavailable {
        errno: Some(5),
        detail: "database is locked".to_string(),
    };
    let StoreError::Unavailable(cause) = StoreError::Unavailable(locked.cause()) else {
        panic!("unavailable");
    };
    assert_eq!(cause.code, "angzarr-store-sqlite-unavailable");
    assert_eq!(
        cause.args,
        [
            ("errno", Arg::Uint(5)),
            ("ext_err_txt", Arg::Str("database is locked".to_string())),
        ]
    );
    assert_eq!(
        locked.to_string(),
        "angzarr-store-sqlite-unavailable errno=5 ext_err_txt=database is locked"
    );
    let StoreError::Unavailable(cause) = unavailable("pool timed out") else {
        panic!("unavailable");
    };
    assert_eq!(
        cause.args,
        [("ext_err_txt", Arg::Str("pool timed out".to_string()))],
        "no errno where the driver gave none"
    );
    let StoreError::Unavailable(cause) = damaged(26, "file is not a database".to_string()) else {
        panic!("unavailable");
    };
    assert_eq!(cause.code, "angzarr-store-sqlite-damaged-file");
    assert_eq!(
        cause.args,
        [
            ("errno", Arg::Uint(26)),
            (
                "ext_err_txt",
                Arg::Str("file is not a database".to_string())
            ),
        ]
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
