//! The crate's shape: a leaf, shared by annals and angzarr.

/// The adapter is angzarr's as much as annals': it depends on angzarr-ports
/// and SQL, and on no crate of annals (its kernel above all), so angzarr's
/// runtimes can use it without annals, and annals' kernel never reaches
/// into it.
#[test]
fn a_store_depends_on_nothing_of_annals() {
    let manifest = include_str!("../Cargo.toml");
    let dependencies: Vec<&str> = manifest
        .lines()
        .map(str::trim)
        .filter(|line| !line.starts_with('#') && !line.starts_with('['))
        .filter_map(|line| line.split_once('=').map(|(name, _)| name.trim()))
        .collect();
    let annals: Vec<&&str> = dependencies
        .iter()
        .filter(|name| name.starts_with("annals"))
        .collect();
    assert!(annals.is_empty(), "depends on {annals:?}");
    assert!(dependencies.contains(&"angzarr-ports"));
}
