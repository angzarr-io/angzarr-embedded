//! Every error code angzarr-embedded's crates can produce has a message in
//! every shipped locale, using exactly the error's args; codes are unique;
//! and no variant goes unsampled.
//!
//! The errors come from each crate's samples, one value of every variant.
//! Each crate's catalogs, `crates/<crate>/locales/<locale>/<crate>.ftl`, are
//! checked against them by angzarr-diagnostic's [`catalog::check`]: a message
//! for every code with exactly its args, an error for every message, codes
//! under the crate's name, and a display of the code and args. A nested
//! cause of another crate's is that crate's to check (angzarr-ports' store
//! errors, which angzarr-embedded forwards under their own codes, are
//! checked against `angzarr_ports::EN_US` in core). A variant added without
//! a sample is caught by reading the crates' sources: every code literal
//! there (`"angzarr-embedded-..."`) must be the code of some sample. Each
//! crate's en-US catalog is also its exported `EN_US`, which must be that
//! file.
//!
//! A condition the spec keys is coded under its key (decision 14.60), its
//! English the spec's catalog (`angzarr_proto::REASONS_EN_US`), never a
//! crate's: each such sample, and each such nested cause, must have its
//! message there, give only args its key declares, and give every arg its
//! message names; and every live key's message renders with its declared
//! args.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use angzarr_diagnostic::catalog::{self, Catalog};
use angzarr_diagnostic::{Arg, Diagnostic};
use angzarr_embedded::angzarr_proto;

/// The crates whose errors are checked, and their samples.
fn samples() -> Vec<(&'static str, Vec<Box<dyn Diagnostic>>)> {
    vec![
        ("angzarr-embedded", angzarr_embedded::diagnostic_samples()),
        (
            "angzarr-embedded-host",
            angzarr_embedded_host::diagnostic_samples(),
        ),
        ("angzarr-store-sqlite", sqlite_samples()),
    ]
}

/// Each checked crate's exported en-US catalog: its name and its text.
fn exported() -> Vec<(&'static str, &'static str)> {
    vec![
        angzarr_embedded::EN_US,
        angzarr_embedded_host::EN_US,
        angzarr_store_sqlite::EN_US,
    ]
}

/// angzarr-store-sqlite's errors: each one it puts in a store error as its
/// cause, and a damage report.
fn sqlite_samples() -> Vec<Box<dyn Diagnostic>> {
    let mut samples: Vec<Box<dyn Diagnostic>> = angzarr_store_sqlite::errors()
        .into_iter()
        .map(|error| -> Box<dyn Diagnostic> { Box::new(error) })
        .collect();
    samples.push(Box::new(angzarr_store_sqlite::damage_sample()));
    samples
}

/// Crates that define no codes of their own.
const NO_CODES: &[&str] = &[
    "angzarr-embedded-catalog-check",
    "angzarr-embedded-conformance",
    "angzarr-embedded-host-config",
    "angzarr-embedded-macros",
];

fn crates_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Every crate's catalogs, by crate then locale: the text of each
/// `locales/<locale>/<crate>.ftl`. Any other file there is a problem.
fn catalogs(problems: &mut Vec<String>) -> BTreeMap<String, BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for krate in fs::read_dir(crates_dir()).unwrap() {
        let krate = krate.unwrap().path();
        let name = krate.file_name().unwrap().to_string_lossy().into_owned();
        let Ok(locales) = fs::read_dir(krate.join("locales")) else {
            continue;
        };
        let mut by_locale = BTreeMap::new();
        for locale in locales {
            let locale = locale.unwrap().path();
            let tag = locale.file_name().unwrap().to_string_lossy().into_owned();
            let file = locale.join(format!("{name}.ftl"));
            for other in fs::read_dir(&locale).unwrap() {
                let other = other.unwrap().path();
                if other != file {
                    problems.push(format!("{}: not {name}.ftl", other.display()));
                }
            }
            match fs::read_to_string(&file) {
                Ok(text) => {
                    by_locale.insert(tag, text);
                }
                Err(_) => problems.push(format!("{}: missing", file.display())),
            }
        }
        out.insert(name, by_locale);
    }
    out
}

/// Every code `d` produces that is `krate`'s: its own and, recursively, its
/// nested causes' under the crate's name.
fn own_codes(d: &dyn Diagnostic, krate: &str, out: &mut BTreeSet<String>) {
    out.insert(d.code().to_string());
    for (_, arg) in d.args() {
        if let Arg::Nested(inner) = arg {
            if inner.code().starts_with(&format!("{krate}-")) {
                own_codes(inner.as_ref(), krate, out);
            }
        }
    }
}

/// Whether `code` is a live key of the spec's (decision 14.60).
fn is_spec_key(code: &str) -> bool {
    angzarr_proto::reason_key_args(code).is_some()
}

/// `d` and each of its nested causes, where coded under a spec key, held to
/// the spec: a message in its catalog, only args its key declares (a
/// flattened `cause.<arg>` counting as `cause`), and every variable its
/// message names given.
fn spec_keyed(d: &dyn Diagnostic, problems: &mut Vec<String>) {
    let catalog =
        Catalog::parse(angzarr_proto::REASONS_EN_US.1).expect("the spec's catalog parses");
    let code = d.code();
    if let Some(declared) = angzarr_proto::reason_key_args(code) {
        let given: BTreeSet<&str> = d.args().iter().map(|(name, _)| *name).collect();
        for name in &given {
            let base = name.split_once('.').map_or(*name, |(base, _)| base);
            if !declared.contains(&base) {
                problems.push(format!("{code}: gives {name}, which it does not declare"));
            }
        }
        match catalog.variables(code) {
            None => problems.push(format!("{code}: no message in the spec's catalog")),
            Some(vars) => {
                for var in vars.iter().filter(|v| !given.contains(v.as_str())) {
                    problems.push(format!(
                        "{code}: its message names {var}, which it does not give"
                    ));
                }
            }
        }
    }
    for (_, arg) in d.args() {
        if let Arg::Nested(inner) = arg {
            spec_keyed(inner.as_ref(), problems);
        }
    }
}

/// Every `"angzarr-embedded-..."` string literal in a crate's library source (not its
/// `*.test.rs` tests), outside comments: the codes its errors can produce.
fn code_literals(krate: &str) -> BTreeSet<String> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for e in fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "rs")
                && !p.to_string_lossy().ends_with(".test.rs")
            {
                out.push(p);
            }
        }
    }
    let mut files = Vec::new();
    walk(&crates_dir().join(krate).join("src"), &mut files);
    let prefix = format!("\"{krate}-");
    let mut codes = BTreeSet::new();
    for file in files {
        let text = fs::read_to_string(&file).unwrap();
        for line in text.lines().filter(|l| !l.trim_start().starts_with("//")) {
            let mut rest = line;
            while let Some(at) = rest.find(&prefix) {
                let body = &rest[at + 1..];
                let end = body.find('"').unwrap_or(body.len());
                let lit = &body[..end];
                if lit
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                {
                    codes.insert(lit.to_string());
                }
                rest = &body[end..];
            }
        }
    }
    codes
}

#[test]
fn every_code_has_a_message_with_exactly_its_args_in_every_locale() {
    let mut problems = Vec::new();
    let catalogs = catalogs(&mut problems);
    let locales: BTreeSet<String> = catalogs.values().flat_map(|c| c.keys().cloned()).collect();
    assert!(
        locales.contains("en-US"),
        "no en-US catalogs under {}",
        crates_dir().display()
    );

    let checked: Vec<&str> = samples().into_iter().map(|(krate, _)| krate).collect();
    let mut seen: BTreeMap<String, &str> = BTreeMap::new();
    for (krate, diagnostics) in samples() {
        if diagnostics.is_empty() {
            problems.push(format!("{krate}: no samples"));
        }
        // A spec key is checked against the spec's catalog, a store error the
        // crate forwards under angzarr-ports' own code against angzarr-ports',
        // every other sample against the crate's own.
        for d in &diagnostics {
            spec_keyed(&**d, &mut problems);
        }
        let (forwarded, own): (Vec<_>, Vec<_>) = diagnostics
            .into_iter()
            .filter(|d| !is_spec_key(d.code()))
            .partition(|d| d.code().starts_with("angzarr-ports-"));
        for locale in &locales {
            match catalogs.get(krate).and_then(|c| c.get(locale)) {
                None => problems.push(format!("{krate}: no {locale} catalog")),
                Some(ftl) => problems.extend(
                    catalog::check(krate, ftl, &own)
                        .into_iter()
                        .map(|p| format!("{krate} ({locale}): {p:?}")),
                ),
            }
        }
        // Every forwarded error has its message there; that each of
        // angzarr-ports' messages has an error is checked in core.
        problems.extend(
            catalog::check("angzarr-ports", angzarr_ports::EN_US.1, &forwarded)
                .into_iter()
                .filter(|p| !matches!(p, catalog::Problem::Unsampled { .. }))
                .map(|p| format!("{krate} forwarding angzarr-ports: {p:?}")),
        );
        let mut codes = BTreeSet::new();
        for d in &own {
            own_codes(&**d, krate, &mut codes);
        }
        for code in codes {
            if let Some(previous) = seen.insert(code.clone(), krate) {
                problems.push(format!("{code}: produced by {previous} and by {krate}"));
            }
        }
    }

    // Every crate with a catalog is checked, and every crate is either
    // checked or known to have no codes.
    for name in catalogs.keys() {
        if !checked.contains(&name.as_str()) {
            problems.push(format!("{name}: a catalog no samples are checked against"));
        }
    }
    let crates = fs::read_dir(crates_dir())
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.path().join("Cargo.toml").exists());
    for name in crates.map(|e| e.file_name().to_string_lossy().into_owned()) {
        if !checked.contains(&name.as_str()) && !NO_CODES.contains(&name.as_str()) {
            problems.push(format!(
                "{name}: a crate with neither samples nor a place in NO_CODES"
            ));
        }
    }

    // No variant without a sample: every code in a crate's source is seen.
    for krate in &checked {
        for code in code_literals(krate) {
            if !seen.contains_key(&code) {
                problems.push(format!("{code}: in {krate}'s source but in no sample"));
            }
        }
    }
    for krate in NO_CODES {
        for code in code_literals(krate) {
            problems.push(format!("{code}: {krate} is listed as having no codes"));
        }
    }

    problems.sort();
    problems.dedup();
    assert!(
        problems.is_empty(),
        "{} problem(s):\n{}",
        problems.len(),
        problems.join("\n")
    );
}

/// A dependent renders from the exported catalog, so it must be the file
/// checked here, under the crate's own name, for every checked crate.
#[test]
fn every_checked_crate_exports_its_en_us_catalog() {
    let exported = exported();
    let names: Vec<&str> = exported.iter().map(|(name, _)| *name).collect();
    let checked: Vec<&str> = samples().iter().map(|(krate, _)| *krate).collect();
    assert_eq!(names, checked);
    for (name, text) in exported {
        let path = crates_dir()
            .join(name)
            .join("locales/en-US")
            .join(format!("{name}.ftl"));
        let file =
            fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        assert_eq!(text, file, "{name}::EN_US is not {}", path.display());
    }
}

/// The store errors angzarr-embedded forwards under their own codes are
/// rendered from angzarr-ports' own catalog, re-exported here: no copy.
#[test]
fn angzarr_embedded_reexports_the_angzarr_ports_catalog() {
    let (name, text) = angzarr_embedded::angzarr_ports::EN_US;
    assert_eq!(name, "angzarr-ports");
    assert_eq!(text, angzarr_ports::EN_US.1);
}

/// Every live key of the spec's three enums, with its declared args.
fn spec_keys() -> Vec<(&'static str, &'static [&'static str])> {
    use angzarr_embedded::v1::{CoordinatorReason, HostReason, RouterReason};
    let router = (1..).map_while(|n| RouterReason::try_from(n).ok());
    let host = (1..).map_while(|n| HostReason::try_from(n).ok());
    let coordinator = (1..).map_while(|n| CoordinatorReason::try_from(n).ok());
    router
        .map(|r| (r.as_str_name(), r.args()))
        .chain(host.map(|r| (r.as_str_name(), r.args())))
        .chain(coordinator.map(|r| (r.as_str_name(), r.args())))
        .collect()
}

/// Every reason angzarr-router refuses with is the kernel's code too,
/// verbatim, with the router's args (decision 14.60): `KernelError::Router`
/// forwards the router's `CodedError` under it, its message the spec's.
#[test]
fn every_router_reason_is_forwarded_under_its_own_key() {
    use angzarr_embedded::angzarr_router::error::CodedError;
    use angzarr_embedded::KernelError;

    let router = (1..).map_while(|n| angzarr_embedded::v1::RouterReason::try_from(n).ok());
    let mut problems = Vec::new();
    for reason in router {
        let key = reason.as_str_name();
        let error = KernelError::Router(CodedError {
            code: key.to_string(),
            extras: [("type_url".to_string(), "/a.B".to_string())].into(),
            grpc: angzarr_embedded::angzarr_router::error::GrpcCode::Internal,
            refusal: None,
        });
        if error.code() != key {
            problems.push(format!("{key} is {}", error.code()));
        }
        let args: Vec<String> = error.args().iter().map(|(n, _)| n.to_string()).collect();
        if args != ["type_url"] {
            problems.push(format!("{key}: args {args:?}"));
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// Every reason angzarr-router refuses with is among angzarr-embedded's
/// samples, every declared arg given, so each forwarded key is held to the
/// spec's catalog as every sample is.
#[test]
fn every_router_reason_is_sampled() {
    let samples = angzarr_embedded::diagnostic_samples();
    let router = (1..).map_while(|n| angzarr_embedded::v1::RouterReason::try_from(n).ok());
    let mut problems = Vec::new();
    for reason in router {
        let key = reason.as_str_name();
        let Some(sample) = samples.iter().find(|d| d.code() == key) else {
            problems.push(format!("{key}: no sample"));
            continue;
        };
        let args: BTreeSet<&str> = sample.args().iter().map(|(n, _)| *n).collect();
        let declared: BTreeSet<&str> = reason.args().iter().copied().collect();
        if args != declared {
            problems.push(format!("{key}: args {args:?}, declared {declared:?}"));
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// The spec's catalog renders every live key, in en-US, with its declared
/// args and no error: the parity the crates' own catalogs have, for the
/// catalog every runtime embeds.
#[test]
fn the_spec_catalog_renders_every_key_with_its_declared_args() {
    let mut problems = Vec::new();
    for (key, declared) in spec_keys() {
        let args: Vec<(&str, Arg)> = declared.iter().map(|a| (*a, Arg::Uint(2))).collect();
        let (text, errors) = render(key, &args, "en-US");
        problems.extend(errors);
        if text.is_empty() {
            problems.push(format!("{key}: renders empty"));
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// A spec-keyed error that gives an arg its key does not declare, or omits
/// one its message names, is caught.
#[test]
fn a_spec_keyed_error_is_held_to_its_keys_args() {
    use angzarr_diagnostic::Report;
    let mut problems = Vec::new();
    spec_keyed(
        &Report::new("COORDINATOR_REASON_CHAIN_TOO_DEEP").arg("stream", "a/b"),
        &mut problems,
    );
    assert_eq!(
        problems,
        [
            "COORDINATOR_REASON_CHAIN_TOO_DEEP: gives stream, which it does not declare",
            "COORDINATOR_REASON_CHAIN_TOO_DEEP: its message names limit, which it does not give",
        ]
    );
    let mut nested = Vec::new();
    spec_keyed(
        &Report::new("angzarr-embedded-downstream").arg(
            "error",
            Arg::nested(&Report::new("COORDINATOR_REASON_STORE_UNAVAILABLE").arg(
                "cause",
                Arg::nested(&Report::new("x").arg("ext_err_txt", "e")),
            )),
        ),
        &mut nested,
    );
    assert!(nested.is_empty(), "{nested:?}");
    let mut stray = Vec::new();
    spec_keyed(&Report::new("COORDINATOR_REASON_UNSPECIFIED"), &mut stray);
    assert!(stray.is_empty(), "UNSPECIFIED is no live key: {stray:?}");
}

/// The kernel's codes were `angzarr-embedded-next-*` and are
/// `angzarr-embedded-*`, the one rule being "strip `next-`"
/// (docs/design/audit.md, *The one rename of the kernel's codes*). The rule collides with nothing: no kernel code is left under the old
/// prefix, in its source or any catalog; none lands under another checked
/// crate's name (`angzarr-embedded-host-…`), where it would read as that
/// crate's; and no two codes, the kernel's or another crate's, are the
/// same, so no old code maps onto one that already meant something else.
#[test]
fn the_kernels_codes_lost_next_and_collide_with_nothing() {
    const OLD: &str = "angzarr-embedded-next-";
    let mut problems = Vec::new();
    let catalogs = catalogs(&mut problems);
    let mut all: BTreeMap<String, &str> = BTreeMap::new();
    let mut kernel = BTreeSet::new();
    for (krate, diagnostics) in samples() {
        let mut codes = BTreeSet::new();
        for d in diagnostics
            .iter()
            .filter(|d| !d.code().starts_with("angzarr-ports-") && !is_spec_key(d.code()))
        {
            own_codes(&**d, krate, &mut codes);
        }
        for code in &codes {
            if let Some(previous) = all.insert(code.clone(), krate) {
                problems.push(format!("{code}: {previous}'s and {krate}'s"));
            }
        }
        if krate == "angzarr-embedded" {
            kernel = codes;
        }
    }
    assert!(!kernel.is_empty(), "no kernel codes sampled");
    for code in &kernel {
        if code.starts_with(OLD) {
            problems.push(format!("{code}: still under {OLD}"));
        }
        if owner(code) != Some("angzarr-embedded") {
            problems.push(format!("{code}: reads as {:?}'s", owner(code)));
        }
    }
    for (krate, by_locale) in &catalogs {
        for (locale, ftl) in by_locale {
            if ftl.contains(OLD) {
                problems.push(format!("{krate} ({locale}): names {OLD}…"));
            }
        }
    }
    for code in code_literals("angzarr-embedded") {
        if code.starts_with(OLD) {
            problems.push(format!("{code}: in angzarr-embedded's source"));
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// The crates outside this workspace whose errors this workspace's carry
/// under their own codes, each with its exported en-US catalog:
/// angzarr-ports (the store errors angzarr-embedded forwards) and
/// angzarr-yaml (the host config's load errors, the cause of
/// angzarr-embedded-host's `ConfigInvalid`).
fn foreign() -> [(&'static str, &'static str); 2] {
    [
        angzarr_ports::EN_US,
        angzarr_embedded_host::angzarr_embedded_host_config::angzarr_yaml::EN_US,
    ]
}

/// The crate a code belongs to: the longest name it starts with, of the
/// checked crates and the [`foreign`] ones.
fn owner(code: &str) -> Option<&'static str> {
    samples()
        .into_iter()
        .map(|(krate, _)| krate)
        .chain(foreign().map(|(krate, _)| krate))
        .filter(|k| code.starts_with(&format!("{k}-")))
        .max_by_key(|k| k.len())
}

/// `code`'s message in `locale`, rendered with `args`: the text, and every
/// error Fluent reported (a variable the message names and `args` lack is
/// one). The catalog is the code's owner's ([`owner`]): a checked crate's
/// file, or a [`foreign`] crate's own `EN_US`.
fn render(code: &str, args: &[(&str, Arg)], locale: &str) -> (String, Vec<String>) {
    use fluent_bundle::{FluentArgs, FluentBundle, FluentResource, FluentValue};
    let spec = is_spec_key(code).then_some(angzarr_proto::REASONS_EN_US);
    let krate = spec
        .map(|(name, _)| name)
        .or_else(|| owner(code))
        .unwrap_or_else(|| panic!("{code}: no crate owns it"));
    let exported = spec
        .into_iter()
        .chain(foreign())
        .find(|(name, _)| *name == krate);
    let (path, text) = match exported {
        Some((name, text)) if locale == "en-US" => {
            (PathBuf::from(format!("{name}::EN_US")), text.to_string())
        }
        Some((name, _)) => {
            return (
                String::new(),
                vec![format!("{code}: {name} ships no {locale} catalog")],
            )
        }
        None => {
            let path = crates_dir()
                .join(krate)
                .join("locales")
                .join(locale)
                .join(format!("{krate}.ftl"));
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
            (path, text)
        }
    };
    let resource = FluentResource::try_new(text).unwrap_or_else(|(r, _)| r);
    let mut bundle = FluentBundle::new(vec![locale.parse().expect("a locale tag")]);
    bundle.set_use_isolating(false);
    bundle
        .add_resource(resource)
        .unwrap_or_else(|e| panic!("{}: {e:?}", path.display()));
    let mut fluent = FluentArgs::new();
    for (name, arg) in args {
        let value = match arg {
            Arg::Int(n) => FluentValue::from(*n),
            Arg::Uint(n) => FluentValue::from(*n),
            Arg::Str(s) => FluentValue::from(s.clone()),
            Arg::Nested(cause) => FluentValue::from(cause.code().to_string()),
        };
        fluent.set(*name, value);
    }
    let Some(pattern) = bundle.get_message(code).and_then(|m| m.value()) else {
        return (
            String::new(),
            vec![format!("{code}: no message in {locale}")],
        );
    };
    let mut errors = Vec::new();
    let rendered = bundle.format_pattern(pattern, Some(&fluent), &mut errors);
    (
        rendered.into_owned(),
        errors.iter().map(|e| format!("{code}: {e:?}")).collect(),
    )
}

/// Every sample's message, in every shipped locale, renders with the
/// sample's own args and no error: no message names a variable its error
/// does not give, whichever args that error has.
#[test]
fn every_samples_message_renders_with_its_args() {
    let mut problems = Vec::new();
    let locales: BTreeSet<String> = catalogs(&mut problems)
        .values()
        .flat_map(|c| c.keys().cloned())
        .collect();
    fn each(d: &dyn Diagnostic, locales: &BTreeSet<String>, problems: &mut Vec<String>) {
        for locale in locales {
            let (_, errors) = render(d.code(), &d.args(), locale);
            problems.extend(errors.into_iter().map(|e| format!("{e} ({locale})")));
        }
        for (_, arg) in d.args() {
            if let Arg::Nested(inner) = arg {
                each(inner.as_ref(), locales, problems);
            }
        }
    }
    for (_, diagnostics) in samples() {
        for d in &diagnostics {
            each(&**d, &locales, &mut problems);
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// SQLite's failure renders whether or not the driver gave SQLite's result
/// code: with it, as the database's failure naming the code; without it
/// (an I/O error, a pool timing out), under a key of its own that names
/// none, so no `$errno` goes missing.
#[test]
fn sqlites_failure_renders_with_and_without_its_errno() {
    use angzarr_store_sqlite::SqlError;
    let detail = "database is locked".to_string();
    let rendered = |error: SqlError| render(error.code(), &Diagnostic::args(&error), "en-US");

    let with = rendered(SqlError::Unavailable {
        errno: Some(5),
        detail: detail.clone(),
    });
    assert_eq!(
        with,
        (
            "the database failed (SQLite code 5): database is locked".to_string(),
            vec![]
        )
    );

    let without = rendered(SqlError::Unavailable {
        errno: None,
        detail,
    });
    assert_eq!(
        without,
        (
            "the database driver failed: database is locked".to_string(),
            vec![]
        )
    );
}
