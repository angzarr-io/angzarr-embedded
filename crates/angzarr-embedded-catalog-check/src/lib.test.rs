//! Every error code angzarr-embedded can produce has a message in every shipped
//! locale, using exactly the error's args; codes are unique; and no variant
//! goes unsampled.
//!
//! The errors come from each crate's `diagnostic_samples()`, one value of
//! every variant (angzarr-store-sqlite's, which depends on no diagnostics
//! crate, through [`Ported`]). A variant added without a sample is caught by
//! reading the crates' sources: every code literal there (`"angzarr-embedded-..."`)
//! must be the code of some sample. The catalogs are every
//! `crates/<crate>/locales/<locale>/<crate>.ftl`, plus those a crate
//! carries for codes not its own ([`CARRIED`]: angzarr-embedded carries
//! angzarr-ports', whose errors it forwards under their own codes); each
//! crate's en-US one is also its exported `EN_US`, which must be that file.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use std::fmt;

use angzarr_embedded_diagnostic::{Arg, Diagnostic};
use fluent_syntax::ast::{Entry, Expression, InlineExpression, Pattern, PatternElement};

/// The crates whose errors are checked, and their samples.
fn samples() -> Vec<(&'static str, Vec<Box<dyn Diagnostic>>)> {
    vec![
        ("angzarr-embedded", angzarr_embedded::diagnostic_samples()),
        (
            "angzarr-embedded-graph",
            angzarr_embedded_graph::diagnostic_samples(),
        ),
        (
            "angzarr-embedded-host",
            angzarr_embedded_host::diagnostic_samples(),
        ),
        ("angzarr-store-sqlite", sqlite_samples()),
        (
            "angzarr-embedded-yaml",
            angzarr_embedded_yaml::diagnostic_samples(),
        ),
    ]
}

/// Each checked crate's exported en-US catalog: its name and its text.
fn exported() -> Vec<(&'static str, &'static str)> {
    vec![
        angzarr_embedded::EN_US,
        angzarr_embedded_graph::EN_US,
        angzarr_embedded_host::EN_US,
        angzarr_store_sqlite::EN_US,
        angzarr_embedded_yaml::EN_US,
    ]
}

/// Catalogs a crate carries for another's codes: (whose codes, the crate
/// carrying them, in its own `locales/<locale>/`, named for the codes'
/// owner). angzarr-ports ships no catalog; angzarr-embedded, which forwards its
/// errors under their own codes, carries one.
const CARRIED: &[(&str, &str)] = &[("angzarr-ports", "angzarr-embedded")];

/// An error of a crate in angzarr-ports' style (a code, args in
/// angzarr-ports' `Arg`, its own `Display`), as a [`Diagnostic`].
#[derive(Clone, Debug)]
struct Ported {
    code: &'static str,
    display: String,
    args: Vec<(&'static str, angzarr_ports::Arg)>,
}

impl Ported {
    /// A wrapped cause, which has no `Display` of its own here: shown as
    /// angzarr-ports shows it, its code and args.
    fn cause(cause: &angzarr_ports::Cause) -> Ported {
        let mut display = cause.code.to_string();
        for (name, arg) in &cause.args {
            match arg {
                angzarr_ports::Arg::Uint(n) => display.push_str(&format!(" {name}={n}")),
                angzarr_ports::Arg::Str(s) => display.push_str(&format!(" {name}={s}")),
                angzarr_ports::Arg::Cause(inner) => {
                    display.push_str(&format!(" {name}={}", inner.code))
                }
            }
        }
        Ported {
            code: cause.code,
            display,
            args: cause.args.clone(),
        }
    }
}

impl fmt::Display for Ported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display)
    }
}

impl Diagnostic for Ported {
    fn code(&self) -> &'static str {
        self.code
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        self.args
            .iter()
            .map(|(name, arg)| {
                let arg = match arg {
                    angzarr_ports::Arg::Uint(n) => Arg::Uint(*n),
                    angzarr_ports::Arg::Str(s) => Arg::Str(s.clone()),
                    angzarr_ports::Arg::Cause(cause) => Arg::nested(&Ported::cause(cause)),
                };
                (*name, arg)
            })
            .collect()
    }
}

/// angzarr-store-sqlite's errors: each one it puts in a store error as its
/// cause, and a damage report, with their own `Display`.
fn sqlite_samples() -> Vec<Box<dyn Diagnostic>> {
    let mut samples: Vec<Box<dyn Diagnostic>> = angzarr_store_sqlite::errors()
        .into_iter()
        .map(|error| -> Box<dyn Diagnostic> {
            Box::new(Ported {
                code: error.code(),
                display: error.to_string(),
                args: error.cause().args,
            })
        })
        .collect();
    let damage = angzarr_store_sqlite::damage_sample();
    samples.push(Box::new(Ported {
        code: damage.code(),
        display: damage.to_string(),
        args: damage.args(),
    }));
    samples
}

/// A diagnostic as decision 14.37 displays it, worked out here on its own:
/// its code, then ` name=value` for each arg, a wrapped cause as its code
/// under the arg's name and its own args under `<name>.<arg>`.
fn keyed(d: &dyn Diagnostic) -> String {
    fn args(d: &dyn Diagnostic, prefix: &str, out: &mut String) {
        for (name, arg) in d.args() {
            match arg {
                Arg::Int(n) => out.push_str(&format!(" {prefix}{name}={n}")),
                Arg::Uint(n) => out.push_str(&format!(" {prefix}{name}={n}")),
                Arg::Str(s) => out.push_str(&format!(" {prefix}{name}={s}")),
                Arg::Nested(cause) => {
                    out.push_str(&format!(" {prefix}{name}={}", cause.code()));
                    args(cause.as_ref(), &format!("{prefix}{name}."), out);
                }
            }
        }
    }
    let mut out = d.code().to_string();
    args(d, "", &mut out);
    out
}

/// Crates that define no codes of their own.
const NO_CODES: &[&str] = &[
    "angzarr-embedded-catalog-check",
    "angzarr-embedded-diagnostic",
    "angzarr-embedded-macros",
];

fn crates_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

/// A message's variables, by name. Message references are refused: a
/// message must name its own args.
fn variables(
    pattern: &Pattern<String>,
    out: &mut BTreeSet<String>,
    problems: &mut Vec<String>,
    id: &str,
) {
    for element in &pattern.elements {
        if let PatternElement::Placeable { expression } = element {
            expression_variables(expression, out, problems, id);
        }
    }
}

fn expression_variables(
    e: &Expression<String>,
    out: &mut BTreeSet<String>,
    problems: &mut Vec<String>,
    id: &str,
) {
    match e {
        Expression::Select { selector, variants } => {
            inline_variables(selector, out, problems, id);
            for v in variants {
                variables(&v.value, out, problems, id);
            }
        }
        Expression::Inline(inline) => inline_variables(inline, out, problems, id),
    }
}

fn inline_variables(
    e: &InlineExpression<String>,
    out: &mut BTreeSet<String>,
    problems: &mut Vec<String>,
    id: &str,
) {
    match e {
        InlineExpression::VariableReference { id: var } => {
            out.insert(var.name.clone());
        }
        InlineExpression::FunctionReference { arguments, .. } => {
            for a in &arguments.positional {
                inline_variables(a, out, problems, id);
            }
            for a in &arguments.named {
                inline_variables(&a.value, out, problems, id);
            }
        }
        InlineExpression::TermReference { arguments, .. } => {
            if let Some(arguments) = arguments {
                for a in &arguments.positional {
                    inline_variables(a, out, problems, id);
                }
                for a in &arguments.named {
                    inline_variables(&a.value, out, problems, id);
                }
            }
        }
        InlineExpression::MessageReference { id: other, .. } => {
            problems.push(format!(
                "{id}: references message {}; name the args instead",
                other.name
            ));
        }
        InlineExpression::Placeable { expression } => {
            expression_variables(expression, out, problems, id)
        }
        InlineExpression::StringLiteral { .. } | InlineExpression::NumberLiteral { .. } => {}
    }
}

/// One crate's catalog in one locale: message id to its variables.
type Catalog = BTreeMap<String, BTreeSet<String>>;

fn read_catalog(path: &Path, problems: &mut Vec<String>) -> Catalog {
    let text = fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let resource = match fluent_syntax::parser::parse(text) {
        Ok(r) => r,
        Err((r, errors)) => {
            problems.push(format!(
                "{}: {} syntax error(s): {errors:?}",
                path.display(),
                errors.len()
            ));
            r
        }
    };
    let mut catalog = Catalog::new();
    for entry in &resource.body {
        match entry {
            Entry::Message(m) => {
                let id = m.id.name.clone();
                let mut vars = BTreeSet::new();
                match &m.value {
                    Some(value) => variables(value, &mut vars, problems, &id),
                    None => problems.push(format!("{}: {id} has no value", path.display())),
                }
                for a in &m.attributes {
                    variables(&a.value, &mut vars, problems, &id);
                }
                if catalog.insert(id.clone(), vars).is_some() {
                    problems.push(format!("{}: {id} twice", path.display()));
                }
            }
            Entry::Junk { content } => {
                problems.push(format!("{}: junk {content:?}", path.display()))
            }
            Entry::Term(_)
            | Entry::Comment(_)
            | Entry::GroupComment(_)
            | Entry::ResourceComment(_) => {}
        }
    }
    catalog
}

/// Every catalog: crate, then locale.
fn catalogs(problems: &mut Vec<String>) -> BTreeMap<String, BTreeMap<String, Catalog>> {
    let mut out = BTreeMap::new();
    let mut carried_catalogs: BTreeMap<String, BTreeMap<String, Catalog>> = BTreeMap::new();
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
                let carried = CARRIED.iter().find(|(owner, carrier)| {
                    *carrier == name && other == locale.join(format!("{owner}.ftl"))
                });
                if let Some((owner, _)) = carried {
                    let catalog = read_catalog(&other, problems);
                    carried_catalogs
                        .entry(owner.to_string())
                        .or_default()
                        .insert(tag.clone(), catalog);
                } else if other != file {
                    problems.push(format!("{}: not {name}.ftl", other.display()));
                }
            }
            if file.exists() {
                by_locale.insert(tag, read_catalog(&file, problems));
            } else {
                problems.push(format!("{}: missing", file.display()));
            }
        }
        out.insert(name, by_locale);
    }
    out.extend(carried_catalogs);
    out
}

/// kebab-case, starting with the crate's name and a dash; a valid Fluent id.
fn code_ok(code: &str, krate: &str) -> bool {
    code.strip_prefix(krate)
        .and_then(|rest| rest.strip_prefix('-'))
        .is_some_and(|rest| {
            !rest.is_empty()
                && rest.split('-').all(|w| {
                    !w.is_empty()
                        && w.bytes()
                            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
                })
        })
}

/// The crate a code belongs to: the longest crate name it starts with.
fn owner<'a>(code: &str, crates: impl Iterator<Item = &'a str>) -> Option<&'a str> {
    crates
        .filter(|k| code.starts_with(&format!("{k}-")))
        .max_by_key(|k| k.len())
}

/// Check one diagnostic and, recursively, its nested causes. Returns every
/// code seen.
fn check(
    d: &dyn Diagnostic,
    krate: &str,
    catalogs: &BTreeMap<String, BTreeMap<String, Catalog>>,
    locales: &BTreeSet<String>,
    problems: &mut Vec<String>,
    seen: &mut BTreeMap<String, String>,
) {
    let code = d.code();
    // A code a crate forwards under its owner's prefix is checked against
    // the catalog the crate carries for that owner.
    let krate = match owner(code, catalogs.keys().map(String::as_str)) {
        Some(owner) if CARRIED.contains(&(owner, krate)) => owner,
        _ => krate,
    };
    // Its `Display` is its code then its args, never English (decision
    // 14.37): English is the catalog's, where the error is shown.
    let shown = d.to_string();
    if shown != keyed(d) {
        problems.push(format!(
            "{code}: displays {shown:?}, not its code and args {:?}",
            keyed(d)
        ));
    }
    if !code_ok(code, krate) || owner(code, catalogs.keys().map(String::as_str)) != Some(krate) {
        problems.push(format!(
            "{code}: not kebab-case starting `{krate}-` (and no longer crate name)"
        ));
    }
    if let Some(previous) = seen.insert(code.to_string(), krate.to_string()) {
        if previous != krate {
            problems.push(format!("{code}: produced by {previous} and by {krate}"));
        }
    }
    let args = d.args();
    let mut names = BTreeSet::new();
    for (name, _) in &args {
        if !names.insert(name.to_string()) {
            problems.push(format!("{code}: arg {name} twice"));
        }
    }
    for locale in locales {
        match catalogs
            .get(krate)
            .and_then(|c| c.get(locale))
            .and_then(|c| c.get(code))
        {
            None => problems.push(format!("{code}: no message in {locale}")),
            Some(vars) if *vars != names => problems.push(format!(
                "{code} ({locale}): the message uses {vars:?}, the error gives {names:?}"
            )),
            Some(_) => {}
        }
    }
    for (_, arg) in &args {
        if let Arg::Nested(inner) = arg {
            match owner(inner.code(), catalogs.keys().map(String::as_str)) {
                Some(k) => check(&**inner, k, catalogs, locales, problems, seen),
                None => problems.push(format!(
                    "{code}: nested {} belongs to no crate",
                    inner.code()
                )),
            }
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

    let samples = samples();
    let mut seen = BTreeMap::new();
    for (krate, diagnostics) in &samples {
        if diagnostics.is_empty() {
            problems.push(format!("{krate}: no samples"));
        }
        for locale in &locales {
            if !catalogs.get(*krate).is_some_and(|c| c.contains_key(locale)) {
                problems.push(format!("{krate}: no {locale} catalog"));
            }
        }
        for d in diagnostics {
            check(&**d, krate, &catalogs, &locales, &mut problems, &mut seen);
        }
    }

    // Every crate is either checked or known to have no codes.
    let crates = fs::read_dir(crates_dir())
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.path().join("Cargo.toml").exists());
    for name in crates.map(|e| e.file_name().to_string_lossy().into_owned()) {
        if !samples.iter().any(|(k, _)| *k == name) && !NO_CODES.contains(&name.as_str()) {
            problems.push(format!(
                "{name}: a crate with neither samples nor a place in NO_CODES"
            ));
        }
    }

    // No variant without a sample: every code in a crate's source is seen.
    for (krate, _) in &samples {
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

    // No message without an error behind it.
    for (krate, by_locale) in &catalogs {
        for (locale, catalog) in by_locale {
            for id in catalog.keys() {
                if !seen.contains_key(id) {
                    problems.push(format!("{id} ({krate}, {locale}): no error has this code"));
                }
            }
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

/// A carried catalog is exported by its carrier, under its owner's name.
#[test]
fn angzarr_embedded_exports_the_angzarr_ports_catalog_it_carries() {
    let (name, text) = angzarr_embedded::ANGZARR_PORTS_EN_US;
    assert_eq!(name, "angzarr-ports");
    let path = crates_dir().join("angzarr-embedded/locales/en-US/angzarr-ports.ftl");
    let file = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    assert_eq!(text, file);
}

/// The reasons angzarr-router refuses with: every `pub const` of its
/// `error::codes` module, read from the source of the very router
/// angzarr-embedded is built with (`cargo metadata` names its checkout), so
/// a reason the router adds is checked here without a list to keep.
fn router_reasons() -> Vec<String> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let metadata = std::process::Command::new(cargo)
        .args([
            "metadata",
            "--format-version",
            "1",
            "--offline",
            "--manifest-path",
        ])
        .arg(crates_dir().join("../Cargo.toml"))
        .output()
        .expect("cargo metadata runs");
    assert!(metadata.status.success(), "cargo metadata failed");
    let text = String::from_utf8(metadata.stdout).unwrap();
    // The router package's manifest path, without a JSON parser: the field
    // after its package entry (a dependency on it names no version).
    let package = text
        .split("{\"name\":\"angzarr-router\",\"version\"")
        .nth(1)
        .expect("angzarr-router is in the graph");
    let manifest = package
        .split("\"manifest_path\":\"")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .expect("its manifest path");
    let source = Path::new(manifest).with_file_name("src/error.rs");
    let source =
        fs::read_to_string(&source).unwrap_or_else(|e| panic!("read {}: {e}", source.display()));
    let codes = source
        .split("pub mod codes {")
        .nth(1)
        .and_then(|rest| rest.split("\n}").next())
        .expect("the router's codes module");
    let reasons: Vec<String> = codes
        .lines()
        .filter_map(|line| line.trim().strip_prefix("pub const "))
        .filter_map(|decl| decl.split('"').nth(1))
        .map(str::to_string)
        .collect();
    assert!(!reasons.is_empty(), "no reasons read from {codes}");
    reasons
}

/// Every reason angzarr-router refuses with is an angzarr-embedded code of
/// its own, `angzarr-embedded-next-` and the reason in kebab-case, with a
/// message in every locale: `KernelError::Router` forwards the router's
/// `CodedError` under it.
#[test]
fn every_router_reason_has_its_own_code_and_message() {
    use angzarr_embedded::next::angzarr_router::error::CodedError;
    use angzarr_embedded::next::KernelError;

    let mut problems = Vec::new();
    let catalogs = catalogs(&mut problems);
    let mine = &catalogs["angzarr-embedded"];
    for reason in router_reasons() {
        let expected = format!(
            "angzarr-embedded-next-{}",
            reason.to_ascii_lowercase().replace('_', "-")
        );
        let error = KernelError::Router(CodedError {
            code: reason.clone(),
            extras: Default::default(),
            grpc: angzarr_embedded::next::angzarr_router::error::GrpcCode::Internal,
            refusal: None,
        });
        if error.code() != expected {
            problems.push(format!("{reason} is {}, not {expected}", error.code()));
        }
        for (locale, catalog) in mine {
            if !catalog.contains_key(&expected) {
                problems.push(format!("{locale}: no message for {expected} ({reason})"));
            }
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[test]
fn the_checks_catch_what_they_should() {
    assert!(code_ok(
        "angzarr-embedded-yaml-too-new",
        "angzarr-embedded-yaml"
    ));
    assert!(!code_ok(
        "angzarr-embedded-yaml-Too-new",
        "angzarr-embedded-yaml"
    ));
    assert!(!code_ok(
        "angzarr-embedded-yaml--new",
        "angzarr-embedded-yaml"
    ));
    assert!(!code_ok("angzarr-embedded-yaml-", "angzarr-embedded-yaml"));
    assert_eq!(
        owner(
            "angzarr-embedded-yaml-tab-indent",
            ["angzarr-embedded", "angzarr-embedded-yaml"].into_iter()
        ),
        Some("angzarr-embedded-yaml")
    );
    assert_eq!(
        owner(
            "angzarr-embedded-replay-x",
            ["angzarr-embedded", "angzarr-embedded-yaml"].into_iter()
        ),
        Some("angzarr-embedded")
    );

    let mut problems = Vec::new();
    let resource = "a = { $x ->\n    [one] one { $y }\n   *[other] { NUMBER($z) } { -t($w) }\n }\n    .tip = { $v }\nb = { c }\n";
    let parsed = fluent_syntax::parser::parse(resource.to_string()).unwrap();
    let mut vars = BTreeSet::new();
    for entry in &parsed.body {
        if let Entry::Message(m) = entry {
            variables(
                m.value.as_ref().unwrap(),
                &mut vars,
                &mut problems,
                &m.id.name,
            );
            for a in &m.attributes {
                variables(&a.value, &mut vars, &mut problems, &m.id.name);
            }
        }
    }
    let want: BTreeSet<String> = ["v", "w", "x", "y", "z"].map(String::from).into();
    assert_eq!(vars, want);
    assert_eq!(problems.len(), 1, "{problems:?}");
}
