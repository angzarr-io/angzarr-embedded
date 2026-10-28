//! Every error code Annals can produce has a message in every shipped
//! locale, using exactly the error's args; codes are unique; and no variant
//! goes unsampled.
//!
//! The errors come from each crate's `diagnostic_samples()`, one value of
//! every variant. A variant added without a sample is caught by reading the
//! crates' sources: every code literal there (`"annals-..."`) must be the
//! code of some sample. The catalogs are every
//! `crates/<crate>/locales/<locale>/<crate>.ftl`; each crate's en-US one is
//! also its exported `EN_US`, which must be that file.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use annals_diagnostic::{Arg, Diagnostic};
use fluent_syntax::ast::{Entry, Expression, InlineExpression, Pattern, PatternElement};

/// The crates whose errors are checked, and their samples.
fn samples() -> Vec<(&'static str, Vec<Box<dyn Diagnostic>>)> {
    vec![
        ("annals", annals::diagnostic_samples()),
        ("annals-graph", annals_graph::diagnostic_samples()),
        ("annals-log", annals_log::diagnostic_samples()),
        ("annals-store-sql", annals_store_sql::diagnostic_samples()),
        ("annals-yaml", annals_yaml::diagnostic_samples()),
    ]
}

/// Each checked crate's exported en-US catalog: its name and its text.
fn exported() -> Vec<(&'static str, &'static str)> {
    vec![
        annals::EN_US,
        annals_graph::EN_US,
        annals_log::EN_US,
        annals_store_sql::EN_US,
        annals_yaml::EN_US,
    ]
}

/// Crates that define no codes of their own.
const NO_CODES: &[&str] = &[
    "annals-catalog-check",
    "annals-diagnostic",
    "annals-macros",
    "annals-sim",
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
            if file.exists() {
                by_locale.insert(tag, read_catalog(&file, problems));
            } else {
                problems.push(format!("{}: missing", file.display()));
            }
        }
        out.insert(name, by_locale);
    }
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
    let english = d.to_string();
    if english.trim().is_empty() {
        problems.push(format!("{code}: empty English Display"));
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

/// Every `"annals-..."` string literal in a crate's library source (not its
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

#[test]
fn the_checks_catch_what_they_should() {
    assert!(code_ok("annals-yaml-too-new", "annals-yaml"));
    assert!(!code_ok("annals-yaml-Too-new", "annals-yaml"));
    assert!(!code_ok("annals-yaml--new", "annals-yaml"));
    assert!(!code_ok("annals-yaml-", "annals-yaml"));
    assert_eq!(
        owner(
            "annals-yaml-tab-indent",
            ["annals", "annals-yaml"].into_iter()
        ),
        Some("annals-yaml")
    );
    assert_eq!(
        owner("annals-replay-x", ["annals", "annals-yaml"].into_iter()),
        Some("annals")
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
