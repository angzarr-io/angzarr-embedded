use super::*;

#[derive(Clone, Debug)]
struct Inner;

impl fmt::Display for Inner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("inner")
    }
}

impl Diagnostic for Inner {
    fn code(&self) -> &'static str {
        "test-inner"
    }
    fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![("n", 3u32.into())]
    }
}

#[test]
fn a_report_captures_code_args_english_and_nested_causes() {
    let r = Report::new("test-outer", "outer: inner")
        .arg("path", "a/b.yaml")
        .arg("cause", Arg::nested(&Inner));
    let again = Report::of(&r);
    assert_eq!(again, r);
    assert_eq!(again.code(), "test-outer");
    assert_eq!(again.to_string(), "outer: inner");
    let args = again.args();
    assert_eq!(args.len(), 2);
    assert!(matches!(&args[0], ("path", Arg::Str(s)) if s == "a/b.yaml"));
    match &args[1] {
        ("cause", Arg::Nested(d)) => {
            assert_eq!(d.code(), "test-inner");
            assert!(matches!(d.args()[..], [("n", Arg::Uint(3))]));
            assert_eq!(d.to_string(), "inner");
        }
        other => panic!("{other:?}"),
    }
}

/// Inner's code and args, with other English.
#[derive(Clone, Debug)]
struct Reworded;

impl fmt::Display for Reworded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("inner, reworded")
    }
}

impl Diagnostic for Reworded {
    fn code(&self) -> &'static str {
        "test-inner"
    }
    fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![("n", 3u32.into())]
    }
}

/// A report is its code and its args, never rendered text: two
/// diagnostics alike in both are one report, whatever English each
/// renders. The text is produced only where it is displayed, from the
/// catalog.
#[test]
fn a_report_is_its_code_and_args_and_keeps_no_rendered_text() {
    assert_eq!(Report::of(&Inner), Report::of(&Reworded));
}
