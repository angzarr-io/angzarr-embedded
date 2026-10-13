//! The round-trip property: whatever the writer writes, the reader reads back
//! as the same value, at every flow depth, for any text. Hand-picked strings
//! cover each hazard by name (scripts, combining marks, odd spaces, line and
//! control characters, YAML indicators); seeded random strings over a mixed
//! alphabet cover their combinations.

// The corpus is written as the characters themselves, not escapes: what
// each string tests should be visible at a glance. Characters that do not
// show (controls, odd spaces, combining marks) stay escaped, and
// invisible_characters and unicode_not_nfc stay denied here.
#![allow(clippy::non_ascii_literal)]

use annals_sim::Rng;
use annals_yaml::{parse, write, Value};

/// Hand-picked text: every kind of character the writer must get right.
const CORPUS: &[&str] = &[
    // Plain words, accents, scripts, emoji.
    "helmet", "épée", "naïve café", "Ærøskøbing", "日本語", "剣: 鋼", "剣：鋼", "한국어", "Привет", "Ελληνικά",
    "🗡️ sword", "👨\u{200d}👩\u{200d}👧", "🏳️\u{200d}🌈", "👍🏽", "🇫🇷",
    // Right to left, and bidi controls.
    "سيف", "חרב", "שלום עולם", "مرحبا: عالم", "\u{200f}rtl-mark", "a\u{202e}b", "\u{200e}",
    // Combining marks, NFC against NFD.
    "\u{e9}", "e\u{301}", "\u{301}lead", "a\u{301}\u{302}\u{303}",
    // Spaces that are not grammar whitespace, and ones that are.
    "a\u{a0}#b", "a\u{3000}#b", "\u{a0}lead", "trail\u{a0}", "\u{3000}ideo", "\u{a0}", "\u{3000}",
    "a\u{2009}b", " lead", "trail ", " ", "  ", "a  b", "a\t#b", "tab\there", "\t", "a\u{200b}b",
    // Line breaks and control characters.
    "a\nb", "\n", "a\r\nb", "a\rb", "a\r", "a\u{2028}b", "a\u{2029}b", "a\u{85}b", "a\u{0}b", "\u{0}",
    "\u{1b}[31m", "a\u{7f}", "a\u{9c}", "\u{feff}bom", "a\u{feff}", "\u{b}\u{c}",
    // Quotes and backslashes.
    "l'épée", "it's", "'", "''", "\"", "\"quoted\"", "'single'", "a \"b\" c", "a 'b", "5'2\"", "\\", "a\\nb",
    "\\u00e9", "a,'b", "[\"x", "{'y",
    "x' #y", "x\" #y", "a [b, 'c", "a [b,'c # d",
    // Indicators, anywhere.
    "#", "#é", "é #x", "a#b", ":", "x:", "x:y", ":x", "a: b", "a:\u{a0}b", "a:\tb", "?", "?é", "-", "- é", "-é",
    "--", "---", "...", "- ", "-1/4", ",", "a,b", "[", "]", "a[1", "a]b", "{", "}", "a{1", "a}b", "[a, b]",
    "{a: 1}", "&a", "*a", "!tag", "|", ">", "%é", "@x", "`x`", "~", "null", "Null", "no", "1.10", ".45 ACP",
    // Empty.
    "",
];

/// Characters the random strings draw from: mostly hazards.
const ALPHABET: &[char] = &[
    'a', 'b', 'z', 'A', '0', '9', ' ', ' ', ' ', '\t', '\n', '\r', '\0', '\u{1b}', '\u{7f}', '\u{85}', '\u{9c}',
    '#', ':', ',', '[', ']', '{', '}', '-', '?', '&', '*', '!', '|', '>', '%', '@', '`', '~', '\'', '"', '\\', '.',
    '\u{a0}', '\u{2009}', '\u{3000}', '\u{2028}', '\u{2029}', '\u{feff}', '\u{200b}', '\u{200d}', '\u{200e}',
    '\u{200f}', '\u{202e}', '\u{301}', '\u{fe0f}', 'é', 'ß', '日', '剣', '：', 'س', 'ח', '🗡', '👨', '🇫',
];

/// Words that mean something to the grammar on their own.
const WORDS: &[&str] = &["", "null", "~", "-", "- x", "---", "...", "#", ":", "[]", "{}", "'", "\""];

/// Random documents per run: seeds `0..SEEDS`.
const SEEDS: u64 = 5_000;

fn round_trip(v: &Value, what: &str) {
    for depth in 0..4 {
        let written = write(v, depth);
        match parse(&written) {
            Ok(back) => assert_eq!(&back, v, "{what}, flow depth {depth}, wrote:\n{written}"),
            Err(e) => panic!("{what}, flow depth {depth}: {e}; wrote:\n{written}"),
        }
    }
}

fn s(x: &str) -> Value {
    Value::scalar(x)
}

fn map(entries: &[(&str, Value)]) -> Value {
    Value::Map(entries.iter().map(|(k, v)| (k.to_string(), v.clone())).collect())
}

/// Every place a scalar can sit: block and flow, key and value, root, nested.
fn placements(t: &str) -> Vec<Value> {
    vec![
        s(t),
        map(&[(t, s("v"))]),
        map(&[("k", s(t))]),
        map(&[(t, s(t)), ("after", s("z"))]),
        map(&[("k", Value::Seq(vec![s(t), s("z")]))]),
        map(&[("k", map(&[(t, s(t)), ("z", s("1"))]))]),
        map(&[(t, map(&[(t, Value::Seq(vec![s(t)]))]))]),
        map(&[(t, Value::Null), ("k", Value::Seq(vec![]))]),
        Value::Seq(vec![s(t), Value::Null]),
        Value::Seq(vec![Value::Seq(vec![s(t)]), map(&[(t, s(t)), ("b", s(t))])]),
        Value::Seq(vec![map(&[(t, map(&[("x", s(t))]))])]),
    ]
}

#[test]
fn every_corpus_string_round_trips_in_every_place() {
    for t in CORPUS {
        for (i, v) in placements(t).iter().enumerate() {
            round_trip(v, &format!("{t:?} in placement {i}"));
        }
    }
}

#[test]
fn every_corpus_pair_round_trips_as_key_and_value() {
    for a in CORPUS {
        let keys = |skip: Option<&str>| {
            CORPUS.iter().filter(|b| Some(**b) != skip).map(|b| (b.to_string(), s(a))).collect::<Vec<_>>()
        };
        round_trip(&Value::Map(keys(None)), &format!("block keys with value {a:?}"));
        round_trip(&map(&[("k", Value::Map(keys(None)))]), &format!("flow keys with value {a:?}"));
        let items = Value::Seq(CORPUS.iter().map(|b| s(b)).collect());
        round_trip(&map(&[(a, items)]), &format!("every string in a sequence under {a:?}"));
    }
}

fn pick<T: Copy>(rng: &mut Rng, from: &[T]) -> T {
    from[rng.below(from.len() as u32) as usize]
}

fn random_text(rng: &mut Rng) -> String {
    match rng.below(8) {
        0 => pick(rng, WORDS).to_string(),
        1 => pick(rng, CORPUS).to_string(),
        _ => (0..rng.below(9)).map(|_| pick(rng, ALPHABET)).collect(),
    }
}

fn random_value(rng: &mut Rng, depth: u32) -> Value {
    match rng.below(if depth == 0 { 2 } else { 4 }) {
        0 => Value::Null,
        1 => Value::Scalar(random_text(rng)),
        2 => Value::Seq((0..rng.below(4)).map(|_| random_value(rng, depth - 1)).collect()),
        _ => {
            let mut entries: Vec<(String, Value)> = Vec::new();
            for _ in 0..rng.below(5) {
                let key = random_text(rng);
                if !entries.iter().any(|(k, _)| *k == key) {
                    entries.push((key, random_value(rng, depth - 1)));
                }
            }
            Value::Map(entries)
        }
    }
}

#[test]
fn random_documents_round_trip() {
    for seed in 0..SEEDS {
        let mut rng = Rng::new(seed, 0x9a17);
        // A mapping at the root, as content files are; anything below.
        let doc = match random_value(&mut rng, 4) {
            m @ Value::Map(_) => m,
            other => Value::Map(vec![("root".into(), other)]),
        };
        round_trip(&doc, &format!("seed {seed}: {doc:?}"));
        let any = random_value(&mut rng, 3);
        round_trip(&any, &format!("seed {seed}, any root: {any:?}"));
    }
}

/// Hand-written documents: parse, write, parse again, same value.
const DOCUMENTS: &[&str] = &[
    "\u{feff}version: 3\nname: épée\n",
    "\u{feff}- a\n- b\n",
    "a: 1\r\nb:\r\n  - x\r\n  - \"y\"\r\n",
    "name: l'épée # the sword\nalt: 'l''épée' # quoted\n",
    "名前: 剣\n説明: \"鋼の剣 # 一\"\nタグ: [武器, 剣]\n",
    "سيف: حاد\nחרב: [חדה, ארוכה]\n",
    "a: a\u{a0}#b\nb: x\u{3000}#y # a real comment\n",
    "k: {\"x:y\": 1, 'a,b': [\"c]\", d]}\n",
    "\"a[1\": 1\na]b: 2\n",
    "e: \"\\t\\r\\0\\n\\\\\\\"\"\n",
    "- {a: é, b: [日, 本]}\n- - x\n  - y\n-\n  k: v\n",
    "outer:\n  inner:\n    - name: 🗡️\n      tags: [👨\u{200d}👩\u{200d}👧, \u{e9}, e\u{301}]\n",
];

#[test]
fn hand_written_documents_survive_parse_write_parse() {
    for src in DOCUMENTS {
        let v = parse(src).unwrap_or_else(|e| panic!("{src:?}: {e}"));
        round_trip(&v, &format!("{src:?}"));
    }
}
