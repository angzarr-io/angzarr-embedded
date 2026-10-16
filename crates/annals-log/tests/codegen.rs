//! The hand-written wire types against the ones `buf generate` makes from the
//! `.proto` files: every value encodes to the same bytes with either, and
//! each decodes the other's bytes to the same value.
//!
//! Needs `gen/` (`just gen`), so it runs only with the `codegen-check`
//! feature, which `just test` turns on.
#![cfg(feature = "codegen-check")]

use std::fmt::Debug;

use prost::Message;

#[allow(dead_code)]
mod toy;

#[allow(clippy::all, dead_code)]
mod generated {
    pub mod v1 {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/gen/annals.log.v1.rs"));
    }
    pub mod toy {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/gen/annals.log.toy.v1.rs"
        ));
    }
}

/// `hand` and `gen` are the same value: same bytes, and each decodes the
/// other's bytes back to itself.
fn wire_identical<H, G>(hand: &H, gen: &G)
where
    H: Message + Default + PartialEq + Debug,
    G: Message + Default + PartialEq + Debug,
{
    let hand_bytes = hand.encode_to_vec();
    let gen_bytes = gen.encode_to_vec();
    assert_eq!(hand_bytes, gen_bytes, "{hand:?} vs {gen:?}");
    assert_eq!(&H::decode(gen_bytes.as_slice()).unwrap(), hand);
    assert_eq!(&G::decode(hand_bytes.as_slice()).unwrap(), gen);
    assert_eq!(
        hand.encode_length_delimited_to_vec(),
        gen.encode_length_delimited_to_vec()
    );
}

fn payloads() -> Vec<Vec<u8>> {
    vec![
        Vec::new(),
        vec![0],
        vec![0xff; 3],
        (0..=255).collect(),
        (0..200_000u32).map(|i| (i * 31 % 251) as u8).collect(),
    ]
}

#[test]
fn log_header() {
    let strings = [
        "",
        "d",
        "toy 1.0",
        "\u{fc}n\u{ef}c\u{f6}d\u{e9} \u{2694}",
        &"x".repeat(100_000),
    ];
    for stream_id in [0, 1, 127, 128, u64::MAX] {
        for schema_version in [0, 1, 300, u32::MAX] {
            for (i, s) in strings.iter().enumerate() {
                let other = strings[(i + 2) % strings.len()];
                let hand = annals_log::LogHeader {
                    stream_id,
                    domain: s.to_string(),
                    schema_version,
                    rules_version: other.to_string(),
                };
                let gen = generated::v1::LogHeader {
                    stream_id,
                    domain: s.to_string(),
                    schema_version,
                    rules_version: other.to_string(),
                };
                wire_identical(&hand, &gen);
            }
        }
    }
}

#[test]
fn envelope() {
    for seq in [0, 1, 300, u64::MAX] {
        for cause in [None, Some(0), Some(1), Some(u64::MAX - 1), Some(u64::MAX)] {
            for event in payloads() {
                let hand = annals_log::Envelope {
                    seq,
                    cause,
                    event: event.clone(),
                };
                let gen = generated::v1::Envelope { seq, cause, event };
                wire_identical(&hand, &gen);
            }
        }
    }
}

#[test]
fn toy_counter() {
    use generated::toy::counter_event::Kind as GenKind;
    use toy::counter_event::Kind as HandKind;
    let kinds = [
        (None, None),
        (Some(HandKind::Added(0)), Some(GenKind::Added(0))),
        (Some(HandKind::Added(-1)), Some(GenKind::Added(-1))),
        (
            Some(HandKind::Added(i64::MAX)),
            Some(GenKind::Added(i64::MAX)),
        ),
        (
            Some(HandKind::Added(i64::MIN)),
            Some(GenKind::Added(i64::MIN)),
        ),
        (
            Some(HandKind::Reset(String::new())),
            Some(GenKind::Reset(String::new())),
        ),
        (
            Some(HandKind::Reset("because".into())),
            Some(GenKind::Reset("because".into())),
        ),
    ];
    for seq in [0, u64::MAX] {
        for cause in [None, Some(0), Some(u64::MAX)] {
            for (hand_kind, gen_kind) in &kinds {
                let hand_event = toy::CounterEvent {
                    kind: hand_kind.clone(),
                };
                let gen_event = generated::toy::CounterEvent {
                    kind: gen_kind.clone(),
                };
                wire_identical(&hand_event, &gen_event);
                for present in [false, true] {
                    let hand = toy::CounterEnvelope {
                        seq,
                        cause,
                        event: present.then(|| hand_event.clone()),
                    };
                    let gen = generated::toy::CounterEnvelope {
                        seq,
                        cause,
                        event: present.then(|| gen_event.clone()),
                    };
                    wire_identical(&hand, &gen);
                }
            }
        }
    }
}

#[test]
fn toy_timed() {
    for tick in [0, 1, 128, u64::MAX] {
        for what in ["", "spawned", &"y".repeat(70_000)] {
            wire_identical(
                &toy::TimedEvent {
                    tick,
                    what: what.into(),
                },
                &generated::toy::TimedEvent {
                    tick,
                    what: what.into(),
                },
            );
        }
    }
}
