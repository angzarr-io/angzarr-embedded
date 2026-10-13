//! annals-sim's promises, proven on a toy domain.

mod common;

use common::*;
use annals::Stream;
use annals_sim::{parallel, JobRecord, Key, Value};

fn line_up(seed: u64) -> Vec<(i64, u8)> {
    let n = 2 + (seed % 4) as u8;
    (0..n)
        .map(|i| (12 + (seed as i64 + i as i64) % 9, i % 2))
        .collect()
}

#[test]
fn the_same_seed_gives_the_same_log_on_any_thread() {
    let seeds: Vec<u64> = (0..64).collect();
    let once = parallel(&seeds, |&s| brawl(s, line_up(s)).0.events().to_vec());
    let twice = parallel(&seeds, |&s| brawl(s, line_up(s)).0.events().to_vec());
    assert_eq!(once, twice);
    for (s, log) in seeds.iter().zip(&once) {
        assert_eq!(log, brawl(*s, line_up(*s)).0.events(), "seed {s}");
    }
}

#[test]
fn things_are_taken_in_key_order() {
    let rules = rules();
    for seed in 0..32 {
        let (stream, _) = brawl(seed, line_up(seed));
        let mut taken: Vec<Key> = Vec::new();
        for (i, e) in stream.events().iter().enumerate() {
            if let Ev::Taken { .. } = e.event {
                let before = stream.state_at(i as u64, &rules);
                taken.push(*before.clock.first().expect("something was due").0);
            }
        }
        assert!(!taken.is_empty());
        assert!(
            taken.windows(2).all(|w| w[0] < w[1]),
            "seed {seed}: {taken:?}"
        );
        // Within a tick, impacts go before swings.
        for w in taken.windows(2) {
            if w[0].tick == w[1].tick {
                assert!(w[0].phase <= w[1].phase, "seed {seed}");
            }
        }
    }
}

/// Replay any prefix cut between commands, then finish: the continuation is
/// the original, event for event. The queue lives in folded state, so the
/// prefix carries everything the clock needs.
#[test]
fn a_run_resumes_from_any_cut() {
    let rules = rules();
    for seed in 0..24 {
        let (whole, cuts) = brawl(seed, line_up(seed));
        let whole = whole.events().to_vec();
        for &cut in &cuts {
            let prefix = whole[..cut as usize].to_vec();
            let mut resumed =
                Stream::<Brawl>::replay(header(seed), prefix, &rules).expect("prefix replays");
            finish(&mut resumed, &rules);
            assert!(
                resumed.events() == whole.as_slice(),
                "seed {seed}: resumed at {cut} and diverged"
            );
        }
    }
}

fn rolls_of(stream: &Stream<Brawl>, who: u32) -> Vec<u32> {
    stream
        .events()
        .iter()
        .filter_map(|e| match e.event {
            Ev::Swung { who: w, roll } if w == who => Some(roll),
            _ => None,
        })
        .collect()
}

#[test]
fn adding_a_fighter_never_shifts_another_fighters_rolls() {
    for seed in 0..32 {
        // Fighter 0 is too tough to fall, so it swings in both fights.
        let (two, _) = brawl(seed, vec![(1_000, 0), (40, 1)]);
        let (three, _) = brawl(seed, vec![(1_000, 0), (40, 1), (40, 1)]);
        let a = rolls_of(&two, 0);
        let b = rolls_of(&three, 0);
        let n = a.len().min(b.len());
        assert!(n >= 3, "seed {seed}: too few swings to compare");
        assert_eq!(a[..n], b[..n], "seed {seed}");
    }
}

#[test]
fn every_registered_job_round_trips() {
    let r = rules();
    assert_eq!(r.kinds().collect::<Vec<_>>(), ["toy.swing", "toy.land"]);
    assert!(
        r.failing_round_trips().is_empty(),
        "{:?}",
        r.failing_round_trips()
    );
}

#[test]
#[should_panic(expected = "registered twice")]
fn a_kind_registered_twice_panics() {
    let _ = rules().register::<Land>();
}

#[test]
#[should_panic(expected = "unknown job kind \"toy.dance\"")]
fn an_unknown_kind_panics() {
    let rules = rules();
    let (mut stream, _) = brawl(1, vec![(10, 0), (10, 1)]);
    let mut events = stream.events().to_vec();
    events.truncate(2);
    // Corrupt the first scheduled job's kind, then run it.
    if let Ev::Scheduled { due, .. } = &mut events[1].event {
        due.job = JobRecord {
            kind: "toy.dance".to_string(),
            data: Value::map(vec![]),
        };
    }
    stream = Stream::replay(header(1), events, &rules).expect("replays");
    finish(&mut stream, &rules);
}

#[test]
#[should_panic(expected = "could not decode")]
fn undecodable_data_panics() {
    let rules = rules();
    let (whole, _) = brawl(1, vec![(10, 0), (10, 1)]);
    let mut events = whole.events().to_vec();
    events.truncate(2);
    if let Ev::Scheduled { due, .. } = &mut events[1].event {
        due.job = JobRecord {
            kind: "toy.land".to_string(),
            data: Value::Int(3),
        };
    }
    let mut stream = Stream::replay(header(1), events, &rules).expect("replays");
    finish(&mut stream, &rules);
}
