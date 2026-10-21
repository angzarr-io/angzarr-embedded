//! One handler shape and one position contract for every reader.

use crate::next::{Admit, Committed, Gap, Position, Stream, Tracked};

use crate::next::toys::{at, command, cover, registry, root, set, Shelf, ShelfEvent};

#[test]
fn handlers_receive_identity_position_metadata_and_state() {
    let registry = registry();
    let log = crate::next::MemoryDeadLetters::new();
    let mut stream = Stream::<Shelf>::new(root(3), None).unwrap();
    let mut seen = Vec::new();
    let mut handler = |c: &Committed<'_, Shelf>| {
        seen.push((
            c.event_ref(),
            c.envelope().recorded_at().seconds,
            c.state.items.clone(),
            matches!(c.event(), ShelfEvent::WasSet(_)),
        ))
    };
    stream
        .execute(
            command(cover("shelf", 3), set("a", 1)),
            &registry,
            &at(5),
            &log,
            &mut [&mut handler],
        )
        .unwrap();
    assert_eq!(seen.len(), 1);
    let (event, seconds, items, was_set) = &seen[0];
    assert_eq!(event.stream, Some(cover("shelf", 3)));
    assert_eq!(event.sequence, 0);
    assert_eq!(*seconds, 5);
    assert_eq!(items["a"], 1, "the state just after the event");
    assert!(was_set);
}

#[test]
fn a_position_places_next_redelivery_and_gap() {
    assert_eq!(Position::START.next(), 0);
    assert_eq!(Position::START.place(0), Ok(Admit::Next));
    assert_eq!(
        Position::START.place(1),
        Err(Gap {
            expected: 0,
            found: 1
        })
    );
    let mut p = Position::at(4);
    assert_eq!(p.place(4), Ok(Admit::Redelivery));
    assert_eq!(p.advance(5), Ok(Admit::Next));
    assert_eq!(p, Position::at(5));
    let big = u64::from(u32::MAX) + 1;
    assert_eq!(Position::at(big).place(big + 1), Ok(Admit::Next));
}

#[test]
fn a_tracked_handler_drops_redeliveries_and_records_gaps() {
    let registry = registry();
    let log = crate::next::MemoryDeadLetters::new();
    let mut stream = Stream::<Shelf>::new(root(3), None).unwrap();
    for (n, item) in ["a", "b", "c"].into_iter().enumerate() {
        stream
            .execute(
                command(cover("shelf", 3), set(item, 1)),
                &registry,
                &at(n as i64),
                &log,
                &mut [],
            )
            .unwrap();
    }
    let mut count = 0;
    let mut tracked = Tracked::new(|_: &Committed<'_, Shelf>| count += 1)
        .resume(cover("shelf", 3), Position::at(0));
    stream.dispatch(0, &registry, &mut [&mut tracked]).unwrap();
    assert_eq!(tracked.position(&cover("shelf", 3)), Position::at(2));
    assert_eq!(tracked.gap(&cover("shelf", 3)), None);
    drop(tracked);
    assert_eq!(count, 2, "sequence 0 was a redelivery");
}

#[test]
fn a_tracked_handler_records_the_first_gap_until_it_advances() {
    let registry = registry();
    let log = crate::next::MemoryDeadLetters::new();
    let mut stream = Stream::<Shelf>::new(root(3), None).unwrap();
    for (n, item) in ["a", "b", "c"].into_iter().enumerate() {
        stream
            .execute(
                command(cover("shelf", 3), set(item, 1)),
                &registry,
                &at(n as i64),
                &log,
                &mut [],
            )
            .unwrap();
    }
    let mut seen = Vec::new();
    let mut tracked = Tracked::new(|c: &Committed<'_, Shelf>| seen.push(c.sequence()));
    stream.dispatch(1, &registry, &mut [&mut tracked]).unwrap();
    assert_eq!(
        tracked.gap(&cover("shelf", 3)),
        Some(&Gap {
            expected: 0,
            found: 1
        }),
        "the first gap, kept"
    );
    assert_eq!(tracked.position(&cover("shelf", 3)), Position::START);
    stream.dispatch(0, &registry, &mut [&mut tracked]).unwrap();
    assert_eq!(
        tracked.gap(&cover("shelf", 3)),
        None,
        "cleared on advancing"
    );
    assert_eq!(tracked.position(&cover("shelf", 3)), Position::at(2));
    drop(tracked);
    assert_eq!(seen, [0, 1, 2]);
}
