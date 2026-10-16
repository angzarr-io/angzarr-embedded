//! Handlers: what they are given, and the one position contract.

use annals::next::{
    Admit, CommandId, Committed, Gap, Handler, Meta, Position, RequestId, Seq, Stream, StreamId,
    Tracked,
};

use crate::toys::{fees, header, meta, Account, AccountCmd, AccountEvent, Balance};

/// Everything a handler was handed, owned.
#[derive(Clone, PartialEq, Debug)]
struct Seen {
    stream: StreamId,
    seq: Seq,
    meta: Meta,
    event: AccountEvent,
    after: Balance,
}

fn seen(c: &Committed<'_, Account>) -> Seen {
    Seen {
        stream: c.stream(),
        seq: c.seq(),
        meta: c.meta().clone(),
        event: c.event().clone(),
        after: c.state.clone(),
    }
}

#[test]
#[ignore = "proposal: handlers receive identity, position and metadata (B2)"]
fn handlers_receive_identity_position_and_metadata() {
    let mut s = Stream::<Account>::new(header(5, "account"));
    let rules = fees("fees-1", 0);
    let mut log = Vec::new();
    let mut handler = |c: &Committed<'_, Account>| log.push(seen(c));
    s.execute(&AccountCmd::Open, &meta(1, 10), &rules, &mut [&mut handler])
        .unwrap();
    s.execute(
        &AccountCmd::Deposit(40),
        &meta(2, 20),
        &rules,
        &mut [&mut handler],
    )
    .unwrap();

    assert_eq!(log.len(), 2);
    assert_eq!(log[1].stream, StreamId::from(5));
    assert_eq!(log[1].seq, 1);
    assert_eq!(log[1].meta, s.events()[1].meta);
    assert_eq!(
        log[1].meta.command,
        CommandId::Origin {
            request: RequestId(2),
            index: 0
        }
    );
    assert_eq!(log[1].event, AccountEvent::Deposited(40));
    assert_eq!(
        log[1].after,
        Balance {
            open: true,
            cents: 40
        },
        "the state just after the event"
    );
}

#[test]
#[ignore = "proposal: dispatch replays history to handlers, one pass"]
fn dispatch_hands_history_to_handlers_from_a_seq() {
    let mut s = Stream::<Account>::new(header(5, "account"));
    let rules = fees("fees-1", 0);
    for (i, cmd) in [
        AccountCmd::Open,
        AccountCmd::Deposit(40),
        AccountCmd::Deposit(2),
    ]
    .iter()
    .enumerate()
    {
        s.execute(cmd, &meta(i as u128, 0), &rules, &mut [])
            .unwrap();
    }
    let mut a = Vec::new();
    let mut b = Vec::new();
    let mut ha = |c: &Committed<'_, Account>| a.push((c.seq(), c.state.cents));
    let mut hb = |c: &Committed<'_, Account>| b.push(c.seq());
    s.dispatch(1, &rules, &mut [&mut ha, &mut hb]).unwrap();
    assert_eq!(a, [(1, 40), (2, 42)], "folded from 0, handed from 1");
    assert_eq!(b, [1, 2]);
}

#[test]
#[ignore = "proposal: one position contract, next / redelivery / gap"]
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

    let p = Position::at(4);
    assert_eq!(p.next(), 5);
    assert_eq!(p.place(5), Ok(Admit::Next));
    assert_eq!(p.place(4), Ok(Admit::Redelivery));
    assert_eq!(p.place(0), Ok(Admit::Redelivery));
    assert_eq!(
        p.place(6),
        Err(Gap {
            expected: 5,
            found: 6
        })
    );

    let mut q = Position::at(4);
    assert_eq!(q.advance(4), Ok(Admit::Redelivery));
    assert_eq!(q, Position::at(4));
    assert!(q.advance(6).is_err());
    assert_eq!(q, Position::at(4), "a gap does not move it");
    assert_eq!(q.advance(5), Ok(Admit::Next));
    assert_eq!(q, Position::at(5));
}

#[test]
#[ignore = "proposal: a tracked handler sees each event once, in order"]
fn a_tracked_handler_drops_redeliveries_and_records_gaps() {
    let mut s = Stream::<Account>::new(header(5, "account"));
    let rules = fees("fees-1", 0);
    for (i, cmd) in [
        AccountCmd::Open,
        AccountCmd::Deposit(1),
        AccountCmd::Deposit(2),
        AccountCmd::Deposit(3),
    ]
    .iter()
    .enumerate()
    {
        s.execute(cmd, &meta(i as u128, 0), &rules, &mut [])
            .unwrap();
    }
    let mut seqs = Vec::new();
    let mut tracked = Tracked::new(|c: &Committed<'_, Account>| seqs.push(c.seq()));
    let id = s.id();

    // A bus delivers 0, 1, 1 again, then loses 2 and delivers 3.
    let state = s.state().clone();
    for seq in [0, 1, 1, 3] {
        let c = Committed {
            header: s.header(),
            envelope: &s.events()[seq],
            state: &state,
        };
        Handler::<Account>::handle(&mut tracked, &c);
    }
    assert_eq!(tracked.position(id), Position::at(1));
    assert_eq!(
        tracked.gap(id),
        Some(&Gap {
            expected: 2,
            found: 3
        })
    );

    // Catching up from the position fills the gap.
    s.dispatch(tracked.position(id).next(), &rules, &mut [&mut tracked])
        .unwrap();
    assert_eq!(tracked.position(id), Position::at(3));
    assert_eq!(tracked.gap(id), None);
    drop(tracked);
    assert_eq!(seqs, [0, 1, 2, 3]);
}
