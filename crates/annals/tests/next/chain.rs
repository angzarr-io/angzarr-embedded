//! The command handler: a command and its reactions as one atomic chain.
//!
//! The club of `reactions.rs`, with its members' copies (a translation),
//! its treasury's ledger (a policy) and, below the ledger, an auditor (a
//! policy): club → ledger → auditor is three levels.

use std::fmt;

use annals::next::{
    handle, Committed, Domain, Edition, EventId, EventStore, Failure, InMemoryStore, InMemoryTx,
    KernelError, Policy, Position, ProjectionRoute, Reaction, Recovery, Root, Routes, Rulebook,
    Seq, Slice, Step, StepError, StepKind, StoreError, StreamKey, Tx, UnitOfWork,
};
use annals_diagnostic::{Arg, Diagnostic, Report};

use crate::reactions::{
    joined, member_stream, rules, Club, ClubEvent, ClubToMembers, Dues, Ledger, LedgerState,
    MemberEvent, Pot, Recorded, TREASURY_ROOT,
};
use crate::toys::{correlation, key, meta, meta_in};

fn treasury() -> StreamKey {
    crate::reactions::treasury()
}

// ---------------------------------------------------------------- level three: an auditor

struct Auditor;

#[derive(Default, Clone, PartialEq, Debug)]
struct Audited {
    checkpoint: Option<Seq>,
    approved: i64,
}

#[derive(Clone, PartialEq, Debug)]
struct Approve {
    source_seq: Seq,
    cents: i64,
}

#[derive(Clone, PartialEq, Debug)]
struct Approved(Approve);

#[derive(Clone, PartialEq, Debug)]
struct TooLarge;

impl fmt::Display for TooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("too large to approve")
    }
}

impl Diagnostic for TooLarge {
    fn code(&self) -> &'static str {
        "toy-auditor-too-large"
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![]
    }
}

impl Domain for Auditor {
    type State = Audited;
    type Command = Approve;
    type Event = Approved;
    type Rejection = TooLarge;
    type Rules = ();

    const NAME: &'static str = "auditor";

    fn decide(cmd: &Approve, tx: &mut Tx<'_, Self>) -> Result<(), TooLarge> {
        if cmd.cents > 100 {
            return Err(TooLarge);
        }
        tx.emit(Approved(cmd.clone()));
        Ok(())
    }

    fn apply(s: &mut Audited, Approved(a): &Approved, (): &()) {
        s.checkpoint = Some(a.source_seq);
        s.approved += a.cents;
    }
}

const AUDITOR_ROOT: Root = Root(200);

fn auditor() -> StreamKey {
    StreamKey::main("auditor", AUDITOR_ROOT)
}

/// Every ledger record, sent to the auditor for approval.
struct Audit;

impl Reaction for Audit {
    type Source = Ledger;
    type Target = Auditor;
    type TargetId = ();

    const NAME: &'static str = "audit";

    fn targets(_: &LedgerState) -> Vec<()> {
        vec![()]
    }

    fn root((): &()) -> Root {
        AUDITOR_ROOT
    }

    fn position(auditor: &Audited) -> Position {
        auditor.checkpoint.map_or(Position::START, Position::at)
    }
}

impl Policy for Audit {
    fn command((): &(), c: &Committed<'_, Ledger>) -> Approve {
        Approve {
            source_seq: c.seq(),
            cents: c.event().0.cents,
        }
    }
}

// ---------------------------------------------------------------- one database

/// One database, one table per event type; one adapter implementing the
/// store port once per event type, as hatpick's Postgres adapter would.
#[derive(Default)]
struct Db {
    clubs: InMemoryStore<ClubEvent>,
    members: InMemoryStore<MemberEvent>,
    ledgers: InMemoryStore<Recorded>,
    auditors: InMemoryStore<Approved>,
}

/// A transaction over every table.
struct DbTx<'a> {
    clubs: InMemoryTx<'a, ClubEvent>,
    members: InMemoryTx<'a, MemberEvent>,
    ledgers: InMemoryTx<'a, Recorded>,
    auditors: InMemoryTx<'a, Approved>,
}

impl Db {
    fn begin(&mut self) -> DbTx<'_> {
        DbTx {
            clubs: self.clubs.begin(),
            members: self.members.begin(),
            ledgers: self.ledgers.begin(),
            auditors: self.auditors.begin(),
        }
    }

    fn len<E: Clone>(store: &mut InMemoryStore<E>, key: &StreamKey) -> Seq {
        store.read(key).unwrap().map_or(0, |s| s.len)
    }
}

impl UnitOfWork for DbTx<'_> {
    fn commit(self) -> Result<(), StoreError> {
        self.clubs.commit()?;
        self.members.commit()?;
        self.ledgers.commit()?;
        self.auditors.commit()
    }
}

macro_rules! table {
    ($event:ty, $field:ident) => {
        impl EventStore<$event> for DbTx<'_> {
            fn append(
                &mut self,
                key: &StreamKey,
                expected: Seq,
                events: &[annals::next::Envelope<$event>],
            ) -> Result<Seq, StoreError> {
                self.$field.append(key, expected, events)
            }

            fn read_from(
                &mut self,
                key: &StreamKey,
                from: Seq,
            ) -> Result<Option<Slice<$event>>, StoreError> {
                self.$field.read_from(key, from)
            }
        }
    };
}

table!(ClubEvent, clubs);
table!(MemberEvent, members);
table!(Recorded, ledgers);
table!(Approved, auditors);

fn club() -> StreamKey {
    key(1, "club")
}

/// The whole club: members' copies, the treasury and its auditor.
fn routes<'r, 'a: 'r>(r: &'r Rulebook<()>) -> Routes<'r, DbTx<'a>> {
    Routes::new()
        .translation::<ClubToMembers>(r, r)
        .policy::<Dues>(r, r)
        .policy::<Audit>(r, r)
}

// ---------------------------------------------------------------- tests

#[test]
#[ignore = "proposal: a successful chain commits every stream atomically"]
fn a_successful_chain_commits_every_stream_with_causation_and_correlation() {
    let mut db = Db::default();
    let r = rules();
    handle::<Club, _, _>(
        db.begin(),
        &routes(&r),
        &club(),
        &joined("ann"),
        &meta(1, 10),
        &r,
        &mut (),
    )
    .unwrap();
    let recorded = handle::<Club, _, _>(
        db.begin(),
        &routes(&r),
        &club(),
        &ClubEvent::Paid(30),
        &meta_in(2, 20, 77),
        &r,
        &mut (),
    )
    .unwrap();

    assert_eq!(
        recorded.streams(),
        [
            (club(), 1, 2),
            (member_stream("ann"), 1, 2),
            (treasury(), 1, 2),
            (auditor(), 1, 2),
        ],
        "every stream touched, in the order first touched, depth first"
    );
    let paid = EventId {
        stream: club(),
        seq: 1,
    };
    let record = &recorded.events::<Ledger>(&treasury()).unwrap()[0];
    assert_eq!(record.meta.cause, Some(paid));
    let approval = &recorded.events::<Auditor>(&auditor()).unwrap()[0];
    assert_eq!(
        approval.meta.cause,
        Some(EventId {
            stream: treasury(),
            seq: 1
        }),
        "caused by the ledger's event, not the club's"
    );
    for meta in [&record.meta, &approval.meta] {
        assert_eq!(meta.correlation, correlation(77));
    }
    assert_eq!(
        approval.meta.command,
        annals::next::CommandId::Reaction {
            source: EventId {
                stream: treasury(),
                seq: 1
            },
            component: "audit".to_string(),
            index: 0
        },
        "an idempotency key recomputable from the log"
    );
    assert_eq!(
        Db::len(&mut db.auditors, &auditor()),
        2,
        "committed, not just returned"
    );
}

#[test]
#[ignore = "proposal: a rejection three levels deep leaves every stream untouched"]
fn a_rejection_three_levels_deep_leaves_every_stream_untouched_and_returns_the_path() {
    let mut db = Db::default();
    let r = rules();
    handle::<Club, _, _>(
        db.begin(),
        &routes(&r),
        &club(),
        &joined("ann"),
        &meta(1, 10),
        &r,
        &mut (),
    )
    .unwrap();

    let failed = handle::<Club, _, _>(
        db.begin(),
        &routes(&r),
        &club(),
        &ClubEvent::Paid(500),
        &meta(2, 20),
        &r,
        &mut (),
    );
    let failure = failed.err().unwrap();
    assert_eq!(
        failure,
        Failure::Downstream {
            path: vec![
                Step {
                    stream: treasury(),
                    kind: StepKind::Command,
                    cause: EventId {
                        stream: club(),
                        seq: 1
                    },
                },
                Step {
                    stream: auditor(),
                    kind: StepKind::Command,
                    cause: EventId {
                        stream: treasury(),
                        seq: 1
                    },
                },
            ],
            error: StepError::Rejected(Report::of(&TooLarge)),
        }
    );
    assert_eq!(failure.recovery(), Recovery::Business);
    assert_eq!(Db::len(&mut db.clubs, &club()), 1, "not even the club's");
    assert_eq!(Db::len(&mut db.members, &member_stream("ann")), 1);
    assert_eq!(Db::len(&mut db.ledgers, &treasury()), 1);
    assert_eq!(Db::len(&mut db.auditors, &auditor()), 1);
}

#[test]
#[ignore = "proposal: projections run after commit, never on a failed chain"]
fn projections_never_see_events_from_a_failed_chain() {
    let mut db = Db::default();
    let r = rules();
    let mut pot = Pot::default();
    for (i, n) in [30, 500, 4].iter().enumerate() {
        let _ = handle::<Club, _, _>(
            db.begin(),
            &routes(&r),
            &club(),
            &ClubEvent::Paid(*n),
            &meta(i as u128, 0),
            &r,
            &mut ProjectionRoute::<Club, _> {
                projection: &mut pot,
                rules: &r,
            },
        );
    }
    assert_eq!(pot.0, 34, "500 was refused downstream and never projected");
}

/// A unit of work in which someone else has always just appended to the
/// treasury: its appends there conflict, after the club's has been taken.
#[derive(Default)]
struct Contended {
    clubs: InMemoryStore<ClubEvent>,
    committed: bool,
}

impl EventStore<ClubEvent> for Contended {
    fn append(
        &mut self,
        key: &StreamKey,
        expected: Seq,
        events: &[annals::next::Envelope<ClubEvent>],
    ) -> Result<Seq, StoreError> {
        self.clubs.append(key, expected, events)
    }

    fn read_from(
        &mut self,
        key: &StreamKey,
        from: Seq,
    ) -> Result<Option<Slice<ClubEvent>>, StoreError> {
        self.clubs.read_from(key, from)
    }
}

impl EventStore<Recorded> for Contended {
    fn append(
        &mut self,
        _: &StreamKey,
        expected: Seq,
        _: &[annals::next::Envelope<Recorded>],
    ) -> Result<Seq, StoreError> {
        Err(StoreError::Conflict {
            stream: treasury(),
            expected,
            actual: expected + 1,
        })
    }

    fn read_from(&mut self, _: &StreamKey, _: Seq) -> Result<Option<Slice<Recorded>>, StoreError> {
        Ok(None)
    }
}

impl UnitOfWork for &mut Contended {
    fn commit(self) -> Result<(), StoreError> {
        self.committed = true;
        Ok(())
    }
}

impl<E> EventStore<E> for &mut Contended
where
    Contended: EventStore<E>,
{
    fn append(
        &mut self,
        key: &StreamKey,
        expected: Seq,
        events: &[annals::next::Envelope<E>],
    ) -> Result<Seq, StoreError> {
        (**self).append(key, expected, events)
    }

    fn read_from(&mut self, key: &StreamKey, from: Seq) -> Result<Option<Slice<E>>, StoreError> {
        (**self).read_from(key, from)
    }
}

#[test]
#[ignore = "proposal: a conflict on any stream touched fails the commit, retryably"]
fn a_conflict_on_any_stream_touched_fails_the_whole_chain() {
    let mut contended = Contended::default();
    let r = rules();
    let failed = {
        let routes = Routes::new().policy::<Dues>(&r, &r);
        handle::<Club, _, _>(
            &mut contended,
            &routes,
            &club(),
            &ClubEvent::Paid(3),
            &meta(1, 0),
            &r,
            &mut (),
        )
    };
    let failure = failed.err().unwrap();
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::Store(StoreError::Conflict {
            stream: treasury(),
            expected: 0,
            actual: 1
        }))
    );
    assert_eq!(failure.recovery(), Recovery::Retry);
    assert!(!contended.committed, "the unit of work was never committed");
}

// ---------------------------------------------------------------- a rally: a chain that comes back

/// Two balls hitting each other back: A's hit commands B, B's commands A,
/// until the rally reaches `TO`. Each hit must be the next in the rally as
/// the stream's own staged state has it.
struct Ball;

const A: Root = Root(10);
const B: Root = Root(11);

/// Ball `root`'s stream, on the main timeline.
fn ball(root: Root) -> StreamKey {
    StreamKey::main("ball", root)
}
const TO: u32 = 4;

#[derive(Default, Clone, PartialEq, Debug)]
struct Rally {
    me: Option<Root>,
    /// The last rally number this ball hit.
    last: u32,
    /// The other ball's last seq this ball took a hit from.
    checkpoint: Option<Seq>,
}

#[derive(Clone, PartialEq, Debug)]
struct Hit {
    me: Root,
    rally: u32,
    source_seq: Option<Seq>,
}

#[derive(Clone, PartialEq, Debug)]
struct OutOfTurn;

impl fmt::Display for OutOfTurn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("out of turn")
    }
}

impl Diagnostic for OutOfTurn {
    fn code(&self) -> &'static str {
        "toy-ball-out-of-turn"
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![]
    }
}

impl Domain for Ball {
    type State = Rally;
    type Command = Hit;
    type Event = Hit;
    type Rejection = OutOfTurn;
    type Rules = ();

    const NAME: &'static str = "ball";

    /// A ball hits every other rally number: A 1, 3; B 2, 4. Its second
    /// hit is decided against its own staged first.
    fn decide(hit: &Hit, tx: &mut Tx<'_, Self>) -> Result<(), OutOfTurn> {
        let last = tx.state().last;
        if last != 0 && hit.rally != last + 2 {
            return Err(OutOfTurn);
        }
        tx.emit(hit.clone());
        Ok(())
    }

    fn apply(s: &mut Rally, hit: &Hit, (): &()) {
        s.me = Some(hit.me);
        s.last = hit.rally;
        if let Some(seq) = hit.source_seq {
            s.checkpoint = Some(seq);
        }
    }
}

struct Return;

impl Reaction for Return {
    type Source = Ball;
    type Target = Ball;
    type TargetId = Root;

    const NAME: &'static str = "return";

    fn targets(ball: &Rally) -> Vec<Root> {
        match ball.me {
            Some(me) if ball.last < TO => vec![if me == A { B } else { A }],
            _ => vec![],
        }
    }

    fn root(id: &Root) -> Root {
        *id
    }

    fn position(ball: &Rally) -> Position {
        ball.checkpoint.map_or(Position::START, Position::at)
    }
}

impl Policy for Return {
    fn command(target: &Root, c: &Committed<'_, Ball>) -> Hit {
        Hit {
            me: *target,
            rally: c.event().rally + 1,
            source_seq: Some(c.seq()),
        }
    }
}

#[derive(Default)]
struct Balls(InMemoryStore<Hit>);

#[test]
#[ignore = "proposal: a later step sees the staged state of a stream an earlier step touched"]
fn a_chain_that_comes_back_sees_its_own_staged_state() {
    let mut balls = Balls::default();
    let r = rules();
    let routes = Routes::new().policy::<Return>(&r, &r);
    let recorded = handle::<Ball, _, _>(
        balls.0.begin(),
        &routes,
        &ball(A),
        &Hit {
            me: A,
            rally: 1,
            source_seq: None,
        },
        &meta(1, 0),
        &r,
        &mut (),
    )
    .unwrap();
    let rallies = |id| -> Vec<u32> {
        recorded
            .events::<Ball>(&ball(id))
            .unwrap()
            .iter()
            .map(|e| e.event.rally)
            .collect()
    };
    assert_eq!(rallies(A), [1, 3], "A's 3 was decided against its staged 1");
    assert_eq!(rallies(B), [2, 4]);
}

#[test]
#[ignore = "proposal: a cascade past the depth limit is a kernel error, terminal"]
fn a_chain_deeper_than_its_limit_is_refused() {
    let mut balls = Balls::default();
    let r = rules();
    let failed = {
        let routes = Routes::new().policy::<Return>(&r, &r);
        let mut unit = balls.0.begin();
        let mut chain = annals::next::Chain::new(&mut unit, &routes).limit(2);
        chain.execute::<Ball>(
            &ball(A),
            &Hit {
                me: A,
                rally: 1,
                source_seq: None,
            },
            &meta(1, 0),
            &r,
        )
    };
    let Err(Failure::Downstream { path, error }) = failed else {
        panic!("expected a downstream failure, got {failed:?}")
    };
    assert_eq!(path.len(), 3, "B, A, then B again: one step past the limit");
    assert_eq!(
        error,
        StepError::Kernel(KernelError::ChainTooDeep { limit: 2 })
    );
    assert_eq!(error.recovery(), Recovery::Bug);
    assert!(
        balls.0.read(&ball(A)).unwrap().is_none(),
        "nothing appended"
    );
}

#[test]
#[ignore = "proposal: failures are classified retryable or terminal for a wrapping runtime"]
fn failures_are_classified_for_a_wrapping_runtime() {
    let conflict = StoreError::Conflict {
        stream: ball(A),
        expected: 0,
        actual: 1,
    };
    let kernel = |e| Failure::<OutOfTurn>::Kernel(e).recovery();
    assert_eq!(conflict.recovery(), Recovery::Retry);
    assert_eq!(
        StoreError::Unavailable(Report::new("db-down", "the database is down")).recovery(),
        Recovery::Retry
    );
    assert_eq!(
        StoreError::Duplicate {
            stream: ball(A),
            request: annals::next::RequestId::from("req-1"),
            command: annals::next::CommandId::Origin { index: 0 },
            from: 3,
            to: 5
        }
        .recovery(),
        Recovery::Done
    );
    assert_eq!(
        StoreError::Unreadable {
            stream: ball(A),
            seq: 3,
            reason: Report::new("toy-unknown-type", "no decoder for /toy.v9.Hit"),
        }
        .recovery(),
        Recovery::Bug,
        "strict replay: an unreadable event is a bug, never retried"
    );
    assert_eq!(
        kernel(KernelError::WrongDomain {
            stream: key(1, "club"),
            expected: "ball"
        }),
        Recovery::Bug
    );
    assert_eq!(
        StoreError::Discontinuous {
            stream: ball(A),
            expected: 1,
            found: 2
        }
        .recovery(),
        Recovery::Bug
    );
    assert_eq!(kernel(KernelError::Store(conflict)), Recovery::Retry);
    assert_eq!(
        kernel(KernelError::Gap {
            stream: ball(A),
            gap: annals::next::Gap {
                expected: 1,
                found: 2
            }
        }),
        Recovery::Retry,
        "catch up, then retry"
    );
    assert_eq!(
        kernel(KernelError::Stale {
            stream: ball(A),
            expected: 1,
            actual: 2
        }),
        Recovery::Bug
    );
    assert_eq!(
        kernel(KernelError::ChainTooDeep { limit: 8 }),
        Recovery::Bug
    );
    assert_eq!(Failure::Rejected(OutOfTurn).recovery(), Recovery::Business);
    assert_eq!(
        Failure::<OutOfTurn>::Downstream {
            path: vec![],
            error: StepError::Rejected(Report::of(&OutOfTurn)),
        }
        .recovery(),
        Recovery::Business
    );
    assert_eq!(
        StepError::Kernel(KernelError::FactRefused {
            stream: ball(A),
            reason: Report::new("toy-member-retired", "the member has left"),
        })
        .recovery(),
        Recovery::Integrity
    );
}

#[test]
#[ignore = "proposal: request/response, the command handler returns what it recorded"]
fn the_command_handler_returns_what_the_command_recorded() {
    use crate::toys::{Drawn, Hat, HatCmd, HatEvent};

    let mut store = InMemoryStore::<HatEvent>::new();
    let hat = key(8, "hat");
    let r = rules();
    for (i, slip) in ["a", "b", "c"].iter().enumerate() {
        handle::<Hat, _, _>(
            store.begin(),
            &Routes::new(),
            &hat,
            &HatCmd::Put(slip.to_string()),
            &meta(i as u128, 0),
            &r,
            &mut (),
        )
        .unwrap();
    }
    let recorded = handle::<Hat, _, _>(
        store.begin(),
        &Routes::new(),
        &hat,
        &HatCmd::Draw { roll: 7 },
        &meta(9, 1_234),
        &r,
        &mut (),
    )
    .unwrap();
    assert_eq!(recorded.streams(), [(hat.clone(), 3, 4)]);
    let drawn = &recorded.events::<Hat>(&hat).unwrap()[0];
    assert_eq!(
        drawn.event,
        HatEvent::Drawn(Drawn {
            slip: "b".to_string(),
            roll: 7,
            at: 1_234,
        }),
        "the caller's random value and the handler's clock, in the payload"
    );
    assert_eq!(drawn.meta.recorded_at.0, 1_234);
}

#[test]
#[ignore = "proposal: a chain stays in its origin's edition"]
fn a_chain_in_an_edition_touches_only_that_edition() {
    let mut db = Db::default();
    let r = rules();
    let v2 = Edition::new("v2");
    let club_v2 = StreamKey::new("club", v2.clone(), Root::from(1));
    for (i, cmd) in [joined("ann"), ClubEvent::Paid(30)].iter().enumerate() {
        let recorded = handle::<Club, _, _>(
            db.begin(),
            &routes(&r),
            &club_v2,
            cmd,
            &meta(i as u128, 0),
            &r,
            &mut (),
        )
        .unwrap();
        assert!(
            recorded.streams().iter().all(|(k, _, _)| k.edition == v2),
            "every stream the chain touched is in v2: {:?}",
            recorded.streams()
        );
    }
    assert_eq!(
        Db::len(
            &mut db.ledgers,
            &StreamKey::new("ledger", v2, TREASURY_ROOT)
        ),
        2
    );
    assert_eq!(
        Db::len(&mut db.ledgers, &treasury()),
        0,
        "the main timeline's ledger is another stream"
    );
}

#[test]
#[ignore = "proposal: the origin's key must be the command's domain"]
fn a_command_for_a_key_of_another_domain_is_refused() {
    let mut db = Db::default();
    let r = rules();
    let failed = handle::<Club, _, _>(
        db.begin(),
        &routes(&r),
        &treasury(),
        &ClubEvent::Paid(1),
        &meta(1, 0),
        &r,
        &mut (),
    );
    let failure = failed.err().unwrap();
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::WrongDomain {
            stream: treasury(),
            expected: "club"
        })
    );
    assert_eq!(failure.recovery(), Recovery::Bug);
}
