//! The command handler: a command and its reactions as one atomic chain.
//!
//! The club of `reactions.rs`, with its members' copies (a policy), its
//! treasury's ledger (a policy) and, below the ledger, an auditor (a
//! policy): club → ledger → auditor is three levels. A chain commits whole
//! or not at all: annals' deliberate divergence from angzarr, whose
//! reactions run asynchronously, each its own write.

use std::convert::Infallible;
use std::fmt;

use annals::next::{
    handle, Book, Causation, Committed, Domain, EventId, EventStore, Failure, InMemoryStore,
    InMemoryTx, KernelError, Malformed, Origin, Policy, Position, ProjectionRoute, Provenance,
    Reaction, Recovery, Root, Routes, Rulebook, Seq, Slice, Step, StepError, StoreError, StreamKey,
    Tx, UnitOfWork,
};
use annals_diagnostic::{Arg, Diagnostic, Report};

use crate::reactions::{
    joined, member_stream, rules, Club, ClubEvent, ClubToMembers, Dues, Ledger, LedgerState,
    MemberEvent, Pot, Recorded, TREASURY_ROOT,
};
use crate::toys::{ann_in, at, book_at, correlation, edition, key};

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
    type Fact = Infallible;
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

    fn ingest(_: &Audited, fact: &Infallible, (): &()) -> Vec<Approved> {
        match *fact {}
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
        .policy::<ClubToMembers>(r, r)
        .policy::<Dues>(r, r)
        .policy::<Audit>(r, r)
}

// ---------------------------------------------------------------- tests

/// Club `cmd` as the first command of request `n`.
fn club_book(cmd: ClubEvent, n: u128) -> annals::next::BookOf<Club> {
    book_at::<Club>(&club(), cmd, n)
}

#[test]
#[ignore = "proposal: a successful chain commits every stream atomically"]
fn a_successful_chain_commits_every_stream_with_causation_and_correlation() {
    let mut db = Db::default();
    let r = rules();
    handle::<Club, _, _>(
        db.begin(),
        &routes(&r),
        &club_book(joined("ann"), 1),
        &at(10),
        &r,
        &mut (),
    )
    .unwrap();
    let paid = Book::command(
        club(),
        ClubEvent::Paid(30),
        Origin::request(crate::toys::request(2), 0),
        ann_in(77),
    );
    let recorded =
        handle::<Club, _, _>(db.begin(), &routes(&r), &paid, &at(20), &r, &mut ()).unwrap();

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
    assert_eq!(record.meta.origin.cause(), Some(&paid));
    let approval = &recorded.events::<Auditor>(&auditor()).unwrap()[0];
    assert_eq!(
        approval.meta.origin,
        Origin::Causation(Causation {
            event: EventId {
                stream: treasury(),
                seq: 1
            },
            component: "audit".to_string(),
            index: 0
        }),
        "caused by the ledger's event, not the club's: recomputable from the log"
    );
    for meta in [&record.meta, &approval.meta] {
        assert_eq!(meta.correlation, correlation(77), "the whole chain's");
        assert_eq!(
            meta.actor,
            ann_in(77).actor,
            "on the origin's actor's behalf"
        );
        assert_eq!(meta.recorded_at.0, 20, "one instant for the chain");
        assert_eq!(meta.base_sequence, 1, "each stream's own head");
    }
    assert!(recorded.projector_failures().is_empty());
    assert_eq!(
        Db::len(&mut db.auditors, &auditor()),
        2,
        "committed, not just returned"
    );
}

#[test]
#[ignore = "proposal: a rejection three levels deep fails the origin and leaves every stream untouched"]
fn a_rejection_three_levels_deep_leaves_every_stream_untouched_and_returns_the_path() {
    let mut db = Db::default();
    let r = rules();
    handle::<Club, _, _>(
        db.begin(),
        &routes(&r),
        &club_book(joined("ann"), 1),
        &at(10),
        &r,
        &mut (),
    )
    .unwrap();

    let failed = handle::<Club, _, _>(
        db.begin(),
        &routes(&r),
        &club_book(ClubEvent::Paid(500), 2),
        &at(20),
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
                    cause: EventId {
                        stream: club(),
                        seq: 1
                    },
                },
                Step {
                    stream: auditor(),
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
    for (i, n) in [30, 500, 4].into_iter().enumerate() {
        let _ = handle::<Club, _, _>(
            db.begin(),
            &routes(&r),
            &club_book(ClubEvent::Paid(n), i as u128),
            &at(0),
            &r,
            &mut ProjectionRoute::<Club, _> {
                name: "pot",
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
            &club_book(ClubEvent::Paid(3), 1),
            &at(0),
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
    type Fact = Infallible;
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

    fn ingest(_: &Rally, fact: &Infallible, (): &()) -> Vec<Hit> {
        match *fact {}
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
    let serve = book_at::<Ball>(
        &ball(A),
        Hit {
            me: A,
            rally: 1,
            source_seq: None,
        },
        1,
    );
    let recorded =
        handle::<Ball, _, _>(balls.0.begin(), &routes, &serve, &at(0), &r, &mut ()).unwrap();
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
        let sampled = at(0);
        let mut chain = annals::next::Chain::new(&mut unit, &routes, &sampled).limit(2);
        let serve = book_at::<Ball>(
            &ball(A),
            Hit {
                me: A,
                rally: 1,
                source_seq: None,
            },
            1,
        );
        chain.execute::<Ball>(&serve, &r)
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
            origin: Origin::fact("pi_1", "Stripe webhook"),
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
        Recovery::Retry,
        "re-read, then re-issue against the new head"
    );
    assert_eq!(
        kernel(KernelError::Unresolved {
            stream: ball(A),
            expected: 1,
            actual: 2
        }),
        Recovery::Retry,
        "fails as stale would"
    );
    assert_eq!(
        kernel(KernelError::Malformed {
            stream: ball(A),
            malformed: Malformed::FactWithoutFactOrigin
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
}

#[test]
#[ignore = "proposal: request/response, the command handler returns what it recorded"]
fn the_command_handler_returns_what_the_command_recorded() {
    use crate::toys::{Drawn, Hat, HatCmd, HatEvent};

    let mut store = InMemoryStore::<HatEvent>::new();
    let hat = key(8, "hat");
    let r = rules();
    for (i, slip) in ["a", "b", "c"].into_iter().enumerate() {
        let put = book_at::<Hat>(&hat, HatCmd::Put(slip.to_string()), i as u128);
        handle::<Hat, _, _>(store.begin(), &Routes::new(), &put, &at(0), &r, &mut ()).unwrap();
    }
    let draw = book_at::<Hat>(&hat, HatCmd::Draw { roll: 7 }, 9);
    let recorded = handle::<Hat, _, _>(
        store.begin(),
        &Routes::new(),
        &draw,
        &at(1_234),
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
        "the caller's random value and the recorded instant, in the payload"
    );
    assert_eq!(drawn.meta.recorded_at.0, 1_234);
}

#[test]
#[ignore = "proposal: a chain stays in its origin's edition"]
fn a_chain_in_an_edition_touches_only_that_edition() {
    let mut db = Db::default();
    let r = rules();
    let v2 = edition("v2");
    let club_v2 = StreamKey::new("club", v2.clone(), Root::from(1));
    for (i, cmd) in [joined("ann"), ClubEvent::Paid(30)].into_iter().enumerate() {
        let b = book_at::<Club>(&club_v2, cmd, i as u128);
        let recorded =
            handle::<Club, _, _>(db.begin(), &routes(&r), &b, &at(0), &r, &mut ()).unwrap();
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
        &book_at::<Club>(&treasury(), ClubEvent::Paid(1), 1),
        &at(0),
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

// ---------------------------------------------------------------- keys: the two at-least-once edges

/// Account 1, opened by request 1.
fn opened(store: &mut InMemoryStore<crate::toys::AccountEvent>) -> StreamKey {
    use crate::toys::{fees, Account, AccountCmd};
    let account = key(1, "account");
    let open = book_at::<Account>(&account, AccountCmd::Open, 1);
    handle::<Account, _, _>(
        store.begin(),
        &Routes::new(),
        &open,
        &at(1),
        &fees("fees-1", 0),
        &mut (),
    )
    .unwrap();
    account
}

#[test]
#[ignore = "proposal: a redelivered webhook fact is a duplicate, by (stream, external id)"]
fn a_redelivered_webhook_fact_is_a_duplicate() {
    use crate::toys::{fees, Account, AccountFact};
    let mut store = InMemoryStore::new();
    let account = opened(&mut store);
    let r = fees("fees-1", 0);
    let payment = Book::fact(
        account.clone(),
        AccountFact::PaymentReceived(5),
        Origin::fact("pi_1", "Stripe webhook"),
        Provenance::default(),
    );
    handle::<Account, _, _>(store.begin(), &Routes::new(), &payment, &at(2), &r, &mut ()).unwrap();

    let again =
        handle::<Account, _, _>(store.begin(), &Routes::new(), &payment, &at(3), &r, &mut ());
    let failure = again.err().unwrap();
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::Store(StoreError::Duplicate {
            stream: account.clone(),
            origin: Origin::fact("pi_1", "Stripe webhook"),
            from: 1,
            to: 2
        }))
    );
    assert_eq!(
        failure.recovery(),
        Recovery::Done,
        "read the events instead"
    );
    assert_eq!(
        store.read(&account).unwrap().unwrap().len,
        2,
        "recorded once"
    );
}

#[test]
#[ignore = "proposal: an HTTP retry of a keyed request is a duplicate; a rejection is released"]
fn a_retried_request_is_a_duplicate_and_a_rejection_is_released() {
    use crate::toys::{fees, Account, AccountCmd};
    let mut store = InMemoryStore::new();
    let account = opened(&mut store);
    let r = fees("fees-1", 0);
    let run = |store: &mut InMemoryStore<_>, b: &annals::next::BookOf<Account>| {
        handle::<Account, _, _>(store.begin(), &Routes::new(), b, &at(2), &r, &mut ())
    };

    let withdraw = book_at::<Account>(&account, AccountCmd::Withdraw(5), 7);
    assert!(matches!(
        run(&mut store, &withdraw),
        Err(Failure::Rejected(_))
    ));
    let deposit = book_at::<Account>(&account, AccountCmd::Deposit(10), 8);
    run(&mut store, &deposit).unwrap();
    assert!(
        run(&mut store, &withdraw).is_ok(),
        "the rejected request 7 was never recorded: its retry runs again"
    );
    assert!(matches!(
        run(&mut store, &deposit),
        Err(Failure::Kernel(KernelError::Store(StoreError::Duplicate {
            from: 1,
            to: 2,
            ..
        })))
    ));

    let keyless = Book::command(
        account.clone(),
        AccountCmd::Deposit(1),
        Origin::keyless(),
        Provenance::default(),
    );
    run(&mut store, &keyless).unwrap();
    run(&mut store, &keyless).unwrap();
    assert_eq!(
        store.read(&account).unwrap().unwrap().len,
        5,
        "a keyless request is never deduplicated"
    );
}
