//! Policies, translations and projections on the proposed kernel: a club,
//! its members' copies (a translation: facts, as hatpick's list feeds its
//! draw systems), its treasury's ledger (a policy: commands, rejectable)
//! and its pot (a projection).

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::fmt;

use annals::next::{
    deliver, facts_owed, inject, orders_owed, policy, projection, translation, CommandId,
    Committed, CorrelationId, Delivered, Domain, EventId, Fact, Failure, Gap, Header, KernelError,
    Order, Policy, Position, Projection, Reaction, Rulebook, Seq, Stream, StreamId, Tx,
};
use annals_diagnostic::{Arg, Diagnostic, Report};

use crate::toys::{header, meta, meta_in};

// ---------------------------------------------------------------- the source: a club

pub(crate) struct Club;

#[derive(Default, Clone, PartialEq, Debug)]
pub(crate) struct ClubState {
    pub(crate) members: BTreeSet<String>,
    pub(crate) leaving: Option<String>,
    pub(crate) pot: i64,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum ClubEvent {
    Joined(String),
    Left(String),
    Paid(i64),
}

impl Domain for Club {
    type State = ClubState;
    type Command = ClubEvent;
    type Event = ClubEvent;
    type Rejection = Infallible;
    type Rules = ();

    const NAME: &'static str = "club";
    const SCHEMA: u32 = 1;

    fn decide(cmd: &ClubEvent, tx: &mut Tx<'_, Self>) -> Result<(), Infallible> {
        tx.emit(cmd.clone());
        Ok(())
    }

    fn apply(s: &mut ClubState, e: &ClubEvent, (): &()) {
        s.leaving = None;
        match e {
            ClubEvent::Joined(name) => {
                s.members.insert(name.clone());
            }
            ClubEvent::Left(name) => {
                s.members.remove(name);
                s.leaving = Some(name.clone());
            }
            ClubEvent::Paid(n) => s.pot += n,
        }
    }
}

// ---------------------------------------------------------------- a translation target: a member's copy

pub(crate) struct Member;

#[derive(Default, Clone, PartialEq, Debug)]
pub(crate) struct MemberState {
    pub(crate) name: Option<String>,
    pub(crate) checkpoint: Option<Seq>,
    pub(crate) club: ClubState,
    pub(crate) departed: bool,
}

/// A member takes facts only. Its commands are its own business; none here.
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum MemberCmd {}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum MemberEvent {
    Enrolled {
        name: String,
        source_seq: Seq,
        club: ClubState,
    },
    FromClub {
        source_seq: Seq,
        event: ClubEvent,
    },
}

impl Domain for Member {
    type State = MemberState;
    type Command = MemberCmd;
    type Event = MemberEvent;
    type Rejection = Infallible;
    type Rules = ();

    const NAME: &'static str = "member";
    const SCHEMA: u32 = 1;

    fn decide(cmd: &MemberCmd, _: &mut Tx<'_, Self>) -> Result<(), Infallible> {
        match *cmd {}
    }

    fn apply(s: &mut MemberState, e: &MemberEvent, (): &()) {
        match e {
            MemberEvent::Enrolled {
                name,
                source_seq,
                club,
            } => {
                s.name = Some(name.clone());
                s.checkpoint = Some(*source_seq);
                s.club = club.clone();
            }
            MemberEvent::FromClub { source_seq, event } => {
                s.checkpoint = Some(*source_seq);
                Club::apply(&mut s.club, event, &());
                if matches!(event, ClubEvent::Left(name) if s.name.as_ref() == Some(name)) {
                    s.departed = true;
                }
            }
        }
    }

    /// Integrity only: a departed member is retired, and only enrolment
    /// creates one.
    fn admit_fact(s: &MemberState, fact: &MemberEvent) -> Result<(), Report> {
        if s.departed {
            return Err(Report::new("toy-member-retired", "the member has left"));
        }
        match fact {
            MemberEvent::FromClub { .. } if s.name.is_none() => {
                Err(Report::new("toy-member-unknown", "not enrolled"))
            }
            _ => Ok(()),
        }
    }
}

pub(crate) fn member_stream(name: &str) -> StreamId {
    StreamId::from(match name {
        "ann" => 2u64,
        "bob" => 3,
        _ => 9,
    })
}

pub(crate) struct ClubToMembers;

impl Reaction for ClubToMembers {
    type Source = Club;
    type Target = Member;
    type TargetId = String;

    const NAME: &'static str = "club-to-members";

    fn targets(club: &ClubState) -> Vec<String> {
        let mut targets: Vec<String> = club.members.iter().cloned().collect();
        targets.extend(club.leaving.iter().cloned());
        targets.sort();
        targets
    }

    fn stream(name: &String) -> StreamId {
        member_stream(name)
    }

    fn position(member: &MemberState) -> Position {
        member.checkpoint.map_or(Position::START, Position::at)
    }
}

impl annals::next::Translation for ClubToMembers {
    fn fact(target: &String, c: &Committed<'_, Club>) -> MemberEvent {
        match c.event() {
            ClubEvent::Joined(name) if name == target => MemberEvent::Enrolled {
                name: name.clone(),
                source_seq: c.seq(),
                club: c.state.clone(),
            },
            event => MemberEvent::FromClub {
                source_seq: c.seq(),
                event: event.clone(),
            },
        }
    }
}

// ---------------------------------------------------------------- a policy target: the treasury's ledger

pub(crate) struct Ledger;

#[derive(Default, Clone, PartialEq, Debug)]
pub(crate) struct LedgerState {
    pub(crate) checkpoint: Option<Seq>,
    pub(crate) cents: i64,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct Record {
    pub(crate) source_seq: Seq,
    pub(crate) cents: i64,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct Recorded(pub(crate) Record);

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct NotARefund;

impl fmt::Display for NotARefund {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the ledger records no refunds")
    }
}

impl Diagnostic for NotARefund {
    fn code(&self) -> &'static str {
        "toy-ledger-not-a-refund"
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![]
    }
}

impl Domain for Ledger {
    type State = LedgerState;
    type Command = Record;
    type Event = Recorded;
    type Rejection = NotARefund;
    type Rules = ();

    const NAME: &'static str = "ledger";
    const SCHEMA: u32 = 1;

    fn decide(cmd: &Record, tx: &mut Tx<'_, Self>) -> Result<(), NotARefund> {
        if cmd.cents < 0 {
            return Err(NotARefund);
        }
        tx.emit(Recorded(cmd.clone()));
        Ok(())
    }

    fn apply(s: &mut LedgerState, Recorded(r): &Recorded, (): &()) {
        s.checkpoint = Some(r.source_seq);
        s.cents += r.cents;
    }
}

pub(crate) const TREASURY: StreamId = StreamId(100);

/// Every payment, commanded into the treasury's ledger. The ledger is a
/// target from the club's first event on, so its position covers them all.
pub(crate) struct Dues;

impl Reaction for Dues {
    type Source = Club;
    type Target = Ledger;
    type TargetId = ();

    const NAME: &'static str = "dues";

    fn targets(_: &ClubState) -> Vec<()> {
        vec![()]
    }

    fn stream((): &()) -> StreamId {
        TREASURY
    }

    fn position(ledger: &LedgerState) -> Position {
        ledger.checkpoint.map_or(Position::START, Position::at)
    }
}

impl Policy for Dues {
    fn command((): &(), c: &Committed<'_, Club>) -> Record {
        let cents = match c.event() {
            ClubEvent::Paid(n) => *n,
            _ => 0,
        };
        Record {
            source_seq: c.seq(),
            cents,
        }
    }
}

// ---------------------------------------------------------------- a projection: the pot

#[derive(Default)]
pub(crate) struct Pot(pub(crate) i64);

impl Projection<Club> for Pot {
    fn project(&mut self, c: &Committed<'_, Club>) {
        if let ClubEvent::Paid(n) = c.event() {
            self.0 += n;
        }
    }
}

// ---------------------------------------------------------------- helpers

pub(crate) fn rules() -> Rulebook<()> {
    Rulebook::new("none", ())
}

pub(crate) fn joined(name: &str) -> ClubEvent {
    ClubEvent::Joined(name.to_string())
}

/// Seqs: 0 paid 5, 1 ann joined, 2 paid 3, 3 bob joined, 4 paid 2.
pub(crate) fn ann_and_bob() -> Stream<Club> {
    let mut s = Stream::new(header(1, "club"));
    for (i, e) in [
        ClubEvent::Paid(5),
        joined("ann"),
        ClubEvent::Paid(3),
        joined("bob"),
        ClubEvent::Paid(2),
    ]
    .iter()
    .enumerate()
    {
        s.execute(e, &meta(i as u128, 0), &rules(), &mut [])
            .unwrap();
    }
    s
}

pub(crate) fn copy_of(name: &str) -> Stream<Member> {
    Stream::new(Header {
        id: member_stream(name),
        domain: "member".to_string(),
    })
}

// ---------------------------------------------------------------- tests

#[test]
#[ignore = "proposal: one dispatch serves a projection, a policy and a translation"]
fn one_dispatch_serves_all_three_kinds() {
    let mut club = ann_and_bob();
    let mut pot = Pot::default();
    let mut orders: Vec<Order<Dues>> = Vec::new();
    let mut facts: Vec<Fact<ClubToMembers>> = Vec::new();
    {
        let mut p = projection(&mut pot);
        let mut d = policy::<Dues, _>(|o| orders.push(o));
        let mut t = translation::<ClubToMembers, _>(|f| facts.push(f));
        club.execute(
            &ClubEvent::Paid(7),
            &meta(9, 90),
            &rules(),
            &mut [&mut p, &mut d, &mut t],
        )
        .unwrap();
    }
    assert_eq!(pot.0, 7);
    assert_eq!(orders.len(), 1);
    assert_eq!(
        orders[0].output,
        Record {
            source_seq: 5,
            cents: 7
        }
    );
    assert_eq!(
        facts.iter().map(|f| f.target.as_str()).collect::<Vec<_>>(),
        ["ann", "bob"]
    );
    assert!(facts.iter().all(|f| f.output
        == MemberEvent::FromClub {
            source_seq: 5,
            event: ClubEvent::Paid(7)
        }));
}

#[test]
#[ignore = "proposal: metadata propagates through a reaction (A1, A2)"]
fn reactions_keep_correlation_and_point_their_cause_at_the_source_event() {
    let mut club = Stream::<Club>::new(header(1, "club"));
    let mut ann = copy_of("ann");
    let mut facts = Vec::new();
    let mut orders = Vec::new();
    {
        let mut t = translation::<ClubToMembers, _>(|f| facts.push(f));
        let mut d = policy::<Dues, _>(|o| orders.push(o));
        club.execute(
            &joined("ann"),
            &meta_in(100, 1_000, 555),
            &rules(),
            &mut [&mut t, &mut d],
        )
        .unwrap();
    }
    let source = EventId {
        stream: club.id(),
        seq: 0,
    };
    for meta in [&facts[0].meta, &orders[0].meta] {
        assert_eq!(meta.cause, Some(source));
        assert_eq!(meta.correlation, CorrelationId(555));
        assert!(matches!(
            &meta.command,
            CommandId::Reaction { source: s, index: 0, .. } if *s == source
        ));
        assert_eq!(meta.actor, club.events()[0].meta.actor);
    }
    assert_eq!(facts[0].source, source);
    assert_eq!(
        facts[0].meta.command,
        CommandId::Reaction {
            source,
            component: "club-to-members".to_string(),
            index: 0
        }
    );
    assert_eq!(
        orders[0].meta.command,
        CommandId::Reaction {
            source,
            component: "dues".to_string(),
            index: 0
        }
    );

    let Delivered::Next(events) = inject(&mut ann, &facts[0], &rules(), &mut []).unwrap() else {
        panic!("the first fact is recorded")
    };
    assert_eq!(events[0].meta.cause, Some(source), "on the target's event");
    assert_eq!(events[0].meta.correlation, CorrelationId(555));
}

#[test]
#[ignore = "proposal: a fact is placed, then admitted, never decided"]
fn a_fact_is_refused_only_for_integrity() {
    let mut club = ann_and_bob();
    let mut ann = copy_of("ann");
    let owed = facts_owed::<ClubToMembers>(&club, &rules(), &BTreeMap::new()).unwrap();
    for fact in &owed["ann"] {
        inject(&mut ann, fact, &rules(), &mut []).unwrap();
    }
    assert_eq!(ann.state().club, *club.state());
    let len = ann.len();

    for fact in &owed["ann"] {
        assert_eq!(
            inject(&mut ann, fact, &rules(), &mut []),
            Ok(Delivered::Redelivery)
        );
    }
    assert_eq!(ann.len(), len, "redelivery records nothing");

    let mut later = Vec::new();
    for (i, e) in [ClubEvent::Left("ann".to_string()), ClubEvent::Paid(1)]
        .iter()
        .enumerate()
    {
        let mut t = translation::<ClubToMembers, _>(|f| later.push(f));
        club.execute(e, &meta(50 + i as u128, 0), &rules(), &mut [&mut t])
            .unwrap();
    }
    let to_ann: Vec<_> = later.iter().filter(|f| f.target == "ann").collect();
    assert_eq!(to_ann.len(), 1, "her departure, then nothing");
    let departure = to_ann[0];
    let bobs_next = later.iter().find(|f| f.target == "bob").unwrap();
    let mut bob = copy_of("bob");
    assert_eq!(
        inject(&mut bob, bobs_next, &rules(), &mut []).unwrap_err(),
        KernelError::FactRefused {
            stream: bob.id(),
            reason: Report::new("toy-member-unknown", "not enrolled"),
        },
        "a copy never created has no position to place against: its admit_fact decides"
    );

    inject(&mut ann, departure, &rules(), &mut []).unwrap();
    assert!(ann.state().departed);
    let source = EventId {
        stream: club.id(),
        seq: 7,
    };
    let stray: Fact<ClubToMembers> = annals::next::Owed {
        target: "ann".to_string(),
        source,
        meta: annals::next::CommandMeta::reacting_to(
            &club.events()[6].meta,
            source,
            "club-to-members",
            0,
        ),
        output: MemberEvent::FromClub {
            source_seq: 7,
            event: ClubEvent::Paid(1),
        },
    };
    assert_eq!(
        inject(&mut ann, &stray, &rules(), &mut []).unwrap_err(),
        KernelError::Gap {
            stream: ann.id(),
            gap: Gap {
                expected: 6,
                found: 7
            }
        },
        "the position is checked before integrity"
    );
}

#[test]
#[ignore = "proposal: a policy's command goes through the target's decide"]
fn a_policys_command_may_be_rejected_by_its_target() {
    let mut club = Stream::<Club>::new(header(1, "club"));
    let mut ledger = Stream::<Ledger>::new(Header {
        id: TREASURY,
        domain: "ledger".to_string(),
    });
    let mut orders = Vec::new();
    for (i, n) in [4, -1].iter().enumerate() {
        let mut d = policy::<Dues, _>(|o| orders.push(o));
        club.execute(
            &ClubEvent::Paid(*n),
            &meta(i as u128, 0),
            &rules(),
            &mut [&mut d],
        )
        .unwrap();
    }
    assert!(matches!(
        deliver(&mut ledger, &orders[0], &rules(), &mut []),
        Ok(Delivered::Next(_))
    ));
    assert_eq!(
        deliver(&mut ledger, &orders[1], &rules(), &mut []).unwrap_err(),
        Failure::Rejected(NotARefund)
    );
    assert_eq!(ledger.state().cents, 4);
}

#[test]
#[ignore = "proposal: a backlog serves every target in one pass (B1)"]
fn a_backlog_serves_every_target_from_its_own_position() {
    let club = ann_and_bob();
    let positions = BTreeMap::from([("ann".to_string(), Position::at(2))]);
    let owed = facts_owed::<ClubToMembers>(&club, &rules(), &positions).unwrap();
    let seqs = |target: &str| -> Vec<Seq> { owed[target].iter().map(|f| f.source.seq).collect() };
    assert_eq!(seqs("ann"), [3, 4], "after her position");
    assert_eq!(seqs("bob"), [3, 4], "from where he first appears");
    assert!(matches!(
        owed["bob"][0].output,
        MemberEvent::Enrolled { .. }
    ));
    assert_eq!(owed.len(), 2);

    let orders = orders_owed::<Dues>(&club, &rules(), &BTreeMap::new()).unwrap();
    assert_eq!(orders[&()].len(), 5, "the same pass, for a policy");
}

/// Targets ann whenever the pot is even: so she can leave and come back,
/// which the contiguity invariant forbids.
pub(crate) struct Fickle;

impl Reaction for Fickle {
    type Source = Club;
    type Target = Member;
    type TargetId = String;

    const NAME: &'static str = "fickle";

    fn targets(club: &ClubState) -> Vec<String> {
        if club.pot % 2 == 0 {
            vec!["ann".to_string()]
        } else {
            vec![]
        }
    }

    fn stream(name: &String) -> StreamId {
        member_stream(name)
    }

    fn position(member: &MemberState) -> Position {
        member.checkpoint.map_or(Position::START, Position::at)
    }
}

impl annals::next::Translation for Fickle {
    fn fact(_: &String, c: &Committed<'_, Club>) -> MemberEvent {
        MemberEvent::FromClub {
            source_seq: c.seq(),
            event: c.event().clone(),
        }
    }
}

#[test]
#[ignore = "proposal: a backlog checks the contiguity invariant (B1)"]
fn a_backlog_refuses_a_target_that_comes_back() {
    let mut club = Stream::<Club>::new(header(1, "club"));
    for (i, n) in [2, 1, 1].iter().enumerate() {
        club.execute(&ClubEvent::Paid(*n), &meta(i as u128, 0), &rules(), &mut [])
            .unwrap();
    }
    assert_eq!(
        facts_owed::<Fickle>(&club, &rules(), &BTreeMap::new()).err(),
        Some(KernelError::TargetReturned {
            left: 1,
            returned: 2
        })
    );
}
