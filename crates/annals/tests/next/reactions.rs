//! Policies and projections on the proposed kernel: a club; its members'
//! copies, kept by a policy whose commands each member records in its own
//! events (as hatpick's list feeds its draw systems); its treasury's ledger
//! (a policy whose target may refuse); and its pot (a projection).

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::fmt;

use annals::next::{
    deliver, orders_owed, policy, projection, Body, Book, Causation, Committed, Delivered, Domain,
    EventId, Failure, Gap, KernelError, Order, Origin, Policy, Position, Projection, Provenance,
    Reaction, Root, Rulebook, Seq, Stream, StreamKey, Tx,
};
use annals_diagnostic::{Arg, Diagnostic};

use crate::toys::{ann, ann_in, at, book, correlation, edition, stream};

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
    type Fact = Infallible;
    type Event = ClubEvent;
    type Rejection = Infallible;
    type Rules = ();

    const NAME: &'static str = "club";

    fn decide(cmd: &ClubEvent, tx: &mut Tx<'_, Self>) -> Result<(), Infallible> {
        tx.emit(cmd.clone());
        Ok(())
    }

    fn ingest(_: &ClubState, fact: &Infallible, (): &()) -> Vec<ClubEvent> {
        match *fact {}
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

// ---------------------------------------------------------------- a policy target: a member's copy

pub(crate) struct Member;

#[derive(Default, Clone, PartialEq, Debug)]
pub(crate) struct MemberState {
    pub(crate) name: Option<String>,
    pub(crate) checkpoint: Option<Seq>,
    pub(crate) club: ClubState,
    pub(crate) departed: bool,
}

/// The club's changes, commanded into a member's copy: a normal command,
/// decided by the member. Each carries its source seq, because `apply`
/// never sees metadata and the member's position is a fold.
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum MemberCmd {
    Enroll {
        name: String,
        source_seq: Seq,
        club: ClubState,
    },
    ApplyClubChange {
        source_seq: Seq,
        event: ClubEvent,
    },
}

/// What the member records: its own events, which reuse the club's fold.
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum MemberEvent {
    Enrolled {
        name: String,
        source_seq: Seq,
        club: ClubState,
    },
    ClubChanged {
        source_seq: Seq,
        event: ClubEvent,
    },
}

/// A member refuses a club change only when it cannot be one: never
/// enrolled, or departed. Neither happens while the club's targets keep the
/// contiguity invariant; in a chain either fails the club's command whole.
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum MemberRefused {
    NotEnrolled,
    Retired,
}

impl fmt::Display for MemberRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MemberRefused::NotEnrolled => f.write_str("not enrolled"),
            MemberRefused::Retired => f.write_str("the member has left"),
        }
    }
}

impl Diagnostic for MemberRefused {
    fn code(&self) -> &'static str {
        match self {
            MemberRefused::NotEnrolled => "toy-member-not-enrolled",
            MemberRefused::Retired => "toy-member-retired",
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![]
    }
}

impl Domain for Member {
    type State = MemberState;
    type Command = MemberCmd;
    type Fact = Infallible;
    type Event = MemberEvent;
    type Rejection = MemberRefused;
    type Rules = ();

    const NAME: &'static str = "member";

    fn decide(cmd: &MemberCmd, tx: &mut Tx<'_, Self>) -> Result<(), MemberRefused> {
        let s = tx.state();
        if s.departed {
            return Err(MemberRefused::Retired);
        }
        match cmd.clone() {
            MemberCmd::Enroll {
                name,
                source_seq,
                club,
            } => {
                tx.emit(MemberEvent::Enrolled {
                    name,
                    source_seq,
                    club,
                });
            }
            MemberCmd::ApplyClubChange { source_seq, event } => {
                if s.name.is_none() {
                    return Err(MemberRefused::NotEnrolled);
                }
                tx.emit(MemberEvent::ClubChanged { source_seq, event });
            }
        }
        Ok(())
    }

    fn ingest(_: &MemberState, fact: &Infallible, (): &()) -> Vec<MemberEvent> {
        match *fact {}
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
            MemberEvent::ClubChanged { source_seq, event } => {
                s.checkpoint = Some(*source_seq);
                Club::apply(&mut s.club, event, &());
                if matches!(event, ClubEvent::Left(name) if s.name.as_ref() == Some(name)) {
                    s.departed = true;
                }
            }
        }
    }
}

pub(crate) fn member_root(name: &str) -> Root {
    Root::from(match name {
        "ann" => 2u64,
        "bob" => 3,
        _ => 9,
    })
}

/// A member's copy, on the main timeline.
pub(crate) fn member_stream(name: &str) -> StreamKey {
    StreamKey::main("member", member_root(name))
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

    fn root(name: &String) -> Root {
        member_root(name)
    }

    fn position(member: &MemberState) -> Position {
        member.checkpoint.map_or(Position::START, Position::at)
    }
}

impl Policy for ClubToMembers {
    fn command(target: &String, c: &Committed<'_, Club>) -> MemberCmd {
        match c.event() {
            ClubEvent::Joined(name) if name == target => MemberCmd::Enroll {
                name: name.clone(),
                source_seq: c.seq(),
                club: c.state.clone(),
            },
            event => MemberCmd::ApplyClubChange {
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
    type Fact = Infallible;
    type Event = Recorded;
    type Rejection = NotARefund;
    type Rules = ();

    const NAME: &'static str = "ledger";

    fn decide(cmd: &Record, tx: &mut Tx<'_, Self>) -> Result<(), NotARefund> {
        if cmd.cents < 0 {
            return Err(NotARefund);
        }
        tx.emit(Recorded(cmd.clone()));
        Ok(())
    }

    fn ingest(_: &LedgerState, fact: &Infallible, (): &()) -> Vec<Recorded> {
        match *fact {}
    }

    fn apply(s: &mut LedgerState, Recorded(r): &Recorded, (): &()) {
        s.checkpoint = Some(r.source_seq);
        s.cents += r.cents;
    }
}

pub(crate) const TREASURY_ROOT: Root = Root(100);

/// The treasury's ledger, on the main timeline.
pub(crate) fn treasury() -> StreamKey {
    StreamKey::main("ledger", TREASURY_ROOT)
}

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

    fn root((): &()) -> Root {
        TREASURY_ROOT
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
    let mut s = stream::<Club>(1);
    for (i, e) in [
        ClubEvent::Paid(5),
        joined("ann"),
        ClubEvent::Paid(3),
        joined("bob"),
        ClubEvent::Paid(2),
    ]
    .into_iter()
    .enumerate()
    {
        let b = book(&s, e, i as u128);
        s.execute(&b, &at(0), &rules(), &mut []).unwrap();
    }
    s
}

pub(crate) fn copy_of(name: &str) -> Stream<Member> {
    Stream::new(None, member_root(name))
}

fn body<C: Clone, F>(order: &Book<C, F>) -> C {
    match &order.body {
        Body::Command(c) => c.clone(),
        Body::Fact(_) => panic!("a policy's order is always a command"),
    }
}

// ---------------------------------------------------------------- tests

#[test]
#[ignore = "proposal: one dispatch serves a projection and policies"]
fn one_dispatch_serves_a_projection_and_policies() {
    let mut club = ann_and_bob();
    let mut pot = Pot::default();
    let mut dues: Vec<Order<Dues>> = Vec::new();
    let mut copies: Vec<Order<ClubToMembers>> = Vec::new();
    {
        let mut p = projection(&mut pot);
        let mut d = policy::<Dues, _>(|o| dues.push(o));
        let mut m = policy::<ClubToMembers, _>(|o| copies.push(o));
        let paid = book(&club, ClubEvent::Paid(7), 9);
        club.execute(&paid, &at(90), &rules(), &mut [&mut p, &mut d, &mut m])
            .unwrap();
    }
    assert_eq!(pot.0, 7);
    assert_eq!(dues.len(), 1);
    assert_eq!(
        body(&dues[0].book),
        Record {
            source_seq: 5,
            cents: 7
        }
    );
    assert_eq!(dues[0].book.stream, treasury());
    assert_eq!(
        copies.iter().map(|o| o.target.as_str()).collect::<Vec<_>>(),
        ["ann", "bob"]
    );
    assert!(copies.iter().all(|o| body(&o.book)
        == MemberCmd::ApplyClubChange {
            source_seq: 5,
            event: ClubEvent::Paid(7)
        }));
}

#[test]
#[ignore = "proposal: a policy's command carries the trigger's provenance and a causation (A1, A2)"]
fn a_policys_command_carries_the_triggers_provenance_and_a_causation() {
    let mut club = stream::<Club>(1);
    let mut ann_copy = copy_of("ann");
    let mut copies = Vec::new();
    {
        let mut m = policy::<ClubToMembers, _>(|o| copies.push(o));
        for (i, name) in ["ann", "bob"].into_iter().enumerate() {
            let join = Book::command(
                club.key().clone(),
                joined(name),
                Origin::request(crate::toys::request(100 + i as u128)),
                ann_in(555),
            )
            .fingerprinted(crate::toys::fingerprint(i as u8));
            club.execute(&join, &at(1_000), &rules(), &mut [&mut m])
                .unwrap();
        }
    }
    let bobs_join = EventId {
        stream: club.key().clone(),
        seq: 1,
    };
    let to_bob: Vec<&Order<ClubToMembers>> = copies
        .iter()
        .filter(|o| o.book.origin.as_ref().and_then(Origin::cause) == Some(&bobs_join))
        .collect();
    assert_eq!(
        to_bob
            .iter()
            .map(|o| (o.target.as_str(), o.book.origin.clone()))
            .collect::<Vec<_>>(),
        [
            (
                "ann",
                Some(Origin::Causation(Causation {
                    event: bobs_join.clone(),
                    component: "club-to-members".to_string(),
                    index: 0
                }))
            ),
            (
                "bob",
                Some(Origin::Causation(Causation {
                    event: bobs_join.clone(),
                    component: "club-to-members".to_string(),
                    index: 1
                }))
            ),
        ],
        "one causation per command the policy issued for the event"
    );
    for order in &copies {
        assert_eq!(
            order.book.provenance,
            Provenance {
                actor: ann().actor,
                correlation: Some(correlation(555)),
            },
            "the trigger's actor and correlation"
        );
        assert_eq!(order.book.handling, Default::default(), "no handling");
        assert_eq!(order.book.validate(), Ok(()));
    }

    let Delivered::Next(events) =
        deliver(&mut ann_copy, &copies[0], &at(2_000), &rules(), &mut []).unwrap()
    else {
        panic!("the first command is decided")
    };
    let joined_ann = EventId {
        stream: club.key().clone(),
        seq: 0,
    };
    assert_eq!(events[0].meta.origin.cause(), Some(&joined_ann));
    assert_eq!(events[0].meta.correlation, correlation(555));
    assert_eq!(events[0].meta.actor, ann().actor);
}

#[test]
#[ignore = "proposal: a propagated change is placed by position, then decided like any command"]
fn a_propagated_change_is_placed_then_decided() {
    let mut club = ann_and_bob();
    let mut ann_copy = copy_of("ann");
    let owed = orders_owed::<ClubToMembers>(&club, &rules(), &BTreeMap::new()).unwrap();
    for order in &owed["ann"] {
        deliver(&mut ann_copy, order, &at(1), &rules(), &mut []).unwrap();
    }
    assert_eq!(ann_copy.state().club, *club.state());
    let len = ann_copy.len();
    for order in &owed["ann"] {
        assert_eq!(
            deliver(&mut ann_copy, order, &at(1), &rules(), &mut []),
            Ok(Delivered::Redelivery)
        );
    }
    assert_eq!(ann_copy.len(), len, "a redelivery decides nothing");

    let mut later = Vec::new();
    for (i, e) in [ClubEvent::Left("ann".to_string()), ClubEvent::Paid(1)]
        .into_iter()
        .enumerate()
    {
        let mut m = policy::<ClubToMembers, _>(|o| later.push(o));
        let b = book(&club, e, 50 + i as u128);
        club.execute(&b, &at(2), &rules(), &mut [&mut m]).unwrap();
    }
    let to_ann: Vec<_> = later.iter().filter(|o| o.target == "ann").collect();
    assert_eq!(
        to_ann.len(),
        1,
        "her departure, then nothing: she left the targets"
    );

    let bobs_next = later.iter().find(|o| o.target == "bob").unwrap();
    let mut bob_copy = copy_of("bob");
    assert_eq!(
        deliver(&mut bob_copy, bobs_next, &at(3), &rules(), &mut []).unwrap_err(),
        Failure::Rejected(MemberRefused::NotEnrolled),
        "a copy never created has no position to place against: its decide refuses"
    );

    deliver(&mut ann_copy, to_ann[0], &at(3), &rules(), &mut []).unwrap();
    assert!(ann_copy.state().departed);
    let stray = |source_seq: Seq| Order::<ClubToMembers> {
        target: "ann".to_string(),
        book: Book::command(
            member_stream("ann"),
            MemberCmd::ApplyClubChange {
                source_seq,
                event: ClubEvent::Paid(1),
            },
            Origin::Causation(Causation {
                event: EventId {
                    stream: club.key().clone(),
                    seq: source_seq,
                },
                component: "club-to-members".to_string(),
                index: 0,
            }),
            // A policy's command carries its trigger's correlation.
            Provenance {
                actor: None,
                correlation: Some(correlation(555)),
            },
        ),
    };
    assert_eq!(
        deliver(&mut ann_copy, &stray(7), &at(4), &rules(), &mut []).unwrap_err(),
        Failure::Kernel(KernelError::Gap {
            stream: ann_copy.key().clone(),
            gap: Gap {
                expected: 6,
                found: 7
            }
        }),
        "the position is checked before decide"
    );
    assert_eq!(
        deliver(&mut ann_copy, &stray(6), &at(4), &rules(), &mut []).unwrap_err(),
        Failure::Rejected(MemberRefused::Retired),
        "a stray past retirement is refused, loudly"
    );
}

#[test]
#[ignore = "proposal: a policy's command goes through the target's decide"]
fn a_policys_command_may_be_rejected_by_its_target() {
    let mut club = stream::<Club>(1);
    let mut ledger = Stream::<Ledger>::new(None, TREASURY_ROOT);
    let mut orders = Vec::new();
    for (i, n) in [4, -1].into_iter().enumerate() {
        let mut d = policy::<Dues, _>(|o| orders.push(o));
        let b = book(&club, ClubEvent::Paid(n), i as u128);
        club.execute(&b, &at(0), &rules(), &mut [&mut d]).unwrap();
    }
    assert!(matches!(
        deliver(&mut ledger, &orders[0], &at(1), &rules(), &mut []),
        Ok(Delivered::Next(_))
    ));
    assert_eq!(
        deliver(&mut ledger, &orders[1], &at(1), &rules(), &mut []).unwrap_err(),
        Failure::Rejected(NotARefund)
    );
    assert_eq!(ledger.state().cents, 4);
}

#[test]
#[ignore = "proposal: a backlog serves every target in one pass (B1)"]
fn a_backlog_serves_every_target_from_its_own_position() {
    let club = ann_and_bob();
    let positions = BTreeMap::from([("ann".to_string(), Position::at(2))]);
    let owed = orders_owed::<ClubToMembers>(&club, &rules(), &positions).unwrap();
    let seqs = |target: &str| -> Vec<Seq> {
        owed[target]
            .iter()
            .map(|o| o.book.origin.as_ref().and_then(Origin::cause).unwrap().seq)
            .collect()
    };
    assert_eq!(seqs("ann"), [3, 4], "after her position");
    assert_eq!(seqs("bob"), [3, 4], "from where he first appears");
    assert!(matches!(
        body(&owed["bob"][0].book),
        MemberCmd::Enroll { .. }
    ));
    assert_eq!(owed.len(), 2);

    let orders = orders_owed::<Dues>(&club, &rules(), &BTreeMap::new()).unwrap();
    assert_eq!(orders[&()].len(), 5, "the same pass, for another policy");
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

    fn root(name: &String) -> Root {
        member_root(name)
    }

    fn position(member: &MemberState) -> Position {
        member.checkpoint.map_or(Position::START, Position::at)
    }
}

impl Policy for Fickle {
    fn command(_: &String, c: &Committed<'_, Club>) -> MemberCmd {
        MemberCmd::ApplyClubChange {
            source_seq: c.seq(),
            event: c.event().clone(),
        }
    }
}

#[test]
#[ignore = "proposal: a backlog checks the contiguity invariant (B1)"]
fn a_backlog_refuses_a_target_that_comes_back() {
    let mut club = stream::<Club>(1);
    for (i, n) in [2, 1, 1].into_iter().enumerate() {
        let b = book(&club, ClubEvent::Paid(n), i as u128);
        club.execute(&b, &at(0), &rules(), &mut []).unwrap();
    }
    assert_eq!(
        orders_owed::<Fickle>(&club, &rules(), &BTreeMap::new()).err(),
        Some(KernelError::TargetReturned {
            left: 1,
            returned: 2
        })
    );
}

#[test]
#[ignore = "proposal: a reaction's target is in its source's edition"]
fn a_reaction_never_crosses_timelines() {
    let source = StreamKey::new("club", edition("v2"), Root::from(1));
    assert_eq!(
        annals::next::target_key::<ClubToMembers>(&source, &"ann".to_string()),
        StreamKey::new("member", edition("v2"), member_root("ann")),
        "the target's domain, the source's edition, the target's root"
    );

    let mut club = Stream::<Club>::new(edition("v2"), Root::from(1));
    let mut orders = Vec::new();
    {
        let mut m = policy::<ClubToMembers, _>(|o| orders.push(o));
        let b = book(&club, joined("ann"), 1);
        club.execute(&b, &at(0), &rules(), &mut [&mut m]).unwrap();
    }
    let order: &Order<ClubToMembers> = &orders[0];
    assert_eq!(
        order.book.stream,
        StreamKey::new("member", edition("v2"), member_root("ann"))
    );
    assert_eq!(
        order
            .book
            .origin
            .as_ref()
            .and_then(Origin::cause)
            .map(|c| &c.stream),
        Some(&source),
        "the causation names the source in its edition"
    );
}
