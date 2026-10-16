//! Policies, proven on a toy club and the members' copies of it: targets from
//! the source's fold, backlogs from any checkpoint, retirement, and
//! idempotent delivery.

use std::collections::BTreeSet;
use std::fmt;

use annals::policy::{admit, backlog, Admit, Gap, Injected, Policy};
use annals::{Domain, Envelope, Header, Seq, Stream, StreamId, Tx};
use annals_diagnostic::{Arg, Diagnostic};

fn header(id: u64, domain: &str) -> Header {
    Header {
        id: StreamId(id),
        domain: domain.to_string(),
        schema: 1,
        rules: "test".to_string(),
    }
}

// ---------------------------------------------------------------- the source: a club

struct Club;

#[derive(Default, Clone, PartialEq, Debug)]
struct ClubState {
    members: BTreeSet<String>,
    /// Who the last event saw leave: still owed that one event.
    leaving: Option<String>,
    pot: i64,
}

#[derive(Clone, PartialEq, Debug)]
enum ClubEvent {
    Joined(String),
    Left(String),
    Paid(i64),
}

impl Domain for Club {
    type State = ClubState;
    type Command = ClubEvent;
    type Event = ClubEvent;
    type Rejection = std::convert::Infallible;
    type Rules = ();
    type Ctx = ();

    fn decide(cmd: &ClubEvent, tx: &mut Tx<Self>) -> Result<(), Self::Rejection> {
        tx.emit(cmd.clone());
        Ok(())
    }

    fn apply(s: &mut ClubState, e: &ClubEvent, _: &()) {
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

// ---------------------------------------------------------------- the target: a member's copy

struct Member;

#[derive(Default, Clone, PartialEq, Debug)]
struct MemberState {
    name: Option<String>,
    /// The last club seq folded into `club`.
    checkpoint: Option<Seq>,
    club: ClubState,
    departed: bool,
}

#[derive(Clone, PartialEq, Debug)]
enum MemberCmd {
    Enrol {
        name: String,
        source_seq: Seq,
        club: ClubState,
    },
    Inject(Injected<ClubEvent>),
}

#[derive(Clone, PartialEq, Debug)]
enum MemberEvent {
    Enrolled {
        name: String,
        source_seq: Seq,
        club: ClubState,
    },
    FromClub(Injected<ClubEvent>),
}

#[derive(Clone, PartialEq, Debug)]
enum MemberRefused {
    AlreadyEnrolled,
    NotEnrolled,
    Departed,
    Gap(Gap),
}

impl fmt::Display for MemberRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MemberRefused::AlreadyEnrolled => f.write_str("already enrolled"),
            MemberRefused::NotEnrolled => f.write_str("not enrolled"),
            MemberRefused::Departed => f.write_str("the member has left"),
            MemberRefused::Gap(gap) => gap.fmt(f),
        }
    }
}

impl Diagnostic for MemberRefused {
    fn code(&self) -> &'static str {
        match self {
            MemberRefused::AlreadyEnrolled => "toy-member-already-enrolled",
            MemberRefused::NotEnrolled => "toy-member-not-enrolled",
            MemberRefused::Departed => "toy-member-departed",
            MemberRefused::Gap(_) => "toy-member-gap",
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match self {
            MemberRefused::Gap(gap) => vec![("cause", Arg::nested(gap))],
            _ => vec![],
        }
    }
}

impl Domain for Member {
    type State = MemberState;
    type Command = MemberCmd;
    type Event = MemberEvent;
    type Rejection = MemberRefused;
    type Rules = ();
    type Ctx = ();

    fn decide(cmd: &MemberCmd, tx: &mut Tx<Self>) -> Result<(), MemberRefused> {
        let s = tx.state();
        match cmd {
            MemberCmd::Enrol {
                name,
                source_seq,
                club,
            } => {
                if s.checkpoint.is_some() {
                    return Err(MemberRefused::AlreadyEnrolled);
                }
                tx.emit(MemberEvent::Enrolled {
                    name: name.clone(),
                    source_seq: *source_seq,
                    club: club.clone(),
                });
            }
            MemberCmd::Inject(injected) => {
                let Some(checkpoint) = s.checkpoint else {
                    return Err(MemberRefused::NotEnrolled);
                };
                match admit(checkpoint, injected.source_seq).map_err(MemberRefused::Gap)? {
                    Admit::Redelivery => {}
                    Admit::Next if s.departed => return Err(MemberRefused::Departed),
                    Admit::Next => {
                        tx.emit(MemberEvent::FromClub(injected.clone()));
                    }
                }
            }
        }
        Ok(())
    }

    fn apply(s: &mut MemberState, e: &MemberEvent, rules: &()) {
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
            MemberEvent::FromClub(injected) => {
                s.checkpoint = Some(injected.source_seq);
                Club::apply(&mut s.club, &injected.event, rules);
                if let ClubEvent::Left(name) = &injected.event {
                    if s.name.as_ref() == Some(name) {
                        s.departed = true;
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------- the policy

struct ClubToMembers;

impl Policy for ClubToMembers {
    type Source = Club;
    type Target = Member;
    type TargetId = String;

    fn targets(club: &ClubState) -> Vec<String> {
        let mut targets: Vec<String> = club.members.iter().cloned().collect();
        targets.extend(club.leaving.iter().cloned());
        targets.sort();
        targets
    }

    fn command(target: &String, club: &ClubState, envelope: &Envelope<ClubEvent>) -> MemberCmd {
        match &envelope.event {
            ClubEvent::Joined(name) if name == target => MemberCmd::Enrol {
                name: name.clone(),
                source_seq: envelope.seq,
                club: club.clone(),
            },
            event => MemberCmd::Inject(Injected {
                source_seq: envelope.seq,
                event: event.clone(),
            }),
        }
    }
}

// ---------------------------------------------------------------- helpers

fn club(events: &[ClubEvent]) -> Stream<Club> {
    let mut s = Stream::new(header(1, "club"));
    for e in events {
        s.execute(e, &(), &mut ()).unwrap();
    }
    s
}

fn member(id: u64) -> Stream<Member> {
    Stream::new(header(id, "member"))
}

fn joined(name: &str) -> ClubEvent {
    ClubEvent::Joined(name.to_string())
}

fn left(name: &str) -> ClubEvent {
    ClubEvent::Left(name.to_string())
}

fn inject(source: &Stream<Club>, seq: Seq) -> MemberCmd {
    MemberCmd::Inject(Injected {
        source_seq: seq,
        event: source.events()[seq as usize].event.clone(),
    })
}

/// Executes `name`'s backlog on its copy, from the copy's own checkpoint,
/// and returns how many events that emitted.
fn deliver(source: &Stream<Club>, name: &str, copy: &mut Stream<Member>) -> usize {
    let checkpoint = copy.state().checkpoint;
    let mut emitted = 0;
    for cmd in backlog::<ClubToMembers>(source, &(), &name.to_string(), checkpoint) {
        emitted += copy.execute(&cmd, &(), &mut ()).unwrap().len();
    }
    emitted
}

/// Seqs: 0 paid 5, 1 ann joined, 2 paid 3, 3 bob joined, 4 paid 2.
fn ann_and_bob() -> Stream<Club> {
    club(&[
        ClubEvent::Paid(5),
        joined("ann"),
        ClubEvent::Paid(3),
        joined("bob"),
        ClubEvent::Paid(2),
    ])
}

// ---------------------------------------------------------------- tests

#[test]
fn the_targets_are_a_fold_of_the_source() {
    let mut s = ann_and_bob();
    assert_eq!(ClubToMembers::targets(s.state()), ["ann", "bob"]);
    s.execute(&left("ann"), &(), &mut ()).unwrap();
    assert_eq!(
        ClubToMembers::targets(s.state()),
        ["ann", "bob"],
        "ann is owed her own departure"
    );
    s.execute(&ClubEvent::Paid(1), &(), &mut ()).unwrap();
    assert_eq!(ClubToMembers::targets(s.state()), ["bob"]);
}

#[test]
fn a_backlog_with_no_checkpoint_starts_where_the_target_first_appears() {
    let s = ann_and_bob();
    let cmds = backlog::<ClubToMembers>(&s, &(), &"bob".to_string(), None);
    assert_eq!(
        cmds,
        [
            MemberCmd::Enrol {
                name: "bob".to_string(),
                source_seq: 3,
                club: s.state_at(4, &()),
            },
            inject(&s, 4),
        ],
        "the snapshot is the state after the event that made bob a target"
    );

    let mut bob = member(2);
    assert_eq!(deliver(&s, "bob", &mut bob), 2);
    assert_eq!(bob.state().club, *s.state());
    assert_eq!(bob.state().checkpoint, Some(4));
}

#[test]
fn a_backlog_from_a_mid_stream_checkpoint_holds_only_what_follows_it() {
    let mut s = ann_and_bob();
    let mut ann = member(1);
    deliver(&s, "ann", &mut ann);
    assert_eq!(ann.state().checkpoint, Some(4));

    s.execute(&ClubEvent::Paid(7), &(), &mut ()).unwrap();
    s.execute(&joined("cy"), &(), &mut ()).unwrap();
    assert_eq!(
        backlog::<ClubToMembers>(&s, &(), &"ann".to_string(), Some(4)),
        [inject(&s, 5), inject(&s, 6)]
    );
    assert_eq!(
        backlog::<ClubToMembers>(&s, &(), &"ann".to_string(), Some(2)),
        [inject(&s, 3), inject(&s, 4), inject(&s, 5), inject(&s, 6)]
    );

    assert_eq!(deliver(&s, "ann", &mut ann), 2);
    assert_eq!(
        ann.state().club,
        *s.state(),
        "the source's own apply folds the copy"
    );
    assert!(
        backlog::<ClubToMembers>(&s, &(), &"ann".to_string(), Some(6)).is_empty(),
        "up to date: checkpoint + 1 is the source's length"
    );
}

#[test]
fn a_retired_target_receives_its_retirement_and_nothing_after() {
    let mut s = ann_and_bob();
    let mut ann = member(1);
    deliver(&s, "ann", &mut ann);

    s.execute(&left("ann"), &(), &mut ()).unwrap();
    s.execute(&ClubEvent::Paid(9), &(), &mut ()).unwrap();
    assert_eq!(
        backlog::<ClubToMembers>(&s, &(), &"ann".to_string(), Some(4)),
        [inject(&s, 5)],
        "her departure, then nothing"
    );
    deliver(&s, "ann", &mut ann);
    assert!(ann.state().departed);
    assert!(
        backlog::<ClubToMembers>(&s, &(), &"ann".to_string(), ann.state().checkpoint).is_empty()
    );

    let stray = ann.execute(&inject(&s, 6), &(), &mut ());
    assert_eq!(
        stray.unwrap_err(),
        MemberRefused::Departed,
        "a stray delivery is refused"
    );
}

#[test]
fn redelivery_emits_nothing_and_a_gap_is_refused() {
    let mut s = ann_and_bob();
    let mut ann = member(1);
    deliver(&s, "ann", &mut ann);
    let before = ann.state().clone();

    // Deliver everything again, from a stale checkpoint and from none.
    for checkpoint in [None, Some(1), Some(3)] {
        for cmd in backlog::<ClubToMembers>(&s, &(), &"ann".to_string(), checkpoint) {
            match ann.execute(&cmd, &(), &mut ()) {
                Ok(emitted) => assert!(emitted.is_empty(), "{cmd:?} was already delivered"),
                Err(refused) => assert_eq!(refused, MemberRefused::AlreadyEnrolled),
            }
        }
    }
    assert_eq!(*ann.state(), before);
    assert_eq!(ann.len(), 4, "enrolled at 1, then 2, 3 and 4");

    s.execute(&ClubEvent::Paid(1), &(), &mut ()).unwrap();
    s.execute(&ClubEvent::Paid(1), &(), &mut ()).unwrap();
    let gapped = ann.execute(&inject(&s, 6), &(), &mut ());
    assert_eq!(
        gapped.unwrap_err(),
        MemberRefused::Gap(Gap {
            expected: 5,
            found: 6
        })
    );
    assert_eq!(*ann.state(), before, "a refused gap changes nothing");
    assert_eq!(
        deliver(&s, "ann", &mut ann),
        2,
        "the backlog fills the gap in order"
    );
}

#[test]
fn admit_places_a_source_seq_against_a_checkpoint() {
    assert_eq!(admit(4, 5), Ok(Admit::Next));
    assert_eq!(admit(4, 4), Ok(Admit::Redelivery));
    assert_eq!(admit(4, 0), Ok(Admit::Redelivery));
    assert_eq!(
        admit(4, 6),
        Err(Gap {
            expected: 5,
            found: 6
        })
    );
}

#[test]
fn a_gap_is_a_diagnostic() {
    let gap = Gap {
        expected: 5,
        found: 7,
    };
    assert_eq!(gap.code(), "annals-policy-gap");
    let args: Vec<_> = gap
        .args()
        .into_iter()
        .map(|(name, arg)| (name, format!("{arg:?}")))
        .collect();
    assert_eq!(
        args,
        [
            ("expected", "Uint(5)".to_string()),
            ("found", "Uint(7)".to_string())
        ]
    );
    assert_eq!(
        gap.to_string(),
        "source event 7 skips ahead of event 5, the next expected"
    );
}

#[test]
fn visiting_states_sees_each_event_with_the_state_just_after_it() {
    let s = ann_and_bob();
    let mut seen = Vec::new();
    s.visit_states(&(), |envelope, state| {
        seen.push((envelope.seq, state.clone()))
    });
    let expected: Vec<_> = (0..s.len())
        .map(|seq| (seq, s.state_at(seq + 1, &())))
        .collect();
    assert_eq!(seen, expected);
}
