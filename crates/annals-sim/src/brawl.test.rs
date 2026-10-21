//! A toy domain with nothing to do with any game: fighters on two sides
//! swing at each other on a clock. Every swing is a job; a swing schedules a
//! landing (an impact, resolved before swings on the same tick) and the
//! fighter's next swing.

#![allow(dead_code)]

use std::fmt;

use crate::{
    advance, Advance, Clock, Job, JobContext, JobRecord, Rec, Registry, Rng, Scheduler, Tick, Value,
};
use annals::{Domain, Header, Stream, StreamId, Tx};
use annals_diagnostic::{Arg, Diagnostic};

pub const LIMIT: Tick = 10_000;
pub const PHASE_IMPACT: u8 = 0;
pub const PHASE_SWING: u8 = 1;

pub struct Brawl;

/// Why the brawl refused a command.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Refused {
    Over,
    AlreadyOpen,
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Refused::Over => "the brawl is over",
            Refused::AlreadyOpen => "the brawl is already open",
        })
    }
}

impl Diagnostic for Refused {
    fn code(&self) -> &'static str {
        match self {
            Refused::Over => "toy-brawl-over",
            Refused::AlreadyOpen => "toy-brawl-already-open",
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![]
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct At {
    pub who: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Due {
    pub at: At,
    pub job: JobRecord,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fighter {
    pub hp: i64,
    pub side: u8,
    /// Swings taken: the use index every roll is derived from.
    pub swings: u64,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Wins(u8),
    Draw,
    Timeout,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Arena {
    pub seed: u64,
    pub fighters: Vec<Fighter>,
    pub clock: Scheduler<Due>,
    pub now: Tick,
    pub ended: Option<Outcome>,
}

pub enum Cmd {
    Open { seed: u64, fighters: Vec<(i64, u8)> },
    Advance,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ev {
    Opened {
        seed: u64,
        fighters: Vec<Fighter>,
    },
    Scheduled {
        tick: Tick,
        phase: u8,
        side: u8,
        due: Due,
    },
    Taken {
        tick: Tick,
    },
    Swung {
        who: u32,
        roll: u32,
    },
    Hit {
        target: u32,
        damage: i64,
    },
    Ended(Outcome),
}

impl Domain for Brawl {
    type State = Arena;
    type Command = Cmd;
    type Event = Ev;
    type Rejection = Refused;
    type Rules = Registry<Brawl>;
    type Ctx = ();

    fn decide(cmd: &Cmd, tx: &mut Tx<Self>) -> Result<(), Refused> {
        if tx.state().ended.is_some() {
            return Err(Refused::Over);
        }
        match cmd {
            Cmd::Open { seed, fighters } => {
                if !tx.state().fighters.is_empty() {
                    return Err(Refused::AlreadyOpen);
                }
                let fighters: Vec<Fighter> = fighters
                    .iter()
                    .map(|&(hp, side)| Fighter {
                        hp,
                        side,
                        swings: 0,
                    })
                    .collect();
                let n = fighters.len() as u32;
                tx.emit(Ev::Opened {
                    seed: *seed,
                    fighters,
                });
                for who in 0..n {
                    schedule(tx, 1 + who as Tick % 3, PHASE_SWING, At { who }, &Swing);
                }
            }
            Cmd::Advance => match advance(tx, LIMIT) {
                Advance::Idle | Advance::Beyond(_) => {
                    let o = outcome(tx.state()).unwrap_or(Outcome::Timeout);
                    tx.emit(Ev::Ended(o));
                }
                Advance::Ran { .. } => {
                    if let Some(o) = outcome(tx.state()) {
                        tx.emit(Ev::Ended(o));
                    }
                }
            },
        }
        Ok(())
    }

    fn apply(s: &mut Arena, e: &Ev, _: &Registry<Brawl>) {
        match e {
            Ev::Opened { seed, fighters } => {
                s.seed = *seed;
                s.fighters = fighters.clone();
            }
            Ev::Scheduled {
                tick,
                phase,
                side,
                due,
            } => {
                s.clock.push(*tick, *phase, *side, due.clone());
            }
            Ev::Taken { tick } => {
                s.now = *tick;
                s.clock.pop();
            }
            Ev::Swung { who, .. } => s.fighters[*who as usize].swings += 1,
            Ev::Hit { target, damage } => s.fighters[*target as usize].hp -= damage,
            Ev::Ended(o) => s.ended = Some(*o),
        }
    }
}

impl JobContext for Brawl {
    type Cx<'a> = Tx<'a, Brawl>;
    type At = At;
}

impl Clock for Brawl {
    type Due = Due;

    fn scheduler(s: &Arena) -> &Scheduler<Due> {
        &s.clock
    }

    fn taken(tick: Tick) -> Ev {
        Ev::Taken { tick }
    }

    fn run(tx: &mut Tx<Brawl>, _: Tick, due: Due) {
        let rules = tx.rules();
        rules.run(&due.job, tx, due.at);
    }
}

fn schedule<J: Job<Brawl>>(tx: &mut Tx<Brawl>, delay: Tick, phase: u8, at: At, job: &J) {
    let s = tx.state();
    let tick = s.now + delay;
    let side = s.fighters[at.who as usize].side;
    let due = Due {
        at,
        job: JobRecord::of::<Brawl, J>(job),
    };
    tx.emit(Ev::Scheduled {
        tick,
        phase,
        side,
        due,
    });
}

fn up(s: &Arena, who: u32) -> bool {
    s.fighters[who as usize].hp > 0
}

pub fn outcome(s: &Arena) -> Option<Outcome> {
    let side_up = |side| s.fighters.iter().any(|f| f.side == side && f.hp > 0);
    match (side_up(0), side_up(1)) {
        (true, true) => None,
        (false, false) => Some(Outcome::Draw),
        (true, false) => Some(Outcome::Wins(0)),
        (false, true) => Some(Outcome::Wins(1)),
    }
}

/// A fighter swings: one roll for damage and one for when the blow lands,
/// both from the fighter's stream for this swing. Then it swings again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Swing;

impl Job<Brawl> for Swing {
    const KIND: &'static str = "toy.swing";

    fn encode(&self) -> Value {
        Value::map(vec![])
    }

    fn decode(_: &Value) -> Option<Swing> {
        Some(Swing)
    }

    fn run(self, tx: &mut Tx<Brawl>, at: At) {
        let s = tx.state();
        if !up(s, at.who) {
            return;
        }
        let me = &s.fighters[at.who as usize];
        let Some(target) = (0..s.fighters.len() as u32)
            .find(|&t| s.fighters[t as usize].side != me.side && up(s, t))
        else {
            return;
        };
        let mut dice = Rng::for_use(s.seed, at.who as u64, me.swings);
        let roll = dice.below(6) + 1;
        let flight = 1 + dice.below(3) as Tick;
        let recover = 2 + dice.below(4) as Tick;
        tx.emit(Ev::Swung { who: at.who, roll });
        let land = Land {
            target,
            damage: roll as i64,
        };
        schedule(tx, flight, PHASE_IMPACT, at, &land);
        schedule(tx, recover, PHASE_SWING, at, &Swing);
    }

    fn sample() -> Swing {
        Swing
    }
}

/// A blow lands, whether or not its striker is still up.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Land {
    pub target: u32,
    pub damage: i64,
}

impl Job<Brawl> for Land {
    const KIND: &'static str = "toy.land";

    fn encode(&self) -> Value {
        Value::map(vec![
            ("target", self.target.to_value()),
            ("damage", self.damage.to_value()),
        ])
    }

    fn decode(v: &Value) -> Option<Land> {
        Some(Land {
            target: v.field("target")?,
            damage: v.field("damage")?,
        })
    }

    fn run(self, tx: &mut Tx<Brawl>, _: At) {
        if up(tx.state(), self.target) {
            tx.emit(Ev::Hit {
                target: self.target,
                damage: self.damage,
            });
        }
    }

    fn sample() -> Land {
        Land {
            target: 3,
            damage: -2,
        }
    }
}

pub fn rules() -> Registry<Brawl> {
    Registry::new().register::<Swing>().register::<Land>()
}

pub fn header(seed: u64) -> Header {
    Header {
        id: StreamId(seed),
        domain: "brawl".to_string(),
        schema: 1,
        rules: "toy".to_string(),
    }
}

/// Advance until the brawl ends.
pub fn finish(stream: &mut Stream<Brawl>, rules: &Registry<Brawl>) {
    while stream.state().ended.is_none() {
        stream
            .execute(&Cmd::Advance, rules, &mut ())
            .expect("advancing an open brawl");
    }
}

/// A whole brawl, and the stream length after every command — where it can
/// be cut.
pub fn brawl(seed: u64, fighters: Vec<(i64, u8)>) -> (Stream<Brawl>, Vec<u64>) {
    let rules = rules();
    let mut stream = Stream::<Brawl>::new(header(seed));
    stream
        .execute(&Cmd::Open { seed, fighters }, &rules, &mut ())
        .expect("open");
    let mut cuts = vec![stream.len()];
    while stream.state().ended.is_none() {
        stream
            .execute(&Cmd::Advance, &rules, &mut ())
            .expect("advance");
        cuts.push(stream.len());
    }
    (stream, cuts)
}
