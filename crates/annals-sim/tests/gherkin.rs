//! The Gherkin runner, driving the toy domain through a feature file.

mod common;

use std::path::Path;

use common::*;
use annals_sim::gherkin::{self, Step};

#[derive(Default)]
struct World {
    seed: u64,
    fighters: Vec<(i64, u8)>,
    arena: Option<Arena>,
}

impl World {
    fn arena(&self) -> &Arena {
        self.arena.as_ref().expect("the brawl has not run")
    }
}

fn step(w: &mut World, s: &Step) {
    let t = s.text.as_str();
    let n = s.numbers();
    if t.starts_with("a seed of") {
        w.seed = n[0] as u64;
    } else if t.starts_with("these fighters") {
        let header = &s.table[0];
        let col = |name: &str| header.iter().position(|h| h == name).expect(name);
        for row in &s.table[1..] {
            let hp = row[col("hp")].parse().expect("hp");
            let side = row[col("side")].parse().expect("side");
            w.fighters.push((hp, side));
        }
    } else if t == "the brawl runs to its end" {
        let (stream, _) = brawl(w.seed, w.fighters.clone());
        w.arena = Some(stream.state().clone());
    } else if t.starts_with("side") && t.ends_with("wins") {
        assert_eq!(w.arena().ended, Some(Outcome::Wins(n[0] as u8)));
    } else if t.starts_with("fighter") && t.ends_with("is down") {
        assert!(w.arena().fighters[n[0] as usize].hp <= 0);
    } else if t.starts_with("fighter") && t.contains("swung at least") {
        assert!(w.arena().fighters[n[0] as usize].swings >= n[1] as u64);
    } else if t == "nothing is left on the clock" || t == "something is left on the clock" {
        assert_eq!(w.arena().clock.is_empty(), t.starts_with("nothing"));
    } else {
        panic!("unknown step: {} {t}", s.keyword);
    }
}

fn main() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/features");
    gherkin::main(&dir, World::default, step);
}
