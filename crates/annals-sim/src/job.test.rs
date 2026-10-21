use super::*;

/// Jobs that add to a running total.
struct Tally;

impl JobContext for Tally {
    type Cx<'a> = i64;
    type At = ();
}

/// Adds its amount: written and read back whole.
#[derive(Debug, PartialEq)]
struct Add(i64);

impl Job<Tally> for Add {
    const KIND: &'static str = "toy.add";

    fn encode(&self) -> Value {
        Value::Int(self.0)
    }

    fn decode(v: &Value) -> Option<Add> {
        match v {
            Value::Int(n) => Some(Add(*n)),
            _ => None,
        }
    }

    fn run(self, total: &mut i64, _: ()) {
        *total += self.0;
    }

    fn sample() -> Add {
        Add(4)
    }
}

/// Loses its amount on the way into a log: the bug the round-trip check is for.
#[derive(Debug, PartialEq)]
struct Lossy(i64);

impl Job<Tally> for Lossy {
    const KIND: &'static str = "toy.lossy";

    fn encode(&self) -> Value {
        Value::map(vec![])
    }

    fn decode(_: &Value) -> Option<Lossy> {
        Some(Lossy(0))
    }

    fn run(self, total: &mut i64, _: ()) {
        *total += self.0;
    }

    fn sample() -> Lossy {
        Lossy(9)
    }
}

#[test]
fn a_kind_whose_sample_does_not_survive_the_log_is_named() {
    let r = Registry::<Tally>::new()
        .register::<Add>()
        .register::<Lossy>();
    assert_eq!(r.failing_round_trips(), ["toy.lossy"]);
}

#[test]
fn a_registry_runs_a_job_by_its_kind() {
    let r = Registry::<Tally>::new().register::<Add>();
    let mut total = 1;
    r.run(&JobRecord::of::<Tally, Add>(&Add(5)), &mut total, ());
    assert_eq!(total, 6);
}

#[test]
fn a_registry_prints_its_kinds() {
    let r = Registry::<Tally>::new()
        .register::<Add>()
        .register::<Lossy>();
    assert_eq!(format!("{r:?}"), r#"["toy.add", "toy.lossy"]"#);
}
