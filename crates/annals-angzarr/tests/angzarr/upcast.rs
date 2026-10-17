//! Upcasting by type URL, with the router's semantics, and checked whole.

use annals_angzarr::pb::{EventPage, UpcastRequest};
use annals_angzarr::{pack, ChainProblem, Registry, UpcastChainError, Upcaster};

use crate::toys::{account_upcaster, one_to_two, two_to_three, v1, v2, v3, v9, Account, Ledger};

fn page(any: prost_types::Any) -> EventPage {
    EventPage {
        sequence: u64::from(u32::MAX) + 1,
        payload: Some(any),
        ..EventPage::default()
    }
}

#[test]
#[ignore = "proposal: upcasting by type URL runs v1 -> v2 -> v3, as the router's dispatch"]
fn upcasting_by_type_url_runs_a_v1_to_v2_to_v3_chain() {
    let upcaster = account_upcaster();
    assert_eq!(
        upcaster.source_types(),
        ["toy.account.v1.Deposit", "toy.account.v2.Deposited"]
    );
    let current = pack(&v3::Deposited {
        cents: 200,
        currency: "EUR".to_string(),
    });

    assert_eq!(
        upcaster.upcast_event(&pack(&v1::Deposit { amount: 2 })),
        Ok(current.clone()),
        "each rule's output is the next one's input"
    );
    assert_eq!(
        upcaster.upcast_event(&pack(&v2::Deposited { cents: 200 })),
        Ok(current.clone()),
        "from wherever the event starts"
    );
    assert_eq!(upcaster.upcast_event(&current), Ok(current.clone()));
    let unknown = pack(&v9::Bonus { cents: 1 });
    assert_eq!(
        upcaster.upcast_event(&unknown),
        Ok(unknown.clone()),
        "the router passes other types through; strictness is at decode"
    );

    let old = page(pack(&v1::Deposit { amount: 2 }));
    let upcast = upcaster.upcast_page(&old).unwrap();
    assert_eq!(upcast.sequence, old.sequence, "sequence kept");
    assert_eq!(upcast.payload, Some(current.clone()));

    let request = UpcastRequest {
        domain: "ledger".to_string(),
        events: vec![old.clone()],
    };
    assert_eq!(
        upcaster.dispatch(&request).unwrap().events,
        std::slice::from_ref(&old),
        "another domain's request passes through"
    );
    let request = UpcastRequest {
        domain: "account".to_string(),
        events: vec![old],
    };
    assert_eq!(upcaster.dispatch(&request).unwrap().events, [upcast]);
}

#[test]
#[ignore = "proposal: an upcaster that cannot finish is refused before anything is read"]
fn an_upcaster_is_checked_whole() {
    let problems = |upcaster: Upcaster| -> Vec<ChainProblem> {
        Registry::new()
            .domain::<Account>(upcaster)
            .domain::<Ledger>(Upcaster::new("ledger-upcaster", "ledger"))
            .check()
            .err()
            .unwrap_or_default()
            .into_iter()
            .map(|UpcastChainError { problem, .. }| problem)
            .collect()
    };

    assert_eq!(problems(account_upcaster()), []);
    assert_eq!(
        problems(
            Upcaster::new("u", "account")
                .rule::<v2::Deposited, v3::Deposited>(two_to_three)
                .rule::<v1::Deposit, v2::Deposited>(one_to_two)
        ),
        [ChainProblem::OutOfOrder {
            source: "toy.account.v2.Deposited".to_string(),
            produced_by: "toy.account.v1.Deposit".to_string()
        }],
        "registration order is run order: v1 would stop at v2"
    );
    assert_eq!(
        problems(account_upcaster().rule::<v2::Deposited, v3::Deposited>(two_to_three)),
        [ChainProblem::Duplicate {
            source: "toy.account.v2.Deposited".to_string()
        }]
    );
    assert_eq!(
        problems(Upcaster::new("u", "account").rule::<v1::Deposit, v2::Deposited>(one_to_two)),
        [ChainProblem::DeadEnd {
            source: "toy.account.v1.Deposit".to_string(),
            ends_at: "toy.account.v2.Deposited".to_string()
        }],
        "v2 is not current, and nothing takes it on"
    );
    assert_eq!(
        problems(Upcaster::new("u", "ledger")),
        [ChainProblem::WrongDomain {
            found: "ledger".to_string(),
            expected: "account"
        }]
    );
}
