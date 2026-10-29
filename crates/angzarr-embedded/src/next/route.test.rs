use std::sync::Arc;

use angzarr_router::PayloadUpcaster;
use prost_types::{Any, Timestamp};

use super::{is_nothing, recorded_at, recording, CommandUpcaster, Delivery, NOTHING};
use crate::next::toys::account_upcaster;
use crate::next::v1::EventPage;

fn at(seconds: i64) -> Delivery {
    Delivery {
        recorded_at: Timestamp { seconds, nanos: 0 },
        ..Delivery::default()
    }
}

/// The instant a dispatch records at is what handlers read inside it, the
/// one outside it again after, nested or not.
#[test]
fn a_dispatchs_instant_is_read_inside_it_only() {
    assert_eq!(recorded_at(), Timestamp::default(), "outside any dispatch");
    let inner = recording(at(5), || {
        let nested = recording(at(7), recorded_at);
        (recorded_at(), nested)
    });
    assert_eq!(inner.0.seconds, 5, "restored after the nested one");
    assert_eq!(inner.1.seconds, 7);
    assert_eq!(recorded_at(), Timestamp::default(), "reset after");
}

/// A fact's record of nothing is known by its type URL alone.
#[test]
fn a_record_of_nothing_is_known_by_its_type_url() {
    let page = |type_url: &str| EventPage {
        payload: Some(Any {
            type_url: type_url.to_string(),
            value: Vec::new(),
        }),
        ..EventPage::default()
    };
    assert!(is_nothing(&page(NOTHING)));
    assert!(!is_nothing(&page("/toy.account.v1.Opened")));
    assert!(!is_nothing(&EventPage::default()));
}

/// The router reads an upcaster's sources: the types it upcasts from.
#[test]
fn the_router_reads_an_upcasters_sources() {
    let upcaster = CommandUpcaster(Arc::new(account_upcaster()));
    let mut sources = upcaster.source_types();
    sources.sort();
    assert_eq!(
        sources,
        ["toy.account.cmd.v0.Credit", "toy.account.v0.Credited"]
    );
}
