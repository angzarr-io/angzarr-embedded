//! Payloads are `Any`, packed from prost messages by their `prost::Name`.
//! This runs: packing and decoding are implemented.

use annals::next::{pack, type_name, Any, Payload, PayloadError};
use prost::Message;

use crate::toys::{account, AccountEvent};

#[test]
fn payloads_are_any_packed_by_their_prost_name() {
    let deposited = account::Deposited { cents: 250 };
    let any = pack(&deposited);
    assert_eq!(any.type_url, "/toy.account.v1.Deposited", "angzarr's '/'");
    assert_eq!(any.value, deposited.encode_to_vec());

    assert_eq!(
        type_name("/toy.account.v1.Deposited"),
        "toy.account.v1.Deposited"
    );
    assert_eq!(
        type_name("type.googleapis.com/toy.account.v1.Deposited"),
        "toy.account.v1.Deposited",
        "any prefix names the same type"
    );
    assert_eq!(
        type_name("toy.account.v1.Deposited"),
        "toy.account.v1.Deposited"
    );

    let event = AccountEvent::Deposited(250);
    assert_eq!(event.to_any(), any);
    assert_eq!(AccountEvent::from_any(&any), Ok(event));
    assert_eq!(
        AccountEvent::type_urls(),
        [
            "/toy.account.v1.Deposited",
            "/toy.account.v1.Opened",
            "/toy.account.v1.Received",
            "/toy.account.v1.Withdrew",
        ],
        "the current types, in name order"
    );

    let old = pack(&account::v0::Credited { euros: 2 });
    assert_eq!(
        AccountEvent::from_any(&old),
        Err(PayloadError::UnknownType {
            type_url: "/toy.account.v0.Credited".to_string()
        }),
        "an old type is upcast first, never decoded as if current"
    );
    let garbled = Any {
        type_url: "/toy.account.v1.Deposited".to_string(),
        value: vec![0xff],
    };
    assert!(matches!(
        AccountEvent::from_any(&garbled),
        Err(PayloadError::Decode { type_url, .. }) if type_url == "/toy.account.v1.Deposited"
    ));
}

#[test]
fn a_domain_that_takes_no_facts_decodes_none() {
    use annals::next::Payload;
    use std::convert::Infallible;
    assert!(<Infallible as Payload>::type_urls().is_empty());
    assert_eq!(
        <Infallible as Payload>::from_any(&annals::next::Any {
            type_url: "/a.B".to_string(),
            value: Vec::new(),
        })
        .err(),
        Some(annals::next::PayloadError::UnknownType {
            type_url: "/a.B".to_string()
        })
    );
}
