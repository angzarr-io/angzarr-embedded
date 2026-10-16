//! Payloads are `Any`, packed from prost messages by their `prost::Name`.

use annals_angzarr::{pack, type_name, Payload, PayloadError};
use prost::Message;

use crate::toys::{v1, v3, AccountEvent};

#[test]
#[ignore = "proposal: Any is packed by prost::Name; the type URL is the version"]
fn payloads_are_any_packed_by_their_prost_name() {
    let deposited = v3::Deposited {
        cents: 250,
        currency: "EUR".to_string(),
    };
    let any = pack(&deposited);
    assert_eq!(
        any.type_url, "/toy.account.v3.Deposited",
        "angzarr's '/' prefix"
    );
    assert_eq!(any.value, deposited.encode_to_vec());

    assert_eq!(
        type_name("/toy.account.v3.Deposited"),
        "toy.account.v3.Deposited"
    );
    assert_eq!(
        type_name("type.googleapis.com/toy.account.v3.Deposited"),
        "toy.account.v3.Deposited",
        "any prefix names the same type"
    );

    let event = AccountEvent::Deposited {
        cents: 250,
        currency: "EUR".to_string(),
    };
    assert_eq!(event.to_any(), any);
    assert_eq!(AccountEvent::from_any(&any), Ok(event));
    assert_eq!(
        AccountEvent::type_urls(),
        [
            "/toy.account.v3.Deposited",
            "/toy.account.v3.Opened",
            "/toy.account.v3.Withdrew",
        ],
        "the current types, in order"
    );

    let old = pack(&v1::Deposit { amount: 2 });
    assert_eq!(
        AccountEvent::from_any(&old),
        Err(PayloadError::UnknownType {
            type_url: "/toy.account.v1.Deposit".to_string()
        }),
        "an old type is upcast first, never decoded as if current"
    );
    let garbled = prost_types::Any {
        type_url: "/toy.account.v3.Deposited".to_string(),
        value: vec![0xff],
    };
    assert!(matches!(
        AccountEvent::from_any(&garbled),
        Err(PayloadError::Decode { type_url, .. }) if type_url == "/toy.account.v3.Deposited"
    ));
}
