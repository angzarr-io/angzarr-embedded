use super::*;

/// Loopback is admitted by "loopback" and "ip"; the network by "ip"
/// alone; "none" admits nothing.
#[test]
fn an_exposure_admits_the_arrivals_at_or_inside_it() {
    let admitted = |arrival: Arrival| {
        [Expose::None, Expose::Loopback, Expose::Ip]
            .into_iter()
            .filter(|expose| arrival.admitted_by(*expose))
            .collect::<Vec<_>>()
    };
    assert_eq!(admitted(Arrival::Loopback), [Expose::Loopback, Expose::Ip]);
    assert_eq!(admitted(Arrival::Ip), [Expose::Ip]);
}

/// The local log line names the domain, where the request arrived from
/// and the component's exposure, as code and args.
#[test]
fn not_exposed_is_its_code_and_args() {
    let line = NotExposed {
        domain: "table".to_string(),
        arrival: Arrival::Ip,
        expose: Expose::None,
    }
    .to_string();
    assert_eq!(
        line,
        "angzarr-embedded-host-not-exposed domain=table arrival=ip expose=none"
    );
}
