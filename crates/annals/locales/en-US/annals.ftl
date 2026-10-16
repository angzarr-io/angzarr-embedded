# Messages for the annals crate's errors. Each id is an error's code; the
# variables are its args. Messages are lowercase fragments, like the English
# Display, so a nested cause reads well inside another message.

annals-replay-out-of-sequence = event { $index } claims sequence number { $found }, not { $index }
annals-replay-cause-not-earlier = event { $seq } names event { $cause } as its cause, which is not earlier

annals-store-conflict =
    stream { $stream } holds { $actual ->
        [one] one event
       *[other] { $actual } events
    }, not the { $expected } expected: someone else appended first

annals-policy-gap = source event { $found } skips ahead of event { $expected }, the next expected
