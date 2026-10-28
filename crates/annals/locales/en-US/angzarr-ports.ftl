# Messages for angzarr-ports' store errors, which annals forwards under
# their own codes (KernelError::Store). angzarr-ports ships no catalog; the
# runtime that surfaces its errors carries one. Each id is a StoreError
# code; the variables are its args. $stream names a stream as domain/root,
# with @edition outside the main timeline; $cause is the adapter's own
# error, rendered by its own message.

angzarr-ports-store-conflict =
    stream { $stream } holds { $actual ->
        [one] one event
       *[other] { $actual } events
    }, not the { $expected } expected: someone else appended first
angzarr-ports-store-discontinuous = an append to stream { $stream } gives sequence { $found } where { $expected } was due
angzarr-ports-store-malformed-stream = { $stream } is no stream: it needs a 16-byte root and, if it names an edition, a non-empty one
angzarr-ports-store-unavailable = the store is unavailable: { $cause }
angzarr-ports-store-unreadable = event { $sequence } of stream { $stream } cannot be read: { $cause }
angzarr-ports-store-edition-not-forked = stream { $stream } is not yet created in its edition: only its creation may be written first
angzarr-ports-edition-not-found = edition { $name } does not exist: no stream is forked in it
