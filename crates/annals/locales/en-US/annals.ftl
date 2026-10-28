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

## The audited kernel, annals::next. $stream names a stream as domain/root,
## with @edition outside the main timeline.

annals-next-malformed-no-cover = the book has no cover
annals-next-malformed-unknown-domain = no domain "{ $domain }" is registered
annals-next-malformed-no-root = the cover has no root
annals-next-malformed-bad-root = the root is { $len ->
        [one] one byte
       *[other] { $len } bytes
    }, not a UUID's 16
annals-next-malformed-empty-edition = the edition is set but empty: the main timeline is the unset edition
annals-next-malformed-no-page = the book has no page
annals-next-malformed-no-body = the page has neither a command nor a fact
annals-next-malformed-no-payload = the command or fact has no payload
annals-next-malformed-bad-command-id = the command id is { $len ->
        [one] one byte
       *[other] { $len } bytes
    }, not a UUID's 16
annals-next-malformed-bad-correlation-id = the correlation id is { $len ->
        [one] one byte
       *[other] { $len } bytes
    }, not a UUID's 16
annals-next-malformed-causation-from-outside = a client's book names a causation: only the kernel's chain issues reactions
annals-next-malformed-id-on-reaction = a policy's book names its own ids: the kernel derives them from the trigger
annals-next-malformed-fact-from-reaction = a policy's book holds a fact: a reaction never injects one
annals-next-malformed-handling-on-reaction = a policy's book has handling: its target stamps its own head
annals-next-malformed-bad-trigger = the reaction's trigger cannot name a command id: { $reason ->
        [bad-root] its root is not a UUID's 16 bytes
        [missing-trigger] it needs a stream with a root
        [empty-edition] its stream's edition is set but empty
        [empty-component] the reacting component has no name
       *[other] { $reason }
    }
annals-next-malformed-fact-without-origin = a fact without its origin
annals-next-malformed-empty-external-id = the fact's external id is empty: it is the fact's identity
annals-next-malformed-command-with-fact-origin = a command with a fact origin
annals-next-malformed-fact-command-id-not-derived = the fact's command id is not the one derived from its external id
annals-next-malformed-handling-on-fact = a fact with handling: a fact is never checked against a sequence nor conflict-resolved
annals-next-malformed-unknown-conflict-resolution = conflict resolution { $value } is not one this build knows
annals-next-malformed-unsupported-conflict-resolution = domain "{ $domain }"'s state does not support { $requested }

annals-next-projection-failed = the command committed, then { $failures ->
    [one] projector { $projector } failed: { $error }
   *[other] { $failures } projectors failed, the first { $projector }: { $error }
}
annals-next-downstream = a reaction down the chain failed, at depth { $depth }: { $error }
annals-next-stale = stream { $stream } is at { $actual }, not the { $expected } expected: re-read and re-issue
annals-next-unresolved = decide never acknowledged the window of stream { $stream } from { $expected } to { $actual }
annals-next-unknown-type = stream { $stream }'s domain does not decode the type { $type_url }
annals-next-invalid-payload = a { $type_url } for stream { $stream } is not a valid value: { $reason }
annals-next-upcast = upcasting a payload for stream { $stream } failed: { $error }
annals-next-ingest-panicked = ingesting fact { $command_id } into stream { $stream } panicked: a bug
annals-next-gap = a delivery to stream { $stream } skips ahead to { $found } from { $expected }, the next due
annals-next-wrong-domain = stream { $stream } was given to the { $expected } domain's code
annals-next-chain-too-deep = the command chain went deeper than its limit of { $limit }
annals-next-too-many-steps = the decision on stream { $stream } ran more than its limit of { $limit } continuation steps
annals-next-before-snapshot = stream { $stream } holds its events from { $first } on only, after its snapshot
annals-next-unregistered-domain = a stored { $domain } stream was read, but no domain "{ $domain }" is registered

annals-next-replay-out-of-sequence = page { $index } of the log claims sequence { $found }, not the one due
annals-next-replay-cause-not-earlier = event { $sequence } names event { $cause } of its own stream as its trigger, which is not earlier
annals-next-replay-recorded-backwards = event { $sequence } was recorded before the event before it
annals-next-replay-incomplete-meta = event { $sequence }'s metadata lacks a well-formed { $field }
annals-next-replay-wrong-domain = a { $found } log was given to the { $expected } domain's code
annals-next-replay-unknown-rules = event { $sequence } was decided under rules { $version }, which the rulebook does not hold

annals-next-store-conflict =
    stream { $stream } holds { $actual ->
        [one] one event
       *[other] { $actual } events
    }, not the { $expected } expected: someone else appended first
annals-next-store-discontinuous = an append to stream { $stream } gives sequence { $found } where { $expected } was due
annals-next-store-unavailable = the store is unavailable: { $error }
annals-next-store-unreadable = event { $sequence } of stream { $stream } cannot be read: { $reason }
annals-next-store-unknown-edition = stream { $stream } names an edition that was never created

annals-next-payload-unknown-type = no decoder for the type { $type_url }
annals-next-payload-decode = a payload of type { $type_url } does not decode
annals-next-payload-invalid = a payload of type { $type_url } is not a valid value: { $reason }
annals-next-wire-missing = { $message }.{ $field } is required and absent
annals-next-wire-unknown-enum = { $message }.{ $field } holds { $value }, which no variant defines
annals-next-upcast-rule = the upcast rule from { $source } failed: { $error }
annals-next-upcaster-duplicate = domain { $domain }'s upcaster has two typed rules from { $source }: the second never runs
annals-next-upcaster-out-of-order = domain { $domain }'s upcaster registers the rule from { $source } before the rule from { $produced_by }, which produces it
annals-next-upcaster-dead-end = domain { $domain }'s upcaster takes { $source } to { $ends_at }, which the domain does not decode

annals-next-misconfigured-already-registered = domain "{ $domain }" is registered already
annals-next-misconfigured-upcaster-for-another-domain = the upcaster registered for domain "{ $domain }" is for "{ $upcaster }"
annals-next-misconfigured-empty-rules-version = domain "{ $domain }"'s current rules version is empty
annals-next-misconfigured-bad-state-descriptor = domain "{ $domain }"'s state descriptor lists a value that is not a resolution, or lacks STRICT or AGGREGATE_HANDLES
annals-next-misconfigured-unsupported-resolution = domain "{ $domain }" is configured { $resolution }, which its state does not support
annals-next-misconfigured-unsupported-command-resolution = domain "{ $domain }" configures { $command_type } { $resolution }, which its state does not support

annals-next-position-gap = event { $found } came where { $expected } was due: catch up first
