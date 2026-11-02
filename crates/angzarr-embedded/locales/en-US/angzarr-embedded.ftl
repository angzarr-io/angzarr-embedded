# Messages for the angzarr-embedded crate's errors. Each id is an error's code; the
# variables are its args. Messages are lowercase fragments, like the English
# Display, so a nested cause reads well inside another message. A condition
# the spec keys is coded under its key, whose English is the spec's catalog
# (angzarr_proto::REASONS_EN_US; decision 14.60), never here.

## The audited kernel, angzarr_embedded's crate root. $stream names a stream as domain/root,
## with @edition outside the main timeline.

angzarr-embedded-malformed-no-cover = the book has no cover
angzarr-embedded-malformed-unknown-domain = no domain "{ $domain }" is registered
angzarr-embedded-malformed-no-root = the cover has no root
angzarr-embedded-malformed-bad-root = the root is { $len ->
        [one] one byte
       *[other] { $len } bytes
    }, not a UUID's 16
angzarr-embedded-malformed-empty-edition = the edition is set but empty: the main timeline is the unset edition
angzarr-embedded-malformed-no-page = the book has no page
angzarr-embedded-malformed-no-body = the page has neither a command nor a fact
angzarr-embedded-malformed-no-payload = the command or fact has no payload
angzarr-embedded-malformed-bad-command-id = the command id is { $len ->
        [one] one byte
       *[other] { $len } bytes
    }, not a UUID's 16
angzarr-embedded-malformed-bad-correlation-id = the correlation id is { $len ->
        [one] one byte
       *[other] { $len } bytes
    }, not a UUID's 16
angzarr-embedded-malformed-causation-from-outside = a client's book names a causation: only the kernel's chain issues policy commands
angzarr-embedded-malformed-id-on-policy-command = a policy's book names its own { $field }: the kernel derives its ids from the trigger
angzarr-embedded-malformed-fact-from-policy = a policy's book holds a fact: a policy never injects one
angzarr-embedded-malformed-handling-on-policy-command = a policy's book has handling: its target stamps its own head
angzarr-embedded-malformed-bad-trigger = the policy command's trigger cannot name a command id: { $reason ->
        [bad-root] its root is not a UUID's 16 bytes
        [missing-trigger] it needs a stream with a root
        [empty-edition] its stream's edition is set but empty
        [empty-component] the reacting component has no name
       *[other] { $reason }
    }
angzarr-embedded-malformed-fact-without-origin = a fact without its origin
angzarr-embedded-malformed-empty-external-id = the fact's external id is empty: it is the fact's identity
angzarr-embedded-malformed-command-with-fact-origin = a command with a fact origin
angzarr-embedded-malformed-fact-command-id-not-derived = the fact's command id is not the one derived from its external id
angzarr-embedded-malformed-handling-on-fact = a fact with handling: a fact is never checked against a sequence nor conflict-resolved
angzarr-embedded-malformed-unknown-conflict-resolution = conflict resolution { $value } is not one this build knows
angzarr-embedded-malformed-unsupported-conflict-resolution = domain "{ $domain }"'s state does not support { $requested }
angzarr-embedded-malformed-refusal-misaddressed = the refusal fact is for another stream than the one its trigger names

angzarr-embedded-projection-failed = the command committed, then { $failures ->
    [one] projector { $projector } failed: { $error }
   *[other] { $failures } projectors failed, the first { $projector }: { $error }
}
angzarr-embedded-downstream = a policy command down the chain failed, at depth { $depth }: { $error }
angzarr-embedded-unknown-type = stream { $stream }'s domain does not decode the type { $type_url }
angzarr-embedded-invalid-payload = a { $type_url } for stream { $stream } is not a valid value: { $reason }
angzarr-embedded-upcast = upcasting a payload for stream { $stream } failed: { $error }
angzarr-embedded-wrong-domain = stream { $stream } was given to the { $expected } domain's code
angzarr-embedded-before-snapshot = stream { $stream } holds its events from { $first } on only, after its snapshot
angzarr-embedded-unregistered-domain = a stored { $domain } stream was read, but no domain "{ $domain }" is registered
angzarr-embedded-replay-out-of-sequence = page { $index } of the log claims sequence { $found }, not the one due
angzarr-embedded-replay-recorded-backwards = event { $sequence } was recorded before the event before it
angzarr-embedded-replay-incomplete-meta = event { $sequence }'s metadata lacks a well-formed { $field }
angzarr-embedded-replay-wrong-domain = a { $found } log was given to the { $expected } domain's code

angzarr-embedded-unreadable = event { $sequence } of stream { $stream } cannot be replayed: { $reason }

angzarr-embedded-payload-unknown-type = no decoder for the type { $type_url }
angzarr-embedded-payload-decode = a payload of type { $type_url } does not decode
angzarr-embedded-not-its-stream = a command whose { $domain } key is { $key } was sent to another stream: its stream is the one that key names
angzarr-embedded-payload-invalid = a payload of type { $type_url } is not a valid value: { $reason }
angzarr-embedded-wire-missing = { $message }.{ $field } is required and absent
angzarr-embedded-wire-unknown-enum = { $message }.{ $field } holds { $value }, which no variant defines
angzarr-embedded-upcast-unavailable = the upcaster sidecar is not serving ({ $cause }): retry
angzarr-embedded-upcast-rule = the upcast rule from { $source } failed: { $error }
angzarr-embedded-upcaster-duplicate = domain { $domain }'s upcaster has two typed rules from { $source }: the second never runs
angzarr-embedded-upcaster-out-of-order = domain { $domain }'s upcaster registers the rule from { $source } before the rule from { $produced_by }, which produces it
angzarr-embedded-upcaster-dead-end = domain { $domain }'s upcaster takes { $source } to { $ends_at }, which the domain does not decode

angzarr-embedded-misconfigured-already-registered = domain "{ $domain }" is registered already
angzarr-embedded-misconfigured-component-registered = a component named "{ $component }" is registered already
angzarr-embedded-misconfigured-not-registered = an upcaster is given for domain "{ $domain }", which is not registered
angzarr-embedded-misconfigured-upcaster-for-another-domain = the upcaster registered for domain "{ $domain }" is for "{ $upcaster }"
angzarr-embedded-misconfigured-empty-rules-version = domain "{ $domain }"'s current rules version is empty
angzarr-embedded-misconfigured-bad-state-descriptor = domain "{ $domain }"'s state descriptor lists a value that is not a resolution, or lacks STRICT or AGGREGATE_HANDLES
angzarr-embedded-misconfigured-unsupported-resolution = domain "{ $domain }" is configured { $resolution }, which its state does not support
angzarr-embedded-misconfigured-unsupported-command-resolution = domain "{ $domain }" configures { $command_type } { $resolution }, which its state does not support
angzarr-embedded-misconfigured-remote-source = "{ $component }" handles the events of domain "{ $domain }", which is remote: its events are its coordinator's, never this process's
