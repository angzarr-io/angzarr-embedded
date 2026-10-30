# Messages for the angzarr-embedded crate's errors. Each id is an error's code; the
# variables are its args. Messages are lowercase fragments, like the English
# Display, so a nested cause reads well inside another message.

## The audited kernel, angzarr_embedded::next. $stream names a stream as domain/root,
## with @edition outside the main timeline.

angzarr-embedded-next-malformed-no-cover = the book has no cover
angzarr-embedded-next-malformed-unknown-domain = no domain "{ $domain }" is registered
angzarr-embedded-next-malformed-no-root = the cover has no root
angzarr-embedded-next-malformed-bad-root = the root is { $len ->
        [one] one byte
       *[other] { $len } bytes
    }, not a UUID's 16
angzarr-embedded-next-malformed-empty-edition = the edition is set but empty: the main timeline is the unset edition
angzarr-embedded-next-malformed-no-page = the book has no page
angzarr-embedded-next-malformed-no-body = the page has neither a command nor a fact
angzarr-embedded-next-malformed-no-payload = the command or fact has no payload
angzarr-embedded-next-malformed-bad-command-id = the command id is { $len ->
        [one] one byte
       *[other] { $len } bytes
    }, not a UUID's 16
angzarr-embedded-next-malformed-bad-correlation-id = the correlation id is { $len ->
        [one] one byte
       *[other] { $len } bytes
    }, not a UUID's 16
angzarr-embedded-next-malformed-causation-from-outside = a client's book names a causation: only the kernel's chain issues policy commands
angzarr-embedded-next-malformed-id-on-policy-command = a policy's book names its own ids: the kernel derives them from the trigger
angzarr-embedded-next-malformed-fact-from-policy = a policy's book holds a fact: a policy never injects one
angzarr-embedded-next-malformed-handling-on-policy-command = a policy's book has handling: its target stamps its own head
angzarr-embedded-next-malformed-bad-trigger = the policy command's trigger cannot name a command id: { $reason ->
        [bad-root] its root is not a UUID's 16 bytes
        [missing-trigger] it needs a stream with a root
        [empty-edition] its stream's edition is set but empty
        [empty-component] the reacting component has no name
       *[other] { $reason }
    }
angzarr-embedded-next-malformed-fact-without-origin = a fact without its origin
angzarr-embedded-next-malformed-empty-external-id = the fact's external id is empty: it is the fact's identity
angzarr-embedded-next-malformed-command-with-fact-origin = a command with a fact origin
angzarr-embedded-next-malformed-fact-command-id-not-derived = the fact's command id is not the one derived from its external id
angzarr-embedded-next-malformed-handling-on-fact = a fact with handling: a fact is never checked against a sequence nor conflict-resolved
angzarr-embedded-next-malformed-unknown-conflict-resolution = conflict resolution { $value } is not one this build knows
angzarr-embedded-next-malformed-unsupported-conflict-resolution = domain "{ $domain }"'s state does not support { $requested }
angzarr-embedded-next-malformed-refusal-misaddressed = the refusal fact is for another stream than the one its trigger names

angzarr-embedded-next-projection-failed = the command committed, then { $failures ->
    [one] projector { $projector } failed: { $error }
   *[other] { $failures } projectors failed, the first { $projector }: { $error }
}
angzarr-embedded-next-downstream = a policy command down the chain failed, at depth { $depth }: { $error }
angzarr-embedded-next-stale = stream { $stream } is at { $actual }, not the { $expected } expected: re-read and re-issue
angzarr-embedded-next-expected-sequence-ahead = stream { $stream } is at { $head }, behind the { $expected } expected: the command names a sequence the stream never reached
angzarr-embedded-next-overlap = the window of stream { $stream } from { $expected } to { $actual } changed { $window_units }, which the command's change to { $command_units } overlaps: re-read and re-issue
angzarr-embedded-next-retry-cap-exhausted = gave up on stream { $stream } after { $attempts ->
        [one] one run
       *[other] { $attempts } runs
    }, each losing its write to another write to the stream, at the cap of { $cap }: dead-lettered, nothing persisted
angzarr-embedded-next-unresolved = decide never acknowledged the window of stream { $stream } from { $expected } to { $actual }
angzarr-embedded-next-unknown-type = stream { $stream }'s domain does not decode the type { $type_url }
angzarr-embedded-next-invalid-payload = a { $type_url } for stream { $stream } is not a valid value: { $reason }
angzarr-embedded-next-upcast = upcasting a payload for stream { $stream } failed: { $error }
angzarr-embedded-next-wrong-domain = stream { $stream } was given to the { $expected } domain's code
angzarr-embedded-next-chain-too-deep = the command chain went deeper than its limit of { $limit }
angzarr-embedded-next-too-many-steps = the decision on stream { $stream } ran more than its limit of { $limit } continuation steps
angzarr-embedded-next-before-snapshot = stream { $stream } holds its events from { $first } on only, after its snapshot
angzarr-embedded-next-unregistered-domain = a stored { $domain } stream was read, but no domain "{ $domain }" is registered
angzarr-embedded-next-handler-unavailable = the handler of stream { $stream } could not be reached ({ $cause }): run it again

angzarr-embedded-next-replay-out-of-sequence = page { $index } of the log claims sequence { $found }, not the one due
angzarr-embedded-next-replay-recorded-backwards = event { $sequence } was recorded before the event before it
angzarr-embedded-next-replay-incomplete-meta = event { $sequence }'s metadata lacks a well-formed { $field }
angzarr-embedded-next-replay-wrong-domain = a { $found } log was given to the { $expected } domain's code
angzarr-embedded-next-replay-unknown-rules = event { $sequence } was decided under rules { $version }, which the rulebook does not hold

angzarr-embedded-next-unreadable = event { $sequence } of stream { $stream } cannot be replayed: { $reason }

angzarr-embedded-next-payload-unknown-type = no decoder for the type { $type_url }
angzarr-embedded-next-payload-decode = a payload of type { $type_url } does not decode
angzarr-embedded-next-payload-invalid = a payload of type { $type_url } is not a valid value: { $reason }
angzarr-embedded-next-wire-missing = { $message }.{ $field } is required and absent
angzarr-embedded-next-wire-unknown-enum = { $message }.{ $field } holds { $value }, which no variant defines
angzarr-embedded-next-upcast-unavailable = the upcaster sidecar is not serving ({ $cause }): retry
angzarr-embedded-next-upcast-rule = the upcast rule from { $source } failed: { $error }
angzarr-embedded-next-upcaster-duplicate = domain { $domain }'s upcaster has two typed rules from { $source }: the second never runs
angzarr-embedded-next-upcaster-out-of-order = domain { $domain }'s upcaster registers the rule from { $source } before the rule from { $produced_by }, which produces it
angzarr-embedded-next-upcaster-dead-end = domain { $domain }'s upcaster takes { $source } to { $ends_at }, which the domain does not decode

angzarr-embedded-next-misconfigured-already-registered = domain "{ $domain }" is registered already
angzarr-embedded-next-misconfigured-upcaster-for-another-domain = the upcaster registered for domain "{ $domain }" is for "{ $upcaster }"
angzarr-embedded-next-misconfigured-empty-rules-version = domain "{ $domain }"'s current rules version is empty
angzarr-embedded-next-misconfigured-bad-state-descriptor = domain "{ $domain }"'s state descriptor lists a value that is not a resolution, or lacks STRICT or AGGREGATE_HANDLES
angzarr-embedded-next-misconfigured-unsupported-resolution = domain "{ $domain }" is configured { $resolution }, which its state does not support
angzarr-embedded-next-misconfigured-unsupported-command-resolution = domain "{ $domain }" configures { $command_type } { $resolution }, which its state does not support

# angzarr-router's refusals, one code per reason.
angzarr-embedded-next-any-decode-failed = a payload did not decode as the type it names: send a payload of its type
angzarr-embedded-next-persisted-event-corrupt = a stored event or snapshot does not decode: restore it, or add the upcaster it needs
angzarr-embedded-next-unhandled-handler-error = a handler failed with an error it does not classify: a bug in the handler
angzarr-embedded-next-no-handlers-registered = no component is registered: register one
angzarr-embedded-next-no-fact-handler = no handler is registered for this fact's type: register one, or stop sending it
angzarr-embedded-next-conflict-resolution-unsupported = the domain does not support the conflict resolution asked for: ask for another

# angzarr-router's reasons (decision 14.37), each under its own code.
angzarr-embedded-next-handler-panicked = a handler panicked: a bug in the handler
angzarr-embedded-next-command-refused = the command was refused with a typed refusal
angzarr-embedded-next-fold-failed = a stored event could not be folded into the state: a bug, or a missing upcaster
angzarr-embedded-next-fold-failed-emitted = an event the command emitted could not be folded into the state: a bug in the domain
angzarr-embedded-next-unknown-command-type = no handler takes this command's type, after upcasting: a bug in the sender
angzarr-embedded-next-no-refusal-handler = nothing handles the refusal of this command: give its source a refusal handler
angzarr-embedded-next-no-component-of-kind = the business process hosts no component of the kind dispatched to
angzarr-embedded-next-duplicate-component = a router hosts one component: register the other in its own

# The coordinator's keys (CoordinatorReason, reasons.proto) the kernel
# answers, each under its own code.
angzarr-embedded-next-conflict-unresolved = the handler never acknowledged the events it did not see: a bug in the handler; re-read and re-issue
angzarr-embedded-next-conflict-overlap = the command's changes overlap changes it did not see: re-read and re-issue
angzarr-embedded-next-policy-command-outside-destination = a policy sent a command to a domain other than its one destination: a bug in the policy
angzarr-embedded-next-policy-command-outside-edition = a policy sent a command into an edition other than its trigger's: a bug in the policy
angzarr-embedded-next-policy-command-no-root = a policy sent a command naming no root: a bug in the policy, which names the root of every command
angzarr-embedded-next-policy-command-other-correlation = a policy sent a command in a correlation other than its trigger's: a bug in the policy
angzarr-embedded-next-pm-no-instance = the process manager has no instance here, and only a starting input starts one: start it first
angzarr-embedded-next-refusal-fact-wrong-stream = a refusal fact was addressed to a stream other than its trigger's: a bug in the runtime
angzarr-embedded-next-refusal-fact-addressed = a policy set its refusal fact's cover or handling: a bug in the policy
angzarr-embedded-next-refusal-fact-no-page = a policy's refusal fact has no page: a bug in the policy
angzarr-embedded-next-refusal-fact-not-a-fact = a policy answered a refusal with a command, not a fact: a bug in the policy
angzarr-embedded-next-refusal-fact-identified = a policy set its refusal fact's identity: a bug in the policy
