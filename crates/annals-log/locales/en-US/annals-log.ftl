# Messages for annals-log's errors. Each id is an error's code; the
# variables are its args. Messages are lowercase fragments, like the English
# Display, so they read well inside another message.

# $kind is the I/O error kind's name: NotFound, PermissionDenied,
# UnexpectedEof, ...
annals-log-io = reading the log: { $kind ->
        [NotFound] the file is not there
        [PermissionDenied] permission denied
       *[other] an I/O error ({ $kind })
    }
annals-log-empty = the log is empty: no header
annals-log-truncated = the log ends inside a message
annals-log-bad-length = a length prefix is not a valid varint
annals-log-too-long =
    a message claims { $bytes ->
        [one] one byte
       *[other] { $bytes } bytes
    }, over the { $limit }-byte limit
annals-log-decode = a message does not decode
annals-log-sequence = the log is out of sequence: { $cause }
annals-log-tick-went-back = event { $seq } is at tick { $tick }, before the previous event's { $previous }
annals-log-not-in-log = no event { $seq } in the log
annals-log-behind = cannot go back to event { $wanted }: the next is { $next }
annals-log-not-ticked = the index was built without ticks

## Wire format.

annals-log-wire-field-zero = bad wire format: field number 0
annals-log-wire-length-too-large = bad wire format: length too large
annals-log-wire-unsupported-type = bad wire format: unsupported wire type { $wire_type }
annals-log-wire-field-past-end = bad wire format: field runs past the message
annals-log-wire-varint-past-end = bad wire format: varint runs past the message
annals-log-wire-varint-over-64-bits = bad wire format: varint over 64 bits
annals-log-wire-varint-over-ten-bytes = bad wire format: varint over ten bytes
annals-log-wire-envelope-wrong-type = bad wire format: envelope field { $field } has the wrong wire type
annals-log-wire-tick-not-varint = bad wire format: tick (field 1) is not a varint
