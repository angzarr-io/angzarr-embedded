# Messages for angzarr-embedded-host's errors. Each id is an error's code; the
# variables are its args. Messages are lowercase fragments, like the English
# Display, so they read well inside another message.

angzarr-embedded-host-handshake-malformed = malformed handshake "{ $line }": expected angzarr-handshake v1 unix:<absolute path>
angzarr-embedded-host-handshake-timeout = no handshake within { $timeout_ms } ms
angzarr-embedded-host-handshake-closed = stdout closed before a handshake
angzarr-embedded-host-handshake-unreadable = stdout could not be read ({ $kind })
angzarr-embedded-host-duplicate-domain = domain "{ $domain }" is configured twice
angzarr-embedded-host-no-launch = domain "{ $domain }": language "{ $language }" has no default launch command; give one
angzarr-embedded-host-empty-launch = domain "{ $domain }": the launch command is empty
angzarr-embedded-host-spawn = domain "{ $domain }": could not start "{ $program }" ({ $kind })
angzarr-embedded-host-handshake = domain "{ $domain }": { $cause }
angzarr-embedded-host-connect = domain "{ $domain }": could not connect to { $socket }
angzarr-embedded-host-describe = domain "{ $domain }": Describe failed with gRPC code { $grpc_code }
angzarr-embedded-host-runtime = the host's runtime could not be built ({ $kind })
angzarr-embedded-host-socket-dir-too-long = socket_dir "{ $dir }" is too long for a Unix socket path: at most { $max_bytes ->
        [one] { $max_bytes } byte
       *[other] { $max_bytes } bytes
    }
angzarr-embedded-host-config-unreadable = the config { $path } could not be read ({ $kind })
angzarr-embedded-host-config-invalid = the config { $path } is not valid, at byte { $offset }
angzarr-embedded-host-register = a domain could not be registered: { $cause }
angzarr-embedded-host-store = the store could not be opened: { $cause }
angzarr-embedded-host-descriptor-missing-rules-version = component "{ $component }": its descriptor has no rules version
angzarr-embedded-host-descriptor-starting-types-on-non-pm = component "{ $component }": a { $component_kind } lists starting types; only a process manager may
angzarr-embedded-host-descriptor-missing-state-type = component "{ $component }": a { $component_kind } has no state type
angzarr-embedded-host-descriptor-state-on-stateless-kind = component "{ $component }": a { $component_kind } has a state; it may not
angzarr-embedded-host-descriptor-policy-input-count = component "{ $component }": a policy has { $input_count ->
        [one] { $input_count } input
       *[other] { $input_count } inputs
    }; it must have exactly one
angzarr-embedded-host-descriptor-policy-no-output-domain = component "{ $component }": a policy has no output domain
angzarr-embedded-host-descriptor-output-domain-on-non-policy = component "{ $component }": a { $component_kind } has an output domain; only a policy may
