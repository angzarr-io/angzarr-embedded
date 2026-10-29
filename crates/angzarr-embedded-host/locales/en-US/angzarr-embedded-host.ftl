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
angzarr-embedded-host-name-mismatch = domain "{ $domain }": the child describes a component named "{ $described }"
angzarr-embedded-host-runtime = the host's runtime could not be built ({ $kind })
