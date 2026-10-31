# Messages for angzarr-ports' store errors, which angzarr-embedded forwards under
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
angzarr-ports-store-newer-than-build = the store was migrated by a newer build, to level { $applied }; this build knows up to { $build }, so it neither reads nor migrates it
angzarr-ports-store-migration-failed = migration { $version } failed part way in this store, which is left as it is: repair it, or restore a backup
angzarr-ports-store-migration-changed = migration { $version } was changed since it was applied to this store
angzarr-ports-store-migration-missing = migration { $version } was applied to this store, but this build no longer has it
angzarr-ports-store-migration-unknown = { $version } is not a migration of this build
angzarr-ports-store-migration-irreversible = migration { $version } has no down, so the store cannot be migrated back past it; nothing was changed
angzarr-ports-store-backup-exists = no backup was written: something is already at { $target }
