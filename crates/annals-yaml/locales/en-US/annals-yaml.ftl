# Messages for annals-yaml's errors. Each id is an error's code; the
# variables are its args. Messages are lowercase fragments, like the English
# Display, so they read well inside another message. A literal brace is
# written {"{"} or {"}"}.

## Parsing. Every parse error has $line, 1-based (0 for the end of input).

annals-yaml-lone-carriage-return = line { $line }: a carriage return without a line feed: lines end in \n or \r\n
annals-yaml-tab-indent = line { $line }: tabs can't indent
annals-yaml-non-ascii-indent = line { $line }: indentation is ASCII spaces only, not { $character }
annals-yaml-document-marker = line { $line }: one document per file: no { $marker }
annals-yaml-anchor = line { $line }: anchors (&) aren't supported
annals-yaml-alias = line { $line }: aliases (*) aren't supported
annals-yaml-tag = line { $line }: tags (!) aren't supported
annals-yaml-block-scalar = line { $line }: block scalars (| and >) aren't supported
annals-yaml-unexpected-indent = line { $line }: unexpected indentation
annals-yaml-expected-key-value = line { $line }: expected `key: value`, found "{ $found }"
annals-yaml-key-not-scalar = line { $line }: keys must be scalars
annals-yaml-duplicate-key = line { $line }: "{ $key }" twice
annals-yaml-unexpected = line { $line }: unexpected "{ $found }"
annals-yaml-expected-seq-separator = line { $line }: expected , or ] in a flow sequence
annals-yaml-expected-colon = line { $line }: expected : in a flow mapping
annals-yaml-expected-map-separator = line { $line }: expected , or {"}"} in a flow mapping
annals-yaml-bad-unicode-escape = line { $line }: bad escape { $escape }: \u takes 4 hex digits and \U 8, naming a Unicode scalar value; surrogates are refused (write \U0001F5E1, not 🗡)
annals-yaml-unknown-escape = line { $line }: unknown escape { $escape }
annals-yaml-unterminated-quote = line { $line }: unterminated quote

## Versioned documents. $kind is the document kind the game named; $source
## is where the document came from (a path, say).

annals-yaml-load-failed = { $kind } document: { $problem }
annals-yaml-load-failed-at = { $kind } document { $source }: { $problem }

annals-yaml-root-not-a-mapping = the root must be a mapping with a `version`
annals-yaml-version-empty = `version` must be a non-negative integer, not an empty value
annals-yaml-version-sequence = `version` must be a non-negative integer, not a sequence
annals-yaml-version-mapping = `version` must be a non-negative integer, not a mapping
annals-yaml-version-not-an-integer = `version` must be a non-negative integer, not "{ $text }"
annals-yaml-version-too-large = `version` must be a non-negative integer, not "{ $text }" (too large)
annals-yaml-too-new = version { $found } was written by a newer build; this build reads up to version { $current }
annals-yaml-too-old = version { $found } is older than this build migrates; the oldest it reads is version { $oldest }
annals-yaml-step-failed = migrating version { $from } to { $to }: { $error }

## Migration steps. $from and $to are key paths, written dotted.

annals-yaml-rename-not-a-mapping = rename "{ $from }": not a mapping
annals-yaml-rename-target-exists = rename "{ $from }" to "{ $to }": "{ $to }" is already there
annals-yaml-move-empty-path = move { $from } to { $to }: empty key path
annals-yaml-move-not-a-mapping = move { $from } to { $to }: not a mapping
annals-yaml-move-through-not-a-mapping = move { $from } to { $to }: "{ $key }": not a mapping
annals-yaml-move-target-exists = move { $from } to { $to }: "{ $key }" is already there
annals-yaml-default-not-a-mapping = default "{ $key }": not a mapping
annals-yaml-remove-not-a-mapping = remove "{ $key }": not a mapping
annals-yaml-map-not-a-mapping = map over "{ $key }": not a mapping
annals-yaml-map-not-a-sequence = map over "{ $key }": not a sequence
annals-yaml-map-item = { $key }[{ $index }]: { $error }
annals-yaml-step-root-not-a-mapping = the step left the root not a mapping

## Migration chains.

annals-yaml-chain-duplicate = { $kind } migrations: two steps from version { $from }
annals-yaml-chain-beyond-current = { $kind } migrations: a step from version { $from }, but the current version is { $current }
annals-yaml-chain-gap = { $kind } migrations: no step from version { $from } to { $to }

## Editing a document in place. $path is the key path, written dotted, or
## "the root".

annals-yaml-edit-not-found = { $path }: not found
annals-yaml-edit-not-a-mapping = { $path }: not a mapping
annals-yaml-edit-not-a-sequence = { $path }: not a sequence
annals-yaml-edit-inside-flow = { $path }: inside a flow collection, which is edited only as a whole
annals-yaml-edit-item-first-key = { $path }: the key on a sequence item's dash line, which is edited only with the whole item
annals-yaml-edit-no-such-item = { $path }: no item { $index }: the sequence has { $len ->
    [one] { $len } item
   *[other] { $len } items
}
annals-yaml-edit-unfaithful = { $path }: the edit would not read back as written; nothing was changed
