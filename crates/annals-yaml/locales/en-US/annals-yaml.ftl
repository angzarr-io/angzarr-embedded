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
