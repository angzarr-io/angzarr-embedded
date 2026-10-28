# Messages for the SQLite adapter's errors. A cause (carried in an
# angzarr-ports StoreError as code and English) takes no variables: its
# detail is in the English. A Damage, which an integrity check returns, has
# its args.

angzarr-store-sqlite-unavailable = the database failed
angzarr-store-sqlite-undecodable = a stored value does not decode
angzarr-store-sqlite-unmigrated = the database, opened to read, has migrations not yet applied
angzarr-store-sqlite-damaged-file = the database file is too damaged to open
angzarr-store-sqlite-damaged =
    the database file is damaged: { $first }{ $more ->
        [0] {""}
        [one] {" "}(and 1 more problem)
       *[other] {" "}(and { $more } more problems)
    }
