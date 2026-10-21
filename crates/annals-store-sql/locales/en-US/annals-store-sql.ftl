annals-store-sql-unavailable = the database failed: { $detail }
annals-store-sql-undecodable = a stored value does not decode: { $detail }
annals-store-sql-unmigrated =
    the database, opened to read, has { $pending ->
        [one] a migration
       *[other] { $pending } migrations
    } not yet applied
annals-store-sql-damaged =
    the database file is damaged: { $first }{ $more ->
        [0] {""}
        [one] {" "}(and 1 more problem)
       *[other] {" "}(and { $more } more problems)
    }
