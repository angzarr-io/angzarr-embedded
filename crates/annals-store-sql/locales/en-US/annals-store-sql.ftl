annals-store-sql-unavailable = the database failed: { $detail }
annals-store-sql-undecodable = a stored value does not decode: { $detail }
annals-store-sql-unmigrated =
    the database, opened to read, has { $pending ->
        [one] a migration
       *[other] { $pending } migrations
    } not yet applied
