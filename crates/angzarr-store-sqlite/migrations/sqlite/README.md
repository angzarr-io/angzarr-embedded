# SQLite migrations

`SqliteStore::open` runs these, through sqlx's migrator, inside one
`BEGIN IMMEDIATE` transaction, so stores opening a new file at once take turns.
A migration therefore cannot opt out of a transaction (`-- no-transaction`) or
change `PRAGMA journal_mode` (the store keeps every file in WAL mode itself):
neither can run inside a transaction. `every_migration_runs_inside_the_open_transaction`
in `src/sqlite.test.rs` checks both.

An applied migration is never edited: sqlx records its checksum, and a store
refuses a database whose applied migrations differ from its own.
