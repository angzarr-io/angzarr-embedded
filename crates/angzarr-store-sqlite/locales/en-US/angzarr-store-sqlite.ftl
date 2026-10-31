# Messages for the SQLite adapter's errors, carried in an angzarr-ports
# StoreError as a cause's code and args. A third party's text (SQLite's,
# the driver's, prost's) is the arg ext_err_txt, verbatim; SQLite's primary
# result code is errno, where SQLite answered: a driver failure without one
# is angzarr-store-sqlite-driver-failed. A Damage, which an integrity check returns, has its
# args.

angzarr-store-sqlite-unavailable = the database failed (SQLite code { $errno }): { $ext_err_txt }
angzarr-store-sqlite-driver-failed = the database driver failed: { $ext_err_txt }
angzarr-store-sqlite-undecodable = a stored value does not decode: { $ext_err_txt }
angzarr-store-sqlite-backup-uncreatable = the backup { $path } could not be created: { $ext_err_txt }
angzarr-store-sqlite-damaged-file = the database file is too damaged to open (SQLite code { $errno }): { $ext_err_txt }
angzarr-store-sqlite-no-data-version = the database gave no data version
angzarr-store-sqlite-bad-root = a stored root of { $length } bytes, not a UUID's 16
angzarr-store-sqlite-damaged =
    the database file is damaged: { $first }{ $more ->
        [0] {""}
        [one] {" "}(and 1 more problem)
       *[other] {" "}(and { $more } more problems)
    }
