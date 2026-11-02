//! [`TestDir`]: a test's directory under the temporary directory, removed
//! when the guard drops, on a pass and on a panic alike, so a failing (or
//! mutated) test leaves nothing behind; and [`TestFile`], one file in a
//! `TestDir` of its own (a fake child's call log, a control socket, a
//! mark), removed with it.
//!
//! The rule is angzarr-cli's (its `test-support/test_dir.rs`), so a test's
//! scratch goes the same way in both:
//!
//! - A directory is `<temp_dir>/<prefix>-<pid>-<n>`, `n` counting within
//!   the test binary: short, for the tests that put a socket in it, and
//!   fresh (a leftover of that name is removed first). A directory for a
//!   host's own sockets ([`TestDir::for_sockets`], this crate's alone) is
//!   under the host's default socket directory instead, which is short
//!   whatever `$TMPDIR` is.
//! - It is removed when the guard drops. Bind the guard for as long as the
//!   path is used: `let log = TestFile::new(..)`, never
//!   `TestFile::new(..).to_path_buf()`, whose guard drops at once.
//! - To keep a failing test's files for debugging, set
//!   `ANGZARR_KEEP_TEST_DIRS` (to anything): a guard dropped while its test
//!   panics then keeps the directory and names it on stderr. A passing
//!   test's directory goes either way.

use std::ffi::OsStr;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

/// The environment variable that keeps a panicking test's directory.
pub(crate) const KEEP: &str = "ANGZARR_KEEP_TEST_DIRS";

/// A directory removed when this drops, unless it drops in a panic with
/// [`KEEP`] set.
#[derive(Debug)]
pub(crate) struct TestDir {
    path: PathBuf,
    keep_on_panic: bool,
}

impl TestDir {
    /// A fresh, empty `<temp_dir>/<prefix>-<pid>-<n>`, kept on a panic when
    /// [`KEEP`] is set.
    pub(crate) fn new(prefix: &str) -> TestDir {
        TestDir::keeping_on_panic(prefix, std::env::var_os(KEEP).is_some())
    }

    /// [`TestDir::new`], kept on a panic when `keep_on_panic`, whatever the
    /// environment holds.
    pub(crate) fn keeping_on_panic(prefix: &str, keep_on_panic: bool) -> TestDir {
        TestDir::under(&std::env::temp_dir(), prefix, keep_on_panic)
    }

    /// [`TestDir::new`] for a host's sockets: under the host's default
    /// socket directory (`$XDG_RUNTIME_DIR`, else `/tmp`), not the
    /// temporary one, so it is short enough to hold the host's socket
    /// names whatever `$TMPDIR` is.
    pub(crate) fn for_sockets(prefix: &str) -> TestDir {
        let base =
            crate::host::socket_dir(None, std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from))
                .expect("the default socket directory is short");
        TestDir::under(&base, prefix, std::env::var_os(KEEP).is_some())
    }

    /// A fresh, empty `<base>/<prefix>-<pid>-<n>`.
    fn under(base: &Path, prefix: &str, keep_on_panic: bool) -> TestDir {
        static RUN: AtomicUsize = AtomicUsize::new(0);
        let path = base.join(format!(
            "{prefix}-{}-{}",
            std::process::id(),
            RUN.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path)
            .unwrap_or_else(|e| panic!("creating {}: {e}", path.display()));
        TestDir {
            path,
            keep_on_panic,
        }
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        if self.keep_on_panic && std::thread::panicking() {
            eprintln!("{KEEP}: kept {}", self.path.display());
            return;
        }
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

impl Deref for TestDir {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<Path> for TestDir {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<OsStr> for TestDir {
    fn as_ref(&self) -> &OsStr {
        self.path.as_os_str()
    }
}

/// `<TestDir>/<name>`, not created: the file is the test's (or its
/// child's) to write. It goes with its directory, on the directory's rule.
#[derive(Debug)]
pub(crate) struct TestFile {
    path: PathBuf,
    // Held for its drop: the directory, and the file in it, go with this.
    _dir: TestDir,
}

impl TestFile {
    /// `name` in a fresh [`TestDir`] of `prefix`.
    pub(crate) fn new(prefix: &str, name: &str) -> TestFile {
        TestFile::in_dir(TestDir::new(prefix), name)
    }

    /// `name` in `dir`, which this then owns.
    pub(crate) fn in_dir(dir: TestDir, name: &str) -> TestFile {
        TestFile {
            path: dir.join(name),
            _dir: dir,
        }
    }
}

impl Deref for TestFile {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<Path> for TestFile {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<OsStr> for TestFile {
    fn as_ref(&self) -> &OsStr {
        self.path.as_os_str()
    }
}

mod tests {
    use std::panic::{catch_unwind, AssertUnwindSafe};

    use super::*;

    /// `hold(keep)`'s file written, its path recorded, then a panic while
    /// it is held: the path, the guard dropped in the unwind.
    fn panicking_with<G: Deref<Target = Path>>(hold: impl FnOnce() -> G) -> PathBuf {
        let mut seen = None;
        let unwound = catch_unwind(AssertUnwindSafe(|| {
            let guard = hold();
            let file = if guard.is_dir() {
                guard.join("file")
            } else {
                guard.to_path_buf()
            };
            std::fs::write(&file, "x").unwrap();
            seen = Some(file);
            panic!("the test fails while its file is held");
        }));
        assert!(unwound.is_err(), "the closure panicked");
        seen.expect("the file was written")
    }

    #[test]
    fn a_test_dir_is_fresh_and_empty_under_the_temporary_directory() {
        let dir = TestDir::new("angzarr-host-test-dir");
        assert!(dir.is_dir());
        assert!(std::fs::read_dir(&dir).unwrap().next().is_none());
        assert_eq!(dir.parent(), Some(std::env::temp_dir().as_path()));
        let name = dir.file_name().unwrap().to_string_lossy().into_owned();
        let pid = format!("angzarr-host-test-dir-{}-", std::process::id());
        assert!(name.starts_with(&pid), "{name}");
        let other = TestDir::new("angzarr-host-test-dir");
        assert_ne!(dir.path, other.path, "each guard has its own");
    }

    #[test]
    fn a_test_dir_and_its_contents_go_when_it_drops() {
        let dir = TestDir::new("angzarr-host-test-dir");
        std::fs::create_dir_all(dir.join("a/b")).unwrap();
        std::fs::write(dir.join("a/b/file"), "x").unwrap();
        let path = dir.to_path_buf();
        drop(dir);
        assert!(!path.exists(), "{}", path.display());
    }

    #[test]
    fn a_test_dir_goes_when_its_test_panics() {
        let file =
            panicking_with(|| TestDir::keeping_on_panic("angzarr-host-test-dir-panic", false));
        assert!(!file.parent().unwrap().exists(), "{}", file.display());
    }

    #[test]
    fn a_test_dir_kept_on_panic_stays_when_its_test_panics() {
        let file = panicking_with(|| TestDir::keeping_on_panic("angzarr-host-test-dir-kept", true));
        let kept = file.is_file();
        std::fs::remove_dir_all(file.parent().unwrap()).unwrap();
        assert!(kept, "{}", file.display());
    }

    #[test]
    fn a_test_dir_kept_on_panic_still_goes_when_its_test_passes() {
        let dir = TestDir::keeping_on_panic("angzarr-host-test-dir-kept", true);
        let path = dir.to_path_buf();
        drop(dir);
        assert!(!path.exists(), "{}", path.display());
    }

    #[test]
    fn a_test_file_is_named_in_a_test_dir_of_its_own_and_not_made() {
        let file = TestFile::new("angzarr-host-test-file", "calls.log");
        assert_eq!(file.file_name(), Some(OsStr::new("calls.log")));
        let dir = file.parent().unwrap();
        assert!(dir.is_dir());
        assert_eq!(dir.parent(), Some(std::env::temp_dir().as_path()));
        assert!(!file.exists(), "the file is the test's to write");
        let as_path: &Path = file.as_ref();
        let as_os: &OsStr = file.as_ref();
        assert_eq!(as_path, &*file);
        assert_eq!(as_os, file.as_os_str());
    }

    #[test]
    fn a_test_file_goes_with_its_dir_when_it_drops() {
        let file = TestFile::new("angzarr-host-test-file", "calls.log");
        std::fs::write(&file, "x").unwrap();
        let path = file.to_path_buf();
        drop(file);
        assert!(!path.exists(), "{}", path.display());
        assert!(!path.parent().unwrap().exists(), "{}", path.display());
    }

    /// The leak this guards against: a fake child's call log, written,
    /// then its test panicking. The log and its directory are gone.
    #[test]
    fn a_test_file_goes_when_its_test_panics() {
        let file = panicking_with(|| {
            TestFile::in_dir(
                TestDir::keeping_on_panic("angzarr-host-test-file-panic", false),
                "calls.log",
            )
        });
        assert!(!file.exists(), "{}", file.display());
        assert!(!file.parent().unwrap().exists(), "{}", file.display());
    }

    #[test]
    fn a_test_dir_reads_as_its_path() {
        let dir = TestDir::new("angzarr-host-test-dir");
        let as_path: &Path = dir.as_ref();
        let as_os: &OsStr = dir.as_ref();
        assert_eq!(as_path, dir.path.as_path());
        assert_eq!(as_os, dir.path.as_os_str());
    }
}
