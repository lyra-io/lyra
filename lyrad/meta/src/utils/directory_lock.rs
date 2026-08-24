//! Process-local ownership of a directory lock file.

use std::fs::{File, OpenOptions};
use std::io::{Error, Result as IoResult};
use std::path::Path;

#[cfg(not(unix))]
use std::io::ErrorKind;

/// An exclusive, non-blocking lock held until this value is dropped.
pub struct DirectoryLock {
    // Control state
    _file: File,
}

impl DirectoryLock {
    /// Acquires an exclusive lock on `path`, creating the lock file if needed.
    pub fn acquire(path: impl AsRef<Path>) -> IoResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;

        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;

            let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if result == -1 {
                return Err(Error::last_os_error());
            }
        }

        #[cfg(not(unix))]
        return Err(Error::new(
            ErrorKind::Unsupported,
            "directory locking is unsupported on this platform",
        ));

        Ok(Self { _file: file })
    }
}
