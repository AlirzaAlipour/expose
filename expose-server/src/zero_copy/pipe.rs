//! Pipe helper for splice-based zero-copy transfers.

use nix::unistd::{close, pipe};
use std::io;
use std::os::unix::io::RawFd;

/// RAII wrapper for pipe file descriptors.
pub struct Pipe {
    pub(crate) read_fd: RawFd,
    pub(crate) write_fd: RawFd,
}

impl Pipe {
    /// Create a new pipe.
    pub fn new() -> io::Result<Self> {
        let (read_fd, write_fd) =
            pipe().map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
        Ok(Self { read_fd, write_fd })
    }
}

impl Drop for Pipe {
    fn drop(&mut self) {
        let _ = close(self.read_fd);
        let _ = close(self.write_fd);
    }
}
