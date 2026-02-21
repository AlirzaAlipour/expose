//! Splice-based zero-copy proxying helpers.

use crate::zero_copy::pipe::Pipe;
use nix::fcntl::{splice, SpliceFFlags};
use std::io;
use std::os::unix::io::RawFd;

const SPLICE_CHUNK_SIZE: usize = 64 * 1024;

/// Zero-copy proxy using splice() syscalls.
pub struct SpliceProxier {
    pipe: Pipe,
}

impl SpliceProxier {
    /// Create a new proxier instance with its own pipe.
    pub fn new() -> io::Result<Self> {
        Ok(Self { pipe: Pipe::new()? })
    }

    /// Splice data from source to destination using kernel zero-copy.
    ///
    /// # Safety
    /// - File descriptors must be valid TCP sockets.
    /// - Sockets must be unencrypted (no TLS).
    pub fn splice_connection(
        &self,
        source_fd: RawFd,
        dest_fd: RawFd,
        total_bytes: usize,
    ) -> io::Result<usize> {
        let mut transferred = 0;

        while transferred < total_bytes {
            let remaining = total_bytes - transferred;
            let chunk_size = remaining.min(SPLICE_CHUNK_SIZE);

            let n = splice(
                source_fd,
                None,
                self.pipe.write_fd,
                None,
                chunk_size,
                SpliceFFlags::SPLICE_F_MOVE | SpliceFFlags::SPLICE_F_NONBLOCK,
            )
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

            if n == 0 {
                break;
            }

            let written = splice(
                self.pipe.read_fd,
                None,
                dest_fd,
                None,
                n,
                SpliceFFlags::SPLICE_F_MOVE | SpliceFFlags::SPLICE_F_NONBLOCK,
            )
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

            transferred += written;

            if written != n {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "splice write mismatch",
                ));
            }
        }

        Ok(transferred)
    }
}
