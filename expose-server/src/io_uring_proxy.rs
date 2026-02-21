//! Compatibility shim for io_uring proxy helpers.

#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub use crate::platform::io_uring::*;
