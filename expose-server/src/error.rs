//! Server-facing error aliases built on top of the shared [`ExposeError`].

use std::io::{Error, ErrorKind};

pub use expose_common::error::{ConfigError, ExposeError, ServerResult as Result};

/// Helper for constructing ad-hoc IO errors (used for network glue code).
pub fn other_io_error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Other, message.into())
}
