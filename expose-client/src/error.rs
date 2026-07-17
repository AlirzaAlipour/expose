//! Client error aliases built on top of the shared [`expose_common::error::ExposeError`].

use std::io::{Error, ErrorKind};

pub use expose_common::error::{ClientResult as Result, ExposeError as ClientError};

/// Helper for constructing ad-hoc IO errors.
pub fn other_io_error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Other, message.into())
}
