//! Shared buffer helpers for zero-copy operations.

use bytes::Bytes;

/// Convert various inputs into [`Bytes`] efficiently.
pub trait IntoBytes {
    fn into_bytes(self) -> Bytes;
}

impl IntoBytes for Vec<u8> {
    fn into_bytes(self) -> Bytes {
        Bytes::from(self)
    }
}

impl IntoBytes for &[u8] {
    fn into_bytes(self) -> Bytes {
        Bytes::copy_from_slice(self)
    }
}
