//! Linux zero-copy helpers (splice-based).

#[cfg(target_os = "linux")]
pub mod pipe;
#[cfg(target_os = "linux")]
pub mod splice;

#[cfg(target_os = "linux")]
pub use splice::SpliceProxier;

/// Determines if splice-based zero-copy can be used.
pub fn can_use_splice(source_is_tls: bool, dest_is_tls: bool) -> bool {
    #[cfg(target_os = "linux")]
    {
        !source_is_tls && !dest_is_tls
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = (source_is_tls, dest_is_tls);
        false
    }
}
