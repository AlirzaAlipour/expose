//! Platform-specific capabilities and accelerated proxy helpers.

#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub mod io_uring;

#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub use io_uring::*;

/// Runtime-detected platform capabilities.
#[derive(Debug, Clone)]
pub struct PlatformCapabilities {
    pub io_uring_available: bool,
    pub io_uring_version: Option<String>,
}

/// Detect supported platform capabilities.
pub fn detect_capabilities() -> PlatformCapabilities {
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    {
        PlatformCapabilities {
            io_uring_available: check_io_uring_support(),
            io_uring_version: get_io_uring_version(),
        }
    }

    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    {
        PlatformCapabilities {
            io_uring_available: false,
            io_uring_version: None,
        }
    }
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
fn check_io_uring_support() -> bool {
    tokio_uring::uring_builder().build(256).is_ok()
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
fn get_io_uring_version() -> Option<String> {
    None
}
