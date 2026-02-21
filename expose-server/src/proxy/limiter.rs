use crate::error::ExposeError;
use crate::tunnel_manager::ActiveTunnel;

pub struct RequestLimiter;

impl RequestLimiter {
    pub fn check(tunnel: &ActiveTunnel) -> Result<(), ExposeError> {
        tunnel.check_rate_limit()
    }
}
