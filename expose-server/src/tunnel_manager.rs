//! Manages the set of active tunnels and the lifecycle for pending requests.

use crate::config::PendingRequestConfig;
use crate::error::{ExposeError, Result};
use crate::metrics::ServerMetrics;
use axum::extract::ws::Message as WsMessage;
use bytes::Bytes;
use dashmap::DashMap;
use expose_common::protocol::Message;
use expose_common::types::{RequestLimits, TunnelProtocol};
use expose_common::utils;
use governor::clock::{Clock, DefaultClock};
use governor::state::InMemoryState;
use governor::{Quota, RateLimiter};
use hex;
use rand::{distributions::Alphanumeric, Rng};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Frames queued for sending to a connected client.
#[derive(Debug)]
pub enum OutgoingFrame {
    /// Protocol message encoded with bincode.
    Protocol(Message),
    /// Raw websocket control frame.
    Control(WsMessage),
}

pub type PendingResponseSender = oneshot::Sender<Message>;

/// Represents a pending HTTP request awaiting response from the tunnel client.
#[derive(Debug)]
pub struct PendingRequest {
    sender: PendingResponseSender,
    created_at: Instant,
    deadline: Instant,
    request_id: Uuid,
    tunnel_id: Uuid,
}

impl PendingRequest {
    fn timed_out_response(&self) -> Message {
        Message::HttpResponse {
            id: self.request_id,
            status: 504,
            headers: vec![(
                "x-expose-error".into(),
                "tunnel response deadline exceeded".into(),
            )],
            body: Bytes::new(),
        }
    }
}

/// Runtime metrics describing pending request activity.
#[derive(Debug, Default)]
pub struct PendingRequestMetrics {
    pending_gauge: AtomicUsize,
    expired_total: AtomicU64,
}

impl PendingRequestMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn increment(&self) {
        self.pending_gauge.fetch_add(1, Ordering::Relaxed);
        ServerMetrics::pending_requests_changed(self.pending());
    }

    pub fn decrement(&self) {
        self.pending_gauge
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_sub(1)
            })
            .ok();
        ServerMetrics::pending_requests_changed(self.pending());
    }

    pub fn increment_expired(&self, count: u64) {
        self.expired_total.fetch_add(count, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn pending(&self) -> usize {
        self.pending_gauge.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
struct PendingStore {
    requests: Arc<DashMap<Uuid, PendingRequest>>,
    per_tunnel: DashMap<Uuid, usize>,
    metrics: Arc<PendingRequestMetrics>,
}

impl PendingStore {
    fn new(metrics: Arc<PendingRequestMetrics>) -> Self {
        ServerMetrics::pending_requests_changed(0);
        Self {
            requests: Arc::new(DashMap::new()),
            per_tunnel: DashMap::new(),
            metrics,
        }
    }

    fn len(&self) -> usize {
        self.requests.len()
    }

    fn insert(&self, tunnel_id: &Uuid) {
        self.metrics.increment();
        self.per_tunnel
            .entry(*tunnel_id)
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }

    fn count_for_tunnel(&self, tunnel_id: &Uuid) -> usize {
        self.per_tunnel
            .get(tunnel_id)
            .map(|entry| *entry)
            .unwrap_or(0)
    }

    fn remove(&self, request_id: &Uuid) -> Option<PendingRequest> {
        let removed = self.requests.remove(request_id).map(|(_, value)| value);
        if let Some(ref pending) = removed {
            self.metrics.decrement();
            let mut remove_entry = false;
            if let Some(mut entry) = self.per_tunnel.get_mut(&pending.tunnel_id) {
                if *entry > 0 {
                    *entry -= 1;
                }
                if *entry == 0 {
                    remove_entry = true;
                }
            }
            if remove_entry {
                self.per_tunnel.remove(&pending.tunnel_id);
            }
        }
        removed
    }

    fn drain_for_tunnel(&self, tunnel_id: &Uuid) -> Vec<PendingRequest> {
        let mut drained = Vec::new();
        let keys: Vec<Uuid> = self
            .requests
            .iter()
            .filter(|entry| entry.value().tunnel_id == *tunnel_id)
            .map(|entry| *entry.key())
            .collect();
        for id in keys {
            if let Some(request) = self.remove(&id) {
                drained.push(request);
            }
        }
        drained
    }
}

/// Shared sender type used by active tunnels.
pub type TunnelSender = mpsc::Sender<OutgoingFrame>;

/// Tracks every active tunnel.
#[derive(Debug)]
pub struct TunnelManager {
    domain: String,
    limits: RequestLimits,
    tunnels: DashMap<String, Arc<ActiveTunnel>>,
    rate_limit_per_minute: NonZeroU32,
    rate_limit_burst: NonZeroU32,
    pending_store: Arc<PendingStore>,
    pending_config: PendingRequestConfig,
    sweeper_cancel: CancellationToken,
    max_tunnels: usize,
    max_tunnels_per_key: usize,
}

#[derive(Debug)]
pub struct ActiveTunnel {
    pub id: Uuid,
    pub subdomain: String,
    pub protocol: TunnelProtocol,
    sender: TunnelSender,
    pending_store: Arc<PendingStore>,
    rate_limiter: RateLimiter<governor::state::NotKeyed, InMemoryState, DefaultClock>,
    requests_forwarded: AtomicU64,
    rate_limit_hits: AtomicU64,
    rate_limit_per_minute: NonZeroU32,
    rate_limit_burst: NonZeroU32,
    api_key_hash: Option<[u8; 32]>,
}

/// Simplified description for admin APIs.
#[derive(Debug, Clone, Serialize)]
pub struct TunnelSummary {
    pub tunnel_id: Uuid,
    pub subdomain: String,
    pub protocol: TunnelProtocol,
    pub requests_forwarded: u64,
    pub rate_limit_hits: u64,
    pub rate_limit_per_minute: u32,
    pub rate_limit_burst: u32,
    pub pending_requests: usize,
}

impl TunnelManager {
    /// Build a new tunnel manager for the provided domain.
    pub fn new(
        domain: String,
        limits: RequestLimits,
        per_minute: u32,
        burst: u32,
        pending_config: PendingRequestConfig,
        max_tunnels: usize,
        max_tunnels_per_key: usize,
    ) -> Self {
        let rate_limit_per_minute = NonZeroU32::new(per_minute.max(1)).unwrap();
        let rate_limit_burst = NonZeroU32::new(burst.max(1)).unwrap();
        let pending_metrics = Arc::new(PendingRequestMetrics::new());
        let pending_store = Arc::new(PendingStore::new(pending_metrics));
        let sweeper_cancel = CancellationToken::new();
        let sweeper_store = pending_store.clone();
        let sweeper_config = pending_config.clone();
        let sweeper_token = sweeper_cancel.clone();
        tokio::spawn(async move {
            pending_request_sweeper(sweeper_store, sweeper_config, sweeper_token).await;
        });

        Self {
            domain,
            limits,
            tunnels: DashMap::new(),
            rate_limit_per_minute,
            rate_limit_burst,
            pending_store,
            pending_config,
            sweeper_cancel,
            max_tunnels,
            max_tunnels_per_key,
        }
    }

    /// Convenience constructor that uses standard rate limits.
    pub fn with_defaults(
        domain: String,
        limits: RequestLimits,
        per_minute: u32,
        burst: u32,
        pending_config: PendingRequestConfig,
        max_tunnels: usize,
        max_tunnels_per_key: usize,
    ) -> Self {
        Self::new(
            domain,
            limits,
            per_minute,
            burst,
            pending_config,
            max_tunnels,
            max_tunnels_per_key,
        )
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }

    pub fn limits(&self) -> RequestLimits {
        self.limits.clone()
    }

    /// Registers a new tunnel, enforcing global and per-key limits.
    pub fn register_tunnel(
        &self,
        tunnel_id: Uuid,
        subdomain: String,
        protocol: TunnelProtocol,
        sender: TunnelSender,
        api_key: Option<&str>,
    ) -> Result<Arc<ActiveTunnel>> {
        let current = self.tunnels.len();
        if self.max_tunnels > 0 && current >= self.max_tunnels {
            warn!(
                current,
                limit = self.max_tunnels,
                "Global tunnel limit reached"
            );
            return Err(ExposeError::CapacityExceeded {
                resource: format!(
                    "server has reached maximum tunnel capacity ({}/{})",
                    current, self.max_tunnels
                ),
            });
        }

        let api_key_hash = api_key.map(hash_api_key);
        if self.max_tunnels_per_key > 0 {
            if let Some(ref hash) = api_key_hash {
                let per_key = self.count_tunnels_for_key(hash);
                if per_key >= self.max_tunnels_per_key {
                    warn!(
                        api_key_hash = %short_hash(hash),
                        current = per_key,
                        limit = self.max_tunnels_per_key,
                        "Per-key tunnel limit reached"
                    );
                    return Err(ExposeError::CapacityExceeded {
                        resource: format!(
                            "API key has reached maximum tunnel limit ({}/{})",
                            per_key, self.max_tunnels_per_key
                        ),
                    });
                }
            }
        }

        if self.tunnels.contains_key(&subdomain) {
            return Err(ExposeError::SubdomainTaken { subdomain });
        }

        let handle = self.new_handle(tunnel_id, subdomain.clone(), protocol, sender, api_key_hash);
        self.tunnels.insert(subdomain, handle.clone());
        ServerMetrics::tunnels_active(self.tunnels.len());
        Ok(handle)
    }

    pub fn remove(&self, subdomain: &str) {
        self.tunnels.remove(subdomain);
        ServerMetrics::tunnels_active(self.tunnels.len());
    }

    pub fn get(&self, subdomain: &str) -> Option<Arc<ActiveTunnel>> {
        self.tunnels.get(subdomain).map(|entry| entry.clone())
    }

    pub fn list(&self) -> Vec<TunnelSummary> {
        self.tunnels
            .iter()
            .map(|entry| {
                let stats = entry.value().stats();
                TunnelSummary {
                    tunnel_id: entry.value().id,
                    subdomain: entry.key().clone(),
                    protocol: entry.value().protocol,
                    requests_forwarded: stats.requests_forwarded,
                    rate_limit_hits: stats.rate_limit_hits,
                    rate_limit_per_minute: stats.rate_limit_per_minute,
                    rate_limit_burst: stats.rate_limit_burst,
                    pending_requests: self.pending_store.count_for_tunnel(&entry.value().id),
                }
            })
            .collect()
    }

    pub fn active_tunnel_count(&self) -> usize {
        self.tunnels.len()
    }

    pub fn pending_request_count(&self) -> usize {
        self.pending_store.len()
    }

    pub fn summary_by_id(&self, id: &Uuid) -> Option<TunnelSummary> {
        self.tunnels
            .iter()
            .find(|entry| entry.value().id == *id)
            .map(|entry| {
                let stats = entry.value().stats();
                TunnelSummary {
                    tunnel_id: entry.value().id,
                    subdomain: entry.key().clone(),
                    protocol: entry.value().protocol,
                    requests_forwarded: stats.requests_forwarded,
                    rate_limit_hits: stats.rate_limit_hits,
                    rate_limit_per_minute: stats.rate_limit_per_minute,
                    rate_limit_burst: stats.rate_limit_burst,
                    pending_requests: self.pending_store.count_for_tunnel(&entry.value().id),
                }
            })
    }

    pub fn allocate_subdomain(&self, desired: Option<String>) -> Result<String> {
        if let Some(raw) = desired {
            let sanitized =
                utils::sanitize_subdomain(&raw).ok_or_else(|| ExposeError::InvalidSubdomain {
                    subdomain: raw.clone(),
                    reason: "invalid subdomain requested".into(),
                })?;
            if self.tunnels.contains_key(&sanitized) {
                return Err(ExposeError::SubdomainTaken {
                    subdomain: sanitized,
                });
            }
            return Ok(sanitized);
        }

        for _ in 0..5 {
            let candidate = random_subdomain();
            if !self.tunnels.contains_key(&candidate) {
                return Ok(candidate);
            }
        }
        Err(ExposeError::CapacityExceeded {
            resource: "unable to allocate unique subdomain".into(),
        })
    }

    pub fn new_handle(
        &self,
        id: Uuid,
        subdomain: String,
        protocol: TunnelProtocol,
        sender: TunnelSender,
        api_key_hash: Option<[u8; 32]>,
    ) -> Arc<ActiveTunnel> {
        let quota =
            Quota::per_minute(self.rate_limit_per_minute).allow_burst(self.rate_limit_burst);
        let rate_limiter = RateLimiter::direct(quota);
        Arc::new(ActiveTunnel {
            id,
            subdomain,
            protocol,
            sender,
            pending_store: self.pending_store.clone(),
            rate_limiter,
            requests_forwarded: AtomicU64::new(0),
            rate_limit_hits: AtomicU64::new(0),
            rate_limit_per_minute: self.rate_limit_per_minute,
            rate_limit_burst: self.rate_limit_burst,
            api_key_hash,
        })
    }

    fn count_tunnels_for_key(&self, key_hash: &[u8; 32]) -> usize {
        self.tunnels
            .iter()
            .filter(|entry| entry.value().api_key_hash.as_ref() == Some(key_hash))
            .count()
    }

    pub fn register_pending_request(
        &self,
        tunnel_id: &Uuid,
        request_id: Uuid,
        timeout: Option<Duration>,
    ) -> Result<oneshot::Receiver<Message>> {
        if self.pending_store.len() >= self.pending_config.max_global {
            return Err(ExposeError::CapacityExceeded {
                resource: "too many pending requests globally".into(),
            });
        }

        let tunnel_pending = self.pending_store.count_for_tunnel(tunnel_id);
        if tunnel_pending >= self.pending_config.max_per_tunnel {
            return Err(ExposeError::CapacityExceeded {
                resource: "tunnel has too many pending requests".into(),
            });
        }

        let (tx, rx) = oneshot::channel();
        let now = Instant::now();
        let deadline = now + timeout.unwrap_or(self.pending_config.default_timeout);
        let pending = PendingRequest {
            sender: tx,
            created_at: now,
            deadline,
            request_id,
            tunnel_id: *tunnel_id,
        };
        self.pending_store.requests.insert(request_id, pending);
        self.pending_store.insert(tunnel_id);

        Ok(rx)
    }

    pub fn disconnect_tunnel(&self, tunnel_id: &Uuid) -> bool {
        let key = self
            .tunnels
            .iter()
            .find(|entry| entry.value().id == *tunnel_id)
            .map(|entry| entry.key().clone());
        if let Some(subdomain) = key {
            if let Some((_, tunnel)) = self.tunnels.remove(&subdomain) {
                tunnel.close();
                return true;
            }
        }
        false
    }
}

impl Drop for TunnelManager {
    fn drop(&mut self) {
        self.sweeper_cancel.cancel();
    }
}

impl ActiveTunnel {
    pub async fn send(&self, message: Message) -> Result<()> {
        self.sender
            .send(OutgoingFrame::Protocol(message))
            .await
            .map_err(|_| ExposeError::TunnelDisconnected {
                reason: Some("tunnel writer closed".into()),
            })
    }

    pub fn fulfill(&self, response: Message) {
        let response_id = match response {
            Message::HttpResponse { id, .. } => id,
            other => {
                warn!(?other, "unexpected message type while fulfilling response");
                return;
            }
        };

        if let Some(pending) = self.pending_store.remove(&response_id) {
            let _ = pending.sender.send(response);
        } else {
            warn!(%response_id, "no waiter for response");
        }
    }

    pub fn close(&self) {
        let drained = self.pending_store.drain_for_tunnel(&self.id);
        for req in drained {
            let _ = req.sender.send(Message::HttpResponse {
                id: req.request_id,
                status: 502,
                headers: vec![("x-expose-error".into(), "tunnel disconnected".into())],
                body: Bytes::new(),
            });
        }
    }

    pub fn check_rate_limit(&self) -> Result<()> {
        match self.rate_limiter.check() {
            Ok(()) => {
                self.requests_forwarded.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(negative) => {
                self.rate_limit_hits.fetch_add(1, Ordering::Relaxed);
                ServerMetrics::rate_limit_hit(&self.subdomain);
                let wait = negative.wait_time_from(DefaultClock::default().now());
                Err(ExposeError::RateLimited {
                    retry_after_secs: wait.as_secs().max(1),
                })
            }
        }
    }

    fn stats(&self) -> TunnelStats {
        TunnelStats {
            requests_forwarded: self.requests_forwarded.load(Ordering::Relaxed),
            rate_limit_hits: self.rate_limit_hits.load(Ordering::Relaxed),
            rate_limit_per_minute: self.rate_limit_per_minute.get(),
            rate_limit_burst: self.rate_limit_burst.get(),
        }
    }
}

struct TunnelStats {
    requests_forwarded: u64,
    rate_limit_hits: u64,
    rate_limit_per_minute: u32,
    rate_limit_burst: u32,
}

/// Background task that removes expired pending requests.
///
/// # Arguments
/// * `store` - Shared pending request store to inspect and prune.
/// * `config` - Pending request configuration containing sweep interval.
/// * `cancel` - Cancellation token used to stop the sweeper during shutdown.
///
/// # Returns
/// A future that completes when the sweeper is cancelled.
///
/// # Errors
/// The task does not return errors; it logs failures internally.
///
/// # Panics
/// Never panics.
async fn pending_request_sweeper(
    store: Arc<PendingStore>,
    config: PendingRequestConfig,
    cancel: CancellationToken,
) {
    let mut interval = time::interval(config.sweep_interval);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("Pending request sweeper shutting down");
                break;
            }
            _ = interval.tick() => {
                let now = Instant::now();
                let mut expired_ids = Vec::new();
                for entry in store.requests.iter() {
                    if now >= entry.value().deadline {
                        expired_ids.push(*entry.key());
                    }
                }
                if expired_ids.is_empty() {
                    continue;
                }
                for id in expired_ids.iter() {
                    if let Some(request) = store.remove(id) {
                        let age_ms = request.created_at.elapsed().as_millis();
                        debug!(
                            request_id = %request.request_id,
                            tunnel_id = %request.tunnel_id,
                            age_ms,
                            "Sweeping expired pending request"
                        );
                        let response = request.timed_out_response();
                        let sender = request.sender;
                        let _ = sender.send(response);
                    }
                }
                store.metrics.increment_expired(expired_ids.len() as u64);
                warn!(count = expired_ids.len(), "Swept expired pending requests");
            }
        }
    }
}

fn random_subdomain() -> String {
    let mut rng = rand::thread_rng();
    (0..8)
        .map(|_| rng.sample(Alphanumeric) as char)
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

fn hash_api_key(key: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hasher.finalize().into()
}

fn short_hash(hash: &[u8; 32]) -> String {
    hex::encode(&hash[..8])
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tokio::time::sleep;

    fn test_config() -> PendingRequestConfig {
        PendingRequestConfig::default()
    }

    #[tokio::test]
    async fn test_register_pending_request_within_limits_succeeds() {
        let manager = TunnelManager::with_defaults(
            "domain".into(),
            RequestLimits::default(),
            10,
            10,
            test_config(),
            100,
            0,
        );
        let tunnel_id = Uuid::new_v4();
        let request_id = Uuid::new_v4();
        let result = manager.register_pending_request(&tunnel_id, request_id, None);
        assert!(result.is_ok());
        assert_eq!(manager.pending_store.len(), 1);
    }

    #[tokio::test]
    async fn test_register_pending_request_exceeds_global_limit_returns_error() {
        let mut config = test_config();
        config.max_global = 1;
        let manager = TunnelManager::with_defaults(
            "domain".into(),
            RequestLimits::default(),
            10,
            10,
            config,
            100,
            0,
        );
        let tunnel_id = Uuid::new_v4();
        manager
            .register_pending_request(&tunnel_id, Uuid::new_v4(), None)
            .unwrap();
        let result = manager.register_pending_request(&tunnel_id, Uuid::new_v4(), None);
        assert!(matches!(result, Err(ExposeError::CapacityExceeded { .. })));
    }

    #[tokio::test]
    async fn test_sweeper_removes_expired_requests() {
        let metrics = Arc::new(PendingRequestMetrics::new());
        let store = Arc::new(PendingStore::new(metrics.clone()));
        let config = PendingRequestConfig {
            sweep_interval: Duration::from_millis(50),
            default_timeout: Duration::from_millis(100),
            ..PendingRequestConfig::default()
        };
        let cancel = CancellationToken::new();
        let (tx, _rx) = oneshot::channel();
        store.requests.insert(
            Uuid::new_v4(),
            PendingRequest {
                sender: tx,
                created_at: Instant::now(),
                deadline: Instant::now() + Duration::from_millis(100),
                request_id: Uuid::new_v4(),
                tunnel_id: Uuid::new_v4(),
            },
        );
        tokio::spawn(pending_request_sweeper(
            store.clone(),
            config,
            cancel.clone(),
        ));
        sleep(Duration::from_millis(200)).await;
        assert_eq!(store.len(), 0);
        cancel.cancel();
    }

    #[tokio::test]
    async fn rate_limiter_blocks_after_burst() {
        let manager = TunnelManager::with_defaults(
            "test".into(),
            RequestLimits::default(),
            1,
            1,
            PendingRequestConfig::default(),
            100,
            0,
        );
        let (tx, _rx) = mpsc::channel(1);
        let tunnel = manager.new_handle(
            Uuid::new_v4(),
            "alpha".into(),
            TunnelProtocol::Http,
            tx,
            None,
        );
        assert!(tunnel.check_rate_limit().is_ok());
        assert!(tunnel.check_rate_limit().is_err());
    }

    #[tokio::test]
    async fn test_register_tunnel_respects_global_limit() {
        let manager = TunnelManager::with_defaults(
            "domain".into(),
            RequestLimits::default(),
            10,
            10,
            PendingRequestConfig::default(),
            1,
            0,
        );
        let (tx, _rx) = mpsc::channel(1);
        manager
            .register_tunnel(
                Uuid::new_v4(),
                "alpha".into(),
                TunnelProtocol::Http,
                tx.clone(),
                None,
            )
            .expect("first tunnel");
        let err = manager
            .register_tunnel(
                Uuid::new_v4(),
                "beta".into(),
                TunnelProtocol::Http,
                tx,
                None,
            )
            .unwrap_err();
        assert!(matches!(err, ExposeError::CapacityExceeded { .. }));
    }

    #[tokio::test]
    async fn test_register_tunnel_respects_per_key_limit() {
        let manager = TunnelManager::with_defaults(
            "domain".into(),
            RequestLimits::default(),
            10,
            10,
            PendingRequestConfig::default(),
            10,
            1,
        );
        let (tx, _rx) = mpsc::channel(1);
        manager
            .register_tunnel(
                Uuid::new_v4(),
                "alpha".into(),
                TunnelProtocol::Http,
                tx.clone(),
                Some("api-key"),
            )
            .expect("first tunnel");
        let err = manager
            .register_tunnel(
                Uuid::new_v4(),
                "beta".into(),
                TunnelProtocol::Http,
                tx,
                Some("api-key"),
            )
            .unwrap_err();
        assert!(matches!(err, ExposeError::CapacityExceeded { .. }));
    }
}
