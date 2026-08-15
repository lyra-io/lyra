use super::{Conn, ConnOptions};
use crate::error_inner::InnerError;
use dashmap::DashMap;
use meta::proto::pb_ext::lyra_client::LyraClient;
use std::sync::atomic::{AtomicUsize, Ordering};
use tonic::transport::{Channel, Endpoint};
use tracing::info;

struct PoolEntry {
    conns: Vec<Conn>,
    next: AtomicUsize,
}

/// Connection pool that maintains multiple [`Conn`]s per endpoint, all sharing
/// a single gRPC channel. Conns are created lazily on first access and reused
/// via round-robin.
pub struct ConnPool {
    entries: DashMap<String, PoolEntry>,
    opts: ConnOptions,
}

impl ConnPool {
    pub fn new(opts: ConnOptions) -> Self {
        Self {
            entries: DashMap::new(),
            opts,
        }
    }

    /// Return an existing conn for the endpoint (round-robin) or create a new
    /// pool entry with `conns_per_unit` conns sharing one gRPC channel.
    pub fn get_or_connect(&self, endpoint: &str) -> Result<Conn, InnerError> {
        if let Some(entry) = self.entries.get(endpoint) {
            let idx = entry.next.fetch_add(1, Ordering::Relaxed) % entry.conns.len();
            return Ok(entry.conns[idx].clone());
        }
        // One channel shared across all conns for this endpoint.
        let channel = Self::build_channel(endpoint, &self.opts)?;
        let client = LyraClient::new(channel);

        let mut conns = Vec::with_capacity(self.opts.conns_per_unit);
        for _ in 0..self.opts.conns_per_unit {
            conns.push(Conn::new(endpoint.to_string(), client.clone()));
        }
        info!(address = %endpoint, count = self.opts.conns_per_unit, "connected to unit");
        let entry = PoolEntry {
            conns,
            next: AtomicUsize::new(0),
        };
        let conn = entry.conns[0].clone();
        self.entries.insert(endpoint.to_string(), entry);
        Ok(conn)
    }

    fn build_channel(endpoint: &str, opts: &ConnOptions) -> Result<Channel, InnerError> {
        let channel = Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| InnerError::Transport(format!("{}: {}", endpoint, e)))?
            .connect_timeout(opts.connect_timeout)
            .timeout(opts.request_timeout)
            .http2_keep_alive_interval(opts.keep_alive_interval)
            .keep_alive_timeout(opts.keep_alive_timeout)
            .keep_alive_while_idle(true)
            .connect_lazy();
        Ok(channel)
    }
}
