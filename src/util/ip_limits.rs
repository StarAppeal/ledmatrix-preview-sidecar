use dashmap::{mapref::entry::Entry, DashMap};
use std::{net::IpAddr, sync::Arc};

use super::rate_limit::TokenBucket;

/// Caps open WebSockets and AUTH attempts (each one costs backend calls) per client address.
pub struct IpLimits {
    conns: Arc<DashMap<IpAddr, usize>>,
    auth: DashMap<IpAddr, TokenBucket>,
    max_conns: usize,
    auth_per_minute: f64,
}

/// One open connection; frees its slot on drop.
pub struct ConnGuard {
    conns: Arc<DashMap<IpAddr, usize>>,
    key: IpAddr,
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        if let Entry::Occupied(mut entry) = self.conns.entry(self.key) {
            *entry.get_mut() -= 1;
            if *entry.get() == 0 {
                entry.remove();
            }
        }
    }
}

// An IPv6 client usually controls a whole /64 or more, so limits apply per /56 (like the backend)
fn limit_key(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(_) => ip,
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6((u128::from(v6) & !((1u128 << 72) - 1)).into()),
        },
    }
}

impl IpLimits {
    pub fn new(max_conns: usize, auth_per_minute: usize) -> Self {
        Self {
            conns: Arc::new(DashMap::new()),
            auth: DashMap::new(),
            max_conns,
            auth_per_minute: auth_per_minute as f64,
        }
    }

    pub fn try_acquire(&self, ip: IpAddr) -> Option<ConnGuard> {
        let key = limit_key(ip);
        let mut count = self.conns.entry(key).or_insert(0);
        if *count >= self.max_conns {
            return None;
        }
        *count += 1;
        Some(ConnGuard {
            conns: self.conns.clone(),
            key,
        })
    }

    pub fn allow_auth(&self, ip: IpAddr) -> bool {
        let n = self.auth_per_minute;
        self.auth
            .entry(limit_key(ip))
            .or_insert_with(|| TokenBucket::new(n, n / 60.0))
            .try_take(1.0)
    }

    /// Drops buckets that have refilled completely; a fresh one would behave the same.
    pub fn prune(&self) {
        self.auth.retain(|_, bucket| !bucket.is_full());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn caps_connections_per_ip_and_frees_them_on_drop() {
        let limits = IpLimits::new(10, 30);
        let guards: Vec<_> = (0..10)
            .map(|_| limits.try_acquire(ip("203.0.113.7")).unwrap())
            .collect();
        assert!(limits.try_acquire(ip("203.0.113.7")).is_none());
        assert!(limits.try_acquire(ip("203.0.113.8")).is_some());

        drop(guards);
        assert!(!limits.conns.contains_key(&ip("203.0.113.7")));
        assert!(limits.try_acquire(ip("203.0.113.7")).is_some());
    }

    #[test]
    fn groups_ipv6_by_56_prefix() {
        let limits = IpLimits::new(1, 30);
        let _guard = limits.try_acquire(ip("2001:db8:0:1::1")).unwrap();
        assert!(limits.try_acquire(ip("2001:db8:0:ff:abcd::2")).is_none());
        assert!(limits.try_acquire(ip("2001:db8:0:100::1")).is_some());
    }

    #[test]
    fn treats_ipv4_mapped_addresses_as_ipv4() {
        let limits = IpLimits::new(1, 30);
        let _guard = limits.try_acquire(ip("203.0.113.7")).unwrap();
        assert!(limits.try_acquire(ip("::ffff:203.0.113.7")).is_none());
    }

    #[test]
    fn caps_auth_attempts_per_ip() {
        let limits = IpLimits::new(10, 30);
        let allowed = (0..100)
            .filter(|_| limits.allow_auth(ip("203.0.113.7")))
            .count();
        assert_eq!(allowed, 30);
        assert!(limits.allow_auth(ip("203.0.113.8")));
    }

    #[test]
    fn prune_keeps_only_buckets_in_use() {
        let limits = IpLimits::new(10, 30);
        limits.allow_auth(ip("203.0.113.7"));
        limits.prune();
        assert_eq!(limits.auth.len(), 1);

        limits
            .auth
            .insert(ip("203.0.113.8"), TokenBucket::new(30.0, 0.5));
        limits.prune();
        assert!(!limits.auth.contains_key(&ip("203.0.113.8")));
    }
}
