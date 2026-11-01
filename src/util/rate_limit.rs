use std::time::Instant;

/// Token bucket: `capacity` tokens at most, refilled continuously at `per_second`.
pub struct TokenBucket {
    capacity: f64,
    per_second: f64,
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    pub fn new(capacity: f64, per_second: f64) -> Self {
        Self {
            capacity,
            per_second,
            tokens: capacity,
            last: Instant::now(),
        }
    }

    pub fn try_take(&mut self, amount: f64) -> bool {
        self.try_take_at(amount, Instant::now())
    }

    fn try_take_at(&mut self, amount: f64, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.per_second).min(self.capacity);
        self.last = now;
        if self.tokens >= amount {
            self.tokens -= amount;
            true
        } else {
            false
        }
    }
}

/// Limits what one viewer may push into the shared command channel, by message count and bytes.
pub struct ViewerLimiter {
    messages: TokenBucket,
    bytes: TokenBucket,
}

impl ViewerLimiter {
    pub fn new() -> Self {
        Self {
            messages: TokenBucket::new(60.0, 30.0),
            bytes: TokenBucket::new(2.0 * 1024.0 * 1024.0, 1024.0 * 1024.0),
        }
    }

    pub fn allow(&mut self, size: usize) -> bool {
        self.messages.try_take(1.0) && self.bytes.try_take(size as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn bucket_refills_over_time_up_to_capacity() {
        let mut bucket = TokenBucket::new(2.0, 1.0);
        let start = Instant::now();
        assert!(bucket.try_take_at(2.0, start));
        assert!(!bucket.try_take_at(1.0, start));
        assert!(bucket.try_take_at(1.0, start + Duration::from_secs(1)));
        assert!(!bucket.try_take_at(3.0, start + Duration::from_secs(100)));
        assert!(bucket.try_take_at(2.0, start + Duration::from_secs(100)));
    }

    #[test]
    fn viewer_limiter_stops_a_flood_of_messages() {
        let mut limiter = ViewerLimiter::new();
        let allowed = (0..1000).filter(|_| limiter.allow(10)).count();
        assert!((60..=70).contains(&allowed), "allowed {}", allowed);
    }

    #[test]
    fn viewer_limiter_stops_large_payload_floods() {
        let mut limiter = ViewerLimiter::new();
        let allowed = (0..20).filter(|_| limiter.allow(1024 * 1024)).count();
        assert!(allowed <= 3, "allowed {}", allowed);
    }
}
