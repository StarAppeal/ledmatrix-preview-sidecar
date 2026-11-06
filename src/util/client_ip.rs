use axum::http::HeaderMap;
use std::net::{IpAddr, SocketAddr};

// CF-Connecting-IP is trusted from any peer, so only cloudflared and our own services may reach
// port 8765 in production (no published port, no foreign containers on its networks).
pub fn client_ip(headers: &HeaderMap, peer: SocketAddr) -> IpAddr {
    headers
        .get("cf-connecting-ip")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<IpAddr>().ok())
        .unwrap_or_else(|| peer.ip())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer() -> SocketAddr {
        "172.18.0.5:41000".parse().unwrap()
    }

    fn headers(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("cf-connecting-ip", value.parse().unwrap());
        headers
    }

    #[test]
    fn uses_the_cloudflare_header() {
        assert_eq!(
            client_ip(&headers(" 203.0.113.7 "), peer()),
            "203.0.113.7".parse::<IpAddr>().unwrap()
        );
        assert_eq!(
            client_ip(&headers("2001:db8::1"), peer()),
            "2001:db8::1".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn falls_back_to_the_peer() {
        assert_eq!(client_ip(&HeaderMap::new(), peer()), peer().ip());
        assert_eq!(client_ip(&headers("not-an-ip"), peer()), peer().ip());
        assert_eq!(
            client_ip(&headers("203.0.113.7, 10.0.0.1"), peer()),
            peer().ip()
        );
    }
}
