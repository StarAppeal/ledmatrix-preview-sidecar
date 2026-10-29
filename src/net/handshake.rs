use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_SECRET_LEN: usize = 256;

/// Every TCP client must send `<secret>\n` before anything else.
/// Without a configured secret the line is still consumed but not checked.
pub async fn authenticate<S: AsyncRead + Unpin>(socket: &mut S, secret: Option<&str>) -> bool {
    let line = match tokio::time::timeout(HANDSHAKE_TIMEOUT, read_line(socket)).await {
        Ok(Some(line)) => line,
        _ => return false,
    };
    match secret {
        Some(secret) => constant_time_eq(&line, secret.as_bytes()),
        None => true,
    }
}

// Reads byte by byte so nothing after the newline is buffered away from the frame reader.
async fn read_line<S: AsyncRead + Unpin>(socket: &mut S) -> Option<Vec<u8>> {
    let mut line = Vec::new();
    loop {
        let byte = socket.read_u8().await.ok()?;
        if byte == b'\n' {
            return Some(line);
        }
        if line.len() >= MAX_SECRET_LEN {
            return None;
        }
        line.push(byte);
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    async fn run(input: &[u8], secret: Option<&str>) -> (bool, Vec<u8>) {
        let (mut client, mut server) = tokio::io::duplex(1024);
        client.write_all(input).await.unwrap();
        drop(client);
        let ok = authenticate(&mut server, secret).await;
        let mut rest = Vec::new();
        server.read_to_end(&mut rest).await.unwrap();
        (ok, rest)
    }

    #[tokio::test]
    async fn accepts_matching_secret_and_leaves_following_bytes() {
        let (ok, rest) = run(b"s3cret\n\x05hello", Some("s3cret")).await;
        assert!(ok);
        assert_eq!(rest, b"\x05hello");
    }

    #[tokio::test]
    async fn rejects_wrong_secret() {
        assert!(!run(b"wrong\n", Some("s3cret")).await.0);
        assert!(!run(b"s3cre\n", Some("s3cret")).await.0);
        assert!(!run(b"\n", Some("s3cret")).await.0);
    }

    #[tokio::test]
    async fn rejects_missing_newline_and_oversized_line() {
        assert!(!run(b"s3cret", Some("s3cret")).await.0);
        assert!(!run(&[b'a'; MAX_SECRET_LEN + 1], None).await.0);
    }

    #[tokio::test]
    async fn accepts_any_line_without_configured_secret() {
        assert!(run(b"\n", None).await.0);
        assert!(run(b"whatever\n", None).await.0);
    }
}
