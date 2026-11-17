use axum::body::Bytes;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use flate2::{write::DeflateEncoder, Compression};
use std::io::Write;

// Fast PNG encoding optimized for streaming.
pub fn encode_fast_png(raw_rgb: &[u8]) -> String {
    let mut buffer = Vec::with_capacity(16384);
    {
        let mut encoder = png::Encoder::new(&mut buffer, 64, 64);
        encoder.set_color(png::ColorType::Rgb);
        encoder.set_depth(png::BitDepth::Eight);
        // Fast compression is CPU-efficient for streams.
        encoder.set_compression(png::Compression::Fast);

        let mut writer = encoder.write_header().unwrap();
        writer.write_image_data(raw_rgb).unwrap();
    }
    STANDARD.encode(&buffer)
}

/// Raw deflate (RFC 1951) of a 64x64 RGB frame; mostly dark frames shrink from 12 KiB to a few hundred bytes.
pub fn deflate_frame(raw_rgb: &[u8]) -> Bytes {
    let mut encoder = DeflateEncoder::new(Vec::with_capacity(2048), Compression::fast());
    encoder
        .write_all(raw_rgb)
        .expect("writing to a Vec cannot fail");
    Bytes::from(encoder.finish().expect("writing to a Vec cannot fail"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::DeflateDecoder;
    use std::io::Read;

    #[test]
    fn deflated_frames_inflate_to_the_original() {
        let mut frame = vec![0u8; 12288];
        frame[100..160].fill(245);
        let compressed = deflate_frame(&frame);
        assert!(compressed.len() < 200);

        let mut restored = Vec::new();
        DeflateDecoder::new(&compressed[..])
            .read_to_end(&mut restored)
            .unwrap();
        assert_eq!(restored, frame);
    }
}
