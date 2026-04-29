use base64::{engine::general_purpose::STANDARD, Engine as _};

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


