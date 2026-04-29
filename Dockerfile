FROM rust:1-bookworm as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/rust-preview /app/rust-preview

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

ENV PORT=8765
EXPOSE 8765 5001 5002

CMD ["./rust-preview"]