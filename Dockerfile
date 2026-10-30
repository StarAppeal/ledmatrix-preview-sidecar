FROM rust:1.95-bookworm AS base
WORKDIR /app
COPY . .

FROM base AS test
RUN rustup component add clippy
RUN --mount=type=cache,id=sidecar-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=sidecar-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=sidecar-target,target=/app/target \
    cargo clippy --all-targets -- -D warnings && cargo test

FROM base AS builder
RUN --mount=type=cache,id=sidecar-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=sidecar-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=sidecar-target,target=/app/target \
    cargo build --release && cp target/release/rust-preview /usr/local/bin/rust-preview

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /usr/local/bin/rust-preview /app/rust-preview

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

ENV PORT=8765
EXPOSE 8765 5001 5002

CMD ["./rust-preview"]
