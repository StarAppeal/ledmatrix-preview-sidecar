# Rust Preview Service

## Overview
This service bridges WebSocket clients and a Rust backend for LED matrix preview frames. It:
- Accepts WebSocket connections from clients on port 8765.
- Receives raw RGB frames from Rust over TCP on port 5001.
- Forwards client events/commands to Rust over TCP on port 5002.

## Architecture
- WebSocket server (Axum) handles client auth and messaging.
- TCP frame listener ingests raw 64x64 RGB frames (12288 bytes) and sends PNG previews to the WebSocket client.
- TCP command broadcaster forwards client events to the Rust process.

## Protocols
### WebSocket (client -> service)
1. Client must send an AUTH message first:
   {"type":"AUTH","token":"<jwt>","device":"<virtual device id, optional>","format":"png|raw|deflate"}

   `format` picks how frames arrive: `png` (default) as `PREVIEW_FRAME` text messages, `raw` as
   binary 12288-byte RGB frames, `deflate` as the same raw frames, raw-deflated (RFC 1951). A
   mostly dark frame shrinks from 12 KiB to a few hundred bytes, which matters because every
   viewer gets up to 30 frames per second.
2. On success, server responds:
   {"type":"AUTH_SUCCESS"}
3. On failure, server responds:
   {"type":"ERROR"}
4. Too many AUTH attempts from the client address, server responds and closes (retry later):
   {"type":"RATE_LIMITED"}

Connections over the per-address cap are refused with HTTP 429 before the upgrade.
The client address is `CF-Connecting-IP` if it holds a valid IP, otherwise the TCP peer, so
only cloudflared and internal services may reach port 8765 in production. IPv6 addresses
count per /56.

Subsequent client messages are forwarded to Rust as JSON (see below).

### TCP frames (Rust -> service) on port 5001
Binary protocol:
- 1 byte: user_id length (N)
- N bytes: user_id (UTF-8)
- 12288 bytes: raw RGB data (64x64x3)

The service sends the frame to the matching WebSocket clients in their format, for PNG viewers as:
{"type":"PREVIEW_FRAME","payload":"data:image/png;base64,<...>"}

### TCP commands (service -> Rust) on port 5002
The service sends newline-delimited JSON:
- {"event":"connect","user_id":"..."}
- {"event":"message","user_id":"...","data":{...}}
- {"event":"disconnect","user_id":"..."}

## Configuration
Environment variables:
- BACKEND_INTERNAL_URL (default: http://ledmatrix-backend:3000)
- MAX_CONNECTIONS_PER_IP (default: 10) – open WebSockets per client address
- MAX_AUTH_PER_IP_MINUTE (default: 30) – AUTH attempts per client address and minute
- MAX_CONNECTION_MINUTES (default: 60) – a WebSocket is closed after this time; clients reconnect with a fresh token
- MAX_DEVICE_SESSIONS (default: 50) – virtual matrices running at once; further ones get `{"type":"BUSY"}`

## Health
`GET /healthz` on port 8765 answers
`{"status":"ok|degraded","preview_connected":true,"frame_link":true,"device_sessions":3,"max_device_sessions":50,"viewers":4}`.
`degraded` means the preview service's command socket (port 5002) is not connected. The frame
socket (port 5001) is only opened once there is a frame to send, so `frame_link` may be `false`
while idle and does not affect the status. Requests with a
`CF-Connecting-IP` header (everything coming through Cloudflare) get a 404, so only the backend
and Docker see it. `rust-preview healthcheck` probes it from inside the container (the image has no curl).

## Build and Run
### Docker
The provided Dockerfile builds and runs the service.

### Local
If you run locally, ensure the backend URL is reachable.

## Ports
- 8765: WebSocket server
- 5001: TCP frame listener
- 5002: TCP command broadcaster