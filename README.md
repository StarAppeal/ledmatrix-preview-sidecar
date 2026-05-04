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
   {"type":"AUTH","token":"<jwt>"}
2. On success, server responds:
   {"type":"AUTH_SUCCESS"}
3. On failure, server responds:
   {"type":"ERROR"}

Subsequent client messages are forwarded to Rust as JSON (see below).

### TCP frames (Rust -> service) on port 5001
Binary protocol:
- 1 byte: user_id length (N)
- N bytes: user_id (UTF-8)
- 12288 bytes: raw RGB data (64x64x3)

The service encodes the frame as PNG and sends to the matching WebSocket client:
{"type":"PREVIEW_FRAME","payload":"data:image/png;base64,<...>"}

### TCP commands (service -> Rust) on port 5002
The service sends newline-delimited JSON:
- {"event":"connect","user_id":"..."}
- {"event":"message","user_id":"...","data":{...}}
- {"event":"disconnect","user_id":"..."}

## Configuration
Environment variables:
- BACKEND_INTERNAL_URL (default: http://ledmatrix-backend:3000)

## Build and Run
### Docker
The provided Dockerfile builds and runs the service.

### Local
If you run locally, ensure the backend URL is reachable.

## Ports
- 8765: WebSocket server
- 5001: TCP frame listener
- 5002: TCP command broadcaster