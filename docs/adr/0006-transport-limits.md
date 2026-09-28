# ADR 0006 — Transport limits on by default

- Status: accepted
- Date: 2026-09-28

## Context

The security review found that tonic 0.14 passes `None` to hyper for
`max_concurrent_streams` and `http2_max_local_error_reset_streams`.
`None` removes the hyper limits (200 streams; 1024 local resets, the fix
for reset floods such as CVE-2025-8671). The first version also had no
connection limit, no keepalive and no TLS handshake timeout. The dev
profile listened on all interfaces with reflection on.

## Decision

- Always set `max_concurrent_streams` (default 200) and
  `http2_max_local_error_reset_streams` (default 1024). Both must be more
  than 0.
- `max_connections` (default 1000): the accept stream takes a semaphore
  permit before each accept. The connection keeps it until it closes.
- HTTP/2 keepalive is on (60 s interval, 20 s timeout).
- `tls.handshake_timeout_ms` (default 10 s).
- Health and reflection decode 16 KiB at most. No guard wraps them.
- An empty `bind` gives loopback in `dev`/`test` and all interfaces in
  other profiles, like Autumn core. The plugin warns when reflection is on
  and the listener is not on loopback.

## Consequences

- An app with more than 1000 open gRPC connections must raise
  `max_connections`.
- User services keep the tonic decode limit (4 MiB). The README tells
  users to lower it per service.
