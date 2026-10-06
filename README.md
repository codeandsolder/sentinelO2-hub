# sentinelO2-hub

Open Sentinel0² Hub: an observable-contract-compatible server for SentinelX-compatible agents.

The implementation is split into a portable state/compatibility core and deployment adapters:

- `sentinel0-hub-core`: host routing, tool projection, idempotency, jobs and transfer state machines with no runtime/platform assumptions.
- `sentinel0-hub-native`: native Rust reference implementation and differential-test oracle.
- `sentinel0-hub-worker`: Cloudflare Workers + Durable Objects deployment (next vertical slice).

## Current vertical slice

The native adapter accepts a current Sentinel0² agent on `/agent/connect`, performs the v1 hello/welcome handshake, maintains host routing state, and forwards `POST /v1/op` requests over the agent WebSocket.

This is deliberately not yet production authentication/storage. It is the smallest end-to-end slice needed to run the real agent against the open Hub and turn hosted-SentinelX behavior into differential tests.
