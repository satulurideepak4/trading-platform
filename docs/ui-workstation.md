# Trading workstation UI

`trading-ui` is a React control-plane client for the local platform. It deliberately has no direct
dependency on Kafka, Postgres, the matching engine or the TCP order-entry listener. Browser traffic
uses the gateway's authenticated REST API and the public market-data WebSocket only.

## Local topology

```text
Browser
  │ same-origin HTTPS/HTTP
  ▼
trading-ui (Nginx)
  ├── /api/* ──────► trading-gateway REST API
  └── /marketdata ─► trading-gateway WebSocket
```

The Compose proxy makes the browser client same-origin with the gateway, so the development stack
does not need a permissive CORS policy. The API key is retained only in `sessionStorage` for a local
session; it is never included in a frontend image, source file or URL.

## Operator behaviour

* Submit and cancel are real gateway commands. No local success state is displayed unless the
  gateway acknowledged the command.
* The ticket is disabled until authenticated REST reads have succeeded.
* Positions, executions and P&L are labelled as a durable asynchronous projection. The synchronous
  risk view is shown separately because it can lead the portfolio projection.
* Market updates retain their backend sequence outcome. A `GAP`, `DUPLICATE` or unavailable feed is
  visible rather than being quietly converted to a clean quote.
* The UI re-fetches REST state every five seconds and refreshes after a trading action. This is
  intentional until the gateway exposes a user-scoped order/execution push stream.

## Production follow-ups

The local API-key model is appropriate for the sample environment, not a browser-facing production
identity system. A real deployment needs session-based authentication or an OAuth/OIDC BFF, CSRF
protection, audited user identity and a user-scoped execution stream. It also inherits the gateway's
documented single-instance and restart/idempotency limitations.
