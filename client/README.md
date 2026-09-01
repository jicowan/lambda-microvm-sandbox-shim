# Client-side transport

How the agent-sandbox SDK reaches `sandboxd` inside a Lambda MicroVM, over the
MicroVM's public HTTPS endpoint.

> This directory is the **standalone, fork-free** REST helper (a plain
> `http.RoundTripper`) — useful for raw REST access or testing without the SDK.
> The full SDK integration (REST **and** gRPC, via `NewMicrovm`) lives in
> [`../sdk-fork/`](../sdk-fork/) and requires the agent-sandbox fork.

## The two surfaces, and their status

| Surface | Transport | Status |
|---|---|---|
| Filesystem REST (`/v1/files`, `/v1/health`, `/v1/metadata`) | HTTPS + `X-aws-proxy-auth` + `X-aws-proxy-port: 8080` | ✅ achievable today via a custom `http.RoundTripper` |
| ProcessService gRPC (`Start`/`Execute`/…) | HTTPS(h2) + `X-aws-proxy-auth` + `X-aws-proxy-port: 9090` | ⚠ needs an upstream SDK change |

## REST — works today

The SDK connector accepts a pluggable `http.RoundTripper`
(`connectorConfig.HTTPTransport`) and a `DirectStrategy{URL}`. Point the
strategy at the endpoint and use `ProxyRoundTripper` (see
[`microvm_transport.go`](microvm_transport.go)) as the transport:

```go
rt := &microvmclient.ProxyRoundTripper{
    Tokens: microvmclient.StaticToken(jweToken), // from CreateMicrovmAuthToken
    Port:   microvmclient.SandboxdRESTPort,       // "8080"
}
// Wire rt + DirectStrategy{URL: "https://<microvm-endpoint>"} into the SDK's
// connector. Every REST call now carries the proxy headers; Lambda's edge
// authenticates, strips them, and forwards to sandboxd's REST listener.
```

The MicroVM endpoint terminates TLS + JWE at the edge, so no in-VM auth is
needed — the shim just proxies `/v1/*` to loopback `sandboxd`.

## gRPC — needs an upstream change

The SDK's `connector` currently:

- dials gRPC with `insecure.NewCredentials()` (plaintext), and
- takes the gRPC target only from `podTunnelStrategy` (a local port-forward).

For a MicroVM we instead need to:

1. Dial the **endpoint** with **TLS** transport credentials.
2. Attach **per-RPC metadata** on every call:
   `x-aws-proxy-auth: <token>` and `x-aws-proxy-port: 9090`
   (a `grpc.PerRPCCredentials` implementation, or a client interceptor).
3. Refresh the token before it expires — critical for the long-lived server
   stream `Start` (see DESIGN.md #3).

Proposed shape for upstreaming: a first-class `MicrovmConnectionStrategy` plus a
connector option to supply gRPC dial options / credentials, so the plaintext
assumption isn't baked in. Until then, this path requires vendoring a fork of
`clients/go/sandbox/connector.go`.

## Open validation items

- Confirm the endpoint cleanly carries **long-lived h2 server streams** (the
  `Start` RPC streams stdout/stderr until process exit) including trailers and
  keepalive.
- Confirm behavior of `X-aws-proxy-port` for gRPC (does the edge forward h2c to
  the target port as expected).
- First-call-after-suspend latency: auto-resume blocks the first request; SDK
  timeouts/retries must tolerate it.
