# agent-sandbox SDK fork — MicroVM transport

These files are **drop-ins for a fork of `kubernetes-sigs/agent-sandbox`**
(published at [github.com/jicowan/agent-sandbox](https://github.com/jicowan/agent-sandbox),
branch `microvm-transport`), in package `clients/go/sandbox`. They are
intentionally *not* part of this repo's Go modules (they reference the SDK's
unexported types), so nothing here compiles standalone — they document the exact
fork changes. The authoritative source is the fork itself; see
[FORK.md](../FORK.md) to build/install it.

## Why a fork, and why this shape

The SDK has two layers:

- **Lifecycle** — `Sandbox`/`Client`, `New()`/`Open()`/`Close()`. This is
  hard-bound to Kubernetes: `New()` constructs a `K8sHelper`; `Open()` calls
  `createClaim()`, `resolveSandboxName()`, `waitForSandboxReady()`, and sets a
  pod IP. **None of this applies to a MicroVM** (no cluster, no `SandboxClaim`,
  no pod). MicroVM lifecycle is driven out-of-band by the `lambda-microvms`
  API (`RunMicrovm`/`Suspend`/`Terminate`), not by the SDK.
- **Transport core** — `connector` + `Commands` + `Files` + the `process/v1`
  gRPC stubs. This is the part that actually speaks the KEP-539.2 runtime
  protocol, and it is transport-agnostic *except* for two hardcoded
  assumptions.

So the fork **reuses the transport core and skips the lifecycle layer.** The
new `NewMicrovm(...)` constructor assembles `connector`/`Commands`/`Files`
against an already-running MicroVM endpoint.

## The two edits

### 1. Make gRPC dialing pluggable (`connector.go`) — small, backward-compatible

Today `GRPCConn()` hardcodes plaintext:

```go
conn, err := grpc.NewClient(c.grpcTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
```

Add a `grpcDialOptions []grpc.DialOption` field threaded from `connectorConfig`,
defaulting to the current insecure behavior when empty:

```diff
 type connectorConfig struct {
 	Strategy            ConnectionStrategy
 	...
 	DisablePodIPRouting bool
+	// GRPCDialOptions overrides dial options for sandboxd's gRPC
+	// ProcessService. Empty = plaintext insecure creds (the pod
+	// port-forward default). MicroVM transports pass TLS creds +
+	// per-RPC auth metadata here.
+	GRPCDialOptions     []grpc.DialOption
 	Log                 logr.Logger
 	...
 }

 type connector struct {
 	...
 	grpcTarget string
 	grpcConn   *grpc.ClientConn
+	grpcDialOpts []grpc.DialOption
 	...
 }

 func newConnector(cfg connectorConfig) *connector {
 	...
 	return &connector{
 		...
+		grpcDialOpts: cfg.GRPCDialOptions,
 	}
 }

 func (c *connector) GRPCConn() (*grpc.ClientConn, error) {
 	...
-	conn, err := grpc.NewClient(c.grpcTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
+	opts := c.grpcDialOpts
+	if len(opts) == 0 {
+		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
+	}
+	conn, err := grpc.NewClient(c.grpcTarget, opts...)
 	...
 }
```

That's the only edit to existing code. Everything else is additive.

### 2. Add the MicroVM constructor (`microvm.go`) — new file

See [`microvm.go`](microvm.go). It:

- injects `X-aws-proxy-auth` + `X-aws-proxy-port: <rest>` on REST via the
  existing `connectorConfig.HTTPTransport` seam (no core change needed);
- dials gRPC with **TLS** + a `grpc.PerRPCCredentials` that adds
  `x-aws-proxy-auth` + `x-aws-proxy-port: <grpc>` + **`x-aws-proxy-force-h2: true`**
  metadata (uses edit #1). The force-h2 header is what makes the endpoint speak
  HTTP/2 to the backend, so sandboxd's *native* gRPC works — the edge otherwise
  downgrades to HTTP/1.1 and gRPC fails on missing trailers;
- sets `RouterHeaders: false` (like sandboxd — no `X-Sandbox-*`);
- exposes `Run` (unary `Execute`) and `Exec` (streaming `Start` — PTY, stdin,
  signals) via the reused `Commands` + the `process/v1` client, plus
  `Write`/`Read`/`List` via `Files`, with no claim/pod machinery;
- `wrapSessionEnv` prefixes shell commands with sourcing `/etc/microvm/session.env`
  (the shim writes per-session env there at `/run`, since sandboxd's own env is
  fixed at boot).

## What upstream-vs-fork means

- Edit #1 is genuinely upstreamable — it removes a hardcoded assumption without
  changing default behavior. Worth a PR regardless.
- The `NewMicrovm` constructor could be upstreamed as a `MicrovmConnectionStrategy`
  + option fields, but it's the part most likely to want iteration, so keep it
  in the fork during the experiment.

## Validation (resolved — see DESIGN.md)

- **gRPC over the endpoint — works.** The endpoint routes gRPC by the
  `x-aws-proxy-port` metadata header, and `x-aws-proxy-force-h2: true` forces the
  HTTP/2 backend hop. Unary `Execute`, streaming `Start`, PTY, and `WriteStdin`
  all verified against a live MicroVM.
- **Long streams vs. token expiry — fine.** Auth is validated only at the
  handshake: a stream started with a short-lived token survives past its expiry.
  `RefreshingToken` mints a fresh token per connection/RPC; no mid-stream refresh
  needed.
