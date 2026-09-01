# Design: runtime-compatibility shim for Lambda MicroVMs

## Why the runtime track (not the controller track)

agent-sandbox has two layers:

- **Orchestration** — the `Sandbox` CRD + `SandboxReconciler`, which is
  *hard-wired to create Kubernetes Pods* (there is no backend/provider seam).
  Backing a `Sandbox` with a MicroVM would mean a fork or a virtual-kubelet-style
  provider, and it inherits every mismatch: no routable pod IP, no in-cluster
  Service DNS, no PVCs, the 8h cap fighting the "indefinitely resumable" model,
  and PodSpec fields with no MicroVM analog.
- **Runtime** — the KEP-539.2 `sandboxd` daemon + the SDK that talks to it.
  This layer is *explicitly designed to be execution-environment agnostic*
  ("a Firecracker microVM" is named as a target). The SDK reaches `sandboxd`
  through a pluggable `ConnectionStrategy`.

The runtime track gives ~90% of the practical value (agents run code / move
files against a MicroVM through the same SDK) for a fraction of the cost, and
it's the sanctioned extension point. So we start here.

## The key facts we build on (verified against upstream `main`)

1. `sandboxd` exposes only two surfaces and nothing else:
   - `gRPC :9090` `ProcessService` — `Start` (server-stream, optional PTY),
     `Execute` (unary), `WriteStdin`, `SendSignal`, `ResizeTTY`.
   - `HTTP :8080` `/v1/files/{path}` (GET/PUT/DELETE/HEAD), `/v1/health`,
     `/v1/metadata`.
   It binds `--listen-host` (default `0.0.0.0`) and is meant to be reached
   "like any in-pod service." Commands execute in **whatever rootfs hosts the
   `sandboxd` process** — so on a MicroVM they run in the MicroVM image. This is
   upstream "Topology A".
2. Lambda MicroVM connectivity: one **public HTTPS endpoint** per MicroVM.
   All requests carry a **JWE token** (`X-aws-proxy-auth`), default routed to
   port **8080**, other ports via **`X-aws-proxy-port`**. HTTP/2, gRPC,
   WebSockets, SSE are supported. **Lambda terminates auth at the edge** and
   forwards plain HTTP/gRPC (minus the MicroVM subprotocols) to the app port.
   **Caveat that shaped the design:** the edge→backend hop defaults to HTTP/1.1,
   which breaks native gRPC (needs HTTP/2 trailers). Setting the documented
   **`x-aws-proxy-force-h2: true`** header makes the edge speak HTTP/2 to the
   backend, so sandboxd's real gRPC `ProcessService` works end-to-end — no
   in-VM protocol translation needed.
3. Lambda lifecycle hooks are HTTP paths the app must expose:
   `/aws/lambda-microvms/runtime/v1/{run,resume,suspend,terminate}` at runtime,
   `{ready,validate}` at image-build time. Traffic only flows after `/run`
   returns 200. `sandboxd` does **not** serve these.
4. The SDK `ConnectionStrategy` is `Connect(ctx) (baseURL string, err error)` +
   `Close()`. The connector also takes a pluggable `HTTPTransport
   http.RoundTripper`. But gRPC dialing in the connector is hardcoded to
   `insecure` (plaintext) with the target published only by the pod
   port-forward strategy.

## Architecture

### Inside the MicroVM

```
                      Lambda edge (terminates TLS + JWE auth)
                        │  HTTP/1.1 to :8080 (REST + hooks)
                        │  HTTP/2   to :9090 (gRPC, via x-aws-proxy-force-h2)
                                   ▼
        ┌───────────────────────────────────────────────┐
        │ MicroVM image (arm64)                           │
        │                                                 │
        │  microvm-shim  (PID 1, entrypoint)              │
        │   ├─ :8080  HTTP                                │
        │   │   ├─ /aws/lambda-microvms/runtime/v1/*  ──► handled locally
        │   │   └─ everything else  ──► reverse-proxy ──► 127.0.0.1:8081 (sandboxd REST)
        │   └─ supervises child:                          │
        │        sandboxd --listen-host=0.0.0.0 \         │
        │                 --rest-port=8081 --grpc-port=9090 --root-dir=/workspace
        │                                                 │
        │  :9090  gRPC ProcessService ◄── edge routes here DIRECTLY (no shim hop)
        │                                                 │
        │  (+ user tools baked in: python3, python3.11…)  │
        └───────────────────────────────────────────────┘
```

Division of labor between the shim and `sandboxd`:

- The shim owns **`:8080`** because it must answer the Lambda lifecycle hooks;
  to keep `/v1/*` working it reverse-proxies the rest to `sandboxd`'s REST on
  loopback `:8081`. It gates that proxy on `/run`/`/terminate`.
- `sandboxd` binds **`:9090`** on `0.0.0.0` and the edge routes gRPC there
  **directly** (`x-aws-proxy-port: 9090` + `x-aws-proxy-force-h2: true`). Exec
  therefore uses sandboxd's *native* protocol — real PTY, stdin, signals,
  `ResizeTTY` — with no shim byte-proxy and no protocol translation. An earlier
  design fronted `:9090` in the shim (first as a TCP byte-proxy, later a
  WebSocket bridge); both were **removed** once force-h2 proved native gRPC
  works over the endpoint.
- Because sandboxd's process env is fixed at boot, per-session env (from
  `runHookPayload`, incl. Secrets Manager) can't be injected into native exec
  directly. The shim writes it at `/run` to **`/etc/microvm/session.env`**; the
  SDK prefixes shell commands with sourcing that file (see `wrapSessionEnv`).

### Client side (the SDK caller)

```
agent code ─► agent-sandbox SDK
                ├─ REST  : DirectStrategy{ https://<endpoint> } + custom RoundTripper
                │            (injects X-aws-proxy-auth, X-aws-proxy-port: 8080, TLS)
                └─ gRPC  : dial https://<endpoint>:443 with TLS creds + per-RPC metadata
                             (x-aws-proxy-auth, x-aws-proxy-port: 9090,
                              x-aws-proxy-force-h2: true)
                             ⚠ requires the small connector dial-options change (see below)
```

## Request/lifecycle flow

| Phase | Lambda action | Shim behavior |
|---|---|---|
| Image build | `CreateMicrovmImage` runs Dockerfile, starts app, polls `/ready`, samples `/validate`, snapshots | shim starts `sandboxd`; `/ready` returns 200 once `sandboxd` `/v1/health` is ok; `/validate` runs a trivial self-exec |
| Launch | `RunMicrovm` → resume snapshot → `POST /run {microvmId, runHookPayload}` | shim resets per-session state, applies `runHookPayload` (e.g. tenant/session env under `SANDBOX_*`), returns 200 → traffic flows |
| Serving | client hits endpoint with token | edge auth → REST proxied by shim to `sandboxd` (:8081); gRPC exec routed by the edge directly to `sandboxd` (:9090) |
| Idle | idle policy or `SuspendMicrovm` → `POST /suspend` | shim quiesces (drain in-flight, flush), returns 200; Lambda snapshots memory+disk |
| Resume | traffic or `ResumeMicrovm` → `POST /resume` | shim re-validates, returns 200 |
| Teardown | 8h cap / `TerminateMicrovm` → `POST /terminate` | shim triggers `sandboxd` graceful shutdown, returns 200 |

## agent-sandbox ↔ Lambda MicroVM mapping

| agent-sandbox concept | Lambda MicroVM |
|---|---|
| Sandbox (running) | a running MicroVM (`RunMicrovm`) |
| `operatingMode: Suspended` | `SuspendMicrovm` / idle policy |
| resume | `ResumeMicrovm` / auto-resume on traffic |
| `Lifecycle.shutdownTime` | `--maximum-duration-in-seconds` (≤ 28 800) |
| SandboxWarmPool + SandboxClaim | pre-run pool of MicroVMs; a claim **adopts** a ready one (implemented in the provider + the fork's extensions manager) |
| PVC-backed persistence | ⚠ **no analog** — snapshot dies at 8h; externalize to S3/EFS |
| pod port-forward transport | HTTPS endpoint + JWE token transport (this project) |
| `sandboxd` sidecar/runtime container | `sandboxd` baked into the MicroVM image (Topology A) |

## Resolved (validated end-to-end on EKS 1.36)

1. **gRPC over the endpoint — RESOLVED.** The edge downgrades to HTTP/1.1 by
   default (native gRPC fails with "closed stream without trailers"), but with
   **`x-aws-proxy-force-h2: true`** it speaks HTTP/2 to the backend. Unary
   `Execute`, server-streaming `Start`, PTY, `WriteStdin`, and signals all work.
2. **SDK gRPC auth + K8s-bound lifecycle — RESOLVED.** The fork (a) makes gRPC
   dial options pluggable — one small backward-compatible edit to `connector.go`
   (independently upstreamable) — and (b) adds a K8s-free `NewMicrovm`
   constructor reusing only the transport core (`connector` + `Commands` +
   `Files`), skipping the claim/pod machinery. Per-RPC credentials inject the
   three proxy headers over TLS. See [`sdk-fork/`](sdk-fork/).
3. **Token lifetime vs. long streams — RESOLVED.** Auth is validated at the
   handshake only: a stream started with a short-lived token survives past its
   expiry. `RefreshingToken` mints a fresh token per connection/RPC, which is
   sufficient; no mid-stream refresh needed.
4. **`/run` reset + per-session env — RESOLVED.** `/run` scrubs `/workspace` and
   materializes `runHookPayload.env` (+ Secrets Manager values, fetched under
   the execution role) into `/etc/microvm/session.env`. sandboxd's own env is
   fixed at boot, so the SDK sources that file for shell commands.

## Open questions / risks

1. **Auto-resume latency on first call.** First request after suspend blocks on
   resume (+ `/resume` hook). SDK timeouts/retries must tolerate it.
2. **PID 1 / zombie reaping.** shim is PID 1; orphaned grandchildren reparent to
   it. Either add a reaper to the shim or use `tini` as PID 1 with shim as its
   child. (Detached workload daemons — e.g. the chat-agent example — self-`setsid`
   to survive the exec that launched them; the minimal image ships no `setsid`
   binary, so they use `os.setsid()` / equivalent.)
3. **Where do tools live.** Topology A means every tool the agent invokes must
   be in the image. No per-session image swap (unlike a PodSpec). Multiple tool
   profiles = multiple MicroVM images.
4. **PrivateLink hardening (production gap).** The PoC uses the public endpoint;
   a production deployment should use the `com.amazonaws.<region>.lambda-microvm`
   VPC endpoint + endpoint policy, tagging, and tighter least-privilege.
