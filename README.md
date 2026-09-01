# lambda-microvm-sandbox-shim

Make an [AWS Lambda MicroVM](https://docs.aws.amazon.com/lambda/latest/dg/lambda-microvms-guide.html)
work as a backend for [agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox),
speaking its runtime protocol ([KEP-539.2](https://github.com/kubernetes-sigs/agent-sandbox/tree/main/docs/keps/539.2-runtime-standardization)).

An agent using the agent-sandbox SDK can `Run` commands, stream interactive exec,
and read/write files against a Lambda MicroVM the same way it would against a
gVisor/Kata pod-backed Sandbox — and, via a small orchestration seam, an
agent-sandbox `Sandbox` CR can be *backed* by a MicroVM instead of a Pod.

> **Status: working proof-of-concept, validated end-to-end on EKS 1.36.** Not
> production-hardened (see *Production gaps* below). This is a fork experiment;
> see [DESIGN.md](DESIGN.md) for the full architecture, mappings, and rationale.

**Start here:** [USAGE.md](USAGE.md) to run workloads · [BUILDING.md](BUILDING.md)
to build the agent + images · [FORK.md](FORK.md) to build/install the fork.

> The agent-sandbox SDK fork this project depends on lives at
> **[github.com/jicowan/agent-sandbox](https://github.com/jicowan/agent-sandbox)**
> (branch `microvm-transport`).

## How it fits together

`sandboxd` (the KEP-539.2 daemon) exposes exactly two surfaces:

```
gRPC  :9090  ProcessService     — Start / Execute / WriteStdin / SendSignal / ResizeTTY
HTTP  :8080  FilesystemService  — GET/PUT/DELETE/HEAD /v1/files, /v1/health, /v1/metadata
```

A Lambda MicroVM exposes one **public HTTPS endpoint** with a **JWE auth token**
(`X-aws-proxy-auth`) and port routing (`X-aws-proxy-port`). Three pieces bridge
the two:

1. **Image** ([`build/image/Dockerfile`](build/image/Dockerfile)) — bakes
   `sandboxd` + the shim into the MicroVM image (arm64/Graviton). `sandboxd` *is*
   the execution environment (KEP "Topology A"): commands run in the MicroVM's
   own rootfs.
2. **Shim** ([`shim/`](shim/)) — PID 1 inside the VM. It supervises `sandboxd`,
   answers the Lambda lifecycle hooks `sandboxd` doesn't serve (`/ready`,
   `/validate`, `/run`, `/resume`, `/suspend`, `/terminate`), reverse-proxies
   REST on `:8080` to `sandboxd`'s loopback REST, and writes per-session env
   (from `/run`, incl. Secrets Manager) to `/etc/microvm/session.env`.
3. **Transport** ([`sdk-fork/`](sdk-fork/)) — the SDK reaches the endpoint over
   TLS + JWE. REST goes to `:8080`; **exec uses `sandboxd`'s native gRPC** on
   `:9090` directly (no in-VM bridge), enabled by the `x-aws-proxy-force-h2: true`
   header that makes the edge speak HTTP/2 to the backend.

`sandboxd` binds `:9090` on `0.0.0.0` and the Lambda edge routes gRPC to it
directly — an earlier design proxied exec through the shim (TCP byte-proxy, then
a WebSocket bridge); both were removed once force-h2 proved native gRPC works.

## Orchestration seam (optional)

The runtime track above needs no Kubernetes. To also drive MicroVMs *from*
agent-sandbox CRs, [`provider/`](provider/) is an event-driven controller that:

- backs `Sandbox` CRs (annotated `microvm.agents.x-k8s.io/backend=lambda-microvm`)
  with MicroVMs — mapping env / `valueFrom` / `envFrom` / command / workingDir /
  ports / memory / connectors from the PodSpec;
- resolves the MicroVM **execution role** from `serviceAccountName` (IRSA
  annotation or EKS Pod Identity), mirrors k8s Secrets to Secrets Manager;
- publishes the endpoint in **`status.serviceFQDN`** for routing;
- implements **suspend/resume** (`spec.operatingMode`), **SandboxWarmPool** +
  **SandboxClaim** adoption, and finalizer-based termination.

It runs in-cluster (Deployment, EKS Pod Identity, leader election). The fork's
extensions manager runs the Template/WarmPool/Claim controllers without the core
`Sandbox`→Pod reconciler. Chosen over a RuntimeClass (CRI-bound; off-node
MicroVMs can't conform) — see [DESIGN.md](DESIGN.md).

## Layout

| Path | What |
|---|---|
| [`USAGE.md`](USAGE.md) | **User guide**: run a standalone Sandbox, a warm pool + claim, and suspend/resume |
| [`BUILDING.md`](BUILDING.md) | Build the in-VM agent (shim + `sandboxd`), package the MicroVM image, build/deploy the provider |
| [`FORK.md`](FORK.md) | Build & install the agent-sandbox fork (CRDs, `extctl`, SDK clients) |
| [`DESIGN.md`](DESIGN.md) | Architecture, request flow, agent-sandbox↔Lambda mapping, resolved findings, open risks |
| [`build/`](build/) | Image (`build/image/Dockerfile`), k8s manifests, IAM policies |
| [`shim/main.go`](shim/main.go) | In-VM entrypoint: supervises `sandboxd`, serves lifecycle hooks, proxies REST, writes session env |
| [`provider/`](provider/) | Orchestration seam: backs `Sandbox` CRs with MicroVMs (PodSpec mapping, exec role, warm pool, suspend/resume) |
| [`sdk-fork/`](sdk-fork/) | The agent-sandbox SDK fork: pluggable gRPC dial options (`connector.go`) + a K8s-free `NewMicrovm` constructor (REST + native gRPC) |
| [`client/`](client/) | Standalone (fork-free) REST `http.RoundTripper` reference |
| [`examples/`](examples/) | Lambda-handler examples + an interactive Strands chat agent (warm-pool/claim + suspend/resume state test) |

## What's validated

REST files · unary `Run` (gRPC `Execute`) · streaming/PTY/stdin exec (gRPC
`Start`) · `/run` workspace reset + per-session env · Secrets Manager env · JWE
auto-refresh (auth is handshake-only, so per-connection tokens suffice) ·
suspend/resume with **in-memory process state surviving the snapshot** ·
SandboxWarmPool + SandboxClaim adoption (via `status.serviceFQDN`) · a Strands
chat agent reaching Amazon Bedrock (Claude Haiku 4.5) from inside a MicroVM.

## The three MicroVM constraints, and where they land

| Constraint | Impact |
|---|---|
| **8h max lifespan** (running + suspended) | A MicroVM can't be a long-lived/deep-hibernated Sandbox. Fine for ephemeral per-session sandboxes; durable state must be externalized (S3/EFS via egress). |
| **Graviton / arm64 only** | Build `sandboxd`, shim, and base image for `linux/arm64`; the provider rejects non-arm64 requests. |
| **No full PodSpec** | The provider maps the supported subset; the image is the unit of tool config, per-instance tweaks come from the 16 KB `runHookPayload` via `/run`. |

## Production gaps

PrivateLink (`com.amazonaws.<region>.lambda-microvm` VPC endpoint + endpoint
policy), resource tagging, and tighter least-privilege IAM are **not** done — the
PoC uses the public endpoint. See DESIGN.md *Open questions / risks*.
