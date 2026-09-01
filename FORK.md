# Building & installing the agent-sandbox fork

This project uses a fork of
[`kubernetes-sigs/agent-sandbox`](https://github.com/kubernetes-sigs/agent-sandbox),
published at **[`github.com/jicowan/agent-sandbox`](https://github.com/jicowan/agent-sandbox)**
(module `sigs.k8s.io/agent-sandbox`, branch **`microvm-transport`**). The fork
adds a Kubernetes-free MicroVM transport to the SDK and an extensions-only
controller manager; it does **not** change the upstream pod-backed flow.

For the conceptual "why a fork / what each edit does," see
[`sdk-fork/README.md`](sdk-fork/README.md). This doc is the operational
build/install guide.

## 0. Clone the fork

```bash
git clone -b microvm-transport https://github.com/jicowan/agent-sandbox.git
cd agent-sandbox
```

All `cd agent-sandbox` steps below assume this checkout.

## What the fork changes

| Area | File | Change |
|---|---|---|
| SDK transport | `clients/go/sandbox/connector.go` | Make gRPC dial options pluggable (`GRPCDialOptions`). Small, backward-compatible, independently **upstreamable**. |
| SDK transport | `clients/go/sandbox/microvm.go` | New K8s-free `NewMicrovm(...)` constructor. REST → `sandboxd` on 8080; exec → **native gRPC** `ProcessService` on 9090 with `x-aws-proxy-force-h2: true`. Reuses the transport core (`connector`+`Commands`+`Files`), skips the claim/pod lifecycle. |
| Claim adoption | `extensions/controllers/sandboxclaim_controller.go` | Relax the adoption gate from `len(PodIPs)==0 → skip` to `len(PodIPs)==0 && ServiceFQDN=="" → skip`, so a non-Pod backend that publishes `status.serviceFQDN` is treated as network-ready. Pure narrowing — gVisor/Kata pod flow unchanged. |
| Manager | `extctl/main.go` | An **extensions-only** manager (Template/WarmPool/Claim reconcilers, **no** core `Sandbox`→Pod reconciler), so the MicroVM provider owns backing sandboxes. |
| Clients | `clients/go/{execcmd,chatclient,microvmtest,grpctest}` | Test/utility clients built on `NewMicrovm`. |

## Prerequisites

- **Go 1.26+** (`go.mod` pins `go 1.26.0`, `toolchain go1.26.4`; set
  `GOTOOLCHAIN=auto` to fetch it) and **`export GOPROXY=direct`** (see
  [BUILDING.md](BUILDING.md) for why).
- AWS CLI + the artifact bucket `${ARTIFACT_BUCKET}`.

> **Placeholders.** Substitute your own values for `${AWS_REGION}`,
> `${AWS_ACCOUNT_ID}`, `${CLUSTER}`, and `${ARTIFACT_BUCKET}`:
> ```bash
> export AWS_REGION=us-west-2 AWS_ACCOUNT_ID=111122223333
> export CLUSTER=my-cluster ARTIFACT_BUCKET=my-artifact-bucket
> ```

> **Cross-repo note.** This fork provides the SDK, `sandboxd`, `extctl`, and the
> CRDs. The Kubernetes manifests and IAM policies referenced below
> (`build/k8s/…`, `build/aws/…`, `examples/…`) live in the companion
> **lambda-microvm-sandbox-shim** repo, not in this fork.

## 1. `sandboxd` (for the MicroVM image)

The in-VM runtime binary comes from the fork:

```bash
export GOPROXY=direct GOTOOLCHAIN=auto
cd agent-sandbox
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
  go build -trimpath -ldflags='-s -w' -o /tmp/sandboxd ./packages/sandboxd/cmd/sandboxd
```

Then bake it into the image per [BUILDING.md](BUILDING.md#1-build-the-in-vm-binaries-arm64).
(`make build-sandboxd` also works but builds for the host arch — use the
cross-compile above for the MicroVM.)

## 2. SDK client binaries

These run on your workstation (or in a pod) and dial a MicroVM's public endpoint
directly. Build for your own platform:

```bash
cd agent-sandbox
export GOPROXY=direct GOTOOLCHAIN=auto
for c in execcmd chatclient microvmtest grpctest; do
  go build -o "$c.bin" "./clients/go/$c"
done
go vet ./clients/go/execcmd ./clients/go/chatclient ./clients/go/microvmtest
```

- `execcmd` — one-shot: runs a command via gRPC `Execute`, prints
  `{stdout,stderr,exit_code}` JSON (the drop-in for the removed `/exec` bridge;
  used by `examples/run.sh`).
- `chatclient` — interactive: wires local stdin/stdout to a remote process over
  gRPC `Start` (used by the Strands chat example).
- `microvmtest` — the full transport smoke suite (REST + Run + streaming + PTY +
  stdin + `--tenant`/`--secret-env`/`--refresh-test` checks).
- `grpctest` — minimal native-gRPC probe (proved `force-h2` works).

## 3. Install the CRDs

Both the core `Sandbox` CRD and the extensions CRDs live in the fork under
`olm/config/crd/bases/`:

```bash
cd agent-sandbox
kubectl apply -f olm/config/crd/bases/agents.x-k8s.io_sandboxes.yaml
kubectl apply -f olm/config/crd/bases/extensions.agents.x-k8s.io_sandboxtemplates.yaml
kubectl apply -f olm/config/crd/bases/extensions.agents.x-k8s.io_sandboxwarmpools.yaml
kubectl apply -f olm/config/crd/bases/extensions.agents.x-k8s.io_sandboxclaims.yaml
```

> The **core `Sandbox` CRD is installed, but the upstream core controller is
> not** — the MicroVM provider (in the shim repo) reconciles `Sandbox` objects
> instead. `extctl` only runs the extensions reconcilers.

## 4. Build & deploy `extctl` (extensions manager)

Build arm64, push to S3, and deploy via the public `aws-cli` image (which pulls
the binary and execs it — the ECR push path is blocked from the laptop):

```bash
cd agent-sandbox
export GOPROXY=direct GOTOOLCHAIN=auto
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
  go build -trimpath -ldflags='-s -w' -o /tmp/extctl ./extctl
aws s3 cp /tmp/extctl s3://${ARTIFACT_BUCKET}/extctl

cd lambda-microvm-sandbox-shim
# extctl-deploy.yaml is a template (references ${ARTIFACT_BUCKET}); render it:
envsubst '${ARTIFACT_BUCKET}' < build/k8s/extctl-deploy.yaml | kubectl apply -f -
```

Give the `agent-sandbox-ext` ServiceAccount S3 read via EKS Pod Identity so it
can fetch its binary:

```bash
aws eks create-pod-identity-association --cluster-name ${CLUSTER} \
  --namespace default --service-account agent-sandbox-ext \
  --role-arn arn:aws:iam::${AWS_ACCOUNT_ID}:role/AgentSandboxExtRole   # policy: build/aws/extctl-policy.json
```

## 5. Verify

```bash
kubectl get pods -n default | grep -E 'agent-sandbox-ext|microvm-provider'   # both Running
kubectl get crd | grep agents.x-k8s.io
# warm pool + claim adoption end-to-end (render the template first):
envsubst '${AWS_ACCOUNT_ID} ${AWS_REGION}' < examples/strands-chat/warmpool-claim.yaml | kubectl apply -f -
kubectl get sandboxclaim strands-claim -n default -o jsonpath='{.status.sandbox.name}{"\n"}'  # adopted warm sandbox
```

## Keeping the fork in sync

The three SDK edits are small and localized. When rebasing on upstream `main`:

1. `connector.go` — the `GRPCDialOptions` field is additive; re-apply if the
   connector constructor changed.
2. `microvm.go` — self-contained new file; only breaks if `connector` /
   `Commands` / `Files` / the `process/v1` stubs change shape.
3. `sandboxclaim_controller.go` — a one-line predicate change in `getCandidate`;
   re-locate the "no networked pod observed" skip and re-add the `ServiceFQDN`
   escape hatch.

Only edit #1 is a candidate for upstreaming; #2 and #4 (extctl) are
MicroVM-specific; #3 is a behavior change worth proposing separately.
