# Building the agent and packaging Lambda MicroVM images

How to build the in-VM **agent** (the `microvm-shim` + `sandboxd` that make a
Lambda MicroVM speak the agent-sandbox runtime protocol), bake it into a
**MicroVM image**, and build/deploy the **provider** that backs `Sandbox` CRs
with MicroVMs.

For building/installing the agent-sandbox SDK fork (`sandboxd`, `extctl`, the
CRDs, the SDK client), see [FORK.md](FORK.md). This guide assumes you already
have a `sandboxd` binary from there.

## Prerequisites

- **Go 1.26+** (`go.mod` pins `go 1.26`; `GOTOOLCHAIN=auto` fetches it).
- **arm64 target** — Lambda MicroVMs are Graviton-only; everything that runs in
  the VM is cross-compiled `linux/arm64`.
- **AWS CLI** with the `lambda-microvms` service, credentials for the account
  (examples use `${AWS_REGION}`, account `${AWS_ACCOUNT_ID}`).
- An **S3 bucket** for artifacts: `${ARTIFACT_BUCKET}`.
- Two IAM roles (see [IAM](#iam-roles)): a **build role** and an **execution role**.

> **Placeholders / templates.** The manifests under `build/k8s/` and policies
> under `build/aws/` are templates that reference `${AWS_REGION}`,
> `${AWS_ACCOUNT_ID}`, `${CLUSTER}` (your EKS cluster name), and
> `${ARTIFACT_BUCKET}` (your S3 bucket, e.g.
> `microvm-sandbox-shim-${AWS_ACCOUNT_ID}-${AWS_REGION}`). Copy `.env.example` to
> `.env`, fill it in, and export before applying anything:
> ```bash
> cp .env.example .env && $EDITOR .env
> set -a; . ./.env; set +a
> ```
> Render a template on apply with `envsubst`, e.g.
> `envsubst < build/k8s/provider-deploy.yaml | kubectl apply -f -`.

> **Environment quirks that shaped the build** (this dev laptop):
> - `proxy.golang.org` is DNS-blocked → **always `export GOPROXY=direct`**.
> - The local Docker daemon can't reach external registries. We therefore
>   **never `docker build`** the MicroVM image locally — Lambda builds it
>   server-side from a Dockerfile in an S3 zip. The provider/extctl "containers"
>   are just the public `aws-cli` image pulling a prebuilt binary from S3.

## 1. Build the in-VM binaries (arm64)

```bash
export GOPROXY=direct GOTOOLCHAIN=auto
cd lambda-microvm-sandbox-shim

# the shim (this repo)
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
  go build -C shim -trimpath -ldflags='-s -w' -o ../build/image/microvm-shim .

# sandboxd (from the fork — see FORK.md)
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
  go build -C agent-sandbox -trimpath -ldflags='-s -w' \
  -o lambda-microvm-sandbox-shim/build/image/sandboxd \
  ./packages/sandboxd/cmd/sandboxd

file build/image/microvm-shim build/image/sandboxd   # both: ELF ... ARM aarch64
```

## 2. Assemble the image artifact (Dockerfile + binaries)

`build/image/Dockerfile` bakes the two binaries onto the MicroVM base and adds
the workload toolchain (`python3` for stdlib examples; `python3.11` +
`strands-agents` for the chat agent — Strands needs Python ≥3.10, AL2023's
`python3` is 3.9). The `ENTRYPOINT` is the shim; Lambda polls the `/ready` hook
on port 8080 before snapshotting.

Package the Dockerfile + binaries into a zip and upload it — this is the
`codeArtifact` Lambda builds server-side:

```bash
cd build/image
zip -j app.zip Dockerfile sandboxd microvm-shim
aws s3 cp app.zip s3://${ARTIFACT_BUCKET}/app.zip
```

## 3. Build (or update) the MicroVM image

Lambda runs the Dockerfile **on top of the base MicroVM snapshot**
(`baseImageArn`) and produces a versioned, snapshot-ready image. Use an input
file to keep the hook/resource config readable:

```bash
cat > /tmp/image-input.json <<'JSON'
{
  "name": "microvm-sandbox-shim",
  "baseImageArn": "arn:aws:lambda:${AWS_REGION}:aws:microvm-image:al2023-1",
  "buildRoleArn": "arn:aws:iam::${AWS_ACCOUNT_ID}:role/MicrovmSandboxShimBuildRole",
  "codeArtifact": { "uri": "s3://${ARTIFACT_BUCKET}/app.zip" },
  "cpuConfigurations": [ { "architecture": "ARM_64" } ],
  "resources": [ { "minimumMemoryInMiB": 2048 } ],
  "egressNetworkConnectors": [
    "arn:aws:lambda:${AWS_REGION}:aws:network-connector:aws-network-connector:internet-egress"
  ],
  "hooks": {
    "port": 8080,
    "microvmImageHooks": { "ready": "ENABLED", "validate": "ENABLED" },
    "microvmHooks":      { "run": "ENABLED" }
  }
}
JSON

# first time:
aws lambda-microvms create-microvm-image --cli-input-json file:///tmp/image-input.json

# subsequent rebuilds (same image, new version) — note: update takes the ARN:
aws lambda-microvms update-microvm-image \
  --image-identifier arn:aws:lambda:${AWS_REGION}:${AWS_ACCOUNT_ID}:microvm-image:microvm-sandbox-shim \
  --cli-input-json file:///tmp/image-input.json \
  --description "what changed in this version"
```

Hook semantics: `ready`/`validate` are **image-build** hooks (polled before the
snapshot is taken); `run` is a **runtime** hook (traffic only flows after `/run`
returns 200). The shim answers all of them. `port: 8080` is where Lambda sends
the hooks. `egressNetworkConnectors` is needed at build time so `dnf`/`pip` can
reach the internet.

### Wait for the build

The build is asynchronous. Poll until the active version advances:

```bash
aws lambda-microvms get-microvm-image \
  --image-identifier arn:aws:lambda:${AWS_REGION}:${AWS_ACCOUNT_ID}:microvm-image:microvm-sandbox-shim \
  --query '{active:latestActiveImageVersion,failed:latestFailedImageVersion}' --output json
```

When `active` reaches the new version (e.g. `15.0`) it's ready. Watch `failed`
too — if it advances instead, the build failed (check CloudWatch if `logging`
was configured). The provider always launches the **latest active** version, so
no redeploy is needed after a rebuild.

## 4. Build and deploy the provider

The provider is a separate Go module (`provider/`). Build it arm64, ship the
binary to S3, and run it in-cluster via the public `aws-cli` image (which `aws
s3 cp`s the binary and execs it — sidesteps the blocked ECR push).

```bash
export GOPROXY=direct GOTOOLCHAIN=auto
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
  go build -C provider -trimpath -ldflags='-s -w' -o /tmp/microvm-provider .
aws s3 cp /tmp/microvm-provider s3://${ARTIFACT_BUCKET}/microvm-provider

# provider-deploy.yaml is a template; render it (needs ARTIFACT_BUCKET/CLUSTER/AWS_REGION):
envsubst < build/k8s/provider-deploy.yaml | kubectl apply -f -   # ClusterRole + RBAC + Deployment (2 replicas)
```

The Deployment runs `microvm-provider --leader-elect --back-all-sandboxes
--cluster ${CLUSTER} --region ${AWS_REGION}`. It authenticates via **EKS Pod
Identity** (not IRSA): associate the `microvm-provider` ServiceAccount with the
provider IAM policy:

```bash
# one-time: install the Pod Identity agent addon, then associate the SA -> role
aws eks create-addon --cluster-name ${CLUSTER} --addon-name eks-pod-identity-agent
aws eks create-pod-identity-association --cluster-name ${CLUSTER} \
  --namespace default --service-account microvm-provider \
  --role-arn arn:aws:iam::${AWS_ACCOUNT_ID}:role/MicrovmProviderRole
```

The provider policy is `build/aws/provider-policy.json` — note it needs
`lambda:PassNetworkConnector` on the connector ARNs and `iam:PassRole` on the
execution role, in addition to the `lambda:*Microvm*` actions.

## IAM roles

| Role | Trusts | Grants | File |
|---|---|---|---|
| **Build role** (`MicrovmSandboxShimBuildRole`) | the MicroVM image-build service | `s3:GetObject` on the artifact bucket | — |
| **Execution role** (`MicrovmSandboxShimExecRole`) | `lambda.amazonaws.com` (+ `sts:TagSession`) | `secretsmanager:GetSecretValue` on `microvm-shim/*`, `bedrock:InvokeModel*`, logs | trust `build/aws/trust.json`, perms `build/aws/exec-perms.json` |
| **Provider role** (`MicrovmProviderRole`) | EKS Pod Identity | `lambda:*Microvm*`, `PassNetworkConnector`, `PassRole`, ASM, Pod Identity read, S3 get | `build/aws/provider-policy.json` |

The **execution role is what the MicroVM runs as** — it must trust
`lambda.amazonaws.com`. It's how the in-VM shim fetches Secrets Manager values
at `/run` and how the workload reaches Bedrock. The provider resolves it from
the Sandbox's `serviceAccountName` (or the `execution-role-arn` annotation); a
Sandbox with neither runs with **no** AWS identity (Secrets Manager env will be
silently empty).

## End-to-end smoke test

```bash
# build clients (see FORK.md), then:
kubectl apply -f examples/sandbox.yaml       # provider backs it with a MicroVM
examples/run.sh                              # 3 Lambda handlers over native gRPC
examples/strands-chat/run.sh                 # Strands agent -> Bedrock
```
