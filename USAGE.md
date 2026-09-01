# User guide: running Lambda MicroVMs with the agent-sandbox fork

How to run agent-sandbox `Sandbox` workloads backed by AWS Lambda MicroVMs. Three
walkthroughs:

1. [A standalone Sandbox (one MicroVM)](#1-standalone-sandbox)
2. [A warm pool + a SandboxClaim](#2-warm-pool--claim)
3. [Suspend / resume](#3-suspend--resume)

**Prerequisites** — a cluster with the fork installed and both controllers
running (see [FORK.md](FORK.md) and [BUILDING.md](BUILDING.md)):

```bash
kubectl get pods -n default | grep -E 'microvm-provider|agent-sandbox-ext'  # both Running
kubectl get crd | grep -E 'sandboxes|sandboxtemplates|sandboxwarmpools|sandboxclaims'
```

You also need the AWS CLI (for minting endpoint auth tokens) and the SDK client
binaries (`execcmd`, `chatclient`) from [FORK.md](FORK.md#2-sdk-client-binaries).

## How access works (read this once)

A MicroVM isn't a pod — you don't `kubectl exec` into it. Instead:

- The provider publishes the MicroVM's HTTPS endpoint in **`status.serviceFQDN`**
  and its id in the `microvm.agents.x-k8s.io/microvm-id` annotation.
- You mint a short-lived **JWE auth token** with the `lambda-microvms` API,
  scoped to the ports you'll use: **8080** (REST files) and **9090** (gRPC exec).
- Clients dial `https://<serviceFQDN>` with that token. `execcmd`/`chatclient`
  do this for you; raw REST uses the `X-aws-proxy-auth` / `X-aws-proxy-port`
  headers.

A reusable helper:

Throughout, substitute your own values for the placeholders
`${AWS_REGION}` / `${AWS_ACCOUNT_ID}` (and `${CLUSTER}` / `${ARTIFACT_BUCKET}` in
the build docs) — e.g. `export AWS_REGION=us-west-2 AWS_ACCOUNT_ID=111122223333`.

```bash
export AWS_REGION=<your-region>
mint() {  # $1 = sandbox name, $2 = namespace (default: default)
  local sb="$1" ns="${2:-default}"
  MVM=$(kubectl get sandbox "$sb" -n "$ns" -o jsonpath='{.metadata.annotations.microvm\.agents\.x-k8s\.io/microvm-id}')
  EP=$(kubectl get sandbox "$sb" -n "$ns" -o jsonpath='{.status.serviceFQDN}')
  TOK=$(aws lambda-microvms create-microvm-auth-token --microvm-identifier "$MVM" \
    --expiration-in-minutes 30 --allowed-ports '[{"port":8080},{"port":9090}]' \
    --query 'authToken."X-aws-proxy-auth"' --output text)
  echo "sandbox=$sb microvm=$MVM endpoint=https://$EP"
}
```

## 1. Standalone Sandbox

A single MicroVM-backed Sandbox. The only thing that makes it MicroVM-backed is
the `microvm.agents.x-k8s.io/backend: lambda-microvm` annotation.

```yaml
# standalone.yaml
apiVersion: agents.x-k8s.io/v1beta1
kind: Sandbox
metadata:
  name: demo
  namespace: default
  annotations:
    microvm.agents.x-k8s.io/backend: lambda-microvm
spec:
  podTemplate:
    spec:
      nodeSelector: { kubernetes.io/arch: arm64 }   # Graviton only
      containers:
        - name: sandbox
          image: microvm-sandbox-shim               # image name -> latest active version
          resources: { requests: { memory: 2Gi } }  # -> MicroVM memory baseline
```

```bash
kubectl apply -f standalone.yaml
kubectl wait --for=condition=Ready sandbox/demo -n default --timeout=120s
mint demo

# run a command over native gRPC:
agent-sandbox/execcmd.bin --endpoint "$EP" --token "$TOK" \
  --command 'uname -m; python3 -c "print(6*7)"'

# upload/download files over sandboxd's REST filesystem:
curl -sS -X PUT -H "X-aws-proxy-auth: $TOK" -H "X-aws-proxy-port: 8080" \
  --data 'hello' "https://$EP/v1/files/notes/hello.txt"
curl -sS      -H "X-aws-proxy-auth: $TOK" -H "X-aws-proxy-port: 8080" \
  "https://$EP/v1/files/notes/hello.txt"

kubectl delete sandbox demo -n default   # finalizer terminates the MicroVM
```

### Giving the workload an AWS identity (exec role)

The MicroVM runs as an **execution role**. Set it the idiomatic way — a
ServiceAccount the provider resolves to an IAM role (via the SA's
`eks.amazonaws.com/role-arn` annotation, or an EKS Pod Identity association):

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: microvm-bedrock
  namespace: default
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::${AWS_ACCOUNT_ID}:role/MicrovmSandboxShimExecRole
---
apiVersion: agents.x-k8s.io/v1beta1
kind: Sandbox
metadata:
  name: demo-with-role
  namespace: default
  annotations:
    microvm.agents.x-k8s.io/backend: lambda-microvm
    # Optional: inject Secrets Manager values as env at /run (needs the exec role).
    microvm.agents.x-k8s.io/secret-env: microvm-shim/test-secret
spec:
  podTemplate:
    spec:
      serviceAccountName: microvm-bedrock
      nodeSelector: { kubernetes.io/arch: arm64 }
      containers:
        - name: agent
          image: microvm-sandbox-shim
          env:
            - { name: AWS_REGION, value: ${AWS_REGION} }   # plain env -> session env
          resources: { requests: { memory: 2Gi } }
```

> A Sandbox with **no** `serviceAccountName` and no `execution-role-arn`
> annotation runs with no AWS identity — Secrets Manager env will be silently
> empty and Bedrock calls will fail. The provider now logs a warning when
> `secret-env` is set but no role resolves.

Env reaches commands via a session-env file the shim writes at `/run` (sandboxd's
own env is fixed at boot); the SDK sources it automatically. See [DESIGN.md](DESIGN.md).

## 2. Warm pool + claim

Pre-warm a pool of MicroVMs so a claim is satisfied in seconds by **adopting** a
ready one instead of cold-starting. Uses the extensions CRDs
(`SandboxTemplate` / `SandboxWarmPool` / `SandboxClaim`).

A ready-to-run manifest ships at
[`examples/strands-chat/warmpool-claim.yaml`](examples/strands-chat/warmpool-claim.yaml).
Its shape:

```yaml
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxTemplate
metadata: { name: strands-template, namespace: default }
spec:
  podTemplate:
    spec:
      serviceAccountName: microvm-bedrock        # exec role baked into every warm VM
      nodeSelector: { kubernetes.io/arch: arm64 }
      containers:
        - name: agent
          image: microvm-sandbox-shim
          env: [ { name: AWS_REGION, value: ${AWS_REGION} } ]
          resources: { requests: { memory: 2Gi } }
---
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxWarmPool
metadata: { name: strands-pool, namespace: default }
spec:
  replicas: 2
  updateStrategy: { type: Recreate }
  sandboxTemplateRef: { name: strands-template }
---
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxClaim
metadata: { name: strands-claim, namespace: default }
spec:
  warmPoolRef: { name: strands-pool }
  # NOTE: do NOT set spec.env here — per-claim env forces a cold start
  # (the value must be baked into the template to stay warm-adoptable).
```

**Order matters** — the pool must be Ready *before* the claim (adoption has a ~2s
grace). Apply the pool, wait, then the claim:

```bash
# the manifest is a template (references ${AWS_ACCOUNT_ID}/${AWS_REGION}) — render it.
# Apply the SA + template + pool first, wait for Ready, then apply the SandboxClaim.
envsubst '${AWS_ACCOUNT_ID} ${AWS_REGION}' < examples/strands-chat/warmpool-claim.yaml | kubectl apply -f -

# wait for warm VMs to be Ready with an endpoint
kubectl get sandbox -n default -l agents.x-k8s.io/warm-pool-sandbox \
  -o custom-columns='NAME:.metadata.name,READY:.status.conditions[?(@.type=="Ready")].status,FQDN:.status.serviceFQDN'

# the claim binds/adopts one; its bound Sandbox is in .status.sandbox.name
kubectl get sandboxclaim strands-claim -n default -o jsonpath='{.status.sandbox.name}{"\n"}'
```

Adoption signals: the bound Sandbox loses its `agents.x-k8s.io/warm-pool-sandbox`
label, the pool auto-replenishes to keep `replicas`, and the extctl log shows a
`"sandboxFound": true` fast path (no cold create). Adoption works for MicroVMs
because the fork treats a backend that publishes `status.serviceFQDN` as
network-ready (upstream requires a pod IP) — see [FORK.md](FORK.md).

Then talk to the adopted VM exactly as in walkthrough 1:

```bash
SB=$(kubectl get sandboxclaim strands-claim -n default -o jsonpath='{.status.sandbox.name}')
mint "$SB"
printf 'What is 2+2? Reply with just the number.\nexit\n' \
  | agent-sandbox/chatclient.bin --endpoint "$EP" --token "$TOK" \
      --command 'python3.11 /workspace/chat.py'   # after uploading chat.py, see examples/strands-chat
```

Clean up (releases the adopted VM and drains the pool):

```bash
kubectl delete sandboxclaim strands-claim -n default
kubectl delete sandboxwarmpool strands-pool -n default
kubectl delete sandboxtemplate strands-template -n default
```

## 3. Suspend / resume

Drive the MicroVM's lifecycle with `spec.operatingMode`. Suspend snapshots the
whole VM (memory + disk); resume restores it — **running processes and their
in-memory state survive**.

```bash
SB=demo   # any MicroVM-backed Sandbox
state() { kubectl get sandbox $SB -n default \
  -o jsonpath='{.metadata.annotations.microvm\.agents\.x-k8s\.io/state}{"\n"}'; }

# suspend
kubectl patch sandbox $SB -n default --type merge -p '{"spec":{"operatingMode":"Suspended"}}'
until [ "$(state)" = "SUSPENDED" ]; do sleep 3; done   # RUNNING -> SUSPENDING -> SUSPENDED

# resume
kubectl patch sandbox $SB -n default --type merge -p '{"spec":{"operatingMode":"Running"}}'
until [ "$(state)" = "RUNNING" ]; do sleep 3; done      # SUSPENDED -> RESUMING -> RUNNING
```

After resume, re-check `status.serviceFQDN` and re-mint a token before
reconnecting. To see state persistence in action, run a resident process before
suspending and observe the same PID (and its remembered data) after resume — the
[`examples/strands-chat`](examples/strands-chat) chat-agent daemon demonstrates
this: it recalls facts from before the suspend because it's the same process,
snapshot-restored rather than restarted.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Sandbox stuck not-Ready | Provider not running or wrong image name; check `kubectl logs deploy/microvm-provider -n default`. |
| Exec returns 403 | Token missing port 9090 — mint with both `{"port":8080}` and `{"port":9090}`. |
| Secrets Manager env empty | No exec role resolved — set `serviceAccountName` or the `execution-role-arn` annotation. |
| Claim cold-starts instead of adopting | Pool wasn't Ready before the claim (2s grace), or the claim sets `spec.env`. |
| `NoCredentialProviders` from workload | Exec role missing the needed permission (e.g. `bedrock:InvokeModel*`). |
