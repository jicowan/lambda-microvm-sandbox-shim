# Lambda examples on agent-sandbox + MicroVM shim

These run the **business logic** of well-known AWS Lambda examples *inside* a
MicroVM-backed agent-sandbox `Sandbox`, proving the runtime-compatibility shim
executes real Lambda-style workloads. They are the handler compute only (stdlib
Python, no boto3/event-source glue), fed a synthetic event and checked against
an expected result.

Sourced from the AWS Lambda code examples:
- [Scenarios](https://docs.aws.amazon.com/lambda/latest/dg/service_code_examples_scenarios.html)
- [Serverless examples](https://docs.aws.amazon.com/lambda/latest/dg/service_code_examples_serverless_examples.html)

| Example | Lambda pattern | Handler | Event → Expected |
|---|---|---|---|
| Getting started | "Create your first function" (area calculator) | `getting_started.py` | `{length:6,width:7}` → `{area:42}` |
| S3 Object Lambda | Transform retrieved object content | `s3_object_lambda.py` | `{content:"hello…"}` → `{transformed_content:"HELLO…"}` |
| SQS batch item failures | Partial batch response (`batchItemFailures`) | `sqs_batch.py` | 4 records (2 bad) → failures `[msg-2, msg-3]` |

## Run

Requires `kubectl` (pointed at the cluster running the `microvm-provider`) and
`aws` CLI. The provider must be running (in-cluster or local) so the Sandbox is
backed by a MicroVM.

```bash
./run.sh
```

It provisions `sandbox.yaml`, uploads each handler + event via sandboxd's REST
filesystem, execs `python3 handler.py event.json` through sandboxd's native gRPC
`ProcessService` (the `execcmd` client), asserts the output, and deletes the
Sandbox.

## How it maps

A Lambda *function* is event-triggered and ephemeral; a Lambda *MicroVM* is a
long-running sandbox you connect to. So we don't run these as FaaS handlers —
we run their compute inside the sandbox the way an agent would: upload code,
execute it, read the result. The MicroVM image (`microvm-sandbox-shim`) bakes in
`python3` for this.

## Strands chat agent (`strands-chat/`)

An interactive [Strands Agents SDK](https://strandsagents.com) chat agent running
*inside* a MicroVM, talking to Amazon Bedrock (Claude Haiku 4.5). The exec role
is passed the idiomatic way — the Sandbox runs as a `ServiceAccount` and the
provider resolves it to the MicroVM execution role.

- `run.sh` — provisions the Sandbox and scripts a couple of turns over the native
  gRPC streaming exec (`chatclient`).
- `warmpool-claim.yaml` + `chat_daemon.py` — the **suspend/resume state test**: a
  `SandboxWarmPool` of agents, a `SandboxClaim` that adopts one, and a persistent
  self-daemonizing agent that keeps conversation history *in memory*. Establish a
  fact, suspend, resume, and the agent still recalls it — proving the MicroVM
  snapshot restores the live process, not just the disk.
