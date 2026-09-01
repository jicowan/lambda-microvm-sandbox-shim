#!/usr/bin/env bash
# Runs the Lambda example handlers inside a MicroVM-backed agent-sandbox Sandbox.
#
# Flow: provision a Sandbox (our provider backs it with a Lambda MicroVM) -> for
# each example, upload handler.py + event.json via sandboxd's REST filesystem
# (/v1/files) -> exec `python3 handler.py event.json` via sandboxd's native gRPC
# ProcessService (the execcmd client) -> assert the output matches the expected
# result.
#
# Requires: kubectl (pointed at the cluster running the provider) + aws CLI +
# the execcmd client binary (native-gRPC one-shot exec).
set -euo pipefail
export AWS_REGION="${AWS_REGION:-us-east-2}" AWS_DEFAULT_REGION="${AWS_REGION:-us-east-2}"
DIR="$(cd "$(dirname "$0")" && pwd)"
EXECCMD="${EXECCMD:-$HOME/GitHub/Projects/agent-sandbox-fork/execcmd.bin}"
SB=lambda-examples
NS=default

norm() { python3 -c 'import json,sys;print(json.dumps(json.load(sys.stdin),sort_keys=True))'; }

echo "=== provision Sandbox (MicroVM-backed) ==="
kubectl apply -f "$DIR/sandbox.yaml" >/dev/null
for i in $(seq 1 30); do
  R=$(kubectl get sandbox "$SB" -n "$NS" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
  [ "$R" = "True" ] && break
  sleep 5
done
[ "$R" = "True" ] || { echo "Sandbox not Ready"; exit 1; }

MVM=$(kubectl get sandbox "$SB" -n "$NS" -o jsonpath='{.metadata.annotations.microvm\.agents\.x-k8s\.io/microvm-id}')
EP=$(kubectl get sandbox "$SB" -n "$NS" -o jsonpath='{.status.serviceFQDN}')
TOK=$(aws lambda-microvms create-microvm-auth-token --microvm-identifier "$MVM" \
  --expiration-in-minutes 30 --allowed-ports '[{"allPorts":{}}]' \
  --query 'authToken."X-aws-proxy-auth"' --output text)
echo "sandbox=$SB microvm=$MVM endpoint=$EP"
H=(-H "X-aws-proxy-auth: $TOK" -H "X-aws-proxy-port: 8080")

put() { curl -sS -f -X PUT "${H[@]}" --data-binary @"$2" "https://$EP/v1/files/$1" >/dev/null; }
exec_py() { "$EXECCMD" --endpoint "$EP" --token "$TOK" \
  --command "python3 /workspace/$1 /workspace/$2"; }

fail=0
for ex in getting_started s3_object_lambda sqs_batch; do
  echo "--- $ex ---"
  put "$ex.py" "$DIR/lambda-handlers/$ex.py"
  put "$ex.event.json" "$DIR/lambda-handlers/$ex.event.json"
  resp=$(exec_py "$ex.py" "$ex.event.json")
  code=$(echo "$resp" | python3 -c 'import json,sys;print(json.load(sys.stdin)["exit_code"])')
  out=$(echo "$resp"  | python3 -c 'import json,sys;print(json.load(sys.stdin)["stdout"],end="")')
  got=$(printf '%s' "$out" | norm)
  exp=$(norm < "$DIR/lambda-handlers/$ex.expected.json")
  if [ "$code" = "0" ] && [ "$got" = "$exp" ]; then
    echo "  PASS  output=$out"
  else
    echo "  FAIL  exit=$code got=$got exp=$exp"; fail=1
  fi
done

echo "=== cleanup ==="
kubectl delete sandbox "$SB" -n "$NS" --wait=false >/dev/null 2>&1 || true
[ "$fail" = "0" ] && echo "ALL EXAMPLES PASSED" || { echo "SOME EXAMPLES FAILED"; exit 1; }
