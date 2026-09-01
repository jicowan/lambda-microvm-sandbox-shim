#!/usr/bin/env bash
# Runs an interactive Strands chat agent inside a MicroVM sandbox, driven over
# sandboxd's native gRPC streaming exec. Scripts a couple of user turns.
set -euo pipefail
export AWS_REGION="${AWS_REGION:-us-east-2}" AWS_DEFAULT_REGION="${AWS_REGION:-us-east-2}"
: "${AWS_ACCOUNT_ID:?set AWS_ACCOUNT_ID (12-digit) so the exec-role ARN can be rendered}"
DIR="$(cd "$(dirname "$0")" && pwd)"
CLIENT="${CHATCLIENT:-$HOME/GitHub/Projects/agent-sandbox-fork/chatclient.bin}"
SB=strands-chat; NS=default

# sandbox.yaml is a template (references ${AWS_ACCOUNT_ID}/${AWS_REGION}); render it.
envsubst '${AWS_ACCOUNT_ID} ${AWS_REGION}' < "$DIR/sandbox.yaml" | kubectl apply -f - >/dev/null
echo "waiting for Sandbox Ready (provider resolves SA->role, runs MicroVM)..."
for i in $(seq 1 40); do
  R=$(kubectl get sandbox "$SB" -n "$NS" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
  [ "$R" = "True" ] && break; sleep 5
done
[ "$R" = "True" ] || { echo "not Ready"; exit 1; }
MVM=$(kubectl get sandbox "$SB" -n "$NS" -o jsonpath='{.metadata.annotations.microvm\.agents\.x-k8s\.io/microvm-id}')
EP=$(kubectl get sandbox "$SB" -n "$NS" -o jsonpath='{.status.serviceFQDN}')
TOK=$(aws lambda-microvms create-microvm-auth-token --microvm-identifier "$MVM" \
  --expiration-in-minutes 30 --allowed-ports '[{"port":8080},{"port":9090}]' \
  --query 'authToken."X-aws-proxy-auth"' --output text)
echo "sandbox=$SB microvm=$MVM endpoint=$EP"

# upload the agent
curl -sS -f -X PUT -H "X-aws-proxy-auth: $TOK" -H "X-aws-proxy-port: 8080" \
  --data-binary @"$DIR/chat.py" "https://$EP/v1/files/chat.py" >/dev/null
echo "=== interactive chat (scripted turns over gRPC exec) ==="
printf 'What is 2+2? Reply with just the number.\nName the largest planet in one word.\nexit\n' \
  | "$CLIENT" --endpoint "$EP" --token "$TOK" --command "python3.11 /workspace/chat.py"

echo "=== cleanup ==="
kubectl delete sandbox "$SB" -n "$NS" --wait=false >/dev/null 2>&1 || true
