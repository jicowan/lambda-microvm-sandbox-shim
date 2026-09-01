#!/usr/bin/env python3
"""A minimal interactive Strands chat agent, designed to run inside a MicroVM
sandbox and be driven over sandboxd's native gRPC streaming exec (stdin/stdout).

Reads one user message per stdin line, replies via a Strands Agent backed by
Amazon Bedrock (Claude), streaming each turn to stdout. Ends on EOF or "exit".
"""
import os
import sys

from strands import Agent
from strands.models import BedrockModel

MODEL_ID = os.environ.get("CHAT_MODEL_ID", "us.anthropic.claude-haiku-4-5-20251001-v1:0")
REGION = os.environ.get("AWS_REGION", "us-east-2")


def response_text(result) -> str:
    try:
        return "".join(b.get("text", "") for b in result.message["content"])
    except Exception:
        return str(result)


def main() -> None:
    agent = Agent(
        model=BedrockModel(model_id=MODEL_ID, region_name=REGION),
        callback_handler=None,  # we print ourselves for deterministic output
        system_prompt="You are a terse assistant. Answer in one short sentence.",
    )
    print(f"chat ready (model={MODEL_ID})", flush=True)
    for line in sys.stdin:
        msg = line.strip()
        if not msg:
            continue
        if msg.lower() == "exit":
            break
        result = agent(msg)
        print(f"You: {msg}", flush=True)
        print(f"Assistant: {response_text(result)}", flush=True)
    print("chat closed", flush=True)


if __name__ == "__main__":
    main()
