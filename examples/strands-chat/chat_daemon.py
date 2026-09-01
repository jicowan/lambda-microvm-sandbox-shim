#!/usr/bin/env python3
"""A persistent Strands chat daemon for the suspend/resume state-persistence
test. Unlike chat.py (which streams over a live exec that a suspend would sever),
this runs as a long-lived background process and does file-based I/O:

  - reads prompts appended one-per-line to /workspace/chat_in
  - writes each reply one-per-line to /workspace/chat_out

The Strands Agent (and thus the conversation history) lives entirely in this
process's memory. If a Lambda MicroVM suspend/resume truly snapshots and
restores the running VM, this process — and the remembered conversation —
survives, and the agent can still answer questions about earlier turns.
"""
import os
import time

IN = "/workspace/chat_in"
OUT = "/workspace/chat_out"
LOG = "/workspace/daemon.log"
PIDFILE = "/workspace/chat_daemon.pid"
MODEL_ID = os.environ.get("CHAT_MODEL_ID", "us.anthropic.claude-haiku-4-5-20251001-v1:0")
REGION = os.environ.get("AWS_REGION", "us-east-2")


def daemonize() -> None:
    """Double-fork + setsid so the daemon detaches from the launching exec's
    session (the minimal image has no setsid/nohup binary; os.setsid does the
    same). This lets it outlive the one-shot Run RPC that started it — and, as a
    normal running process, be captured by the MicroVM suspend/resume snapshot."""
    if os.fork() > 0:
        os._exit(0)
    os.setsid()
    if os.fork() > 0:
        os._exit(0)
    fd = os.open(LOG, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    os.dup2(fd, 1)
    os.dup2(fd, 2)
    devnull = os.open(os.devnull, os.O_RDONLY)
    os.dup2(devnull, 0)
    with open(PIDFILE, "w") as f:
        f.write(str(os.getpid()))


def response_text(result) -> str:
    try:
        return "".join(b.get("text", "") for b in result.message["content"])
    except Exception:
        return str(result)


def main() -> None:
    daemonize()
    from strands import Agent
    from strands.models import BedrockModel

    open(IN, "a").close()
    open(OUT, "a").close()
    agent = Agent(
        model=BedrockModel(model_id=MODEL_ID, region_name=REGION),
        callback_handler=None,
        system_prompt="You are a terse assistant. Answer in one short sentence.",
    )
    # Start reading only new prompts appended from now on.
    with open(IN) as f:
        f.seek(0, 2)
        pos = f.tell()
    while True:
        with open(IN) as f:
            f.seek(pos)
            new = f.readlines()
            pos = f.tell()
        for line in new:
            msg = line.strip()
            if not msg:
                continue
            reply = response_text(agent(msg)).replace("\n", " ").strip()
            with open(OUT, "a") as o:
                o.write(reply + "\n")
                o.flush()
        time.sleep(0.4)


if __name__ == "__main__":
    main()
