# Lambda MicroVM image: prebuilt linux/arm64 binaries (sandboxd + microvm-shim).
#
# No in-build compilation and no third-party image pulls — the binaries are
# cross-compiled locally and shipped in the zip, so the only build dependency
# is the snapshot-compatible Lambda base container image.
#
# Lambda runs this Dockerfile server-side on top of the MicroVM base image
# (--base-image-arn) during CreateMicrovmImage.
FROM public.ecr.aws/lambda/microvms:al2023-minimal

COPY sandboxd     /usr/local/bin/sandboxd
COPY microvm-shim /usr/local/bin/microvm-shim

# Tooling for running sandbox workloads. python3 (AL2023 default 3.9) runs the
# stdlib Lambda examples; python3.11 + the Strands Agents SDK power the
# interactive chat-agent example (strands-agents requires Python >= 3.10).
RUN dnf install -y python3 python3.11 python3.11-pip && dnf clean all
RUN pip3.11 install --no-cache-dir strands-agents

RUN mkdir -p /workspace \
 && echo "baked-into-snapshot" > /workspace/leftover-from-snapshot.txt

# Shim serves hooks + REST proxy on 8080. gRPC mode:
#   direct (default): sandboxd serves gRPC on 0.0.0.0:9090 (edge talks to it).
#   proxy: shim TCP-proxies 9090 -> sandboxd loopback 9091.
ENV SANDBOXD_BIN=/usr/local/bin/sandboxd \
    WORKSPACE=/workspace \
    PUBLIC_HTTP_ADDR=0.0.0.0:8080 \
    PUBLIC_GRPC_ADDR=0.0.0.0:9090 \
    SHIM_GRPC_MODE=direct

EXPOSE 8080 9090

# Lambda starts this via ENTRYPOINT, then polls the /ready hook (port 8080)
# before snapshotting.
ENTRYPOINT ["/usr/local/bin/microvm-shim"]
