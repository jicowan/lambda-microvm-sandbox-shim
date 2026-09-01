# Multi-stage MicroVM image: sandboxd (KEP-539.2) + the lifecycle shim + user tools.
#
# Topology A: sandboxd IS the execution environment. Every tool the agent
# invokes (python3, node, git, ...) must exist in THIS image's rootfs.
#
# Target platform is linux/arm64 ONLY — Lambda MicroVMs run on Graviton.
#
# Build (from repo root of this project):
#   docker buildx build --platform linux/arm64 -f images/Dockerfile -t microvm-sandbox:dev .
#
# Then package per the Lambda MicroVM image workflow (Dockerfile + zip -> S3 ->
# CreateMicrovmImage). See DESIGN.md.

# ---- stage 1: pull a prebuilt, static arm64 sandboxd binary --------------------
# Pinned by digest in real use. The upstream staging image is built
# CGO_ENABLED=0 GOARCH=arm64, so the binary drops into any base.
# TODO: pin to a released tag/digest instead of latest-main.
FROM --platform=linux/arm64 us-central1-docker.pkg.dev/k8s-staging-images/agent-sandbox/sandboxd:latest-main AS sandboxd

# ---- stage 2: build the lifecycle shim (stdlib only) ---------------------------
FROM --platform=linux/arm64 golang:1.26 AS shim
WORKDIR /src
COPY shim/ ./shim/
# go.mod is minimal (stdlib only); build a static arm64 binary.
RUN cd shim && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
    go build -ldflags="-s -w" -o /microvm-shim .

# ---- stage 3: the actual sandbox environment -----------------------------------
# Swap this base for whatever your agent needs. MUST be arm64.
FROM --platform=linux/arm64 debian:bookworm-slim

# tini as PID 1 handles zombie reaping; the shim runs as its child.
# (See DESIGN.md open question #6.)
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates procps tini \
 # ---- user tools go here (Topology A) ----
 && apt-get install -y --no-install-recommends python3 \
 && rm -rf /var/lib/apt/lists/*

# Inject the runtime binaries via COPY (the "multi-stage copy" injection model).
COPY --from=sandboxd /usr/local/bin/sandboxd /usr/local/bin/sandboxd
COPY --from=shim     /microvm-shim           /usr/local/bin/microvm-shim

# Sandbox workspace root (sandboxd --root-dir).
RUN mkdir -p /workspace && chown 1000:1000 /workspace
WORKDIR /workspace
USER 1000

# Endpoint-facing ports the shim listens on. Lambda routes inbound HTTPS to
# 8080 by default; reach gRPC with the request header `X-aws-proxy-port: 9090`.
EXPOSE 8080 9090

# The shim supervises sandboxd on loopback (8081/9091) and fronts 8080/9090.
ENV SANDBOXD_BIN=/usr/local/bin/sandboxd \
    WORKSPACE=/workspace \
    PUBLIC_HTTP_ADDR=0.0.0.0:8080 \
    PUBLIC_GRPC_ADDR=0.0.0.0:9090 \
    SANDBOXD_REST_ADDR=127.0.0.1:8081 \
    SANDBOXD_GRPC_ADDR=127.0.0.1:9091

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/microvm-shim"]
