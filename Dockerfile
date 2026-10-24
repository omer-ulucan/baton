# syntax=docker/dockerfile:1

# Build stage: one toolchain image builds every binary.
FROM golang:1.27-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
# baton links ONNX Runtime through cgo (ADR 0007); the worker and CLI stay
# pure Go.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o /out/baton ./cmd/baton && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/baton-worker ./cmd/batonctl

# ONNX Runtime, pinned and verified.
FROM debian:trixie-slim AS onnxruntime
ARG ORT_VERSION=1.29.0
ARG ORT_SHA256=c3fddc4f139a045b0c4902c57410f0694f1c2fdf9b6939fbe38b1aeae7cd14ba
ADD --checksum=sha256:${ORT_SHA256} \
    https://github.com/microsoft/onnxruntime/releases/download/v${ORT_VERSION}/onnxruntime-linux-x64-${ORT_VERSION}.tgz /tmp/ort.tgz
RUN mkdir -p /opt/onnxruntime && tar -xzf /tmp/ort.tgz -C /opt/onnxruntime --strip-components=1

# Orchestrator: glibc base, because libonnxruntime.so needs it (ADR 0007).
# Decisions use the rule policy unless BATON_DECISION_POLICY=laya; the
# model is then fetched into BATON_MODEL_DIR on first start (or use the
# baton-laya target, which bakes it in).
FROM debian:trixie-slim AS baton
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* && \
    useradd --system --uid 10001 --no-create-home baton && \
    mkdir -p /var/lib/baton/models && chown 10001 /var/lib/baton/models
COPY --from=onnxruntime /opt/onnxruntime/lib/libonnxruntime.so.1.29.0 /usr/local/lib/
RUN ln -s libonnxruntime.so.1.29.0 /usr/local/lib/libonnxruntime.so.1 && \
    ln -s libonnxruntime.so.1 /usr/local/lib/libonnxruntime.so && ldconfig
COPY --from=build /out/baton /out/batonctl /usr/local/bin/
ENV ONNXRUNTIME_LIB_PATH=/usr/local/lib/libonnxruntime.so \
    BATON_MODEL_DIR=/var/lib/baton/models
USER 10001
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/baton"]

# Orchestrator with the verified fp32 model baked in (about 1.7GB more).
FROM baton AS model
RUN ["/usr/local/bin/baton", "model", "fetch", "--dir", "/var/lib/baton/models"]

FROM baton AS baton-laya
COPY --from=model --chown=10001 /var/lib/baton/models /var/lib/baton/models
ENV BATON_DECISION_POLICY=laya \
    BATON_MODEL_DOWNLOAD=false

# Worker: pure Go, static image.
FROM gcr.io/distroless/static-debian13:nonroot AS baton-worker
COPY --from=build /out/baton-worker /usr/local/bin/baton-worker
ENTRYPOINT ["/usr/local/bin/baton-worker"]

FROM gcr.io/distroless/static-debian13:nonroot AS batonctl
COPY --from=build /out/batonctl /usr/local/bin/batonctl
ENTRYPOINT ["/usr/local/bin/batonctl"]
