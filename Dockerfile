# syntax=docker/dockerfile:1

# Build stage: one toolchain image builds every binary.
FROM golang:1.27-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
# baton switches to CGO_ENABLED=1 in phase 7 (onnxruntime_go, ADR 0007).
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/baton ./cmd/baton-worker ./cmd/batonctl

# Orchestrator: glibc base, because libonnxruntime.so needs it (ADR 0007).
FROM debian:trixie-slim AS baton
RUN useradd --system --uid 10001 --no-create-home baton
COPY --from=build /out/baton /out/batonctl /usr/local/bin/
USER 10001
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/baton"]

# Worker: pure Go, static image.
FROM gcr.io/distroless/static-debian13:nonroot AS baton-worker
COPY --from=build /out/baton-worker /usr/local/bin/baton-worker
ENTRYPOINT ["/usr/local/bin/baton-worker"]

FROM gcr.io/distroless/static-debian13:nonroot AS batonctl
COPY --from=build /out/batonctl /usr/local/bin/batonctl
ENTRYPOINT ["/usr/local/bin/batonctl"]
