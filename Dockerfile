# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -ldflags="-w -s" -o badis .
# Compile the test binary
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go test -c -o badis-e2e ./test/e2e

FROM alpine:latest
RUN apk --no-cache add ca-certificates
RUN adduser -D -g '' appuser && mkdir -p /data && chown -R appuser:appuser /data
USER appuser
WORKDIR /root/
COPY --from=builder /app/badis .
COPY --from=builder /app/badis-e2e .

ENV BADIS_DATA_DIR=/data
ENV BADIS_PORT=:6379
EXPOSE 6379 6380
VOLUME ["/data"]

CMD ["./badis"]