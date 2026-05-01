# Dockerfile
FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o badis .
# Compile the test binary
RUN CGO_ENABLED=0 GOOS=linux go test -c -o badis-e2e ./test/e2e

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