# Dockerfile
FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o badis .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/badis .

ENV BADIS_DATA_DIR=/data
ENV BADIS_PORT=:6379
EXPOSE 6379 6380

CMD ["./badis"]
