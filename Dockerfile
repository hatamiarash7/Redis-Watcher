
# syntax=docker/dockerfile:1.7

# ----- Builder ----------------------------------------------------------------
FROM golang:1.23-alpine AS builder

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

ENV CGO_ENABLED=0 GOFLAGS=-mod=readonly

RUN apk add --no-cache git ca-certificates

WORKDIR /src

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

RUN go build -trimpath -ldflags "-s -w \
      -X main.version=${VERSION} \
      -X main.commit=${COMMIT} \
      -X main.date=${BUILD_DATE}" \
      -o /out/redis-watcher ./cmd/redis-watcher

# ----- Runtime ----------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/redis-watcher /usr/local/bin/redis-watcher

USER nonroot:nonroot

EXPOSE 9100

ENV REDIS_WATCHER_CONFIG=/etc/redis-watcher/config.yaml

ENTRYPOINT ["/usr/local/bin/redis-watcher"]
CMD ["--config", "/etc/redis-watcher/config.yaml"]
