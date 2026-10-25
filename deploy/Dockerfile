# syntax=docker/dockerfile:1
# Builds every binary (chserver, mvworker, replicator) into one small image;
# the compose file picks the binary per service.

FROM golang:1.27-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags="-s -w" -o /out/ ./cmd/chserver ./cmd/mvworker ./cmd/replicator

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata \
    && adduser -D -H -u 10001 app
COPY --from=build /out/ /usr/local/bin/
USER app
EXPOSE 8123
CMD ["chserver"]
