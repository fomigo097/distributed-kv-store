FROM golang:1.26.1 AS builder

WORKDIR /src

COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal

ARG TARGET=./cmd/raftnode
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/app ${TARGET}

FROM alpine:3.21

WORKDIR /app

RUN adduser -D -u 10001 appuser
RUN mkdir -p /data && chown -R appuser:appuser /app /data

COPY --from=builder /out/app /app/app

USER appuser

ENTRYPOINT ["/app/app"]
