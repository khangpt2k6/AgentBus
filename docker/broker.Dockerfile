FROM golang:1.26.1-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/broker ./cmd/broker

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
RUN adduser -D -g '' goqueue
WORKDIR /home/goqueue
RUN mkdir -p /data && chown goqueue:goqueue /data
COPY --from=builder /bin/broker /usr/local/bin/broker
USER goqueue
EXPOSE 9090 9095 2112
ENTRYPOINT ["broker"]
