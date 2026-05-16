# Dockerfile used by GoReleaser. The broker binary is built on the host and
# copied in — DO NOT build Go inside this image.
#
# Manual builds: use docker/broker.Dockerfile instead.

FROM alpine:3.21
RUN apk add --no-cache ca-certificates wget \
 && adduser -D -g '' goqueue \
 && mkdir -p /data && chown goqueue:goqueue /data

COPY broker /usr/local/bin/broker

USER goqueue
WORKDIR /home/goqueue
EXPOSE 9090 9095 2112
ENTRYPOINT ["broker"]
