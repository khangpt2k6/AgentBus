#!/usr/bin/env sh
# GoQueue / Agent Bus installer
#
# Usage:
#   curl -sSfL https://raw.githubusercontent.com/khangpt2k6/AgentBus/main/install.sh | sh
#   curl -sSfL https://raw.githubusercontent.com/khangpt2k6/AgentBus/main/install.sh | sh -s -- --version v0.1.0
#   curl -sSfL https://raw.githubusercontent.com/khangpt2k6/AgentBus/main/install.sh | sh -s -- --prefix $HOME/.local/bin
#
# Env overrides:
#   GOQUEUE_VERSION   release tag to install (default: latest)
#   GOQUEUE_PREFIX    install dir (default: /usr/local/bin, or $HOME/.local/bin if not writable)

set -eu

REPO="khangpt2k6/AgentBus"
VERSION="${GOQUEUE_VERSION:-latest}"
PREFIX="${GOQUEUE_PREFIX:-}"

while [ $# -gt 0 ]; do
  case "$1" in
    --version) VERSION="$2"; shift 2 ;;
    --prefix)  PREFIX="$2"; shift 2 ;;
    --help|-h)
      sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

err() { printf 'error: %s\n' "$*" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

# --- detect OS / arch ----------------------------------------------------
uname_s="$(uname -s)"
uname_m="$(uname -m)"

case "$uname_s" in
  Linux)   GOOS=linux ;;
  Darwin)  GOOS=darwin ;;
  MINGW*|MSYS*|CYGWIN*)
    err "this script does not support Windows shells; download the .zip release from https://github.com/${REPO}/releases" ;;
  *) err "unsupported OS: $uname_s" ;;
esac

case "$uname_m" in
  x86_64|amd64)  GOARCH=amd64 ;;
  arm64|aarch64) GOARCH=arm64 ;;
  *) err "unsupported architecture: $uname_m" ;;
esac

# --- resolve version -----------------------------------------------------
if [ "$VERSION" = "latest" ]; then
  if ! have curl; then err "curl is required"; fi
  VERSION="$(curl -sSfL "https://api.github.com/repos/${REPO}/releases/latest" \
    | grep -m1 '"tag_name"' \
    | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')"
  [ -n "$VERSION" ] || err "could not resolve latest release tag"
fi
case "$VERSION" in v*) ;; *) VERSION="v${VERSION}" ;; esac
VERSION_NO_V="${VERSION#v}"

# --- pick prefix ---------------------------------------------------------
if [ -z "$PREFIX" ]; then
  if [ -w /usr/local/bin ] 2>/dev/null; then
    PREFIX=/usr/local/bin
  else
    PREFIX="$HOME/.local/bin"
  fi
fi
mkdir -p "$PREFIX"

# --- download ------------------------------------------------------------
ARCHIVE="goqueue_${VERSION_NO_V}_${GOOS}_${GOARCH}.tar.gz"
URL="https://github.com/${REPO}/releases/download/${VERSION}/${ARCHIVE}"
CHECKSUMS_URL="https://github.com/${REPO}/releases/download/${VERSION}/checksums.txt"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

printf 'downloading %s\n' "$URL"
curl -sSfL "$URL" -o "$TMP/$ARCHIVE" || err "download failed"

# verify checksum if available
if curl -sSfL "$CHECKSUMS_URL" -o "$TMP/checksums.txt" 2>/dev/null; then
  ( cd "$TMP" && grep " $ARCHIVE\$" checksums.txt | sha256sum -c - >/dev/null ) \
    || err "checksum verification failed"
  printf 'checksum verified\n'
fi

tar -xzf "$TMP/$ARCHIVE" -C "$TMP"
SRC_DIR="$TMP/goqueue_${VERSION_NO_V}_${GOOS}_${GOARCH}"

install -m 0755 "$SRC_DIR/broker"   "$PREFIX/broker"
install -m 0755 "$SRC_DIR/goqueue"  "$PREFIX/goqueue"

printf '\ninstalled to %s\n' "$PREFIX"
printf '  broker  %s\n' "$("$PREFIX/broker" --version 2>/dev/null || echo $VERSION)"
printf '  goqueue %s\n' "$("$PREFIX/goqueue" --version 2>/dev/null || echo $VERSION)"

case ":$PATH:" in
  *":$PREFIX:"*) ;;
  *) printf '\nadd %s to PATH:\n  export PATH="%s:$PATH"\n' "$PREFIX" "$PREFIX" ;;
esac

cat <<EOF

next steps:
  broker --tcp-addr=:9090 --grpc-addr=:9095 --metrics-addr=:2112 --wal-path=./agentbus.wal
  goqueue publish --addr localhost:9090 --topic orders "hello"
  goqueue consume --addr localhost:9090 --topic orders --group demo

docs: https://github.com/${REPO}#quick-start
EOF
