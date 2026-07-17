#!/bin/sh
set -eu
CONFIG_PATH=${EXPOSE_CONFIG:-/app/server.dev.toml}
if [ ! -f "$CONFIG_PATH" ]; then
  echo "Config file not found: $CONFIG_PATH" >&2
  exit 1
fi
exec /app/expose-server --config "$CONFIG_PATH"
