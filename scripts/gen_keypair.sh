#!/usr/bin/env bash
# Generates an unencrypted 2048-bit RSA key pair for the connector's Snowflake service user.
# https://docs.snowflake.com/en/user-guide/key-pair-auth
# Keys are written to .secrets/ (git-ignored). Existing keys are never overwritten.
set -euo pipefail
dir="$(cd "$(dirname "$0")/.." && pwd)/.secrets"
mkdir -p "$dir" && chmod 700 "$dir"
if [ -s "$dir/rsa_key.p8" ]; then
  echo "key already exists: $dir/rsa_key.p8"
else
  openssl genrsa 2048 2>/dev/null | openssl pkcs8 -topk8 -inform PEM -out "$dir/rsa_key.p8" -nocrypt
  openssl rsa -in "$dir/rsa_key.p8" -pubout -out "$dir/rsa_key.pub" 2>/dev/null
  chmod 600 "$dir/rsa_key.p8"
  echo "wrote $dir/rsa_key.p8 and rsa_key.pub"
fi
