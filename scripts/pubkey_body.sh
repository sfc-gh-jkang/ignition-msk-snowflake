#!/usr/bin/env bash
# Prints the public key body (no BEGIN/END lines, no newlines) for ALTER USER ... RSA_PUBLIC_KEY.
set -euo pipefail
grep -v -- '-----' "$(cd "$(dirname "$0")/.." && pwd)/.secrets/rsa_key.pub" | tr -d '\n'
