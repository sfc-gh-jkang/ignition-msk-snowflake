#!/usr/bin/env bash
# Creates (or updates) the Snowflake v4 sink connector on a Kafka Connect REST endpoint.
# Reads settings from the environment (see local/.env.example) and the private key from
# SNOWFLAKE_PRIVATE_KEY_FILE.  Usage: register_connector.sh [connect-url]
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
url="${1:-${CONNECT_URL:-http://localhost:8083}}"

: "${SNOWFLAKE_ACCOUNT_URL:?}" "${SNOWFLAKE_USER:?}" "${SNOWFLAKE_ROLE:?}" "${SNOWFLAKE_DATABASE:?}" "${SNOWFLAKE_SCHEMA:?}"
key_file="${SNOWFLAKE_PRIVATE_KEY_FILE:-$here/.secrets/rsa_key.p8}"
[ -s "$key_file" ] || { echo "private key not found: $key_file (run scripts/gen_keypair.sh)" >&2; exit 1; }

export CONNECTOR_NAME="${CONNECTOR_NAME:-ignition-scada-snowflake}"
export CONNECTOR_TASKS="${CONNECTOR_TASKS:-1}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-scada.demo.line1}"
export SNOWFLAKE_URL="${SNOWFLAKE_ACCOUNT_URL}"
export SNOWFLAKE_USER SNOWFLAKE_ROLE SNOWFLAKE_DATABASE SNOWFLAKE_SCHEMA
# The connector wants the key body only: no BEGIN/END lines, no newlines.
SNOWFLAKE_PRIVATE_KEY="$(grep -v -- '-----' "$key_file" | tr -d '\n')"
export SNOWFLAKE_PRIVATE_KEY

body="$(envsubst < "$here/connect/connector.template.json")"
name="$CONNECTOR_NAME"
cfg="$(printf '%s' "$body" | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["config"]))')"

for _ in $(seq 1 60); do curl -fs "$url/connectors" >/dev/null && break; sleep 2; done
curl -fsS -X PUT -H 'Content-Type: application/json' --data "$cfg" "$url/connectors/$name/config" >/dev/null
echo "connector $name submitted to $url"
sleep 5
curl -fsS "$url/connectors/$name/status"; echo
