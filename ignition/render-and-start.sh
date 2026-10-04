#!/usr/bin/env bash
# envsubst takes the literal variable names to substitute, so single quotes are intended.
# shellcheck disable=SC2016
# Renders the Kafka connection from environment variables, then hands off to the stock
# Ignition entrypoint. Keeps credentials out of the image and out of git.
#
#   KAFKA_BOOTSTRAP_SERVERS  comma-separated host:port list (required)
#   KAFKA_SECURITY_PROTOCOL  PLAINTEXT | SASL_PLAINTEXT | SASL_SSL   (default PLAINTEXT)
#   KAFKA_SASL_MECHANISM     PLAIN | SCRAM-SHA-512 | ...            (default PLAIN)
#   KAFKA_USERNAME           SASL user (only used when SASL is on)
#   KAFKA_PASSWORD_FILE      file holding the SASL password (preferred), or
#   KAFKA_PASSWORD           the password itself, written to a 0600 file at start-up
#   KAFKA_TOPIC              topic the event stream publishes to    (default scada.demo.line1)
set -euo pipefail

: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
export KAFKA_SECURITY_PROTOCOL="${KAFKA_SECURITY_PROTOCOL:-PLAINTEXT}"
export KAFKA_SASL_MECHANISM="${KAFKA_SASL_MECHANISM:-PLAIN}"
export KAFKA_USERNAME="${KAFKA_USERNAME:-}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-scada.demo.line1}"

# The SASL password never goes into the connection config. Ignition's file secret provider
# (8.3.5+) reads it from a file, and the connection holds only a reference to that secret.
# Prefer KAFKA_PASSWORD_FILE (a Docker/Kubernetes secret mount); KAFKA_PASSWORD is written to a
# 0600 file in the container's filesystem as a fallback.
password_ref=null
export KAFKA_PASSWORD_PATH="${KAFKA_PASSWORD_FILE:-/usr/local/bin/ignition/data/var/secrets/kafka_password}"
if [[ "$KAFKA_SECURITY_PROTOCOL" == SASL_* ]]; then
  if [ -z "${KAFKA_PASSWORD_FILE:-}" ]; then
    : "${KAFKA_PASSWORD:?KAFKA_PASSWORD or KAFKA_PASSWORD_FILE is required when SASL is enabled}"
    mkdir -p "$(dirname "$KAFKA_PASSWORD_PATH")" && chmod 700 "$(dirname "$KAFKA_PASSWORD_PATH")"
    (umask 077; printf '%s' "$KAFKA_PASSWORD" > "$KAFKA_PASSWORD_PATH")
  fi
  [ -r "$KAFKA_PASSWORD_PATH" ] || { echo "cannot read $KAFKA_PASSWORD_PATH" >&2; exit 1; }
  password_ref='{"type": "Referenced", "data": {"providerName": "files", "secretName": "kafka-password"}}'
fi
unset KAFKA_PASSWORD
export KAFKA_PASSWORD_REF="$password_ref"
export KAFKA_BOOTSTRAP_JSON
KAFKA_BOOTSTRAP_JSON="$(printf '%s' "$KAFKA_BOOTSTRAP_SERVERS" | awk -F, '{for(i=1;i<=NF;i++){printf "%s\"%s\"", (i>1?",":""), $i}}')"

ext=/usr/local/bin/ignition/data/config/resources/external/ignition
conn="$ext/service-connector/kafka/config.json"
envsubst '${KAFKA_BOOTSTRAP_JSON} ${KAFKA_SECURITY_PROTOCOL} ${KAFKA_SASL_MECHANISM} ${KAFKA_USERNAME} ${KAFKA_PASSWORD_REF}' \
  < /opt/provision/kafka-connector.template.json > "$conn"

envsubst '${KAFKA_PASSWORD_PATH}' < /opt/provision/secret-provider.template.json > "$ext/secret-provider/files/config.json"

stream=/usr/local/bin/ignition/data/projects/ScadaToSnowflake/com.inductiveautomation.eventstream/event-streams/LineToKafka/config.json
envsubst '${KAFKA_TOPIC}' < /opt/provision/line-to-kafka.template.json > "$stream"

exec docker-entrypoint.sh "$@"
