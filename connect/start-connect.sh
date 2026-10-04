#!/usr/bin/env bash
# Starts a single-node distributed Kafka Connect worker.
#   CONNECT_BOOTSTRAP_SERVERS   brokers to use for Connect's own internal topics (required)
#   CONNECT_GROUP_ID            default ignition-snowflake-connect
#   CONNECT_REPLICATION_FACTOR  replication for internal topics (1 locally, 2+ on MSK)
set -euo pipefail
: "${CONNECT_BOOTSTRAP_SERVERS:?CONNECT_BOOTSTRAP_SERVERS is required}"
rf="${CONNECT_REPLICATION_FACTOR:-1}"
cat > /tmp/connect.properties <<EOF
bootstrap.servers=${CONNECT_BOOTSTRAP_SERVERS}
group.id=${CONNECT_GROUP_ID:-ignition-snowflake-connect}
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
config.storage.topic=_connect-configs
offset.storage.topic=_connect-offsets
status.storage.topic=_connect-status
config.storage.replication.factor=${rf}
offset.storage.replication.factor=${rf}
status.storage.replication.factor=${rf}
plugin.path=/opt/connect-plugins
listeners=http://0.0.0.0:8083
rest.advertised.host.name=${HOSTNAME:-connect}
offset.flush.interval.ms=10000
EOF
exec /opt/kafka/bin/connect-distributed.sh /tmp/connect.properties
