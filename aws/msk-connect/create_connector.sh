#!/usr/bin/env bash
# Creates the MSK Connect connector running the Snowflake Connector for Kafka v4.
# Reads stack outputs and settings from the environment (see aws/.env.example).
#
# Kafka Connect 3.7.x is required: it runs on Java 17, and connector 4.2.0 bundles classes
# compiled for Java 17. The version is fixed at create time and cannot be changed later.
# https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect-plugins.html
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
: "${STACK_NAME:?}" "${PLUGIN_ARN:?}" "${SNOWFLAKE_ACCOUNT_URL:?}" "${SNOWFLAKE_USER:?}" "${SNOWFLAKE_ROLE:?}"
: "${SNOWFLAKE_DATABASE:?}" "${SNOWFLAKE_SCHEMA:?}"
name="${CONNECTOR_NAME:-ignition-scada-snowflake}"
key_file="${SNOWFLAKE_PRIVATE_KEY_FILE:-$root/.secrets/rsa_key.p8}"

out() { aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
          --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text; }
cluster_arn="$(out ClusterArn)"; sg="$(out SecurityGroupId)"; subnets="$(out SubnetIds)"
role_arn="$(out ConnectRoleArn)"; log_group="$(out ConnectLogGroup)"
brokers="$(aws kafka get-bootstrap-brokers --cluster-arn "$cluster_arn" --query BootstrapBrokerStringSaslIam --output text)"

export CONNECTOR_NAME="$name" CONNECTOR_TASKS="${CONNECTOR_TASKS:-1}" KAFKA_TOPIC="${KAFKA_TOPIC:-scada.demo.line1}"
export SNOWFLAKE_URL="$SNOWFLAKE_ACCOUNT_URL" SNOWFLAKE_USER SNOWFLAKE_ROLE SNOWFLAKE_DATABASE SNOWFLAKE_SCHEMA
SNOWFLAKE_PRIVATE_KEY="$(grep -v -- '-----' "$key_file" | tr -d '\n')"; export SNOWFLAKE_PRIVATE_KEY
config="$(envsubst < "$root/connect/connector.template.json" | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["config"]))')"

subnet_json="$(printf '%s' "$subnets" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read().strip().split(",")))')"
aws kafkaconnect create-connector \
  --connector-name "$name" \
  --kafka-connect-version 3.7.x \
  --capacity '{"provisionedCapacity":{"mcuCount":1,"workerCount":1}}' \
  --connector-configuration "$config" \
  --kafka-cluster "{\"apacheKafkaCluster\":{\"bootstrapServers\":\"$brokers\",\"vpc\":{\"securityGroups\":[\"$sg\"],\"subnets\":$subnet_json}}}" \
  --kafka-cluster-client-authentication '{"authenticationType":"IAM"}' \
  --kafka-cluster-encryption-in-transit '{"encryptionType":"TLS"}' \
  --plugins "[{\"customPlugin\":{\"customPluginArn\":\"$PLUGIN_ARN\",\"revision\":1}}]" \
  --service-execution-role-arn "$role_arn" \
  --log-delivery "{\"workerLogDelivery\":{\"cloudWatchLogs\":{\"enabled\":true,\"logGroup\":\"$log_group\"}}}" \
  --query connectorArn --output text
