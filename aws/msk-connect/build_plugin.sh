#!/usr/bin/env bash
# Builds the MSK Connect custom plugin zip (connector + Bouncy Castle FIPS jars), uploads it to
# S3 and registers it as an MSK Connect custom plugin.
#   build_plugin.sh <bucket> [plugin-name]
# Prints the plugin ARN.
# https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect-plugins.html
set -euo pipefail
bucket="${1:?usage: build_plugin.sh <bucket> [plugin-name]}"
# Default name is per stack: plugin names are account-wide, so a shared account may already have one.
name="${2:-${STACK_NAME:-ignition-kafka}-snowflake-kafka-4-2-0}"
root="$(cd "$(dirname "$0")/../.." && pwd)"
work="$(mktemp -d)"; trap 'rm -rf "$work"' EXIT

dir="$work/snowflake-kafka-connector-4.2.0/lib"
"$root/connect/fetch_plugin.sh" "$dir" >&2
(cd "$work" && zip -qr plugin.zip snowflake-kafka-connector-4.2.0)
key="plugins/${name}.zip"
aws s3 cp --only-show-errors "$work/plugin.zip" "s3://$bucket/$key"

arn="$(aws kafkaconnect create-custom-plugin --name "$name" --content-type ZIP \
  --location "s3Location={bucketArn=arn:aws:s3:::$bucket,fileKey=$key}" \
  --query customPluginArn --output text)"
echo "waiting for plugin $arn" >&2
for _ in $(seq 1 60); do
  state="$(aws kafkaconnect describe-custom-plugin --custom-plugin-arn "$arn" --query customPluginState --output text)"
  [ "$state" = ACTIVE ] && break
  [ "$state" = CREATE_FAILED ] && { echo "plugin creation failed" >&2; exit 1; }
  sleep 10
done
echo "$arn"
