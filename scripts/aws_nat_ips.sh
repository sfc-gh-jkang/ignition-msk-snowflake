#!/usr/bin/env bash
# Prints the public IPs of the NAT gateways in a VPC, one CIDR per line. MSK Connect workers
# reach Snowflake through these, so the connector's service user must allow them.
#   aws_nat_ips.sh <vpc-id>
set -euo pipefail
vpc="${1:?usage: aws_nat_ips.sh <vpc-id>}"
ips="$(aws ec2 describe-nat-gateways --filter "Name=vpc-id,Values=$vpc" "Name=state,Values=available" \
  --query 'NatGateways[].NatGatewayAddresses[].PublicIp' --output text | tr '\t' '\n' | grep -E '^[0-9.]+$' || true)"
if [ -z "$ips" ]; then
  # Private NAT gateways (egress through a transit gateway) have no public IP to report.
  echo "no public NAT gateway IP in $vpc; use the egress IP seen from a host in the subnets," >&2
  echo "e.g. over SSM: curl -s https://checkip.amazonaws.com" >&2
  exit 1
fi
while read -r ip; do echo "$ip/32"; done <<< "$ips"
