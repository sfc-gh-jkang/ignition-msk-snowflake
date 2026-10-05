# AWS mode: Ignition on EC2 → Amazon MSK → MSK Connect (v4) → Snowflake

## What gets created

`aws/cloudformation/stack.yaml` deploys into an **existing VPC** (it creates no VPC):

- A provisioned MSK cluster (2 brokers, `kafka.t3.small` by default) with SASL/SCRAM **and** IAM
  client auth and TLS in transit. See [msk-auth.md](msk-auth.md) for why both.
- A KMS key and an `AmazonMSK_*` Secrets Manager secret for the Ignition SCRAM user.
- An S3 bucket for the connector plugin, a CloudWatch log group, and the MSK Connect service role.
- An EC2 host (Amazon Linux 2023, SSM access, no SSH) that runs the Ignition container. It stands in
  for the plant gateway and connects to MSK over SCRAM.

You need two private subnets in **two different AZs** with a NAT route (MSK Connect workers reach
Snowflake through the NAT), and a role that can create MSK, MSK Connect, IAM roles, KMS keys,
Secrets Manager secrets, S3 buckets and EC2 instances.

## Steps

```bash
cp aws/.env.example aws/.env      # VPC, subnets, Snowflake settings
set -a; source aws/.env; set +a

# 1. Infrastructure (MSK takes ~20-30 minutes)
aws cloudformation deploy --stack-name "$STACK_NAME" --template-file aws/cloudformation/stack.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides Prefix="$STACK_NAME" VpcId="$VPC_ID" SubnetIds="$SUBNET_IDS" IgnitionSubnetId="$IGNITION_SUBNET_ID"
#   optional: ExistingSecurityGroupId=sg-...  RepoArchiveUrl=https://...  (see "Shared and locked-down accounts")

# 2. Snowflake objects; allow the NAT gateway IP(s) for the connector's service user
scripts/aws_nat_ips.sh "$VPC_ID"          # put the result in SNOWFLAKE_ALLOWED_IP
make ENV=aws/.env snowflake-setup

# 3. Plugin: connector 4.2.0 + Bouncy Castle FIPS jars, uploaded and registered
bucket=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='PluginBucket'].OutputValue" --output text)
export PLUGIN_ARN=$(aws/msk-connect/build_plugin.sh "$bucket")

# 4. Connector on MSK Connect (Kafka Connect 3.7.x, IAM auth to the cluster)
aws/msk-connect/create_connector.sh

# 5. Verify
make ENV=aws/.env local-verify
```

The Ignition gateway is reachable through SSM port forwarding. This needs the AWS Session Manager
plugin on your machine as well as the AWS CLI:
https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html

```bash
aws ssm start-session --target <IgnitionInstanceId> \
  --document-name AWS-StartPortForwardingSession --parameters portNumber=8088,localPortNumber=8088
```

The admin password is in `/root/ignition-admin-password` on the host (root, mode 600). Tested 2026-10-04:
`/StatusPing` returned `{"state":"RUNNING"}` and the web UI answered on `localhost:8088`.

## Gotchas specific to MSK Connect

- **Kafka Connect 3.7.x, not 2.7.1.** The version sets the Java runtime (2.7.1 is Java 11, 3.7.x is
  Java 17) and cannot be changed on an existing connector. Connector 4.2.0 bundles Java 17 classes.
- **The principal that creates the connector** needs `iam:PassRole` for the service role and the EC2
  network-interface actions (`ec2:CreateNetworkInterface`, `ec2:DescribeNetworkInterfaces`,
  `ec2:DeleteNetworkInterface`, `ec2:DescribeVpcs`, `ec2:DescribeSubnets`,
  `ec2:DescribeSecurityGroups`). The service role does not.
  https://docs.aws.amazon.com/msk/latest/developerguide/mkc-iam-policy-examples.html
- **v4 calls a separate Snowpipe Streaming ingest endpoint** in addition to the account endpoint, and
  the network policy is evaluated there too. Allow the NAT IPs on the service user's network policy.
- **Every failed `CreateConnector` costs a full provisioning cycle.** Validate the connector config in
  local mode first; it is the same JSON.

## Shared and locked-down accounts

Five things re-runs in a shared AWS account needed (2026-10-03 and 2026-10-04). Each has a parameter or a clear
error now, so none of them needs a manual step:

| Situation | What happens | What to do |
|---|---|---|
| An SCP denies `ec2:CreateSecurityGroup` | the stack rolls back on `KafkaSecurityGroup` | pass `ExistingSecurityGroupId=sg-…`, a group that allows all traffic between its own members (a VPC's `default` group does) |
| The repo is private, or the host cannot reach GitHub | user data's `git clone` fails (`could not read Username`) and no Ignition container starts | pass `RepoArchiveUrl`: `git archive --format=tar.gz -o repo.tgz HEAD`, upload it to a bucket that already exists (not the stack's plugin bucket, which is created by the same deploy), and pass `aws s3 presign … --expires-in 3600`. The URL is read once, at first boot, so it must be valid when the deploy starts |
| A plugin named like yours already exists | `CreateCustomPlugin` returns `ConflictException` | the default name is now `<STACK_NAME>-snowflake-kafka-4-2-0`; a second argument to `build_plugin.sh` overrides it |
| The VPC's NAT gateways are private (egress through a transit gateway) | `aws_nat_ips.sh` has no public IP to print | it now exits 1 and says so; allow the egress IP seen from the Ignition host instead: `curl -s https://checkip.amazonaws.com` over SSM |
| A second stack in the same account, or a redeploy after a failed delete | the change set fails early validation: the plugin bucket `<Prefix>-plugins-…` and log group `/msk-connect/<Prefix>` already exist | the stack's resource names come from `Prefix` (default `ignition-kafka`), not the stack name, so pass a unique `Prefix=…` per stack (the step above passes `"$STACK_NAME"`; it must be lowercase letters, digits and hyphens, 3-25 characters) |

## Teardown

```bash
aws kafkaconnect delete-connector --connector-arn <arn>
aws kafkaconnect delete-custom-plugin --custom-plugin-arn "$PLUGIN_ARN"
aws s3 rm "s3://$bucket" --recursive
aws cloudformation delete-stack --stack-name "$STACK_NAME"
make ENV=aws/.env snowflake-teardown
```

## Verified

Run on 2026-10-02 in us-west-2, landing in a Snowflake account in AWS us-east-1:

| Check | Result |
|---|---|
| Stack | MSK 3.9.x, 2 x `kafka.t3.small`, SCRAM + IAM, TLS; Ignition 8.3.9 container on EC2, built from this repo by user data |
| Ignition → MSK | SASL_SSL / SCRAM-SHA-512 to the `BootstrapBrokerStringSaslScram` brokers on 9096 |
| MSK Connect | Kafka Connect 3.7.x, 1 MCU x 1 worker, IAM auth, custom plugin = connector 4.2.0 + Bouncy Castle FIPS jars |
| Rows | 3,047 in the first run, Kafka offsets 0-3046 contiguous: no missing, no duplicate offsets |
| Buffering | Ignition published for ~25 minutes before the connector existed; all of it landed on first start |
| Freshness | newest row about 8 seconds behind the tag change |

**Re-run, 2026-10-03,** from a clean clone in a shared AWS account in us-west-2, landing in a Snowflake
account in AWS us-west-2, with the options in "Shared and locked-down accounts" above (existing
security group, Ignition built from a repo archive, a per-stack plugin name, the host's egress IP
allowed):

| Check | Result |
|---|---|
| Connector | `RUNNING` about 10 minutes after `create_connector.sh` |
| Rows | 945, offsets 0–944 contiguous, 0 missing; includes everything Ignition published before the connector existed |
| Freshness | newest row 14 seconds behind |
| Teardown | connector, plugin, bucket contents and stack deleted; no `ign-` cluster or plugin left |

Retested 2026-10-04 from a fresh clone with **no manual step on the host**: `RepoArchiveUrl` (a presigned tarball in a bucket created before the deploy) let user data build and start Ignition by itself (cloud-init `done`, no clone error); `aws_nat_ips.sh` exited 1 with its hint on the private-NAT VPC; `build_plugin.sh` with no name argument registered `ignition-kafka-e2e-snowflake-kafka-4-2-0` alongside the existing `snowflake-kafka-connector-4-2-0`. Connector `RUNNING`, 929 rows, offsets 0–928 contiguous, 0 missing, 13 s behind; then torn down.

MSK Connect has no stop/pause, so the laptop's "stop Connect for two minutes" test has no direct
equivalent here; the backfill above exercises the same path (records waiting in the topic).

## Things that bit us in a locked-down account

- **SCPs can deny `ec2:CreateSecurityGroup` and `s3:PutBucketPublicAccessBlock`** in a shared VPC.
  Pass `ExistingSecurityGroupId` (a group that allows all traffic between its own members, such as
  the VPC default group) and the template creates none. The bucket is private by default without
  the public-access-block property.
- **NAT gateways can be private.** If `aws ec2 describe-nat-gateways` shows no public IP, the VPC
  egresses through another hop. Find the real egress IP from inside the VPC
  (`curl -s https://checkip.amazonaws.com` on the Ignition host, via SSM) and allow that on the
  connector's network policy.
- **Reusing the landing table after another run:** v4 names its channels after the connector, so
  giving the MSK connector its own `CONNECTOR_NAME` starts fresh channels at offset 0 without
  resetting the table (README gotcha 9).
