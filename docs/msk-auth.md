# Ignition and Amazon MSK authentication

Amazon MSK supports several client authentication methods: IAM, SASL/SCRAM, mutual TLS and
unauthenticated. https://docs.aws.amazon.com/msk/latest/developerguide/kafka_apis_iam.html

The two clients in this pipeline do not overlap:

| Client | What it supports | What this repo uses |
|---|---|---|
| Ignition 8.3 Kafka module | Standard Kafka SASL mechanisms (GSSAPI, PLAIN, SCRAM-SHA-256/512) over PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL | **SCRAM-SHA-512 over SASL_SSL** (port 9096) |
| MSK Connect | IAM or no authentication | **IAM** (port 9098) |

MSK IAM auth uses a custom SASL mechanism, `AWS_MSK_IAM`, provided by AWS's `aws-msk-iam-auth`
library (or `OAUTHBEARER` with AWS's signer). Ignition does not ship that library, so it cannot use
IAM auth as installed. MSK Serverless only supports IAM, which is why this repo uses a provisioned
cluster with **both** SCRAM and IAM enabled. AWS documents enabling both on one cluster.

## SCRAM on MSK, the short version

1. Create a customer-managed KMS key; MSK will not associate a secret encrypted with the default key.
2. Create a Secrets Manager secret whose name starts with `AmazonMSK_`, containing
   `{"username": "...", "password": "..."}`.
3. Associate it with the cluster (`AWS::MSK::BatchScramSecret` in the template).
4. Clients connect to the `BootstrapBrokerStringSaslScram` brokers on port 9096 with
   `security.protocol=SASL_SSL` and `sasl.mechanism=SCRAM-SHA-512`.

https://docs.aws.amazon.com/msk/latest/developerguide/msk-password.html

## How the credentials reach Ignition

The connection's `password` field holds a secret object rather than plain text. The container
entrypoint (`ignition/render-and-start.sh`) configures Ignition's file secret provider (added in
8.3.5) and sets the field to a `Referenced` secret pointing at it:

```json
"password": {"type": "Referenced", "data": {"providerName": "files", "secretName": "kafka-password"}}
```

The password itself lives only in a file: `KAFKA_PASSWORD_FILE` if you mount one (Docker or
Kubernetes secret), otherwise a 0600 file the entrypoint writes from `KAFKA_PASSWORD`. On the EC2
host, the user data reads the SCRAM secret from Secrets Manager with the instance role, writes it to
a root-only file and mounts that file read-only into the container. Nothing secret is written to the
image, to git, or to the gateway configuration.
https://www.docs.inductiveautomation.com/docs/8.3/platform/security/secrets-management

## ACLs

With SCRAM enabled and no Kafka ACLs, the template sets `allow.everyone.if.no.acl.found=true` so the
demo works out of the box. For production, add ACLs that let the Ignition SCRAM user write only to
its topics. IAM clients (MSK Connect) are authorized by IAM policy, not by ACLs.
