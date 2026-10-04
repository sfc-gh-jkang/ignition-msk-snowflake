# Running the same pipeline on Azure

## Tested: Azure Event Hubs + this repo's Kafka Connect image

Run on 2026-10-03 against a Snowflake account in Azure East US 2:

- **Kafka:** an Event Hubs **Standard** namespace in East US 2 (Kafka endpoint on 9093), one event hub
  `scada.demo.line1` with 2 partitions, and two hub-scoped SAS policies: `Send` for Ignition, `Listen` for
  Connect.
- **Ignition:** 8.3.9, this repo's image, unchanged except the environment: `KAFKA_BOOTSTRAP_SERVERS=
  <namespace>.servicebus.windows.net:9093`, `SASL_SSL`, `PLAIN`, user `$ConnectionString`, and the Send
  policy's connection string as the password file. The idempotent producer settings worked as they are.
- **Kafka Connect:** this repo's `connect/` image in **standalone** mode, so its offsets live in a local
  file and Event Hubs does not have to hold Connect's internal topics. The worker and consumer use
  `SASL_SSL` / `PLAIN` with the Listen policy's connection string.
- **Connector:** v4 4.2.0, the same settings as `connect/connector.template.json`.

| Check | Result |
|---|---|
| Rows | 968 at the outage check, 6,871 by the end; `snowflake/verify.sql` offsets 0–6870 contiguous, **0 missing, 0 duplicate** |
| Freshness | 8–14 s behind the tag change |
| Outage | Kafka Connect stopped for 2 minutes while Ignition kept publishing: offsets contiguous afterwards, largest gap 1.10 s in the 1-second `Speed` series, 0 duplicates |
| Bad record | a string in the FLOAT `VALUE` column, with `ERROR_LOGGING = TRUE`: it lands in `ERROR_TABLE(SCADA_TAG_EVENTS)`. With `errors.tolerance=none` the task then stopped; with `all` it kept running (README gotcha 13) |

**Re-run, 2026-10-03,** from a clean clone: an Event Hubs Standard namespace in Azure West US 2 (hub
`scada.demo.line1`, 2 partitions, Send/Listen policies, created with `az eventhubs`), the same images
and the worker file below, landing in a Snowflake account in AWS us-west-2. 286 rows after 2.5
minutes, 8 seconds behind. Kafka Connect was stopped for 2 minutes and restarted in a **fresh
container with no offsets file**: 857 rows, offsets 0–856 contiguous, 0 missing, 0 duplicates,
largest gap in the 1-second `Speed` series 1.01 s, 15 seconds behind. A record with a string in
`VALUE` went to `ERROR_TABLE(SCADA_TAG_EVENTS)` and the task kept running. The namespace was then
deleted. (In a shared subscription that is at its resource-group quota, put the namespace in an
existing resource group you own; nothing here needs its own group.)

To reproduce, the worker file for `connect-standalone.sh` (the connector file is
`connect/connector.template.json` rendered as `key=value` lines):

```properties
bootstrap.servers=<namespace>.servicebus.windows.net:9093
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
offset.storage.file.filename=/tmp/connect.offsets
plugin.path=/opt/connect-plugins
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="<Listen policy connection string>";
consumer.security.protocol=SASL_SSL
consumer.sasl.mechanism=PLAIN
consumer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="<Listen policy connection string>";
```

```bash
docker run -v $PWD/worker.properties:/tmp/w.properties:ro -v $PWD/sink.properties:/tmp/s.properties:ro \
  --entrypoint /opt/kafka/bin/connect-standalone.sh ignition-kafka-connect:local /tmp/w.properties /tmp/s.properties
```

Standalone mode keeps offsets on the worker's disk, so mount a volume for `/tmp/connect.offsets` in
anything longer-lived than a test. v4 also tracks committed offsets in Snowflake per channel, so a lost
offsets file does not duplicate rows: three of the runs above started in a fresh container with no
offsets file, and every offset still landed exactly once.

All messages landed on one partition because the Event Stream sends one key. The bad records, produced
without a key, went to the other.

Snowflake's documentation names Apache Kafka, Confluent Platform and Amazon MSK for v4, not Event Hubs.
This run shows it working; it is not a statement of support.

## Not run: Confluent Cloud

Not tested from this repo: there was no Confluent Cloud account to run it in. The row below is from
Confluent's and Snowflake's documentation.

The Ignition side and the Snowflake side do not change. Only the Kafka hop and where Kafka Connect
runs are different. Snowflake recommends running Kafka Connect in the same cloud region as the
Snowflake account. https://docs.snowflake.com/en/user-guide/kafka-connector/setup-kafka

| Option | Kafka | Where the v4 connector runs | Notes |
|---|---|---|---|
| Confluent Cloud on Azure | Confluent cluster | Confluent **custom connector** | Confluent's built-in Snowflake sink is not yet v4, so upload the v4 package as a custom connector. Custom connectors are available in selected regions, share 2 GB of memory across tasks, and need the Snowflake hostnames declared as egress endpoints. https://docs.confluent.io/cloud/current/connectors/bring-your-connector/custom-connector-qs.html |
| Azure Event Hubs (Kafka endpoint) | Event Hubs namespace, Standard tier or above | A small VM or container running this repo's `connect/` image | Event Hubs exposes a Kafka-compatible endpoint, but does not host Kafka Connect. Ignition connects with SASL_SSL / PLAIN using the connection string. Tested here, see above. https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-apache-kafka-overview |
| Self-managed Kafka | Your cluster | This repo's `connect/` image | Same as local mode, on your own hosts |

Snowflake also offers Openflow, whose Kafka connector reads from an existing cluster without running
Kafka Connect yourself: https://docs.snowflake.com/en/user-guide/data-integration/openflow/connectors/kafka/about
