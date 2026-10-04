# Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Task `FAILED`: `Error loading shared library libgcc_s.so.1` | Kafka Connect on a musl (Alpine) image; v4's native SDK needs glibc | Use a glibc base image (see `connect/Dockerfile`) |
| Task `FAILED`: `ClassNotFoundException: BouncyCastleFipsProvider` | Plugin is the connector jar alone | Add the three Bouncy Castle FIPS jars (`connect/plugin-jars.txt`) |
| Connector refuses to start, asks for v3 migration properties | Fresh install with the classic-compatibility validator on | `snowflake.streaming.validate.compatibility.with.classic=false` |
| `390422` or a network-policy rejection | Connector's egress IP not allowed | Add it to the service user's network policy; v4's ingest endpoint checks it separately |
| `UnsupportedClassVersionError` on MSK Connect | Connector created on Kafka Connect 2.7.1 (Java 11) | Recreate it on 3.7.x (Java 17) |
| Ignition gateway `FAULTED`: `Unable to create 'core' resource collection` | Config files copied into `config/resources/core` before first boot | Put them in `config/resources/external` |
| Tags appear as empty folders | Leaf tags written as directories, or nested in the folder resource | Put atomic tags in `tags.json` inside the folder, listed in its `unary-resource.json` |
| Event Stream received events, Kafka topic empty | Kafka connection unhealthy or wrong auth | Gateway → Connections → Service Connectors, or `GET /data/api/v1/resources/list/ignition/service-connector` |
| New columns appear in lowercase or quoted | v4 preserves identifier case by default | Emit UPPERCASE keys (the Event Stream transform does) or set the normalization compatibility property |
| Numeric values truncated to integers | Type inferred from the first records | Pre-create the table with explicit types (`snowflake/setup.sql`) |
| Connector RUNNING, 0 rows; log says `skipping current record - expected offset N but received M` | Topic was recreated; Snowflake still holds the old channel offset | `make snowflake-reset` (or reset the channel by recreating the table) |
| REST mode: `391902 Unsupported Accept header null` | Ignition's HTTP client sends no Accept header | Send `Accept: application/json` (done in `snowstream`) |
| REST mode: TLS `Hostname mismatch` | Account host written with underscores | Use dashes: `my-org-my-account.snowflakecomputing.com` |
| REST mode: same rows land many times | Client error after the server accepted the request, so the batch is resent | Use `HttpURLConnection`, not `system.net.httpClient`; read from the dedup Dynamic Table |
| REST mode: `390144 JWT token is invalid` | Wrong account/user in the JWT, often an exported shell `SNOWFLAKE_*` variable overriding `local/.env` | Check the container env; the Makefile sources `local/.env` before compose |
| `SocketException ... 169.254.169.254` in Connect logs | SDK probing EC2 instance metadata when not on EC2 | Harmless off-AWS |

Ignition stream status without the UI (needs a logged-in session or API token):
`GET /data/event-stream/api/v1/streams/details/ScadaToSnowflake`

Snowflake side: `snowflake/verify.sql` shows freshness and an offset gap check, and
`SNOWFLAKE.ACCOUNT_USAGE.SNOWPIPE_STREAMING_CHANNEL_HISTORY` shows per-channel rows, errors and
latency (`ACCOUNT_USAGE` lags by up to a couple of hours).
https://docs.snowflake.com/en/sql-reference/account-usage/snowpipe_streaming_channel_history
| v4 task `FAILED`: `Channel error count threshold exceeded` | A record the table rejected (e.g. a string in a FLOAT column) with `errors.tolerance=none` | `errors.tolerance=all` plus `ALTER TABLE ... SET ERROR_LOGGING = TRUE`; bad rows then land in `ERROR_TABLE(<table>)` |
| Rows missing, no error anywhere (v4 or REST) | Rows rejected server-side; REST still returns HTTP 200 | Turn on `ERROR_LOGGING` and query `ERROR_TABLE(<table>)` |
| REST mode: hostname/token calls work, row posts time out | Plant firewall allows the account host but not the separate ingest host (`<locator>.ingest.<...>.snowflakecomputing.com`), which `SYSTEM$ALLOWLIST()` does not list | Get it from `GET /v2/streaming/hostname` and allow it outbound on 443 |
| 8.1 transaction group: `Error during group execution`, nothing written | The database connection has no store-and-forward engine | Create an engine with the same name as the connection (Config → Store and Forward) |
| Path 0: store-and-forward at 15 min, warehouse still never suspends | Store-and-forward sends one INSERT per record, even when forwarding in bulk | Buffer rows and write one multi-row INSERT per interval (`docs/ignition81-batch.md`) |
| Path 0: transaction group at a 15 min rate wrote 4 rows in 45 minutes | A group samples once per execution; it does not buffer between runs | Keep the 1 s group rate, or use the multi-row buffer |
