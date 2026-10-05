# Choosing a path

A decision guide for the four paths in this repo, written for the controls engineer and the network
or security owner who decide together. Every figure here comes from the test runs described in the
[README](../README.md#verified) and the per-path docs. Credit figures are Snowflake compute only;
multiply by your own contract rate.

## Recommendation

**Start with path 1, Ignition gateway → Snowpipe Streaming REST.** It gives the freshest data at close
to the lowest cost (2.8 credits a day, against 2.2 for 15-minute batches), and it works on Ignition 8.1 with no upgrade and no new module.

- Data lands 5 to 8 seconds behind the gateway.
- Ingest needs no warehouse. Snowpipe Streaming is billed per uncompressed GB, at 0.0037 credits a GB
  ([cost](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-cost)).
  A 17-hour soak used 0.000083 credits of ingest.
- What you pay for is the Dynamic Table refresh, on a lag you set: about 2.8 credits a day at a
  15-minute lag, measured over the same soak.
- Because ingest is priced by volume and the refresh by schedule, adding lines and tags barely
  moves the bill.

Choose something else only for a specific reason:

| Reason | Path |
|---|---|
| The gateways must never connect to anything outside the plant network | **2**, Openflow + Data Connectivity Proxy |
| You need a stopgap this week and want to keep the existing JDBC connection | **0**, batched JDBC |
| You already run, or plan, a Kafka backbone other systems will share | **3**, Ignition 8.3 → Kafka → Connector v4 |

Snowpipe Streaming does not need the Data Connectivity Proxy. It is a Snowflake-hosted API that the
gateway calls outbound on 443. The proxy exists for path 2, where Openflow, running in Snowflake,
has to read a database inside the plant.

## Cost side by side

| Path | Credits / day | What drives it |
|---|---|---|
| Continuous single-row JDBC inserts (the starting point) | 24 (standard X-Small), 30–32 (Gen2) | The warehouse never suspends |
| 0. Batched JDBC, one insert every 15 min | 2.2 | Four warehouse resumes an hour, 60 s auto-suspend |
| 1. Snowpipe Streaming REST | 2.8 | Dynamic Table refresh; ingest is per GB |
| 2. Openflow + DCP, 15-minute merge | 14.5 | Control pool and runtime run all the time, plus the merge |
| 2. Openflow + DCP, default 1-minute merge | 34.2 | As above, merge warehouse almost never suspends |
| 3. Kafka → Connector v4 | ≤ 2.8, plus Kafka hosting | Same Snowpipe Streaming service as path 1 |

Per-hour and per-month figures, and where each was measured: [What it costs](../README.md#what-it-costs).
Openflow pricing: https://docs.snowflake.com/en/user-guide/data-integration/openflow/cost-spcs

## Path 1: gateway → Snowpipe Streaming REST

How it works:

- A Jython script library on the gateway buffers tag changes in memory.
- Every 5 seconds it signs a key-pair JWT, exchanges it for a short-lived scoped token, and posts the
  batch to the Snowpipe Streaming REST API over HTTPS 443.
- Rows leave the buffer only after Snowflake returns HTTP 200, so an outage is retried rather than lost.
- A retry after an unclear reply can land a row twice, so a Dynamic Table keeps one row per event ID.

What it needs:

- No new module: a project script library and tag scripts, using only the Python standard library
  that ships inside Ignition 8.1.
- In Snowflake: a service user with key-pair authentication, a landing table with error logging on (a
  rejected row is kept for review instead of dropped), and a Dynamic Table.
- Firewall: two hostnames. The gateway calls the account URL, as JDBC does, and also a separate ingest
  host that the account URL hands back. It is a different name under `.snowflakecomputing.com`, so a
  firewall that allows Snowflake by exact hostname rather than `*.snowflakecomputing.com` must add it.

What was tested: Ignition 8.1.42 against AWS and Azure accounts; a two-minute network cut with every
row arriving once; a mixed good-and-bad request that returned HTTP 200 with the bad row in the error
table (so watch the error table, not the gateway log).

Trade-off: the buffer is in gateway memory. A gateway restart while Snowflake is unreachable loses what
was queued; in testing, a restart 60 seconds into an outage left a 70.5-second gap.

Details: [ignition81-rest.md](ignition81-rest.md) ·
REST API: https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api ·
Error logging: https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-error-tables

## Path 2: Openflow with the Data Connectivity Proxy

How it works:

- Ignition keeps using JDBC but writes to a SQL Server inside the plant. Transaction groups only need
  their database connection repointed.
- A Data Connectivity Proxy agent on a plant Linux host opens an outbound tunnel on 443. No inbound rules.
- Openflow, running in Snowflake, reads the tables through that tunnel with Change Tracking and merges
  them into Snowflake on a schedule.

What it needs:

- A SQL Server with Change Tracking on the tables Ignition writes. Express works, since Change Tracking
  is on every edition (https://docs.snowflake.com/en/user-guide/data-integration/openflow/connectors/sql-server-cdc/compare-change-tracking-cdc),
  but Express caps each database at 10 GB (https://learn.microsoft.com/en-us/sql/sql-server/editions-and-components-of-sql-server-2022), so old rows need purging.
- A Linux host for the agent: kernel 5 or later, Docker, at least 1 vCPU and 512 MB. It must reach
  Snowflake directly. A corporate forward proxy is not supported, and TLS inspection must be bypassed
  for its hostnames (https://docs.snowflake.com/en/user-guide/data-connectivity-proxy-security).
  Behind either, the agent loops on `CP connect failed (CP gRPC not ready?)`.
- In Snowflake: an Openflow Snowflake deployment and runtime, a network rule with
  `MODE = DATA_CONNECTIVITY_PROXY_EGRESS`, and an external access integration on both.
- Lead time on a new account: per-account certificate issuance took 8.5 hours in testing.

What was tested: a real 8.1.42 gateway writing through SQL Server, with 0 rows missing or duplicated; a
two-minute agent stop with every row arriving once, because the plant database held them; SQL Server Express.

Trade-off: about five times the cost of path 1, in exchange for the plant only ever talking to a local
database and that database being the buffer.

Details: [openflow-dcp.md](openflow-dcp.md) ·
Setup: https://docs.snowflake.com/en/user-guide/data-integration/openflow/setup-openflow-spcs-dcp ·
Proxy: https://docs.snowflake.com/en/user-guide/data-connectivity-proxy

## Path 0: batched JDBC (stopgap)

Two obvious Ignition settings do not batch the writes, and both were tested:

- **Transaction group execution rate set to 15 minutes loses data.** A historical group wrote one row
  per run; the roughly 900 one-second readings in each interval were never recorded.
- **Store-and-forward set to forward every 15 minutes saves nothing.** It held rows back, then still sent
  each as its own insert in a burst. The warehouse almost never suspended: 1.01 credits an hour.

What works is a one-second tag script that appends readings to a list in gateway memory and a 15-minute
timer script that writes the list as a single multi-row `INSERT`, then removes what it wrote. Pair it with:

```sql
ALTER WAREHOUSE <your_warehouse> SET AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
```

Why it is a stopgap: data is up to 15 minutes old, a gateway restart loses up to one interval, and each
insert grows with tag count, so the warehouse runs longer per write as volume grows. Path 1 has none of these.

Details: [ignition81-batch.md](ignition81-batch.md)

## Path 3: Ignition 8.3 and Kafka

Ignition 8.3 Event Streams publish to Kafka, and Connector v4 streams into Snowflake through the same
Snowpipe Streaming service as path 1. It needs the 8.3 upgrade, the Kafka module, and a Kafka cluster with
Kafka Connect (Amazon MSK and Azure Event Hubs were both tested). Worth it when other systems will share
the stream, not just to land SCADA data.

Details: [aws-msk.md](aws-msk.md) · [azure-variant.md](azure-variant.md)

## Questions that decide it

- Is the security concern only about anything connecting **in**, or also about the gateways connecting
  **out**? Out-only is fine for path 1; "never out" means path 2.
- Does the firewall allow Snowflake by exact hostname? Then path 1 needs the ingest host added.
- Is there already a SQL Server in the plant, and can Change Tracking be enabled on it?
- For path 2: is there a Linux host with Docker, and does outbound traffic pass through a proxy or TLS inspection?
- How do the gateways write today: SQL Bridge transaction groups, the Tag Historian, or scripts?
- How many lines and tags are coming, and how fresh does the floor need the data to be?

## Suggested rollout

1. Pilot path 1 on one line, alongside the existing feed. Compare row counts and credits for a week
   (`SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY`).
2. If it holds, move the remaining lines and retire the JDBC inserts.
3. If the gateways may not connect out, pilot path 2 instead.
4. Revisit path 3 when an 8.3 upgrade or a shared Kafka backbone is on the roadmap anyway.
