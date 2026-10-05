# The best way to stream Ignition SCADA data into Snowflake, tested four ways

*A gateway script to Snowpipe Streaming, batching, Openflow through an outbound-only tunnel, and Kafka.
All four tested end to end on Ignition 8.1 and 8.3, with outage tests, a 17-hour soak and measured
costs. The code is public:*

https://github.com/sfc-gh-jkang/ignition-msk-snowflake

## Why I built this

A lot of manufacturers run Snowflake, and a lot of plants run Inductive Automation Ignition as their
SCADA and MES layer. Getting the two talking is one of the first things a plant team asks for: line
data next to quality, maintenance, ERP and supply chain, so the questions that cross those systems can
finally be answered in one place.

What I kept running into was that there is no single, tested answer to "how should Ignition send data
to Snowflake?" There are database connections, Kafka modules, Openflow connectors and a REST API, each
documented on its own, and none of them compared against the others on the things a plant actually
cares about: how fresh the data is, what happens when the network drops, what the security team will
accept, and what it costs every day for years.

So I set out to find the ideal way, by building every reasonable path and measuring it the same way.

## The problem with the obvious first integration

A plant's first Snowflake integration with Ignition is often a database connection: tag changes become
JDBC `INSERT`s. It's quick to set up and it works. The trouble shows up on the bill. Small inserts every
few seconds mean the warehouse never sits idle long enough to auto-suspend, so an X-Small runs 24 hours
a day, 24 credits a day on a standard warehouse and 30 to 32 on Gen2, to receive a few kilobytes at a
time. Almost all of it is compute spent waiting, and it gets worse, not better, as more lines are added.

The fix is to stop holding a warehouse open for ingestion. I built and tested four ways:

- **Path 1, the one I recommend: an Ignition 8.1 gateway script posting to the Snowpipe Streaming REST
  API.** No upgrade, no new module, no new server, no warehouse for ingest. Data is 5 to 8 seconds old,
  at about 2.8 credits a day.
- **Path 0: batch the JDBC writes you already have.** 2.2 credits a day, but data is up to 15 minutes
  old and each insert grows with volume. A stopgap.
- **Path 2: a plant SQL Server, replicated by Openflow through the Data Connectivity Proxy.** For when
  the gateways must never connect outside the plant. About 14.5 credits a day.
- **Path 3: Ignition 8.3 Event Streams to Kafka (or Amazon MSK) and the Snowflake Connector for Kafka
  v4.** For when a Kafka backbone will be shared by other systems.

All four only send traffic out of the plant, on port 443, so nothing connects in.

## Path 0: batch the writes you already have (a stopgap)

The obvious move is to have Ignition write every 15 minutes and let the warehouse suspend in between:

```sql
ALTER WAREHOUSE INGEST_WH SET AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
```

How you make Ignition "write every 15 minutes" is the whole question, and the obvious setting is the
wrong one. I ran two 8.1.42 gateways side by side for an hour, each writing one row a second through
the Snowflake JDBC driver to its own Gen2 X-Small, against an Azure account.

- **Store-and-forward set to forward every 15 minutes: 1.01 credits an hour.** That's the engine
  transaction groups write through. It held the rows back, then sent every reading as its own
  single-row `INSERT` (with `alter session`, `commit` and `rollback` around them), about 1.2 a second.
  Rows landed across 47 different minutes, up to 18.7 minutes late, and the warehouse barely suspended.
  It delays the writes; it doesn't combine them.
- **One multi-row `INSERT` every 15 minutes: 0.09 credits an hour.** A one-second tag script appends
  the reading to a list in `system.util.getGlobals()`. A 15-minute timer tag writes the whole list as
  one `INSERT ... VALUES (...), (...)`, and removes only the rows it wrote. Three arrivals of about 900
  rows each, and the warehouse woke four times an hour.

I also built a real SQL Bridge historical transaction group and set it to run every 15 minutes. Over 45
minutes it wrote 4 rows, one per run; the ~900 one-second readings in each interval were never recorded.
So slowing the group down isn't batching either.

So the saving comes from the multi-row insert, not the schedule. The pool's `SELECT 1` validation
queries didn't matter either: they ran in cloud services, with no warehouse cluster in query history.
The trade-offs are the same as any in-memory buffer: a gateway restart loses up to one interval. And it
is a stopgap rather than a destination: data is up to 15 minutes old, and as tags are added each insert
gets bigger and the warehouse runs longer per write. Path 1 has neither problem.

## Path 1: Ignition 8.1 straight to Snowpipe Streaming, no Kafka

Plenty of plants run Ignition 8.1, and upgrading to 8.3 plus buying the Kafka module is a real cost
just to land some tag data. You don't need either. Snowpipe Streaming's high-performance architecture
has a REST API, and an 8.1 gateway can call it with nothing but the Python standard library inside
Ignition's Jython.

The repo's `snowstream` script library does four things:

1. A tag "Value Changed" script calls `snowstream.enqueue(tag, value)`, which adds the reading to a
   bounded in-memory buffer.
2. Every 5 seconds a flush tag signs a JWT with the service user's RSA key and exchanges it for a
   scoped token, cached for about 50 minutes.
3. It POSTs the batch as gzip-compressed NDJSON to the ingest host over HTTPS.
4. Rows leave the buffer only on HTTP 200. On any error they stay and go out on the next tick.

Ingestion needs no warehouse; it's billed per uncompressed GB.

One network detail: the gateway talks to two hosts, the account URL and a separate ingest host that
`GET /v2/streaming/hostname` returns. A firewall that allows `*.snowflakecomputing.com` covers both; one
that lists the JDBC hostname only will block the ingest calls. `SYSTEM$ALLOWLIST()` does not list the
ingest host, so ask the account for it rather than relying on that list.

Delivery is at least once. A retry after an unclear reply can land a row twice, so every row carries
an `EVENT_ID` and a Dynamic Table keeps one row per ID.

**What I measured:**

- Ignition 8.1.42 against an AWS account: rows landed 4 to 8 seconds after the tag changed.
- The same gateway against an Azure East US 2 account, changing only the account identifier: 199 rows
  in 90 seconds, 0 duplicates, no gap over 1.08 seconds in the 1-second tags.
- With the gateway's network cut for two minutes, every row arrived exactly once afterwards.
- A 17-hour soak on AWS: 110,772 rows, 0 duplicates, no gap over 2.0 seconds in the 1-second tags. The
  ingest itself was metered at 0.000083 credits for the whole soak. The Dynamic Table refreshing every
  15 minutes used 1.889 credits, a median of 0.117 an hour, about 2.8 a day.

That is why this is the path I recommend. Ingest is billed per GB, so adding lines and tags barely
moves it, and the refresh cost is set by the lag you choose, not by how much data arrives.

**The trade-off:** the buffer lives in gateway memory. I restarted the gateway 60 seconds into a
network outage and got a 70.5-second gap: the 60 seconds it was holding, plus about 10 seconds of boot
time. If you can't accept that, put a database in the middle, which is Path 2.

## Path 2: Openflow through an outbound-only tunnel

Some OT security teams won't let a gateway call anything outside the plant, even outbound. For them
there's this:

1. Ignition keeps writing over JDBC, to a SQL Server inside the plant instead of to Snowflake. You
   only repoint the database connection; transaction groups don't change.
2. A Data Connectivity Proxy agent on a small plant Linux host (Docker, about 1 vCPU and 512 MB) opens
   an outbound tunnel on 443.
3. Openflow, running inside Snowflake on a Snowflake deployment, reads SQL Server with Change Tracking
   back through that tunnel and merges the rows into Snowflake tables on a schedule.

In Snowflake that's a network rule with `MODE = DATA_CONNECTIVITY_PROXY_EGRESS`, and an external
access integration attached to both the Openflow runtime and the proxy. The setup docs require both,
so check both if the connector can't reach the plant.

**What I measured:**

- Simulator writing one row a second: 9,525 rows, with the source key contiguous from 1 to 9,525.
  That held through stopping the agent for two minutes: 0 missing, 0 duplicated.
- With a real Ignition 8.1.42 gateway writing through its own SQL Server connection, a gateway restart
  only lost the readings due while it rebooted, 8.8 seconds, and nothing it had already written.
- The same run against an Azure East US 2 account, with nothing in the setup changed: 14,292 rows,
  contiguous from 1, 0 missing, 0 duplicated.

Three things to know before you plan on it:

- **The agent has to reach Snowflake directly.** A corporate forward proxy is not supported, and TLS
  inspection has to be bypassed for its hostnames
  (https://docs.snowflake.com/en/user-guide/data-connectivity-proxy-security). I put the agent behind a
  TLS-inspecting proxy to check, and it looped forever on `CP connect failed (CP gRPC not ready?)`.
- **SQL Server Express works.** Change Tracking is on every edition, but Express caps each database at
  10 GB, so plan to purge old rows.
- **Budget lead time on a new account.** DCP needs per-account certificates. On a brand-new account
  they took 8.5 hours to issue; the plant database buffered the whole time, and when the tunnel came up
  46,088 rows arrived with none missing.

One more that took a while. I seeded the gateway's database connection by setting the internal
database's plain `PASSWORD` column, and it failed to log in. The gateway reads only the encrypted
`PASSWORDE` column, so the builder passes the password as a JDBC property instead. On a real gateway,
type it into the connection page.

**What it costs (measured from the account's metering history):**

- The Openflow control pool runs whenever the deployment exists: 0.111 credits an hour, 2.7 a day.
- A MEDIUM runtime adds 0.415 credits an hour, 10.0 a day.
- The merge warehouse is the surprise. At the connector's default every-minute merge it used 0.912
  credits an hour, which makes the total about 34.5 credits a day. That is more than the always-on
  JDBC warehouse you were trying to get rid of.
- Set `Merge Task Schedule CRON` to every 15 minutes and the merge drops to 0.103 credits an hour.
  The total is then about 15.1 credits a day. On Azure the same setup measured about 14.5: Openflow
  compute is one rate on every cloud, and only the merge warehouse follows the cloud's rate.

That's the price of "nothing exposed, database as the buffer". It's real, and it's a fair trade for
some security teams. It is not a cost saving compared with Path 1.

## Path 3: Ignition 8.3, Kafka and connector v4

If you're on 8.3 and want a Kafka backbone other systems will share, this is the streaming design.
Event Streams publish tag changes to a topic, and the Snowflake Connector for Kafka v4 lands them
through the same Snowpipe Streaming service as Path 1.

**Ignition as code.** Ignition 8.3 keeps gateway config as JSON on disk, so the tags, the Kafka
connection and the Event Stream are all files in the repo. Three things that weren't obvious:

- Seed the `external` resource collection, not `core`. Writing `core` before first boot faults the
  gateway.
- Atomic tags go in a `tags.json` inside their folder. Other layouts load as empty folders.
- The Kafka connection's password field takes a secret object. Point it at Ignition's file secret
  provider (8.3.5+) with a `Referenced` secret. If you put the password in `sasl.jaas.config`, it
  sits in plain text in the gateway config and comes back from the resources API.

**v4 fails at task start, not at deploy.** These three only show up after the connector is created:

1. It embeds a native Rust SDK, so the Alpine (musl) `apache/kafka` image dies with
   `libgcc_s.so.1`. Use a glibc base.
2. It needs the Bouncy Castle FIPS jars next to the connector jar.
3. A fresh install must set `snowflake.streaming.validate.compatibility.with.classic=false`.

It also remembers Kafka offsets in Snowflake. Recreate a topic without resetting the Snowflake side,
and new records are skipped as already ingested.

**On Amazon MSK,** Ignition's Kafka client speaks SCRAM but not `AWS_MSK_IAM`, while MSK Connect uses
IAM. So MSK Serverless (IAM-only) is out, and the cluster runs SCRAM and IAM side by side. Choose Kafka
Connect 3.7.x on MSK Connect; connector 4.2.0 needs Java 17, and you can't change it later.

**What I measured:**

- On a laptop (Ignition 8.3.9, Kafka 3.9.1): about 5 seconds behind the tag change. With Kafka
  Connect stopped for two minutes, offsets were contiguous afterwards, with no gaps or duplicates.
- On AWS (Ignition on EC2, MSK, MSK Connect): 3,047 rows with contiguous offsets, about 8 seconds
  behind. That included 25 minutes of events published before the connector even existed. The topic
  is the buffer.
- On Azure, with **Azure Event Hubs** as the Kafka cluster (its Kafka endpoint, SASL_SSL/PLAIN with a
  connection string) and Kafka Connect in standalone mode: 6,871 rows, offsets contiguous, 8 to 14
  seconds behind, through a two-minute Connect outage. Snowflake's docs list Kafka, Confluent and MSK
  for v4, not Event Hubs, so call this tested, not supported.

**Turn on table error logging.** v4 warns at start that without `ERROR_LOGGING` on the target table,
invalid records "will be silently dropped". With `ALTER TABLE ... SET ERROR_LOGGING = TRUE`, a record
with a string in a FLOAT column landed in `ERROR_TABLE(...)`. But with `errors.tolerance=none` the task
then stopped (`Channel error count threshold exceeded`) and stayed stopped. With `errors.tolerance=all`
the bad row went to the error table and good rows kept flowing. Use both. The 8.1 REST path behaves the
same way: the request returns HTTP 200, the good rows land and the bad one goes to the error table, so the
gateway log never shows it. Watch the error table.

## What it costs, side by side

Measured on my own demo accounts, Snowflake compute only, X-Small warehouses:

| Path | Credits a day | What drives it |
|---|---|---|
| Continuous single-row JDBC (the starting point) | 24 standard, 30 to 32 Gen2 | The warehouse never suspends |
| 0. Batched JDBC, every 15 minutes | 2.2 | Four warehouse resumes an hour |
| 1. Snowpipe Streaming REST | 2.8 | The 15-minute Dynamic Table refresh; ingest is per GB |
| 2. Openflow + DCP, 15-minute merge | 14.5 | Control pool and runtime run all the time |
| 2. Openflow + DCP, default merge | 34.2 | As above, plus a merge warehouse that never suspends |
| 3. Kafka + connector v4 | about 2.8, plus Kafka hosting | Same Snowpipe Streaming service as Path 1 |

Snowpipe Streaming is billed at 0.0037 credits per uncompressed GB (October 2026 Consumption Table,
https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-cost).
Per-hour and per-month figures, with where each was measured, are in the repo's README.

Three takeaways:

- **Ingestion is almost free.** What you pay for is whatever runs after it, so set the Dynamic Table lag
  and the Openflow merge schedule to what the business needs, not to "as fast as possible".
- **The ideal default is Path 1.** Freshest data at close to the lowest cost, on Ignition 8.1 with no upgrade, and the cost does not grow with tag count.
- **Then pick by constraint.** Openflow if the gateways may never connect out, Kafka when there is a
  shared event bus, and batching only as a stopgap.

The decision guide, with the questions that settle it, is here:
https://github.com/sfc-gh-jkang/ignition-msk-snowflake/blob/main/docs/choosing-a-path.md

## Try it

```bash
git clone https://github.com/sfc-gh-jkang/ignition-msk-snowflake && cd ignition-msk-snowflake
cp local/.env.example local/.env       # passwords and your Snowflake account
make snowflake-setup rest-setup rest-up              # Path 1 on an Ignition 8.1.42 trial
sleep 90 && make rest-verify                         # the gateway boots for about a minute before its first write
```

Every path runs in Ignition's resettable 2-hour trial, so you don't need a license to try them. The
repo has the Openflow plant stack, the MSK CloudFormation template, the verification SQL behind every
number above, and a troubleshooting table with every error I hit.

*All measurements are from my own demo accounts, October 1 to 5, 2026. Check current pricing in
Snowflake's Consumption Table before planning on these numbers.*

*Views are my own. I work at Snowflake; this post describes tests I ran on my own demo accounts.*
