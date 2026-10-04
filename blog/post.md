# Four ways to stream Ignition SCADA data into Snowflake (and stop paying for an idle warehouse)

*Batching, a gateway script, Openflow through an outbound-only tunnel, and Kafka. All four tested
end to end on Ignition 8.1 and 8.3, with outage tests and measured costs. The code is public:*

https://github.com/sfc-gh-jkang/ignition-msk-snowflake

## The problem

A plant's first Snowflake integration with Inductive Automation Ignition is often a database
connection: tag changes become JDBC `INSERT`s. It's quick to set up and it works. The trouble shows up
on the bill. Small inserts every few seconds mean the warehouse never sits idle long enough to
auto-suspend, so an X-Small runs 24 hours a day, about 24 credits a day, to receive a few kilobytes
at a time. Adding lines doesn't change the cost, and almost all of it is compute spent waiting.

The fix is to stop holding a warehouse open for ingestion. Which way to do that depends on two
things: your Ignition version, and what your plant network allows. I built and tested four.

- **Path 0: batch the JDBC writes you already have.** A small gateway script, no new services. Data is
  up to 15 minutes old.
- **Path 1: an Ignition 8.1 gateway script posting to the Snowpipe Streaming REST API.** No new
  modules and no new servers. Data is 5 to 8 seconds old.
- **Path 2: a plant SQL Server, replicated by Openflow through the Data Connectivity Proxy.** Nothing
  in the plant is exposed, and the database buffers through outages. It's also the most expensive.
- **Path 3: Ignition 8.3 Event Streams to Kafka (or Amazon MSK) and the Snowflake Connector for
  Kafka v4.** Seconds-fresh, with a shared event bus. It needs 8.3 and the Kafka module.

All four only send traffic out of the plant, on port 443 to Snowflake, so nothing connects in.

## Path 0: batch the writes you already have

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
The trade-offs are the same as any in-memory buffer: a gateway restart loses up to one interval.

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

One thing that took a while. I seeded the gateway's database connection by setting the internal
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

The repo's `scripts/cost_estimate.py` models a heavier load than most single lines produce: 300 tags
each changing once a second, about 150 GB a month uncompressed at 190 bytes an event. The rates are a standard X-Small at 1
credit an hour, and Snowpipe Streaming at 0.0037 credits per uncompressed GB (October 2026
Consumption Table).

- **Continuous JDBC inserts:** 730 credits a month.
- **Path 0, one multi-row `INSERT` every 15 minutes:** about 97 credits a month modelled, and 0.09
  credits an hour measured on a Gen2 X-Small (Gen2 bills 1.25 credits an hour on Azure).
- **Path 1, gateway to REST, plus a 15-minute Dynamic Table:** about 98 credits a month. The ingest
  is 0.55 credits; the rest is the Dynamic Table refresh.
- **Path 2, Openflow with a 15-minute merge:** about 15 credits a day measured, roughly 450 a month.
- **Path 3, Kafka and v4:** the same ~98 credits in Snowflake as Path 1, plus your Kafka hosting.

Two takeaways:

- **Ingestion is almost free.** What you pay for is whatever runs after it, so set the Dynamic Table
  lag and the Openflow merge schedule to what the business needs, not to "as fast as possible".
- **Pick the path by constraint, not by cost.** Batch with a multi-row insert if minutes are fine. Use the gateway script if
  you need seconds and are on 8.1. Use Openflow if the plant can only talk to a local database. Use
  Kafka when there's a shared event bus.

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

*All measurements are from my own demo accounts on October 1 and 2, 2026. Check current pricing in
Snowflake's Consumption Table before planning on these numbers.*

*Views are my own. I work at Snowflake; this post describes tests I ran on my own demo accounts.*
