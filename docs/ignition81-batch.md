# Path 0: batching the existing JDBC writes (Ignition 8.1)

The cheapest change is to keep the JDBC connection Ignition already has and stop the warehouse running
all day. This page records what that actually takes on Ignition 8.1, because the obvious setting does
not do it.

## What was tested

Two Ignition 8.1.42 trial gateways in Docker, each writing one row a second through the **Snowflake
JDBC driver** (4.3.3, key-pair auth) to its own X-Small Gen2 warehouse with `AUTO_SUSPEND = 60`, against
a Snowflake account in Azure East US 2, on 2026-10-03. Both were set to reach Snowflake every 15 minutes.
The gateway backups are built by `ignition81/build_batch_gwbk.py` (no binary is committed). That script
is a test harness: restoring a backup replaces a gateway's whole configuration, so never restore its output
onto a production gateway. Add the pieces by hand there.

| | Store-and-forward | Multi-row batch |
|---|---|---|
| How the gateway writes | `system.db.runSFPrepUpdate`, one row per call, through the connection's store-and-forward engine with **Forward Write Time = 15 min**, Write Size 5000. This is the path SQL Bridge transaction groups use | A 1-second tag appends the reading to a list in `system.util.getGlobals()`; a 15-minute timer tag writes the whole list in **one** `INSERT ... VALUES (...), (...), ...` with `system.db.runPrepUpdate`, then removes only the rows it wrote |
| What Snowflake received | one single-row `INSERT` per reading, about 1.2 a second while forwarding, plus `alter session`, `commit` and `rollback` | one `INSERT` of ~900 rows every 15 minutes |
| Rows | 3,128 landed, over 47 different minutes | 2,695 landed, in exactly 3 arrivals (898, 899 and 898 rows, 15 minutes apart) |
| Data age when it lands | up to 18.7 minutes | up to 15.0 minutes |
| Warehouse credits, 02:00–02:53 UTC | **1.01**: the warehouse almost never suspended | **0.090** |

Measured with `INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY` per warehouse and `QUERY_HISTORY_BY_USER`
for the service user. A separate check on an AWS account (one 900-row `INSERT` every 15 minutes, 4
batches, Gen2 X-Small) used 0.10 credits for the 4, the same shape.

**Conclusion.** Store-and-forward's forward settings delay the writes but do not combine them: every
reading is still its own `INSERT`, and they arrive as a burst that keeps the warehouse up for most of
each interval. Batching only saves money when each flush is a **single multi-row `INSERT`**. At one
flush every 15 minutes that measured about 0.09 credits an hour on Azure Gen2, about **2.2 credits a
day**, against 30 a day for an X-Small Gen2 left running.

**Outage.** The multi-row gateway's network was disconnected from 03:06:04 to 03:10:05 UTC, across
its 03:06:52 flush. That flush failed and the rows stayed in the list; the 03:21:52 flush wrote
**1,798 rows** (both intervals) in one `INSERT`. Over all 4,493 rows the largest gap between
consecutive readings is 1.08 s and there are 0 duplicate timestamps. As long as the gateway keeps
running, an outage only delays the data; it does not lose it.

**Re-run on a clean account,** 2026-10-03, against a Snowflake account in AWS us-west-2, from a clean
clone (`snowflake/setup_batch.sql`, then the three gateways side by side for ~47 minutes, 22:24–23:11
UTC):

| | Store-and-forward | Transaction group | Multi-row batch |
|---|---|---|---|
| What Snowflake received | one transaction per 1,000-row forward, each row its own `INSERT` (~0.4 s apiece, so a forward stays open ~7 minutes) | one row per 15-minute execution | one `INSERT` per 15 minutes (897 rows) |
| Rows | 914 | 4 | 2,694 |
| Warehouse credits | **0.632** | 0.091 | **0.046** |

The multi-row gateway's network was cut 22:52:17–22:56 across its 22:54 flush. That flush failed;
the 23:09 flush wrote **1,797 rows** in one `INSERT`. 0 duplicate timestamps, largest gap 1.03 s.
Same ordering and shape as the Azure run above.

## What this means for a real gateway

- Lowering a transaction group's execution rate to 15 minutes is **not** batching. Tested with a real
  SQL Bridge historical group (`MODE=group`): over 45 minutes it wrote **4 rows**, one per execution
  (05:09, 05:24, 05:39, 05:54 PT), each the `Speed` value at that instant. The ~900 one-second values in
  each interval were never recorded.
- To batch, replace the transaction group (or add alongside it) a gateway script: one tag-change or
  timer script appends readings to a list in `system.util.getGlobals()`, and a timer tag or gateway
  timer event writes them as one multi-row `INSERT`. The two scripts in `build_batch_gwbk.py` are the
  whole implementation.
- The list lives in gateway memory. A gateway restart loses up to one interval (plus anything held
  over from failed flushes). A failed insert keeps the rows for the next try; they are removed only
  after `runPrepUpdate` returns, as the outage test above shows. Size the
  interval to the loss you can accept.
- The pool's validation `SELECT 1` and session `alter`s run in Snowflake's cloud services layer, not on
  the warehouse (`CLUSTER_NUMBER` is null for them in query history), so they do not keep it awake.
- One multi-row `INSERT` has a statement size limit. At 900 rows of five columns it is far below it; for
  thousands of tags per flush, split the list into chunks of a few thousand rows.

If that much scripting is on the table, compare it with path 1 ([ignition81-rest.md](ignition81-rest.md)),
which is the same kind of script but posts to Snowpipe Streaming: no warehouse for ingest at all, and
data a few seconds old instead of 15 minutes.

## How the transaction group was created

8.1 stores a transaction group as gzipped XML that normally only the Designer writes. `MODE=group` creates
one at gateway start from a tag script: it builds a `GroupConfig` ("historical") with one tag-reference
item through SQL Bridge's own classes (resolved with `ModuleManager.resolveClass`), serializes it with
`XMLSerializer().initDefaults().serializeXMLAndGZip()`, writes it under
`projects/TxgTest/com.inductiveautomation.sqlbridge/transaction-groups/Line1TG/` and requests a project
scan. SQL Bridge then loads and runs it like any Designer-made group. Two things it needed:

- `GroupConfig(typeKey, name)` and `ItemConfig(typeKey, name)` take the type first.
- Groups write through the connection's store-and-forward engine. Without one (the gateway UI adds it
  when a connection is created; the builder seeds it), the group logs "Error during group execution"
  and writes nothing.

## Reproduce

```bash
# Snowflake: the three test tables and X-Small Gen2 warehouses (AUTO_SUSPEND 60); `make batch-teardown` drops them
make snowflake-setup batch-setup
# Gateway: MODE=sf (store-and-forward), MODE=multirow, or MODE=group GROUP_MINUTES=15 (real transaction group)
set -a; source local/.env; set +a
SNOWFLAKE_ACCOUNT_HOST=<org>-<account>.snowflakecomputing.com SNOWFLAKE_BATCH_WAREHOUSE=<WH> \
MODE=multirow FORWARD_SECONDS=900 BATCH_TABLE=<DB>.<SCHEMA>.<TABLE> \
  python3 ignition81/build_batch_gwbk.py ignition81/build/base.gwbk ignition81/build/batch.gwbk \
    snowflake-jdbc-<version>.jar
docker run -d -p 8084:8088 -e ACCEPT_IGNITION_EULA=Y -e IGNITION_EDITION=standard \
  -e GATEWAY_ADMIN_USERNAME=admin -e GATEWAY_ADMIN_PASSWORD=<pw> \
  -v $PWD/ignition81/build/batch.gwbk:/restore.gwbk:ro \
  -v <path to rsa_key.p8>:/run/secrets/snowflake_key.p8:ro \
  inductiveautomation/ignition:8.1.42 -r /restore.gwbk
```

`ignition81/make_gwbk.sh` produces `base.gwbk`. The Snowflake JDBC driver is on Maven Central
(`net.snowflake:snowflake-jdbc`); check its SHA-1 against the published `.sha1` file.
