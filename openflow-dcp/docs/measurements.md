# Option B measurements (2026-10-02)

Raw numbers behind the cost table in [docs/openflow-dcp.md](../../docs/openflow-dcp.md#cost).
Snowflake account in AWS us-east-1; deployment created 15:47 UTC; MEDIUM runtime created 16:14 UTC;
`Merge Task Schedule CRON` changed from the default to `0 0/15 * * * ?` at 17:34 UTC.

## Source vs Snowflake reconcile

Plant side: `SELECT MIN(ndx), MAX(ndx), COUNT(*), COUNT(DISTINCT ndx) FROM IGNITION.dbo.LINE1_TG`.
Snowflake side: `openflow-dcp/snowflake/verify.sql`.

| When (UTC) | Plant SQL Server | Snowflake `IGNITION_OPENFLOW.IGNITION_DBO.LINE1_TG` | Missing / duplicate | Note |
|---|---|---|---|---|
| 19:02 | max `ndx` 9,676 | 1–9,525 contiguous | 0 / 0 | after the 2-minute DCP agent outage; 9,526+ were written after the 19:00 merge cut-off |
| 19:16 | 1–10,367, 10,367 rows, all distinct | 1–10,294 contiguous, 10,294 rows | 0 / 0 | rows after 19:14:46 wait for the 19:30 merge |
| 19:34 | simulator stopped at 19:33; 1–11,254, 11,254 rows, all distinct | 1–11,254 contiguous, 11,254 rows, all distinct | 0 / 0 | **exact match**: one extra merge was triggered (schedule briefly set to every minute) to drain the last interval |

With a 15-minute merge schedule, Snowflake trails the plant by up to one interval plus the merge
time. The check that matters is that the Snowflake range is contiguous from 1 and every `ndx` in it
appears once.

## Credits by hour

`SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY` (`SERVICE_TYPE = 'OPENFLOW_COMPUTE_SNOWFLAKE'`) and
`WAREHOUSE_METERING_HISTORY` (`SFE_IGNITION_OPENFLOW_WH`), via `openflow-dcp/snowflake/cost.sql`.
This deployment's pools are named `OPENFLOW_219937540_*`.

| Hour (UTC) | Control pool | MEDIUM runtime pool | SMALL pool | Merge warehouse | Merge schedule |
|---|---|---|---|---|---|
| 16:00 | 0.1112 | 0.3019 (runtime up from 16:14) | 0.0139 (first runtime, SMALL, suspended) | 0.9117 | default (every minute), includes initial snapshot |
| 17:00 | 0.1113 | **0.4148** | — | 0.6695 | default until 17:34, then 15 min |
| 18:00 | 0.0911 (hour not fully landed) | 0.3396 (hour not fully landed) | — | **0.1027** | 15 min, full hour |
| 19:00 | — | — | — | 0.0286 (partial) | 15 min |

Per-hour rates used in the cost table, from complete hours only:

| Component | Credits/hour | Credits/day | Hour used |
|---|---|---|---|
| Control pool | 0.111 | 2.7 | 16:00 and 17:00 |
| MEDIUM runtime, 1 node | 0.415 | 10.0 | 17:00 |
| Merge warehouse, default schedule | 0.912 | 21.9 | 16:00 (includes the initial snapshot, so an upper bound) |
| Merge warehouse, 15-minute schedule | 0.103 | 2.5 | 18:00 |

The account also bills two pools named `INTERNAL_OPENFLOW_0_CONTROL_POOL` and
`INTERNAL_OPENFLOW_0_SMALL` (about 0.11 credits/hour each). They belong to a different, older
Openflow deployment in the same account (billing since 2026-09-02), not to this demo, and are
excluded above. When attributing Openflow cost, filter on your own deployment's pool prefix.
https://docs.snowflake.com/en/user-guide/data-integration/openflow/cost-spcs
