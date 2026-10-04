#!/usr/bin/env python3
"""Rough monthly cost comparison for landing SCADA tag data in Snowflake.

Compares three ways of loading the same stream:
  1. Row-by-row inserts over JDBC that keep a warehouse running all day.
  2. The same inserts batched every N minutes, with a short auto-suspend.
  3. Snowpipe Streaming via the Snowflake Connector for Kafka v4 (no warehouse for ingest),
     plus a Dynamic Table refresh for the curated layer.
  4. Snowpipe Streaming REST API posted directly from the Ignition gateway (no Kafka), plus the
     same Dynamic Table refresh. Same per-GB ingest rate as option 3.
  5. Openflow on a Snowflake deployment (optional, --openflow-*): Openflow's control pool runs
     continuously, plus the runtime's compute pool nodes. Pass node credits/hour from
     Table 1(d) of the Service Consumption Table; see
     https://docs.snowflake.com/en/user-guide/data-integration/openflow/cost-spcs

It is an estimate, not a quote. Check the inputs against your own account:
  - Snowpipe Streaming high-performance is billed per uncompressed GB ingested:
    https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-cost
    Look up the current credits-per-GB rate in the Snowflake Service Consumption Table and
    pass it with --streaming-credits-per-gb.
  - Warehouse credits per hour by size and generation are in the same table.
  - Kafka infrastructure (MSK, Confluent, Event Hubs, a VM for Kafka Connect) is billed by that
    provider, not by Snowflake, and is passed in with --kafka-monthly-usd.

Example:
  scripts/cost_estimate.py --tags 300 --updates-per-sec 1 --row-bytes 190 \
      --credit-price 3.00 --streaming-credits-per-gb 0.0037 --kafka-monthly-usd 150
"""
import argparse

HOURS_PER_MONTH = 730
MIN_BILLED_SECONDS = 60  # each warehouse resume bills at least 60 seconds


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--tags", type=int, required=True, help="number of tags being streamed")
    p.add_argument("--updates-per-sec", type=float, required=True, help="average change events per tag per second")
    p.add_argument("--row-bytes", type=float, default=190.0, help="average uncompressed JSON bytes per event")
    p.add_argument("--credit-price", type=float, required=True, help="your price per credit, USD")
    p.add_argument("--streaming-credits-per-gb", type=float, required=True,
                   help="Snowpipe Streaming high-performance rate from the Service Consumption Table")
    p.add_argument("--wh-credits-per-hour", type=float, default=1.0, help="credits/hour of the ingest warehouse (X-Small Gen1 = 1)")
    p.add_argument("--batch-minutes", type=float, default=15.0, help="write interval for the batched-insert option")
    p.add_argument("--batch-run-seconds", type=float, default=60.0, help="seconds of work per batch before suspend")
    p.add_argument("--auto-suspend-seconds", type=float, default=60.0)
    p.add_argument("--dt-refreshes-per-hour", type=float, default=4.0, help="Dynamic Table refreshes per hour (4 = 15-minute lag)")
    p.add_argument("--dt-wh-credits-per-hour", type=float, default=1.0)
    p.add_argument("--kafka-monthly-usd", type=float, default=0.0, help="Kafka + Kafka Connect infrastructure, from your provider")
    p.add_argument("--events-per-day", type=float, help="override volume with a measured daily event count")
    p.add_argument("--openflow-control-credits-per-hour", type=float,
                   help="credits/hour of the Openflow control pool node (Service Consumption Table 1(d))")
    p.add_argument("--openflow-runtime-credits-per-hour", type=float,
                   help="credits/hour of the runtime compute pool node(s) while the runtime runs")
    a = p.parse_args()

    events_month = (a.events_per_day * HOURS_PER_MONTH / 24 if a.events_per_day
                    else a.tags * a.updates_per_sec * 3600 * HOURS_PER_MONTH)
    gb_month = events_month * a.row_bytes / 1e9

    always_on = a.wh_credits_per_hour * HOURS_PER_MONTH

    wakes = 60.0 / a.batch_minutes * HOURS_PER_MONTH
    secs_per_wake = max(a.batch_run_seconds, MIN_BILLED_SECONDS) + a.auto_suspend_seconds
    batched = min(always_on, wakes * secs_per_wake / 3600 * a.wh_credits_per_hour)

    ingest = gb_month * a.streaming_credits_per_gb
    dt_secs = max(a.batch_run_seconds, MIN_BILLED_SECONDS) + a.auto_suspend_seconds
    dt = min(a.dt_wh_credits_per_hour * HOURS_PER_MONTH,
             a.dt_refreshes_per_hour * HOURS_PER_MONTH * dt_secs / 3600 * a.dt_wh_credits_per_hour)

    rows = [
        ("1. Continuous JDBC inserts (warehouse always on)", always_on, 0.0),
        (f"2. Batched inserts every {a.batch_minutes:g} min", batched, 0.0),
        ("3. Kafka v4 -> Snowpipe Streaming + Dynamic Table", ingest + dt, a.kafka_monthly_usd),
        ("4. Gateway -> Snowpipe Streaming REST + Dynamic Table", ingest + dt, 0.0),
    ]
    if a.openflow_control_credits_per_hour is not None and a.openflow_runtime_credits_per_hour is not None:
        openflow = (a.openflow_control_credits_per_hour + a.openflow_runtime_credits_per_hour) * HOURS_PER_MONTH
        rows.append(("5. Openflow (Snowflake deployment) + Dynamic Table", openflow + ingest + dt, 0.0))
    print(f"Volume: {events_month:,.0f} events/month, {gb_month:,.2f} GB uncompressed/month\n")
    print(f"{'Option':<52}{'Credits/mo':>12}{'Snowflake $':>13}{'Other $':>10}{'Total $/mo':>12}")
    for name, credits, other in rows:
        sf = credits * a.credit_price
        print(f"{name:<52}{credits:>12,.1f}{sf:>13,.0f}{other:>10,.0f}{sf + other:>12,.0f}")
    print(f"\n   of which Snowpipe Streaming ingest: {ingest:,.3f} credits, Dynamic Table refresh: {dt:,.1f} credits")


if __name__ == "__main__":
    main()
