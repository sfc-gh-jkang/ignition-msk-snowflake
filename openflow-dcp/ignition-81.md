# Ignition 8.1 side of Option B

Option B needs nothing new on the Ignition gateway: no upgrade to 8.3 and no new modules. The
only change is **where** the gateway writes. Today it writes to Snowflake over JDBC, which keeps a
warehouse running. In Option B it writes to a SQL Server inside the plant, and Openflow replicates
that database to Snowflake through the Data Connectivity Proxy (DCP).

```
Ignition 8.1 (unchanged) ──JDBC──► plant SQL Server ◄──── DCP agent ════ outbound 443 ════► Snowflake
                                    (Change Tracking)       (plant host)                    Openflow runtime
                                                                                             → tables
```

## What the plant needs

| Item | Notes |
|---|---|
| SQL Server 2008 or later, reachable from the gateway | An existing instance is fine. Express works for a single line's volume; check its 10 GB per-database cap against retention. |
| A small Linux host with Docker (or Podman/Kubernetes) | Runs the DCP agent: 1 vCPU and 512 MB per the [setup guide](https://docs.snowflake.com/en/user-guide/data-connectivity-proxy-setup). It needs outbound TCP 443 to Snowflake, DNS for Snowflake hostnames, and a route to SQL Server on 1433. |
| No inbound firewall rules | The agent only dials out. Snowflake never connects into the plant. |

## Steps on the gateway (Ignition 8.1.42)

1. **Add the SQL Server connection.** Gateway web page → Config → Databases → Connections →
   Create new Database Connection → *Microsoft SQL Server JDBC Driver* (bundled with 8.1).
   Connect URL `jdbc:sqlserver://<sql-host>:1433;databaseName=IGNITION`.
2. **Repoint the transaction groups.** In the Designer, open each SQL Bridge transaction group
   that currently writes to the Snowflake connection, and change its *Data Source* to the new
   SQL Server connection. Keep the table names (for example `LINE1_TG`). Leave *Automatically create
   table* on, or pre-create the tables (step 3). Each group's auto-created table has an `ndx`
   auto-increment primary key and a `t_stamp` column, which is exactly what the connector needs.
3. **Enable Change Tracking** on the database and on every replicated table, and create a
   read-only login for Openflow. `sqlserver/init.sql` in this folder is the complete script
   for the demo table; for real tables, run the `ALTER TABLE ... ENABLE CHANGE_TRACKING` line per
   table. The connector uses Change Tracking, not SQL Server's CDC feature.
   https://docs.snowflake.com/en/user-guide/data-integration/openflow/connectors/sql-server/setup
4. Once rows land in Snowflake through Openflow, **disable or delete the Snowflake JDBC
   connection** on the gateway. That is the step that stops the always-on warehouse.

## If the data comes from the Tag Historian instead

The Tag Historian writes to partitioned tables (`sqlt_data_<provider>_<yyyy>_<mm>`) that roll over
every month. Each new partition needs Change Tracking enabled before the connector can pick it up,
and the connector's *Included Table Regex* (for example `IGNITION\.dbo\.sqlt_data_.*`) has to match
the new name. Transaction groups write to fixed table names and avoid this, so prefer them for
replication.

## What the demo uses instead

`docker-compose.yml` runs a **simulator** in place of the gateway by default. It writes one row per
second into `IGNITION.dbo.LINE1_TG`, in the same shape an 8.1 transaction group produces. Everything
from SQL Server onward is the real Option B path.

`make dcp-up-gateway` runs a **real Ignition 8.1.42 gateway** instead. Its backup is built by
`ignition81/build_gwbk.py` with a `PlantSQL` database connection (the bundled Microsoft SQL Server
driver, an insert-only `ignition` login) and a 1-second tag script that calls
`system.db.runPrepUpdate` to write one row per second through that connection. That exercises the
gateway's own JDBC pool and driver against the plant database. It is not a SQL Bridge transaction
group: those are Designer resources stored in binary, so on a real gateway you repoint them as in
step 2 above. The table shape is the same.

Two things learned building it:

- A connection seeded into the gateway's internal database with the plain `PASSWORD` column set failed
  to log in (SQL Server: `Password did not match`); the gateway appears to read only the encrypted
  `PASSWORDE` column. The builder passes the password as a JDBC property instead. On a real gateway,
  type it into the connection page.
- Restarting the gateway does not touch rows it already wrote. Readings due while it reboots are
  never produced, so there is a gap of about the restart time (8.8 s here) and no other loss.
