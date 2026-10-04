#!/usr/bin/env bash
# Stands in for an Ignition 8.1 SQL Bridge transaction group: one row per second into
# IGNITION.dbo.LINE1_TG. Runs in the SQL Server image, which ships sqlcmd.
set -euo pipefail
sqlcmd=(/opt/mssql-tools18/bin/sqlcmd -S "${MSSQL_HOST:-sqlserver.plant.local}" -U sa -P "$MSSQL_SA_PASSWORD" -C -b)

until "${sqlcmd[@]}" -Q "SELECT 1" >/dev/null 2>&1; do sleep 2; done
"${sqlcmd[@]}" -v OPENFLOW_SQL_PASSWORD="$OPENFLOW_SQL_PASSWORD" \
  -v IGNITION_SQL_PASSWORD="${IGNITION_SQL_PASSWORD:-$OPENFLOW_SQL_PASSWORD}" -i /init/init.sql
# SIMULATE=0 (gateway profile): create the schema and logins only; the real gateway writes the rows.
if [ "${SIMULATE:-1}" = 0 ]; then echo "schema ready; gateway writes the rows"; exit 0; fi
echo "schema ready; writing one row per second"

i=0
while true; do
  i=$((i + 1))
  "${sqlcmd[@]}" -d IGNITION -Q "SET NOCOUNT ON;
    INSERT INTO dbo.LINE1_TG (t_stamp, temperature, speed, running)
    VALUES (SYSUTCDATETIME(),
            180 + 5*SIN($i/60.0) + RAND()*0.5,
            120 + 10*COS($i/45.0),
            CASE WHEN $i % 300 < 270 THEN 1 ELSE 0 END);" >/dev/null
  sleep "${INTERVAL_SECONDS:-1}"
done
