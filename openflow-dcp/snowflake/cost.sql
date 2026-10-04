-- Openflow credits by compute pool and hour, plus the connector's merge warehouse.
-- ACCOUNT_USAGE lags by up to a few hours; use a window that has fully landed.
-- Openflow compute shows as SERVICE_TYPE = 'OPENFLOW_COMPUTE_SNOWFLAKE', NAME = compute pool:
-- https://docs.snowflake.com/en/user-guide/data-integration/openflow/cost-spcs
SET START_TS = '2026-10-02 15:45:00 +00:00'::TIMESTAMP_TZ;   -- deployment created; explicit UTC offset

SELECT NAME AS COMPUTE_POOL,
       DATE_TRUNC('hour', START_TIME) AS HOUR,
       SUM(CREDITS_USED) AS CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
WHERE SERVICE_TYPE = 'OPENFLOW_COMPUTE_SNOWFLAKE'
  AND START_TIME >= $START_TS
GROUP BY ALL
ORDER BY HOUR, COMPUTE_POOL;

SELECT DATE_TRUNC('hour', START_TIME) AS HOUR, SUM(CREDITS_USED) AS MERGE_WH_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME = 'SFE_IGNITION_OPENFLOW_WH'
  AND START_TIME >= $START_TS
GROUP BY ALL
ORDER BY HOUR;
