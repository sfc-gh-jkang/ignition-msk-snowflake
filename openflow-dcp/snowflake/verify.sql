-- Option B health check. Run as OPENFLOW_ADMIN (or the role that owns the destination).
-- The connector names the destination schema <source db>_<source schema> by default.
-- Sets the warehouse itself, because a new admin user usually has no default warehouse.
USE WAREHOUSE SFE_IGNITION_OPENFLOW_WH;

-- Rows landed and how fresh. T_STAMP is written in UTC by the plant (SYSUTCDATETIME()).
SELECT
  COUNT(*)                                                        AS ROWS_LANDED,
  MAX(NDX)                                                        AS MAX_NDX,
  MAX(T_STAMP)                                                    AS LATEST_SOURCE_TS_UTC,
  DATEDIFF('second', MAX(T_STAMP), SYSDATE())                     AS SECONDS_BEHIND
FROM IGNITION_OPENFLOW.IGNITION_DBO.LINE1_TG;

-- Gap check on the source primary key. NDX is an IDENTITY, so a contiguous range means no
-- lost rows (compare MAX_NDX with SELECT MAX(ndx) on the plant SQL Server).
SELECT
  MIN(NDX) AS MIN_NDX,
  MAX(NDX) AS MAX_NDX,
  COUNT(*) AS ROWS_LANDED,
  MAX(NDX) - MIN(NDX) + 1 - COUNT(DISTINCT NDX) AS MISSING_NDX,
  COUNT(*) - COUNT(DISTINCT NDX)                AS DUPLICATE_NDX
FROM IGNITION_OPENFLOW.IGNITION_DBO.LINE1_TG;

-- The tunnel: agent health and the destinations it can reach.
DESCRIBE DATA CONNECTIVITY PROXY IGNITION_PLANT_DCP;
