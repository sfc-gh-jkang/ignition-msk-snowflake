-- Option B teardown. Order matters, and runtimes and deployments cannot be dropped while they are
-- ACTIVE or SUSPENDED: TERMINATE them first (irreversible), then DROP the record.
-- https://docs.snowflake.com/en/sql-reference/sql/alter-openflow-runtime
-- https://docs.snowflake.com/en/sql-reference/sql/drop-openflow-deployment
-- Stop the flow first in the Openflow UI or with nipyapi (`ci stop_flow`).
-- TERMINATE is asynchronous: wait for SHOW ... to report TERMINATED before each DROP below.
-- Because of that wait, do not run this file with `snow sql -f` (the DROP fails with 513216);
-- `make dcp-teardown` (scripts/dcp_teardown.sh) runs these same statements and waits in between.
USE ROLE OPENFLOW_ADMIN;

-- 1. Runtimes. CASCADE also terminates any connectors still in the runtime.
ALTER OPENFLOW RUNTIME IF EXISTS IGNITION_OPENFLOW.INFRA.IGNITION_SQLSERVER_MED_RT TERMINATE CASCADE;
ALTER OPENFLOW RUNTIME IF EXISTS IGNITION_OPENFLOW.INFRA.IGNITION_SQLSERVER_RT TERMINATE CASCADE;  -- first (SMALL) runtime, if created
SHOW OPENFLOW RUNTIMES IN ACCOUNT;    -- wait until both show TERMINATED
DROP OPENFLOW RUNTIME IF EXISTS IGNITION_OPENFLOW.INFRA.IGNITION_SQLSERVER_MED_RT;
DROP OPENFLOW RUNTIME IF EXISTS IGNITION_OPENFLOW.INFRA.IGNITION_SQLSERVER_RT;

-- 2. Deployment. Terminating it is what stops the always-on control pool.
ALTER OPENFLOW DEPLOYMENT IGNITION_DCP_DEPLOYMENT TERMINATE;
SHOW OPENFLOW DEPLOYMENTS;            -- wait until it shows TERMINATED
DROP OPENFLOW DEPLOYMENT IF EXISTS IGNITION_DCP_DEPLOYMENT;

-- 3. DCP pieces and the destination.
DROP DATA CONNECTIVITY PROXY IF EXISTS IGNITION_PLANT_DCP;      -- also invalidates its bootstrap tokens
USE ROLE ACCOUNTADMIN;
DROP INTEGRATION IF EXISTS PLANT_SQLSERVER_DCP_EAI;
DROP DATABASE IF EXISTS IGNITION_OPENFLOW;                      -- destination tables + network rule
DROP WAREHOUSE IF EXISTS SFE_IGNITION_OPENFLOW_WH;
DROP ROLE IF EXISTS OPENFLOW_IGNITION_RT_EXECUTE_AS_RL;

-- Confirm: no deployment, no Openflow compute pools left running.
SHOW OPENFLOW DEPLOYMENTS;
SHOW COMPUTE POOLS LIKE '%OPENFLOW%';
SHOW DATA CONNECTIVITY PROXIES IN ACCOUNT;
