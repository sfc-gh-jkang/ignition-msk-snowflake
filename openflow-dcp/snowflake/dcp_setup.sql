-- Option B, Snowflake side: Openflow (Snowflake deployment) + Data Connectivity Proxy (DCP)
-- reaching a plant SQL Server that only the plant network can see.
--
-- Run each section in order as a role with the privileges noted. Names below are the demo's;
-- replace them with your own. Statements were executed on 2026-10-02 against an AWS us-east-1
-- account; see docs/openflow-dcp.md for what was verified.
--
-- Docs:
--   Openflow + DCP:  https://docs.snowflake.com/en/user-guide/data-integration/openflow/setup-openflow-spcs-dcp
--   DCP setup:       https://docs.snowflake.com/en/user-guide/data-connectivity-proxy-setup
--   SQL Server CDC:  https://docs.snowflake.com/en/user-guide/data-integration/openflow/connectors/sql-server/setup

-------------------------------------------------------------------------------
-- 1. One-time account prerequisites (ACCOUNTADMIN)
-------------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS OPENFLOW_ADMIN COMMENT = 'Owns Openflow deployments, runtimes and DCP objects';
GRANT CREATE OPENFLOW DEPLOYMENT ON ACCOUNT TO ROLE OPENFLOW_ADMIN;
GRANT CREATE DATA CONNECTIVITY PROXY ON ACCOUNT TO ROLE OPENFLOW_ADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE OPENFLOW_ADMIN;
GRANT CREATE ROLE ON ACCOUNT TO ROLE OPENFLOW_ADMIN;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE OPENFLOW_ADMIN;
GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE OPENFLOW_ADMIN;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE OPENFLOW_ADMIN;   -- section 2 creates the merge warehouse
GRANT ROLE OPENFLOW_ADMIN TO ROLE SYSADMIN;

-- DCP needs account-level TLS certificates. Issuance is asynchronous: wait at least
-- 30 minutes after the first call before starting the agent, or it loops on
-- "CP connect failed ... transport error". Calling it again is a no-op.
SELECT SYSTEM$ISSUE_PER_ACCOUNT_CERTIFICATES();

-------------------------------------------------------------------------------
-- 2. Openflow deployment and destination (OPENFLOW_ADMIN)
-------------------------------------------------------------------------------
USE ROLE OPENFLOW_ADMIN;
CREATE OPENFLOW DEPLOYMENT IGNITION_DCP_DEPLOYMENT
  COMMENT = 'Ignition 8.1 -> SQL Server -> Openflow via DCP';
SELECT SYSTEM$WAIT_FOR_STABLE_OPENFLOW_DEPLOYMENTS(600, 'IGNITION_DCP_DEPLOYMENT');

CREATE DATABASE IF NOT EXISTS IGNITION_OPENFLOW;
CREATE SCHEMA IF NOT EXISTS IGNITION_OPENFLOW.INFRA;          -- runtime, network rule
CREATE WAREHOUSE IF NOT EXISTS SFE_IGNITION_OPENFLOW_WH
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Used by the SQL Server connector for MERGE into destination tables';

-- Runtime execute-as role: what the connector runs as inside Snowflake.
CREATE ROLE IF NOT EXISTS OPENFLOW_IGNITION_RT_EXECUTE_AS_RL;
GRANT USAGE ON DATABASE IGNITION_OPENFLOW TO ROLE OPENFLOW_IGNITION_RT_EXECUTE_AS_RL;
GRANT CREATE SCHEMA ON DATABASE IGNITION_OPENFLOW TO ROLE OPENFLOW_IGNITION_RT_EXECUTE_AS_RL;
GRANT USAGE ON WAREHOUSE SFE_IGNITION_OPENFLOW_WH TO ROLE OPENFLOW_IGNITION_RT_EXECUTE_AS_RL;
GRANT ROLE OPENFLOW_IGNITION_RT_EXECUTE_AS_RL TO ROLE OPENFLOW_ADMIN;

-------------------------------------------------------------------------------
-- 3. Data Connectivity Proxy (OPENFLOW_ADMIN)
-------------------------------------------------------------------------------
-- The network rule names the plant host exactly as the AGENT resolves it. It must be a
-- hostname, not an IP. MODE = DATA_CONNECTIVITY_PROXY_EGRESS is what routes the traffic
-- through the tunnel; MODE = EGRESS would send it to the public internet instead, and MODE
-- cannot be altered later.
CREATE NETWORK RULE IGNITION_OPENFLOW.INFRA.PLANT_SQLSERVER_DCP_RULE
  MODE = DATA_CONNECTIVITY_PROXY_EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('sqlserver.plant.local:1433');

CREATE EXTERNAL ACCESS INTEGRATION PLANT_SQLSERVER_DCP_EAI
  ALLOWED_NETWORK_RULES = (IGNITION_OPENFLOW.INFRA.PLANT_SQLSERVER_DCP_RULE)
  ENABLED = TRUE;

-- The EAI must be linked to the proxy object AND attached to the runtime (section 4).
CREATE DATA CONNECTIVITY PROXY IGNITION_PLANT_DCP
  EXTERNAL_ACCESS_INTEGRATIONS = (PLANT_SQLSERVER_DCP_EAI)
  ENABLED = TRUE;
GRANT USAGE ON INTEGRATION PLANT_SQLSERVER_DCP_EAI TO ROLE OPENFLOW_IGNITION_RT_EXECUTE_AS_RL;

-- One-time JWT the plant agent exchanges for mTLS certificates (validity in days).
-- It is NOT generated here: run `make dcp-token`, which writes it straight to .secrets/dcp-bootstrap-token
-- (0600) without printing it. Selecting it in this file would print it to the terminal.
--   SELECT SYSTEM$GENERATE_DATA_CONNECTIVITY_PROXY_BOOTSTRAP_TOKEN('IGNITION_PLANT_DCP', 7);

-------------------------------------------------------------------------------
-- 4. Runtime (OPENFLOW_ADMIN)
-------------------------------------------------------------------------------
USE SCHEMA IGNITION_OPENFLOW.INFRA;
CREATE OPENFLOW RUNTIME IGNITION_SQLSERVER_MED_RT
  IN DEPLOYMENT IGNITION_DCP_DEPLOYMENT
  MIN_NODES = 1
  MAX_NODES = 1
  NODE_TYPE = MEDIUM            -- the SQL Server connector requires Medium or larger; size is fixed at creation
  EXECUTE_AS_ROLE = 'OPENFLOW_IGNITION_RT_EXECUTE_AS_RL'
  EXTERNAL_ACCESS_INTEGRATIONS = (PLANT_SQLSERVER_DCP_EAI);
SELECT SYSTEM$WAIT_FOR_STABLE_OPENFLOW_RUNTIMES(900, 'IGNITION_OPENFLOW.INFRA.IGNITION_SQLSERVER_MED_RT');

-------------------------------------------------------------------------------
-- 5. Verify before configuring the connector
-------------------------------------------------------------------------------
DESCRIBE DATA CONNECTIVITY PROXY IGNITION_PLANT_DCP;   -- agent_health = HEALTHY, enabled = true
SELECT SYSTEM$VERIFY_EAI_NETWORK_ACCESS('PLANT_SQLSERVER_DCP_EAI', 'sqlserver.plant.local', 1433);
DESCRIBE OPENFLOW RUNTIME IGNITION_OPENFLOW.INFRA.IGNITION_SQLSERVER_MED_RT;  -- status ACTIVE, EAI listed
