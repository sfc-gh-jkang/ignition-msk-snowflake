-- Path 0 (batched JDBC on Ignition 8.1): one landing table and one warehouse per test mode, so
-- each mode's credits can be read from WAREHOUSE_METERING_HISTORY on its own.
-- Run after setup.sql (it reuses the role and the key-pair service user):
--   snow sql -f snowflake/setup_batch.sql -D "db=..." -D "schema=..." -D "role=..."
--
-- The Snowflake JDBC driver writes with a warehouse, unlike Snowpipe Streaming, so the cost being
-- measured is how long each warehouse stays awake. AUTO_SUSPEND = 60 lets it suspend between flushes.
-- Gen2 standard warehouses: https://docs.snowflake.com/en/user-guide/warehouses-gen2

-- MODE=sf: one INSERT per reading through store-and-forward (how transaction groups write).
-- MODE=multirow: one multi-row INSERT per flush from a gateway script.
CREATE TABLE IF NOT EXISTS <% db %>.<% schema %>.LINE1_BATCH_SF (
  T_STAMP     TIMESTAMP_NTZ,
  TEMPERATURE FLOAT,
  SPEED       FLOAT,
  RUNNING     BOOLEAN
);
CREATE TABLE IF NOT EXISTS <% db %>.<% schema %>.LINE1_BATCH_MULTIROW LIKE <% db %>.<% schema %>.LINE1_BATCH_SF;

-- MODE=group: a real SQL Bridge historical transaction group. It writes its own index column,
-- named <TABLE>_NDX, plus the timestamp and the one configured item.
CREATE TABLE IF NOT EXISTS <% db %>.<% schema %>.LINE1_TG_BATCH (
  LINE1_TG_BATCH_NDX NUMBER(38,0),
  T_STAMP            TIMESTAMP_NTZ,
  SPEED              FLOAT
);

CREATE WAREHOUSE IF NOT EXISTS SFE_IGNITION_BATCH_SF_WH
  WAREHOUSE_SIZE = XSMALL GENERATION = '2'
  AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Path 0 test: store-and-forward, one INSERT per reading';
CREATE WAREHOUSE IF NOT EXISTS SFE_IGNITION_BATCH_MULTIROW_WH
  WAREHOUSE_SIZE = XSMALL GENERATION = '2'
  AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Path 0 test: one multi-row INSERT per flush';
CREATE WAREHOUSE IF NOT EXISTS SFE_IGNITION_BATCH_GROUP_WH
  WAREHOUSE_SIZE = XSMALL GENERATION = '2'
  AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Path 0 test: SQL Bridge transaction group at a 15-minute rate';

GRANT USAGE ON WAREHOUSE SFE_IGNITION_BATCH_SF_WH TO ROLE <% role %>;
GRANT USAGE ON WAREHOUSE SFE_IGNITION_BATCH_MULTIROW_WH TO ROLE <% role %>;
GRANT USAGE ON WAREHOUSE SFE_IGNITION_BATCH_GROUP_WH TO ROLE <% role %>;
GRANT SELECT, INSERT ON TABLE <% db %>.<% schema %>.LINE1_BATCH_SF TO ROLE <% role %>;
GRANT SELECT, INSERT ON TABLE <% db %>.<% schema %>.LINE1_BATCH_MULTIROW TO ROLE <% role %>;
GRANT SELECT, INSERT ON TABLE <% db %>.<% schema %>.LINE1_TG_BATCH TO ROLE <% role %>;
