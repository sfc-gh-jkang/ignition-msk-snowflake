-- Snowflake side of the Ignition -> Kafka -> Snowflake demo.
--
-- Run with the Snowflake CLI, passing variables with -D:
--   snow sql -f snowflake/setup.sql \
--     -D "db=SNOWFLAKE_EXAMPLE" -D "schema=IGNITION_KAFKA_DEMO" \
--     -D "wh=SFE_IGNITION_KAFKA_DEMO_WH" \
--     -D "role=IGNITION_KAFKA_RL" -D "user=IGNITION_KAFKA_SVC" \
--     -D "rsa_public_key=$(scripts/pubkey_body.sh)" \
--     -D "allowed_ip=203.0.113.10/32"
--
-- Requires a role that can create roles, users and network policies (for example ACCOUNTADMIN).
-- The connector authenticates as a TYPE = SERVICE user with a key pair:
--   https://docs.snowflake.com/en/user-guide/key-pair-auth
-- Privileges follow the Snowpipe Streaming access-control guidance:
--   https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-access-control

CREATE DATABASE IF NOT EXISTS <% db %>;
CREATE SCHEMA IF NOT EXISTS <% db %>.<% schema %>;

CREATE WAREHOUSE IF NOT EXISTS <% wh %>
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Dynamic Table refresh for the Ignition Kafka demo. Ingestion itself uses no warehouse.';

-- Landing table. Pre-created rather than auto-created so that types are explicit:
-- VALUE is FLOAT so decimals are never inferred as an integer type.
CREATE TABLE IF NOT EXISTS <% db %>.<% schema %>.SCADA_TAG_EVENTS (
  SITE            VARCHAR,
  LINE            VARCHAR,
  TAG_PATH        VARCHAR,
  VALUE           FLOAT,
  QUALITY         VARCHAR,
  EVENT_TS_MS     NUMBER(38,0),
  RECORD_METADATA VARIANT
)
CLUSTER BY (LINE, TO_DATE(TO_TIMESTAMP_LTZ(EVENT_TS_MS, 3)))
COMMENT = 'Tag change events from Ignition, streamed by the Snowflake Connector for Kafka v4.';

-- Without error logging, rows the server rejects are dropped silently; with it they land in
-- ERROR_TABLE(<table>) for review. https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-error-tables
ALTER TABLE <% db %>.<% schema %>.SCADA_TAG_EVENTS SET ERROR_LOGGING = TRUE;

-- One row per tag per minute. The warehouse runs only for each refresh.
-- https://docs.snowflake.com/en/user-guide/dynamic-tables/overview
CREATE DYNAMIC TABLE IF NOT EXISTS <% db %>.<% schema %>.SCADA_TAG_MINUTE
  TARGET_LAG = '15 minutes'
  WAREHOUSE = <% wh %>
AS
SELECT
  SITE,
  LINE,
  TAG_PATH,
  DATE_TRUNC('minute', TO_TIMESTAMP_LTZ(EVENT_TS_MS, 3)) AS MINUTE_TS,
  AVG(VALUE)  AS AVG_VALUE,
  MIN(VALUE)  AS MIN_VALUE,
  MAX(VALUE)  AS MAX_VALUE,
  COUNT(*)    AS READINGS
FROM <% db %>.<% schema %>.SCADA_TAG_EVENTS
WHERE QUALITY = 'Good'
GROUP BY SITE, LINE, TAG_PATH, MINUTE_TS;

-- Least-privilege role for the connector.
CREATE ROLE IF NOT EXISTS <% role %>;
GRANT USAGE ON DATABASE <% db %> TO ROLE <% role %>;
GRANT USAGE, CREATE PIPE ON SCHEMA <% db %>.<% schema %> TO ROLE <% role %>;
GRANT USAGE ON WAREHOUSE <% wh %> TO ROLE <% role %>;
GRANT INSERT, SELECT, UPDATE, EVOLVE SCHEMA ON TABLE <% db %>.<% schema %>.SCADA_TAG_EVENTS TO ROLE <% role %>;

CREATE USER IF NOT EXISTS <% user %>
  TYPE = SERVICE
  DEFAULT_ROLE = <% role %>
  DEFAULT_WAREHOUSE = <% wh %>
  COMMENT = 'Snowflake Connector for Kafka v4 (Ignition demo)';
ALTER USER <% user %> SET RSA_PUBLIC_KEY = '<% rsa_public_key %>';
GRANT ROLE <% role %> TO USER <% user %>;

-- If the account has a network policy, the connector's egress IP must be allowed for this
-- user. v4 also calls a separate ingest endpoint that evaluates the policy independently.
-- A user-level policy overrides the account policy for this user only.
-- https://docs.snowflake.com/en/user-guide/network-policies
-- Named after the user, so a second deployment in the same account (another plant, or the AWS and
-- local modes side by side) gets its own policy instead of silently reusing the first one's IP.
CREATE NETWORK RULE IF NOT EXISTS <% db %>.<% schema %>.IGNITION_KAFKA_EGRESS
  MODE = INGRESS
  TYPE = IPV4
  VALUE_LIST = ('<% allowed_ip %>');
-- IF NOT EXISTS would keep an old IP on a re-run; set it every time.
ALTER NETWORK RULE <% db %>.<% schema %>.IGNITION_KAFKA_EGRESS SET VALUE_LIST = ('<% allowed_ip %>');
CREATE NETWORK POLICY IF NOT EXISTS <% user %>_POLICY
  ALLOWED_NETWORK_RULE_LIST = ('<% db %>.<% schema %>.IGNITION_KAFKA_EGRESS');
ALTER USER <% user %> SET NETWORK_POLICY = <% user %>_POLICY;
