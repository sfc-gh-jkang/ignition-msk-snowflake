-- Ignition 8.1 REST mode: landing table plus a deduplicated Dynamic Table.
-- Run after setup.sql (it reuses the role, service user and network policy):
--   snow sql -f snowflake/setup_rest.sql -D "db=..." -D "schema=..." -D "wh=..." -D "role=..."
--
-- The Snowpipe Streaming REST API's Elastic Channels deliver at least once, so a retried
-- request can land a row twice. Every row carries EVENT_ID (tag path + source timestamp),
-- and SCADA_TAG_EVENTS_REST_DEDUP keeps one row per EVENT_ID.
-- https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api

CREATE TABLE IF NOT EXISTS <% db %>.<% schema %>.SCADA_TAG_EVENTS_REST (
  EVENT_ID     VARCHAR,
  SITE         VARCHAR,
  LINE         VARCHAR,
  TAG_PATH     VARCHAR,
  VALUE        FLOAT,
  QUALITY      VARCHAR,
  EVENT_TS_MS  NUMBER(38,0)
)
COMMENT = 'Tag change events posted by an Ignition 8.1 gateway script via the Snowpipe Streaming REST API.';

-- Without error logging, rows the server rejects are dropped silently; with it they land in
-- ERROR_TABLE(<table>) for review. https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-error-tables
ALTER TABLE <% db %>.<% schema %>.SCADA_TAG_EVENTS_REST SET ERROR_LOGGING = TRUE;

-- On the first append, Snowflake creates the managed pipe SCADA_TAG_EVENTS_REST-STREAMING.
GRANT INSERT, SELECT ON TABLE <% db %>.<% schema %>.SCADA_TAG_EVENTS_REST TO ROLE <% role %>;

CREATE DYNAMIC TABLE IF NOT EXISTS <% db %>.<% schema %>.SCADA_TAG_EVENTS_REST_DEDUP
  TARGET_LAG = '15 minutes'
  WAREHOUSE = <% wh %>
AS
SELECT EVENT_ID, SITE, LINE, TAG_PATH, VALUE, QUALITY, EVENT_TS_MS
FROM <% db %>.<% schema %>.SCADA_TAG_EVENTS_REST
QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY EVENT_TS_MS) = 1;
