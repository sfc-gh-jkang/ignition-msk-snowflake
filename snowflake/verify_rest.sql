-- Health check for the Ignition 8.1 REST mode. Same -D variables as setup.sql.

-- Rows landed, duplicates from at-least-once delivery, and freshness.
SELECT
  COUNT(*)                                                     AS ROWS_LANDED,
  COUNT(DISTINCT EVENT_ID)                                     AS DISTINCT_EVENTS,
  COUNT(*) - COUNT(DISTINCT EVENT_ID)                          AS DUPLICATE_ROWS,
  COUNT(DISTINCT TAG_PATH)                                     AS TAGS,
  DATEDIFF('second', TO_TIMESTAMP_LTZ(MAX(EVENT_TS_MS), 3), CURRENT_TIMESTAMP()) AS SECONDS_BEHIND
FROM <% db %>.<% schema %>.SCADA_TAG_EVENTS_REST;

-- Largest gap between consecutive readings per tag. The simulated tags change every second,
-- so a gap close to an outage's length means rows were lost rather than buffered.
SELECT TAG_PATH, MAX(GAP_MS) / 1000 AS MAX_GAP_SECONDS
FROM (
  SELECT TAG_PATH, EVENT_TS_MS - LAG(EVENT_TS_MS) OVER (PARTITION BY TAG_PATH ORDER BY EVENT_TS_MS) AS GAP_MS
  FROM <% db %>.<% schema %>.SCADA_TAG_EVENTS_REST
)
GROUP BY TAG_PATH
ORDER BY TAG_PATH;
