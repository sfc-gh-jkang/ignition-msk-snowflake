-- Quick health check for the pipeline. Same -D variables as setup.sql.

-- Rows landed, how many, and how fresh.
SELECT
  COUNT(*)                                                     AS ROWS_LANDED,
  COUNT(DISTINCT TAG_PATH)                                     AS TAGS,
  TO_TIMESTAMP_LTZ(MAX(EVENT_TS_MS), 3)                        AS LATEST_EVENT,
  DATEDIFF('second', TO_TIMESTAMP_LTZ(MAX(EVENT_TS_MS), 3), CURRENT_TIMESTAMP()) AS SECONDS_BEHIND
FROM <% db %>.<% schema %>.SCADA_TAG_EVENTS;

-- Latest reading per tag.
SELECT TAG_PATH, VALUE, QUALITY, TO_TIMESTAMP_LTZ(EVENT_TS_MS, 3) AS EVENT_TS
FROM <% db %>.<% schema %>.SCADA_TAG_EVENTS
QUALIFY ROW_NUMBER() OVER (PARTITION BY TAG_PATH ORDER BY EVENT_TS_MS DESC) = 1
ORDER BY TAG_PATH;

-- Gap and duplicate check on Kafka offsets: for each partition, contiguous offsets mean no loss,
-- and COUNT = COUNT(DISTINCT) means no duplicates.
SELECT
  RECORD_METADATA:partition::NUMBER                          AS KAFKA_PARTITION,
  COUNT(*)                                                   AS ROWS_LANDED,
  COUNT(DISTINCT RECORD_METADATA:offset::NUMBER)             AS DISTINCT_OFFSETS,
  MIN(RECORD_METADATA:offset::NUMBER)                        AS MIN_OFFSET,
  MAX(RECORD_METADATA:offset::NUMBER)                        AS MAX_OFFSET,
  MAX(RECORD_METADATA:offset::NUMBER) - MIN(RECORD_METADATA:offset::NUMBER) + 1 - COUNT(DISTINCT RECORD_METADATA:offset::NUMBER) AS MISSING_OFFSETS
FROM <% db %>.<% schema %>.SCADA_TAG_EVENTS
GROUP BY KAFKA_PARTITION
ORDER BY KAFKA_PARTITION;
