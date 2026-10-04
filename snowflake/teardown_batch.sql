-- Drops what setup_batch.sql created (path 0 test tables and warehouses).
-- Run before teardown.sql: it needs the schema to still exist.
DROP TABLE IF EXISTS <% db %>.<% schema %>.LINE1_BATCH_SF;
DROP TABLE IF EXISTS <% db %>.<% schema %>.LINE1_BATCH_MULTIROW;
DROP TABLE IF EXISTS <% db %>.<% schema %>.LINE1_TG_BATCH;
DROP WAREHOUSE IF EXISTS SFE_IGNITION_BATCH_SF_WH;
DROP WAREHOUSE IF EXISTS SFE_IGNITION_BATCH_MULTIROW_WH;
DROP WAREHOUSE IF EXISTS SFE_IGNITION_BATCH_GROUP_WH;
