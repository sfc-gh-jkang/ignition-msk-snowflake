-- Removes everything snowflake/setup.sql created. Same -D variables as setup.sql.
ALTER USER IF EXISTS <% user %> UNSET NETWORK_POLICY;
DROP USER IF EXISTS <% user %>;
DROP NETWORK POLICY IF EXISTS <% user %>_POLICY;
DROP SCHEMA IF EXISTS <% db %>.<% schema %> CASCADE;
DROP ROLE IF EXISTS <% role %>;
DROP WAREHOUSE IF EXISTS <% wh %>;
