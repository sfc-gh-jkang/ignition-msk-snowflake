-- Plant-side SQL Server, standing in for the database an Ignition 8.1 gateway writes to.
-- Tables mirror what an Ignition SQL Bridge transaction group creates: an auto-increment
-- primary key (ndx), a t_stamp column, and one column per tag.
-- The Openflow SQL Server connector replicates with Change Tracking, which needs a primary key
-- on every replicated table and CT enabled on the database and on each table.
-- https://docs.snowflake.com/en/user-guide/data-integration/openflow/connectors/sql-server/setup
IF DB_ID('IGNITION') IS NULL CREATE DATABASE IGNITION;
GO
ALTER DATABASE IGNITION SET ALLOW_SNAPSHOT_ISOLATION ON;
GO
IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID('IGNITION'))
  ALTER DATABASE IGNITION SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
GO
USE IGNITION;
GO
IF OBJECT_ID('dbo.LINE1_TG') IS NULL
BEGIN
  CREATE TABLE dbo.LINE1_TG (
    ndx         INT IDENTITY(1,1) PRIMARY KEY,
    t_stamp     DATETIME2(3) NOT NULL,
    temperature FLOAT NULL,
    speed       FLOAT NULL,
    running     BIT NULL
  );
  ALTER TABLE dbo.LINE1_TG ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
END
GO
-- Read-only login the Openflow connector uses. CT needs SELECT plus VIEW CHANGE TRACKING.
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = 'openflow')
  CREATE LOGIN openflow WITH PASSWORD = '$(OPENFLOW_SQL_PASSWORD)', CHECK_POLICY = ON;
GO
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'openflow')
  CREATE USER openflow FOR LOGIN openflow;
GO
GRANT SELECT ON SCHEMA::dbo TO openflow;
GRANT VIEW CHANGE TRACKING ON SCHEMA::dbo TO openflow;
GRANT VIEW DEFINITION ON SCHEMA::dbo TO openflow;
GO
-- Login the Ignition gateway's database connection uses (gateway profile). Insert only.
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = 'ignition')
  CREATE LOGIN ignition WITH PASSWORD = '$(IGNITION_SQL_PASSWORD)', CHECK_POLICY = ON;
GO
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'ignition')
  CREATE USER ignition FOR LOGIN ignition;
GO
GRANT INSERT, SELECT ON dbo.LINE1_TG TO ignition;
GO
