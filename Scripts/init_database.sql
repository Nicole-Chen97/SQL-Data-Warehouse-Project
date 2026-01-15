/*
=============================================================
Create Database and Schemas 
=============================================================
Script Purpose:
    This script initializes a PostgreSQL data warehouse environment by:
    - Terminating active connections to the target database (if any).
    - Dropping and recreating the 'datawarehouse' database.
    - Creating three schemas within the database: 'bronze', 'silver', and 'gold'.
    - Optionally verifying that the schemas were created successfully.

WARNING:
    Running this script will DROP the entire 'datawarehouse' database if it exists.
    All data in the database will be permanently deleted. Proceed with caution.

Notes:
    - When using Docker, the script must be executed by a user with sufficient privileges.
    - You cannot drop a database you are currently connected to. Run the DROP/CREATE
      statements from a different database connection (e.g., 'postgres'), then reconnect
      to 'datawarehouse' to create schemas.
=============================================================
*/




-- 1) if someone is connect, kick he/she out except me ---
SELECT 
	pg_terminate_backend(pid)
FROM 
	pg_stat_activity
WHERE 
	pid<> pg_backend_pid()
	AND datname = 'datawarehouse'


-- 2)  Drop & Create database--------
DROP DATABASE IF EXISTS datawarehouse;
CREATE DATABASE datawarehouse;



-- 2) Create Schema------
DO $$
BEGIN
    IF current_database() = 'datawarehouse' THEN
        CREATE SCHEMA IF NOT EXISTS bronze;
        CREATE SCHEMA IF NOT EXISTS silver;
        CREATE SCHEMA IF NOT EXISTS gold;
    ELSE
        RAISE EXCEPTION
            '❌ Wrong database: %', current_database();
    END IF;
END $$;




SELECT schema_name
FROM information_schema.schemata
WHERE schema_name IN ('bronze', 'silver', 'gold');

