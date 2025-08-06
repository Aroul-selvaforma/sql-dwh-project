/*
Create database and Schemas
Author : Aroul Selvaradjou
Company : SELVA FORMA E.I.

WARNING : This script will drop the entire DataWarehouse database if it exists. 
So all data in the database will be permanently deleted. Proceed with caution. Ensure you have proper backups
*/
USE master;
GO
-- drop and recreate the DataWarehouse database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABSE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

  -- create the Datawarehouse database
CREATE DATABASE DataWarehouse;
GO
  
USE DataWarehouse;
GO
/*creating Schemas : it's like foldes or containers. helps to get things organized*/

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
