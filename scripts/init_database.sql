
/*

Script checks if there is an existing db named DatawareHouse, if so it drops it and creates a new one.
Script also created 3 new schemas

*/

USE master;
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DatawareHouse')
	BEGIN
		ALTER DATABASE DatawareHouse
		SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

		DROP DATABASE DatawareHouse
	END;
GO -- Separater

CREATE DATABASE DatawareHouse
GO

USE DatawareHouse

-- Databases -> DatawareHouse -> Security  -> Schema

GO
CREATE SCHEMA bronze; 
GO   
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
