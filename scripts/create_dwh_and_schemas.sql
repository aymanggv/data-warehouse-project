
use master;
go

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

create database DataWarehouse;

use DataWarehouse;
go 

create schema bronze;
go --go is a separator which tells sql to run first command completely before going to second command
create schema silver;
go
create schema gold;
go