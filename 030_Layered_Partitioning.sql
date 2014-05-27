/* Demo script for Layered Partitioning
 *
 * Written by David Peter Hansen 
 * @dphansen | davidpeterhansen.com
 *
 * Layered partitioning
 * 
 * This script is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

USE CreditEDW
GO


/* Helper view - show info about partitons
 * With inspiration from Microsoft SQL Server 2008 
 * Internals p. 438. Expanded to include more info 
 */
CREATE VIEW dbo.ShowPartitions
AS
SELECT OBJECT_NAME(i.object_id) As [object]
	, p.partition_number AS [p#]
	, fg.name AS [filegroup]
	, p.rows
	, au.total_pages AS pages
	, CASE boundary_value_on_right
		WHEN 1 THEN 'less than'
		ELSE 'less than or equal to' END as comparison
	, rv.value
	, CONVERT (VARCHAR(6),
      CONVERT (INT, SUBSTRING (au.first_page, 6, 1) +
         SUBSTRING (au.first_page, 5, 1))) +
   ':' + CONVERT (VARCHAR(20),
      CONVERT (INT, SUBSTRING (au.first_page, 4, 1) +
         SUBSTRING (au.first_page, 3, 1) +
         SUBSTRING (au.first_page, 2, 1) +
         SUBSTRING (au.first_page, 1, 1))) AS first_page
FROM sys.partitions p 
INNER JOIN sys.indexes i
	ON p.object_id = i.object_id 
	AND p.index_id = i.index_id
INNER JOIN sys.system_internals_allocation_units au
	ON p.partition_id = au.container_id
INNER JOIN sys.partition_schemes ps
	ON ps.data_space_id = i.data_space_id
INNER JOIN sys.partition_functions f
	ON f.function_id = ps.function_id
LEFT JOIN sys.partition_range_values rv
	ON f.function_id = rv.function_id
	AND p.partition_number = rv.boundary_id
INNER JOIN sys.destination_data_spaces dds
	ON dds.partition_scheme_id = ps.data_space_id
	AND dds.destination_id = p.partition_number
INNER JOIN sys.filegroups fg
	ON dds.data_space_id = fg.data_space_id
WHERE i.index_id < 2;
GO


/* First we add the first partition 
 * (we are actually adding 2... but who is counting?)
 * SK_Date = 20120101
 * SK_Organisation = 1
 * SK_Scenario = 1
 * SK_SourceSystem = 1
 */


/* One filegroup for all the table partitions in 
 * the base table
 */
ALTER DATABASE CreditEDW
ADD FILEGROUP Credit_FG_O1_S1_SS1
GO


/* Then we add some files to the filegroups */
-- Filegroup Credit_FG_O1_S1_SS1
ALTER DATABASE CreditEDW
ADD FILE (
	NAME = N'Credit_FG_O1_S1_SS1_File1'
	, FILENAME = N'C:\sqldemo\Credit_FG_O1_S1_SS1_File1.ndf'
	, SIZE = 10MB
	, MAXSIZE = 400MB
	, FILEGROWTH = 20MB
)
TO FILEGROUP Credit_FG_O1_S1_SS1

ALTER DATABASE CreditEDW
ADD FILE (
	NAME = N'Credit_FG_O1_S1_SS1_File2'
	, FILENAME = N'C:\sqldemo\Credit_FG_O1_S1_SS1_File2.ndf'
	, SIZE = 10MB
	, MAXSIZE = 400MB
	, FILEGROWTH = 20MB
)
TO FILEGROUP Credit_FG_O1_S1_SS1
GO


/* Now, let's create a partition function for each 
 * partitioned table in the partitioned view
 * NOTE: n boundary points = n+1 partitions
 */
CREATE PARTITION FUNCTION CreditDateRangeFunction_O1_S1_SS1
	(int)
AS
RANGE RIGHT FOR VALUES (
	20120101
)
GO


/* And then we have a partition scheme, 
 * binding the partitions to the filegroups 
 */
CREATE PARTITION SCHEME CreditDateRangeScheme_O1_S1_SS1
AS PARTITION CreditDateRangeFunction_O1_S1_SS1 
ALL TO (Credit_FG_O1_S1_SS1)
GO


/* Create the fact partitioned tables on the 
 * partition scheme 
 */
CREATE TABLE Facts.Credit_O1_S1_SS1 (
	CreditId INT NOT NULL 
	, SK_Date INT NOT NULL
	, SK_SourceSystem INT NOT NULL
	, SK_Scenario INT NOT NULL
	, SK_Organisation INT NOT NULL
--	, SK_...
	, Measure1 MONEY NULL
	, Measure2 MONEY NULL
--	...
	, CONSTRAINT CK_SK_Date_O1_S1_SS1 CHECK 
		(SK_Date>= 20120101 AND SK_Date < 20120201)
	, CONSTRAINT CK_SK_Organisation_O1_S1_SS1 CHECK
		(SK_Organisation = 1)
	, CONSTRAINT CK_SK_Scenario_O1_S1_SS1 CHECK
		(SK_Scenario = 1)
	, CONSTRAINT CK_SK_SourceSystem_O1_S1_SS1 CHECK
		(SK_SourceSystem = 1)
	, CONSTRAINT FK_SK_Date_O1_S1_SS1 
		FOREIGN KEY (SK_Date)
		REFERENCES Dimensions.[Date](SK_Date)
	, CONSTRAINT FK_SK_Organisation_O1_S1_SS1 
		FOREIGN KEY (SK_Organisation)
		REFERENCES Dimensions.Organisation(SK_Organisation)
	, CONSTRAINT FK_SK_Scenario_O1_S1_SS1 
		FOREIGN KEY (SK_Scenario)
		REFERENCES Dimensions.Scenario(SK_Scenario)
	, CONSTRAINT FK_SK_SourceSystem_O1_S1_SS1
		FOREIGN KEY (SK_SourceSystem)
		REFERENCES Dimensions.SourceSystem(SK_SourceSystem)
) ON CreditDateRangeScheme_O1_S1_SS1(SK_Date)
GO

-- Note, that SK_Date must be part of a unique index
CREATE UNIQUE CLUSTERED INDEX UCIX_Credit_O1_S1_SS1
ON Facts.Credit_O1_S1_SS1(CreditId, SK_Date)
WITH FILLFACTOR=100
ON CreditDateRangeScheme_O1_S1_SS1(SK_Date)
GO


/* Set lock escalation to partition level */
ALTER TABLE Facts.Credit_O1_S1_SS1
SET (LOCK_ESCALATION = AUTO)
GO


/* Now let's create the partitioned view */
CREATE VIEW Facts.Credit
AS
SELECT * FROM Facts.Credit_O1_S1_SS1
GO


/* Create staging table to switch in data */
CREATE TABLE Staging.Credit_O1_S1_SS1_20120101_In (
	CreditId INT NOT NULL
	, SK_Date INT NOT NULL
	, SK_SourceSystem INT NOT NULL
	, SK_Scenario INT NOT NULL
	, SK_Organisation INT NOT NULL
--	, SK_...
	, Measure1 MONEY NULL
	, Measure2 MONEY NULL
--	...
	, CONSTRAINT CK_SK_Date_O1_S1_SS1_20120101_In CHECK 
		(SK_Date>= 20120101 AND SK_Date < 20120201)
	, CONSTRAINT CK_SK_Organisation_O1_S1_SS1_20120101_In CHECK
		(SK_Organisation = 1)
	, CONSTRAINT CK_SK_Scenario_O1_S1_SS1_20120101_In CHECK
		(SK_Scenario = 1)
	, CONSTRAINT CK_SK_SourceSystem_O1_S1_SS1_20120101_In CHECK
		(SK_SourceSystem = 1)
	, CONSTRAINT FK_SK_Date_O1_S1_SS1_20120101_In 
		FOREIGN KEY (SK_Date)
		REFERENCES Dimensions.[Date](SK_Date)
	, CONSTRAINT FK_SK_Organisation_O1_S1_SS1_20120101_In 
		FOREIGN KEY (SK_Organisation)
		REFERENCES Dimensions.Organisation(SK_Organisation)
	, CONSTRAINT FK_SK_Scenario_O1_S1_SS1_20120101_In 
		FOREIGN KEY (SK_Scenario)
		REFERENCES Dimensions.Scenario(SK_Scenario)
	, CONSTRAINT FK_SK_SourceSystem_O1_S1_SS1_20120101_In 
		FOREIGN KEY (SK_SourceSystem)
		REFERENCES Dimensions.SourceSystem(SK_SourceSystem)
) ON Credit_FG_O1_S1_SS1
GO


-- Create SEQUENCEs for our dummy data
CREATE SEQUENCE Facts.CreditId
    START WITH 1
    INCREMENT BY 1 ;
GO

CREATE SEQUENCE Facts.Measure1
    START WITH 10
    INCREMENT BY 10 ;
GO

CREATE SEQUENCE Facts.Measure2
    START WITH 100
    INCREMENT BY 100 ;
GO


-- Let's insert some data into our staging table
DECLARE @CreditId INT = NEXT VALUE FOR Facts.CreditId
DECLARE @SK_Date INT = 20120101
DECLARE @SK_SourceSystem INT = 1
DECLARE @SK_Scenario INT = 1
DECLARE @SK_Organisation INT = 1
DECLARE @Measure1 MONEY = NEXT VALUE FOR Facts.Measure1
DECLARE @Measure2 MONEY = NEXT VALUE FOR Facts.Measure2

INSERT INTO Staging.Credit_O1_S1_SS1_20120101_In 
	(CreditId, SK_Date, SK_SourceSystem, SK_Scenario
	, SK_Organisation, Measure1, Measure2)
VALUES  (@CreditId, @SK_Date, @SK_SourceSystem, @SK_Scenario
	, @SK_Organisation, @Measure1, @Measure2)
GO 1000


-- Creating the clustered index after inserting data
CREATE UNIQUE CLUSTERED INDEX UCIX_Credit_O1_S1_SS1_20120101_In
ON Staging.Credit_O1_S1_SS1_20120101_In(CreditId, SK_Date)
WITH FILLFACTOR=100
ON Credit_FG_O1_S1_SS1
GO



/* Switch in the new data */
DECLARE @PartitionNumber INT
SELECT @PartitionNumber = 
	$Partition.CreditDateRangeFunction_O1_S1_SS1(20120101)

ALTER TABLE Staging.Credit_O1_S1_SS1_20120101_In 
	SWITCH TO Facts.Credit_O1_S1_SS1 PARTITION @PartitionNumber
GO


/* No stats? */
DBCC SHOW_STATISTICS("Facts.Credit_O1_S1_SS1"
					, UCIX_Credit_O1_S1_SS1)
GO


/* update stats */
UPDATE STATISTICS Facts.Credit_O1_S1_SS1
WITH FULLSCAN
GO


/* Stats! */
DBCC SHOW_STATISTICS("Facts.Credit_O1_S1_SS1"
					, UCIX_Credit_O1_S1_SS1)
GO


/* How does our index fragmentation look? */
SELECT *
FROM sys.dm_db_index_physical_stats (
	DB_ID(N'CreditEDW')
	, OBJECT_ID(N'Facts.Credit_O1_S1_SS1')
	, NULL
	, NULL
	, 'DETAILED'
)
GO


/* We cannot REBUILD a partition online... only offline */
ALTER INDEX UCIX_Credit_O1_S1_SS1 ON Facts.Credit_O1_S1_SS1
REBUILD PARTITION = 2
WITH (ONLINE = ON) -- Not allowed
GO


/* Same goes for REORGANIZE */
ALTER INDEX UCIX_Credit_O1_S1_SS1 ON Facts.Credit_O1_S1_SS1
REORGANIZE PARTITION = 2
WITH (ONLINE = ON) -- Not allowed
GO


/* But we are allowed to rebuild the entire index online */
ALTER INDEX UCIX_Credit_O1_S1_SS1 ON Facts.Credit_O1_S1_SS1
REBUILD
WITH (ONLINE = ON) -- Allowed (if no LOB columns <=2008R2)
GO


/* Let's take a look at our partitions */
SELECT * 
FROM dbo.ShowPartitions
GO


/* Now, let's add a new partition. If the new partition 
 * is not on an existing table for the partitioned view,
 * we create a new table with two new partitions (with 
 * the boundary point of SK_Date)
 *
 * SK_Date = 20120101
 * SK_Organisation = 2
 * SK_Scenario = 1
 * SK_SourceSystem = 1
 */

/* First the filegroups and files */
ALTER DATABASE CreditEDW
ADD FILEGROUP Credit_FG_O2_S1_SS1
GO

-- Filegroup Credit_FG_O2_S1_SS1
ALTER DATABASE CreditEDW
ADD FILE (
	NAME = N'Credit_FG_O2_S1_SS1_File1'
	, FILENAME = N'C:\sqldemo\Credit_FG_O2_S1_SS1_File1.ndf'
	, SIZE = 10MB
	, MAXSIZE = 400MB
	, FILEGROWTH = 20MB
)
TO FILEGROUP Credit_FG_O2_S1_SS1

ALTER DATABASE CreditEDW
ADD FILE (
	NAME = N'Credit_FG_O2_S1_SS1_File2'
	, FILENAME = N'C:\sqldemo\Credit_FG_O2_S1_SS1_File2.ndf'
	, SIZE = 10MB
	, MAXSIZE = 400MB
	, FILEGROWTH = 20MB
)
TO FILEGROUP Credit_FG_O2_S1_SS1
GO


/* Then the partitioning function */
CREATE PARTITION FUNCTION CreditDateRangeFunction_O2_S1_SS1
	(int)
AS
RANGE RIGHT FOR VALUES (
	20120101
)
GO


/* And the partition scheme */
CREATE PARTITION SCHEME CreditDateRangeScheme_O2_S1_SS1
AS PARTITION CreditDateRangeFunction_O2_S1_SS1 
ALL TO (Credit_FG_O2_S1_SS1)
GO


/* Create the fact partitioned tables on 
 * the partition functions 
 */
CREATE TABLE Facts.Credit_O2_S1_SS1 (
	CreditId INT NOT NULL 
	, SK_Date INT NOT NULL
	, SK_SourceSystem INT NOT NULL
	, SK_Scenario INT NOT NULL
	, SK_Organisation INT NOT NULL
--	, SK_...
	, Measure1 MONEY NULL
	, Measure2 MONEY NULL
--	...
	, CONSTRAINT CK_SK_Date_O2_S1_SS1 CHECK 
		(SK_Date>= 20120101 AND SK_Date < 20120201)
	, CONSTRAINT CK_SK_Organisation_O2_S1_SS1 CHECK
		(SK_Organisation = 2)
	, CONSTRAINT CK_SK_Scenario_O2_S1_SS1 CHECK
		(SK_Scenario = 1)
	, CONSTRAINT CK_SK_SourceSystem_O2_S1_SS1 CHECK
		(SK_SourceSystem = 1)
	, CONSTRAINT FK_SK_Date_O2_S1_SS1 
		FOREIGN KEY (SK_Date)
		REFERENCES Dimensions.[Date](SK_Date)
	, CONSTRAINT FK_SK_Organisation_O2_S1_SS1 
		FOREIGN KEY (SK_Organisation)
		REFERENCES Dimensions.Organisation(SK_Organisation)
	, CONSTRAINT FK_SK_Scenario_O2_S1_SS1 
		FOREIGN KEY (SK_Scenario)
		REFERENCES Dimensions.Scenario(SK_Scenario)
	, CONSTRAINT FK_SK_SourceSystem_O2_S1_SS1 
		FOREIGN KEY (SK_SourceSystem)
		REFERENCES Dimensions.SourceSystem(SK_SourceSystem)
) ON CreditDateRangeScheme_O2_S1_SS1(SK_Date)
GO

CREATE UNIQUE CLUSTERED INDEX UCIX_Credit_O2_S1_SS1
ON Facts.Credit_O2_S1_SS1(CreditId, SK_Date)
WITH FILLFACTOR=100
ON CreditDateRangeScheme_O2_S1_SS1(SK_Date)
GO


/* Set lock escalation to partition level */
ALTER TABLE Facts.Credit_O2_S1_SS1
SET (LOCK_ESCALATION = AUTO)
GO


/* Now let's alter the partitioned view */
ALTER VIEW Facts.Credit
AS
SELECT * FROM Facts.Credit_O1_S1_SS1
UNION ALL
SELECT * FROM Facts.Credit_O2_S1_SS1
GO


/* Create staging table to switch in data */
CREATE TABLE Staging.Credit_O2_S1_SS1_20120101_In (
	CreditId INT NOT NULL
	, SK_Date INT NOT NULL
	, SK_SourceSystem INT NOT NULL
	, SK_Scenario INT NOT NULL
	, SK_Organisation INT NOT NULL
--	, SK_...
	, Measure1 MONEY NULL
	, Measure2 MONEY NULL
--	...
	, CONSTRAINT CK_SK_Date_O2_S1_SS1_20120101_In CHECK 
		(SK_Date>= 20120101 AND SK_Date < 20120201)
	, CONSTRAINT CK_SK_Organisation_O2_S1_SS1_20120101_In CHECK
		(SK_Organisation = 2)
	, CONSTRAINT CK_SK_Scenario_O2_S1_SS1_20120101_In CHECK
		(SK_Scenario = 1)
	, CONSTRAINT CK_SK_SourceSystem_O2_S1_SS1_20120101_In CHECK
		(SK_SourceSystem = 1)
	, CONSTRAINT FK_SK_Date_O2_S1_SS1_20120101_In 
		FOREIGN KEY (SK_Date)
		REFERENCES Dimensions.[Date](SK_Date)
	, CONSTRAINT FK_SK_Organisation_O2_S1_SS1_20120101_In 
		FOREIGN KEY (SK_Organisation)
		REFERENCES Dimensions.Organisation(SK_Organisation)
	, CONSTRAINT FK_SK_Scenario_O2_S1_SS1_20120101_In 
		FOREIGN KEY (SK_Scenario)
		REFERENCES Dimensions.Scenario(SK_Scenario)
	, CONSTRAINT FK_SK_SourceSystem_O2_S1_SS1_20120101_In 
		FOREIGN KEY (SK_SourceSystem)
		REFERENCES Dimensions.SourceSystem(SK_SourceSystem)
) ON Credit_FG_O2_S1_SS1
GO


-- Insert some data into the staging table
DECLARE @CreditId INT = NEXT VALUE FOR Facts.CreditId
DECLARE @SK_Date INT = 20120101
DECLARE @SK_SourceSystem INT = 1
DECLARE @SK_Scenario INT = 1
DECLARE @SK_Organisation INT = 2
DECLARE @Measure1 MONEY = NEXT VALUE FOR Facts.Measure1
DECLARE @Measure2 MONEY = NEXT VALUE FOR Facts.Measure2

INSERT INTO Staging.Credit_O2_S1_SS1_20120101_In 
	(CreditId, SK_Date, SK_SourceSystem, SK_Scenario
	, SK_Organisation, Measure1, Measure2)
VALUES  (@CreditId, @SK_Date, @SK_SourceSystem, @SK_Scenario
	, @SK_Organisation, @Measure1, @Measure2)
GO 2000


-- And then create the clustered index
CREATE UNIQUE CLUSTERED INDEX UCIX_Credit_O2_S1_SS1_20120101_In
ON Staging.Credit_O2_S1_SS1_20120101_In(CreditId, SK_Date)
WITH FILLFACTOR=100
ON Credit_FG_O2_S1_SS1
GO


/* Switch in the new data */
DECLARE @PartitionNumber INT
SELECT @PartitionNumber = 
	$Partition.CreditDateRangeFunction_O2_S1_SS1(20120101)

ALTER TABLE Staging.Credit_O2_S1_SS1_20120101_In 
SWITCH TO Facts.Credit_O2_S1_SS1 PARTITION @PartitionNumber
GO


/* We need to update our stats */
UPDATE STATISTICS Facts.Credit_O2_S1_SS1
WITH FULLSCAN
GO


/* Let's take a look at our partitions */
SELECT * 
FROM dbo.ShowPartitions
GO


/* Now, let's add a new partition. This is in an existing
 * table, so we add a new partition to the partitioned
 * table.
 *
 * SK_Date = 20120401
 * SK_Organisation = 2
 * SK_Scenario = 1
 * SK_SourceSystem = 1
 */

/* Add the file group to the partition scheme */
ALTER PARTITION SCHEME CreditDateRangeScheme_O2_S1_SS1
NEXT USED Credit_FG_O2_S1_SS1
GO


/* ... and SPLIT - for 20120201 */
ALTER PARTITION FUNCTION CreditDateRangeFunction_O2_S1_SS1()
SPLIT RANGE (20120201)
GO


/* Add the file group to the partition scheme */
ALTER PARTITION SCHEME CreditDateRangeScheme_O2_S1_SS1
NEXT USED Credit_FG_O2_S1_SS1
GO


/* ... and SPLIT - for 20120301 */
ALTER PARTITION FUNCTION CreditDateRangeFunction_O2_S1_SS1()
SPLIT RANGE (20120301)
GO


/* Add the file group to the partition scheme */
ALTER PARTITION SCHEME CreditDateRangeScheme_O2_S1_SS1
NEXT USED Credit_FG_O2_S1_SS1
GO


/* ... and SPLIT - for 20120401 */
ALTER PARTITION FUNCTION CreditDateRangeFunction_O2_S1_SS1()
SPLIT RANGE (20120401)
GO


/* ... and change the date constraint */
ALTER TABLE Facts.Credit_O2_S1_SS1 
DROP CONSTRAINT CK_SK_Date_O2_S1_SS1
GO

ALTER TABLE Facts.Credit_O2_S1_SS1 
WITH CHECK ADD CONSTRAINT CK_SK_Date_O2_S1_SS1 
CHECK (SK_Date >= 20120101 AND SK_Date < 20120501)
GO

ALTER TABLE Facts.Credit_O2_S1_SS1 
CHECK CONSTRAINT CK_SK_Date_O2_S1_SS1
GO


/* Create staging table to switch in data */
CREATE TABLE Staging.Credit_O2_S1_SS1_20120401_In (
	CreditId INT NOT NULL
	, SK_Date INT NOT NULL
	, SK_SourceSystem INT NOT NULL
	, SK_Scenario INT NOT NULL
	, SK_Organisation INT NOT NULL
--	, SK_...
	, Measure1 MONEY NULL
	, Measure2 MONEY NULL
--	...
	, CONSTRAINT CK_SK_Date_O2_S1_SS1_20120201_In CHECK 
		(SK_Date>= 20120401 AND SK_Date < 20120501)
	, CONSTRAINT CK_SK_Organisation_O2_S1_SS1_20120201_In CHECK
		(SK_Organisation = 2)
	, CONSTRAINT CK_SK_Scenario_O2_S1_SS1_20120201_In CHECK
		(SK_Scenario = 1)
	, CONSTRAINT CK_SK_SourceSystem_O2_S1_SS1_20120201_In CHECK
		(SK_SourceSystem = 1)
	, CONSTRAINT FK_SK_Date_O2_S1_SS1_20120201_In 
		FOREIGN KEY (SK_Date)
		REFERENCES Dimensions.[Date](SK_Date)
	, CONSTRAINT FK_SK_Organisation_O2_S1_SS1_20120201_In 
		FOREIGN KEY (SK_Organisation)
		REFERENCES Dimensions.Organisation(SK_Organisation)
	, CONSTRAINT FK_SK_Scenario_O2_S1_SS1_20120201_In 
		FOREIGN KEY (SK_Scenario)
		REFERENCES Dimensions.Scenario(SK_Scenario)
	, CONSTRAINT FK_SK_SourceSystem_O2_S1_SS1_20120201_In 
		FOREIGN KEY (SK_SourceSystem)
		REFERENCES Dimensions.SourceSystem(SK_SourceSystem)
) ON Credit_FG_O2_S1_SS1
GO


-- Some data
DECLARE @CreditId INT = NEXT VALUE FOR Facts.CreditId
DECLARE @SK_Date INT = 20120401
DECLARE @SK_SourceSystem INT = 1
DECLARE @SK_Scenario INT = 1
DECLARE @SK_Organisation INT = 2
DECLARE @Measure1 MONEY = NEXT VALUE FOR Facts.Measure1
DECLARE @Measure2 MONEY = NEXT VALUE FOR Facts.Measure2

INSERT INTO Staging.Credit_O2_S1_SS1_20120401_In 
	(CreditId, SK_Date, SK_SourceSystem, SK_Scenario
	, SK_Organisation, Measure1, Measure2)
VALUES  (@CreditId, @SK_Date, @SK_SourceSystem, @SK_Scenario
	, @SK_Organisation, @Measure1, @Measure2)
GO 3000



/* Switch in the new data */
DECLARE @PartitionNumber INT
SELECT @PartitionNumber = 
	$Partition.CreditDateRangeFunction_O2_S1_SS1(20120401)

ALTER TABLE Staging.Credit_O2_S1_SS1_20120401_In 
SWITCH TO Facts.Credit_O2_S1_SS1 PARTITION @PartitionNumber
GO












-- Ooops... we forgot the clustered index


-- Let's create a clustered index then
CREATE UNIQUE CLUSTERED INDEX UCIX_Credit_O2_S1_SS1_20120201_In
ON Staging.Credit_O2_S1_SS1_20120401_In(CreditId, SK_Date)
WITH FILLFACTOR=100
ON Credit_FG_O2_S1_SS1
GO


/* Switch in the new data */
DECLARE @PartitionNumber INT
SELECT @PartitionNumber = 
	$Partition.CreditDateRangeFunction_O2_S1_SS1(20120401)

ALTER TABLE Staging.Credit_O2_S1_SS1_20120401_In 
SWITCH TO Facts.Credit_O2_S1_SS1 PARTITION @PartitionNumber
GO


/* Stats? Only the old histogram... */
DBCC SHOW_STATISTICS("Facts.Credit_O2_S1_SS1"
	, UCIX_Credit_O2_S1_SS1)
GO


/* update stats */
UPDATE STATISTICS Facts.Credit_O2_S1_SS1
WITH FULLSCAN
GO


/* Better stats! */
DBCC SHOW_STATISTICS("Facts.Credit_O2_S1_SS1"
	, UCIX_Credit_O2_S1_SS1)
GO


/* Look at our partitions */
SELECT *
FROM dbo.ShowPartitions
GO



-- The end...