/* Demo script for Layered Partitioning
 *
 * Written by David Peter Hansen 
 * @dphansen | davidpeterhansen.com
 *
 * Partition split
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

USE tempdb;
GO

/* Create database */
IF EXISTS (SELECT 1 FROM sys.databases 
			WHERE name = 'PartitionSplitDemo')
	DROP DATABASE PartitionSplitDemo;
GO

CREATE DATABASE PartitionSplitDemo
ON 
( NAME = PartitionSplitDemo_data,
    FILENAME = 'C:\sqldemo\PartitionSplitDemo_data.mdf',
    SIZE = 10MB,
    MAXSIZE = 200MB,
    FILEGROWTH = 5MB )
LOG ON
( NAME = PartitionSplitDemo_log,
    FILENAME = 'C:\sqldemo\PartitionSplitDemo_log.ldf',
    SIZE = 50MB,
    MAXSIZE = UNLIMITED,
    FILEGROWTH = 50MB ) ;
GO

USE PartitionSplitDemo;
GO

CREATE SCHEMA Facts
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


/* Add a filegroup for our partitions
 *
 * Normally we would add more than one filegroup, and more
 * than one file per filegroup, but for the simplicity, 
 * we just use one.
 */
ALTER DATABASE PartitionSplitDemo
ADD FILEGROUP PartitionSplitDemo_FG
GO


ALTER DATABASE PartitionSplitDemo
ADD FILE (
	NAME = N'PartitionSplitDemo_File1'
	, FILENAME 
		= N'C:\sqldemo\PartitionSplitDemo_File1.ndf'
	, SIZE = 10MB
	, MAXSIZE = 400MB
	, FILEGROWTH = 20MB
)
TO FILEGROUP PartitionSplitDemo_FG
GO


/* Now, let's create a partition function 
 * NOTE: 1 boundary points = 2 partitions
 */
CREATE PARTITION FUNCTION CreditPF
	(int)
AS
RANGE RIGHT FOR VALUES (
	20120101
)
GO


/* And then we have a partition function, 
 * binding the partitions to the filegroups
 * Note: For simplicity, we just bind the same 
 *       filegroup to all partitions
 */
CREATE PARTITION SCHEME CreditPS
AS PARTITION CreditPF 
ALL TO (PartitionSplitDemo_FG)
GO


/* Create a very simple fact table on the 
 * partition function 
 */
CREATE TABLE Facts.Credit (
	SK_Date INT NOT NULL
	, Measure1 MONEY NOT NULL
) ON CreditPS(SK_Date);
GO


/* Insert some dummy data for March and May 2012
 * Takes up to 20 sec (slow laptop...). 
 */
INSERT INTO Facts.Credit VALUES (20120301, 200);
GO 10000

INSERT INTO Facts.Credit VALUES (20120501, 300);
GO 10000


/* Let's take a look at our partitions */
SELECT *
FROM dbo.ShowPartitions
GO

/* Add the new file group to the partition scheme */
ALTER PARTITION SCHEME CreditPS
NEXT USED PartitionSplitDemo_FG
GO


/* Test 1: Split 2nd (and last) partition - which has data on 
 *         both sides of the new boundary point
 * Result: 10000 LOP_INSERT_ROWS and 10000 LOP_DELETE_ROWS 
 *         + metadata update
 */
DECLARE @xact_id BIGINT

BEGIN TRAN

-- This is the actual split
ALTER PARTITION FUNCTION CreditPF()
SPLIT RANGE (20120401)
-- End of split 

-- Get the xact ID from our current explicit transaction
SELECT @xact_id = transaction_id 
FROM sys.dm_tran_current_transaction

COMMIT TRAN

-- Get the entries in the transaction log related to the
-- above transaction doing a split
SELECT  [Current LSN], [Operation], [AllocUnitName], [Context]
	, [Transaction ID] , [Transaction Name], [Xact ID]
FROM ::fn_dblog(NULL,NULL)
WHERE [Transaction ID] = (
	SELECT TOP 1 [Transaction ID] 
	FROM ::fn_dblog(NULL,NULL)
	WHERE [Xact ID] = @xact_id)
GO


/* Let's take a look at our partitions */
SELECT *
FROM dbo.ShowPartitions
GO


/* Add the new file group to the partition scheme */
ALTER PARTITION SCHEME CreditPS
NEXT USED PartitionSplitDemo_FG
GO


/* Test 2: Split 3rd (and last) partition - has data only on 
 *         left side of new boundary point (right side is empty)
 * Result: Updating meta data only
 * Note:   If we were using left range partitioning, we would 
 *         have 10000 LOP_INSERT_ROWS and 10000 LOP_DELETE_ROWS 
 *         + metadata
 */
DECLARE @xact_id BIGINT

BEGIN TRAN

ALTER PARTITION FUNCTION CreditPF()
SPLIT RANGE (20120601)

SELECT @xact_id = transaction_id 
FROM sys.dm_tran_current_transaction

COMMIT TRAN

SELECT  [Current LSN], [Operation], [AllocUnitName], [Context]
	, [Transaction ID] , [Transaction Name], [Xact ID]
FROM ::fn_dblog(NULL,NULL)
WHERE [Transaction ID] = (
	SELECT TOP 1 [Transaction ID] 
	FROM ::fn_dblog(NULL,NULL)
	WHERE [Xact ID] = @xact_id)
GO


/* Let's take a look at our partitions */
SELECT *
FROM dbo.ShowPartitions
GO


/* Add the new file group to the partition scheme */
ALTER PARTITION SCHEME CreditPS
NEXT USED PartitionSplitDemo_FG
GO


/* Test 3: split on 4th (and last) partition - is an empty 
 *         partition
 * Result: updating meta data only
 */
DECLARE @xact_id BIGINT

BEGIN TRAN

ALTER PARTITION FUNCTION CreditPF()
SPLIT RANGE (20120801)

SELECT @xact_id = transaction_id 
FROM sys.dm_tran_current_transaction

COMMIT TRAN

SELECT  [Current LSN], [Operation], [AllocUnitName], [Context]
	, [Transaction ID] , [Transaction Name], [Xact ID]
FROM ::fn_dblog(NULL,NULL)
WHERE [Transaction ID] = (
	SELECT TOP 1 [Transaction ID] 
	FROM ::fn_dblog(NULL,NULL)
	WHERE [Xact ID] = @xact_id)
GO


/* Let's take a look at our partitions */
SELECT *
FROM dbo.ShowPartitions


/* Add the new file group to the partition scheme */
ALTER PARTITION SCHEME CreditPS
NEXT USED PartitionSplitDemo_FG
GO


/* Test 4: split on 2nd partition - which is between two other 
 *         partitions - has data (20120301) on right side of the 
 *         new boundary point (20120201)
 * Result: 10000 LOP_INSERT_ROWS + 10000 LOP_DELETE_ROWS + meta 
 *         data update
 * Note:   If we were using left range partitioning, we would 
 *         only have metadata updates
 */
DECLARE @xact_id BIGINT

BEGIN TRAN

ALTER PARTITION FUNCTION CreditPF()
SPLIT RANGE (20120201)

SELECT @xact_id = transaction_id 
FROM sys.dm_tran_current_transaction

COMMIT TRAN

SELECT  [Current LSN], [Operation], [AllocUnitName], [Context]
	, [Transaction ID] , [Transaction Name], [Xact ID]
FROM ::fn_dblog(NULL,NULL)
WHERE [Transaction ID] = (
	SELECT TOP 1 [Transaction ID] 
	FROM ::fn_dblog(NULL,NULL)
	WHERE [Xact ID] = @xact_id)
GO


/* Let's take a look at our partitions 
 * Note: data is moved from partition 2 to partition 3
 */
SELECT *
FROM dbo.ShowPartitions
GO


/* Add the new file group to the partition scheme */
ALTER PARTITION SCHEME CreditPS
NEXT USED PartitionSplitDemo_FG
GO


/* Test 5: split on 4th partition - which is between two other 
 *         partitions - is empty
 * Result: updating meta data only
 */
DECLARE @xact_id BIGINT

BEGIN TRAN

ALTER PARTITION FUNCTION CreditPF()
SPLIT RANGE (20120701)

SELECT @xact_id = transaction_id 
FROM sys.dm_tran_current_transaction

COMMIT TRAN

SELECT  [Current LSN], [Operation], [AllocUnitName], [Context]
	, [Transaction ID] , [Transaction Name], [Xact ID]
FROM ::fn_dblog(NULL,NULL)
WHERE [Transaction ID] = (
	SELECT TOP 1 [Transaction ID] 
	FROM ::fn_dblog(NULL,NULL)
	WHERE [Xact ID] = @xact_id)
GO


/* Let's take a look at our partitions */
SELECT *
FROM dbo.ShowPartitions

/* The end... */