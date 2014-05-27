/* Demo script for Layered Partitioning
 *
 * Written by David Peter Hansen 
 * @dphansen | davidpeterhansen.com
 *
 * Setting up CreditEDW
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

USE tempdb
GO

/* Drop existing database */
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'CreditEDW')
BEGIN
	ALTER DATABASE CreditEDW 
	SET SINGLE_USER WITH ROLLBACK IMMEDIATE
END
GO
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'CreditEDW')
BEGIN
	DROP DATABASE CreditEDW
END
GO

/* Create db */
CREATE DATABASE CreditEDW
ON 
( NAME = CreditEDW_data,
    FILENAME = 'C:\sqldemo\CreditEDW_data.mdf',
    SIZE = 10MB,
    MAXSIZE = 200MB,
    FILEGROWTH = 5MB )
LOG ON
( NAME = CreditEDW_log,
    FILENAME = 'C:\sqldemo\CreditEDW_log.ldf',
    SIZE = 50MB,
    MAXSIZE = UNLIMITED,
    FILEGROWTH = 50MB ) ;
GO

ALTER DATABASE CreditEDW
ADD FILEGROUP ExtractFG
GO

ALTER DATABASE CreditEDW
ADD FILE (
	NAME = N'ExtractFG_File1'
	, FILENAME = N'C:\sqldemo\ExtractFG_File1.ndf'
	, SIZE = 10MB
	, MAXSIZE = 400MB
	, FILEGROWTH = 20MB
)
TO FILEGROUP ExtractFG
GO

ALTER DATABASE CreditEDW
ADD FILE (
	NAME = N'ExtractFG_File2'
	, FILENAME = N'C:\sqldemo\ExtractFG_File2.ndf'
	, SIZE = 10MB
	, MAXSIZE = 400MB
	, FILEGROWTH = 20MB
)
TO FILEGROUP ExtractFG
GO

ALTER DATABASE CreditEDW
ADD FILEGROUP StagingFG
GO

ALTER DATABASE CreditEDW
ADD FILE (
	NAME = N'StagingFG_File1'
	, FILENAME = N'C:\sqldemo\StagingFG_File1.ndf'
	, SIZE = 10MB
	, MAXSIZE = 400MB
	, FILEGROWTH = 20MB
)
TO FILEGROUP StagingFG
GO

ALTER DATABASE CreditEDW
ADD FILE (
	NAME = N'StagingFG_File2'
	, FILENAME = N'C:\sqldemo\StagingFG_File2.ndf'
	, SIZE = 10MB
	, MAXSIZE = 400MB
	, FILEGROWTH = 20MB
)
TO FILEGROUP StagingFG
GO

ALTER DATABASE CreditEDW
ADD FILEGROUP DimensionsFG
GO

ALTER DATABASE CreditEDW
ADD FILE (
	NAME = N'DimensionsFG_File1'
	, FILENAME = N'C:\sqldemo\DimensionsFG_File1.ndf'
	, SIZE = 10MB
	, MAXSIZE = 400MB
	, FILEGROWTH = 20MB
)
TO FILEGROUP DimensionsFG
GO

ALTER DATABASE CreditEDW
ADD FILE (
	NAME = N'DimensionsFG_File2'
	, FILENAME = N'C:\sqldemo\DimensionsFG_File2.ndf'
	, SIZE = 10MB
	, MAXSIZE = 400MB
	, FILEGROWTH = 20MB
)
TO FILEGROUP DimensionsFG
GO

USE CreditEDW
GO


/* And now some schemas */
CREATE SCHEMA Facts
GO
CREATE SCHEMA Dimensions
GO
CREATE SCHEMA [Extract]
GO
CREATE SCHEMA Staging
GO

CREATE TABLE Dimensions.Organisation (
	SK_Organisation INT NOT NULL IDENTITY(1,1) PRIMARY KEY
	, NK_Organisation NVARCHAR(10) NOT NULL DEFAULT 'Unknown'
	, OrganisationName NVARCHAR(100) NULL
) ON DimensionsFG

CREATE TABLE Dimensions.Scenario (
	SK_Scenario INT NOT NULL IDENTITY(1,1) PRIMARY KEY
	, NK_Scenario NVARCHAR(10) NOT NULL DEFAULT 'Unknown'
	, ScenarioName NVARCHAR(100) NULL
) ON DimensionsFG

CREATE TABLE Dimensions.SourceSystem (
	SK_SourceSystem INT NOT NULL IDENTITY(1,1) PRIMARY KEY
	, NK_SourceSystem NVARCHAR(10) NOT NULL DEFAULT 'Unknown'
	, SourceSystemName NVARCHAR(100) NULL
) ON DimensionsFG

CREATE TABLE Dimensions.[Date] (
	SK_Date INT NOT NULL IDENTITY(1,1) PRIMARY KEY
	, NK_Date DATETIME NOT NULL 
	, [Year] SMALLINT
	, [Month] SMALLINT
	, [Day] SMALLINT
) ON DimensionsFG
GO

-- Insert junk data
INSERT INTO Dimensions.Organisation (NK_Organisation, OrganisationName)
VALUES (CHAR(CAST((90 - 65) * RAND() + 65 AS INT)) + CHAR(CAST((90 - 65) * RAND() + 65 AS INT)) + CHAR(CAST((90 - 65) * RAND() + 65 AS INT)) + CHAR(CAST((90 - 65) * RAND() + 65 AS INT)), NEWID())
GO 100

SET IDENTITY_INSERT Dimensions.Organisation ON
INSERT INTO Dimensions.Organisation(SK_Organisation, NK_Organisation, OrganisationName)
VALUES (-1, 'Unknown', 'Unknown')
SET IDENTITY_INSERT Dimensions.Organisation OFF
GO

UPDATE Dimensions.Organisation
SET NK_Organisation = 'MAERSK'
WHERE SK_Organisation = 1
GO

INSERT INTO Dimensions.Scenario (NK_Scenario, ScenarioName)
VALUES (CHAR(CAST((90 - 65) * RAND() + 65 AS INT)) + CHAR(CAST((90 - 65) * RAND() + 65 AS INT)) + CHAR(CAST((90 - 65) * RAND() + 65 AS INT)) + CHAR(CAST((90 - 65) * RAND() + 65 AS INT)), NEWID())
GO 10

SET IDENTITY_INSERT Dimensions.Scenario ON
INSERT INTO Dimensions.Scenario(SK_Scenario, NK_Scenario, ScenarioName)
VALUES (-1, 'Unknown', 'Unknown')
SET IDENTITY_INSERT Dimensions.Scenario OFF
GO

INSERT INTO Dimensions.SourceSystem (NK_SourceSystem, SourceSystemName)
VALUES (CHAR(CAST((90 - 65) * RAND() + 65 AS INT)) + CHAR(CAST((90 - 65) * RAND() + 65 AS INT)) + CHAR(CAST((90 - 65) * RAND() + 65 AS INT)) + CHAR(CAST((90 - 65) * RAND() + 65 AS INT)), NEWID())
GO 5

SET IDENTITY_INSERT Dimensions.SourceSystem ON
INSERT INTO Dimensions.SourceSystem(SK_SourceSystem, NK_SourceSystem, SourceSystemName)
VALUES (-1, 'Unknown', 'Unknown')
SET IDENTITY_INSERT Dimensions.SourceSystem OFF
GO

DECLARE @MaxDate DATETIME = '2014-01-01'
DECLARE @iDate DATETIME = '2008-01-01'

SET IDENTITY_INSERT Dimensions.[Date] ON
WHILE @iDate < @MaxDate BEGIN
	print @iDate
	INSERT INTO Dimensions.[Date] (SK_Date, NK_Date, [Year], [Month], [Day])
	VALUES (YEAR(@iDate) * 10000 + MONTH(@iDate) * 100 + DAY(@iDate), @iDate, YEAR(@iDate), MONTH(@iDate), DAY(@iDate))

	SET @iDate = DATEADD(month, 1, @iDate)
END
SET IDENTITY_INSERT Dimensions.[Date] OFF
GO

