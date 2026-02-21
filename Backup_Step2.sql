/* ================================================================
   STEP 2: Backup specific databases + FINAL REPORT (includes OFFLINE)
   - Uses per-database folders already created
   - Reports NOT_FOUND / OFFLINE / READ_ONLY / FULL / LOG
   ================================================================ */

SET NOCOUNT ON;

DECLARE
      @BackupRoot   NVARCHAR(4000) = N'\\Sql01\mssql'                -- MUST match PowerShell step
    , @DbListCsv    NVARCHAR(MAX)  = N'AdventureWorks2016,NORTHWND,AlwaysOn_Database1,LogShip_Database,Mirrior_Database,ShopDB'       -- your DBs
    , @DoLogBackups BIT            = 1
    , @Stats        TINYINT        = 10;

WHILE RIGHT(@BackupRoot,1) IN ('\','/')
    SET @BackupRoot = LEFT(@BackupRoot, LEN(@BackupRoot)-1);

DECLARE @Stamp CHAR(15) =
    CONVERT(CHAR(8), GETDATE(), 112) + '_' +
    REPLACE(CONVERT(CHAR(8), GETDATE(), 108), ':', '');

--------------------------------------------------------------------
-- REQUESTED LIST
--------------------------------------------------------------------
DECLARE @Wanted TABLE (DatabaseName SYSNAME PRIMARY KEY);

INSERT INTO @Wanted(DatabaseName)
SELECT DISTINCT LTRIM(RTRIM(value))
FROM STRING_SPLIT(@DbListCsv, ',')
WHERE LTRIM(RTRIM(value)) <> '';

--------------------------------------------------------------------
-- METADATA (LEFT JOIN so NOT_FOUND is captured)
--------------------------------------------------------------------
DECLARE @Meta TABLE
(
    DatabaseName SYSNAME PRIMARY KEY,
    StateDesc    NVARCHAR(60) NULL,
    IsReadOnly   BIT NULL,
    Recovery     NVARCHAR(60) NULL
);

INSERT INTO @Meta(DatabaseName, StateDesc, IsReadOnly, Recovery)
SELECT
    w.DatabaseName,
    d.state_desc,
    d.is_read_only,
    d.recovery_model_desc
FROM @Wanted w
LEFT JOIN sys.databases d
    ON d.name = w.DatabaseName;

--------------------------------------------------------------------
-- REPORT TABLE
--------------------------------------------------------------------
DECLARE @Report TABLE
(
    DatabaseName SYSNAME,
    Recovery     NVARCHAR(60) NULL,
    Action       NVARCHAR(20),        -- NOT_FOUND / OFFLINE / READ_ONLY / FULL / LOG
    Succeeded    BIT,
    DurationSec  INT NULL,
    BackupFile   NVARCHAR(4000) NULL,
    StartTime    DATETIME2(0) NULL,
    EndTime      DATETIME2(0) NULL,
    ErrorMessage NVARCHAR(4000) NULL
);

-- NOT FOUND
INSERT INTO @Report (DatabaseName, Recovery, Action, Succeeded, ErrorMessage)
SELECT DatabaseName, Recovery, 'NOT_FOUND', 0, N'Database name not found on this instance.'
FROM @Meta
WHERE StateDesc IS NULL;

-- OFFLINE (includes RESTORING, RECOVERING, SUSPECT, etc.)
INSERT INTO @Report (DatabaseName, Recovery, Action, Succeeded, ErrorMessage)
SELECT DatabaseName, Recovery, 'OFFLINE', 0, CONCAT(N'Database state: ', StateDesc)
FROM @Meta
WHERE StateDesc IS NOT NULL
  AND StateDesc <> 'ONLINE';

-- READ ONLY
INSERT INTO @Report (DatabaseName, Recovery, Action, Succeeded, ErrorMessage)
SELECT DatabaseName, Recovery, 'READ_ONLY', 0, N'Database is read-only.'
FROM @Meta
WHERE StateDesc = 'ONLINE'
  AND IsReadOnly = 1;

--------------------------------------------------------------------
-- TARGETS WE CAN ACTUALLY BACK UP
--------------------------------------------------------------------
DECLARE @Targets TABLE
(
    DatabaseName SYSNAME PRIMARY KEY,
    Recovery     NVARCHAR(60)
);

INSERT INTO @Targets(DatabaseName, Recovery)
SELECT DatabaseName, Recovery
FROM @Meta
WHERE StateDesc = 'ONLINE'
  AND IsReadOnly = 0;

IF NOT EXISTS (SELECT 1 FROM @Targets)
BEGIN
    PRINT 'No ONLINE writable databases to back up. Returning report.';
    SELECT
        DatabaseName, Recovery, Action, Succeeded, DurationSec, BackupFile, StartTime, EndTime, ErrorMessage
    FROM @Report
    ORDER BY DatabaseName,
             CASE Action WHEN 'NOT_FOUND' THEN 0 WHEN 'OFFLINE' THEN 1 WHEN 'READ_ONLY' THEN 2
                         WHEN 'FULL' THEN 3 WHEN 'LOG' THEN 4 ELSE 5 END;
    RETURN;
END;

--------------------------------------------------------------------
-- FULL BACKUPS
--------------------------------------------------------------------
DECLARE
      @Db SYSNAME
    , @Recovery NVARCHAR(60)
    , @File NVARCHAR(4000)
    , @Sql NVARCHAR(MAX)
    , @Start DATETIME2(0)
    , @End   DATETIME2(0);

DECLARE cur_full CURSOR LOCAL FAST_FORWARD FOR
SELECT DatabaseName, Recovery FROM @Targets ORDER BY DatabaseName;

OPEN cur_full;
FETCH NEXT FROM cur_full INTO @Db, @Recovery;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @File = @BackupRoot + '\' + @Db + '\' + @Db + '_FULL_' + @Stamp + '.bak';

    SET @Sql = N'BACKUP DATABASE ' + QUOTENAME(@Db) +
               N' TO DISK = ' + QUOTENAME(@File,'''') +
               N' WITH COMPRESSION, CHECKSUM, INIT, STATS = ' + CAST(@Stats AS NVARCHAR(10));

    SET @Start = SYSDATETIME();

    BEGIN TRY
        EXEC sys.sp_executesql @Sql;
        SET @End = SYSDATETIME();

        INSERT INTO @Report
        VALUES (@Db, @Recovery, 'FULL', 1, DATEDIFF(SECOND,@Start,@End), @File, @Start, @End, NULL);
    END TRY
    BEGIN CATCH
        SET @End = SYSDATETIME();

        INSERT INTO @Report
        VALUES (@Db, @Recovery, 'FULL', 0, DATEDIFF(SECOND,@Start,@End), @File, @Start, @End, ERROR_MESSAGE());
    END CATCH;

    FETCH NEXT FROM cur_full INTO @Db, @Recovery;
END

CLOSE cur_full;
DEALLOCATE cur_full;

--------------------------------------------------------------------
-- LOG BACKUPS (optional, only FULL/BULK_LOGGED)
--------------------------------------------------------------------
IF @DoLogBackups = 1
BEGIN
    DECLARE cur_log CURSOR LOCAL FAST_FORWARD FOR
    SELECT DatabaseName, Recovery
    FROM @Targets
    WHERE Recovery IN ('FULL','BULK_LOGGED')
    ORDER BY DatabaseName;

    OPEN cur_log;
    FETCH NEXT FROM cur_log INTO @Db, @Recovery;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @File = @BackupRoot + '\' + @Db + '\' + @Db + '_LOG_' + @Stamp + '.trn';

        SET @Sql = N'BACKUP LOG ' + QUOTENAME(@Db) +
                   N' TO DISK = ' + QUOTENAME(@File,'''') +
                   N' WITH COMPRESSION, CHECKSUM, INIT, STATS = ' + CAST(@Stats AS NVARCHAR(10));

        SET @Start = SYSDATETIME();

        BEGIN TRY
            EXEC sys.sp_executesql @Sql;
            SET @End = SYSDATETIME();

            INSERT INTO @Report
            VALUES (@Db, @Recovery, 'LOG', 1, DATEDIFF(SECOND,@Start,@End), @File, @Start, @End, NULL);
        END TRY
        BEGIN CATCH
            SET @End = SYSDATETIME();

            INSERT INTO @Report
            VALUES (@Db, @Recovery, 'LOG', 0, DATEDIFF(SECOND,@Start,@End), @File, @Start, @End, ERROR_MESSAGE());
        END CATCH;

        FETCH NEXT FROM cur_log INTO @Db, @Recovery;
    END

    CLOSE cur_log;
    DEALLOCATE cur_log;
END;

--------------------------------------------------------------------
-- FINAL REPORT (clean)
--------------------------------------------------------------------
PRINT '================ FINAL BACKUP REPORT ================';

SELECT
    DatabaseName,
    Recovery,
    Action,
    Succeeded,
    DurationSec,
    BackupFile,
    StartTime,
    EndTime,
    ErrorMessage
FROM @Report
ORDER BY
    DatabaseName,
    CASE Action
        WHEN 'NOT_FOUND' THEN 0
        WHEN 'OFFLINE'   THEN 1
        WHEN 'READ_ONLY' THEN 2
        WHEN 'FULL'      THEN 3
        WHEN 'LOG'       THEN 4
        ELSE 5
    END;
