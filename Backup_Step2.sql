/* ================================================================
   STEP 2: Backup specific databases + FINAL REPORT
   - Uses per-database folders already created
   - FULL for all
   - LOG (optional) only for FULL/BULK_LOGGED
   - Final summary report at the end
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
-- REPORT TABLE
--------------------------------------------------------------------
DECLARE @Report TABLE
(
    DatabaseName SYSNAME,
    Recovery     NVARCHAR(60),
    Action       NVARCHAR(20),        -- FULL / LOG / ERROR
    BackupFile   NVARCHAR(4000),
    StartTime    DATETIME2(0),
    EndTime      DATETIME2(0),
    DurationSec  INT,
    Succeeded    BIT,
    ErrorMessage NVARCHAR(4000)
);

--------------------------------------------------------------------
-- TARGET DATABASES
--------------------------------------------------------------------
DECLARE @Targets TABLE
(
    DatabaseName SYSNAME PRIMARY KEY,
    Recovery     NVARCHAR(60)
);

INSERT INTO @Targets(DatabaseName, Recovery)
SELECT d.name, d.recovery_model_desc
FROM sys.databases d
JOIN (
    SELECT DISTINCT LTRIM(RTRIM(value)) AS name
    FROM STRING_SPLIT(@DbListCsv, ',')
    WHERE LTRIM(RTRIM(value)) <> ''
) x ON x.name = d.name
WHERE d.state_desc = 'ONLINE'
AND d.is_read_only = 0;

IF NOT EXISTS (SELECT 1 FROM @Targets)
BEGIN
    RAISERROR('No valid ONLINE writable databases found.', 16, 1);
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
               N' WITH COMPRESSION, CHECKSUM, INIT, STATS = ' +
               CAST(@Stats AS NVARCHAR(10));

    SET @Start = SYSDATETIME();

    BEGIN TRY
        EXEC sys.sp_executesql @Sql;
        SET @End = SYSDATETIME();

        INSERT INTO @Report
        VALUES (@Db, @Recovery, 'FULL', @File,
                @Start, @End, DATEDIFF(SECOND,@Start,@End), 1, NULL);
    END TRY
    BEGIN CATCH
        SET @End = SYSDATETIME();

        INSERT INTO @Report
        VALUES (@Db, @Recovery, 'FULL', @File,
                @Start, @End, DATEDIFF(SECOND,@Start,@End),
                0, ERROR_MESSAGE());
    END CATCH;

    FETCH NEXT FROM cur_full INTO @Db, @Recovery;
END

CLOSE cur_full;
DEALLOCATE cur_full;

--------------------------------------------------------------------
-- LOG BACKUPS (optional)
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
                   N' WITH COMPRESSION, CHECKSUM, INIT, STATS = ' +
                   CAST(@Stats AS NVARCHAR(10));

        SET @Start = SYSDATETIME();

        BEGIN TRY
            EXEC sys.sp_executesql @Sql;
            SET @End = SYSDATETIME();

            INSERT INTO @Report
            VALUES (@Db, @Recovery, 'LOG', @File,
                    @Start, @End, DATEDIFF(SECOND,@Start,@End), 1, NULL);
        END TRY
        BEGIN CATCH
            SET @End = SYSDATETIME();

            INSERT INTO @Report
            VALUES (@Db, @Recovery, 'LOG', @File,
                    @Start, @End, DATEDIFF(SECOND,@Start,@End),
                    0, ERROR_MESSAGE());
        END CATCH;

        FETCH NEXT FROM cur_log INTO @Db, @Recovery;
    END

    CLOSE cur_log;
    DEALLOCATE cur_log;
END;

--------------------------------------------------------------------
-- FINAL CLEAN REPORT
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
ORDER BY DatabaseName, Action;
