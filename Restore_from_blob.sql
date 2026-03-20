-- ============================================================
--  Restore-Databases.sql
--  Restores databases directly from Azure Blob Storage.
--  Backup file naming: DbName_YYYYMMDD_HHMMSS.bak
-- ============================================================

-- ── CONFIGURATION ────────────────────────────────────────────
DECLARE @BaseUrl  NVARCHAR(500) = N'https://sqlsa.blob.core.windows.net/backup'
DECLARE @DataPath NVARCHAR(500) = N'D:\SQLData\'       -- path for .mdf files
DECLARE @LogPath  NVARCHAR(500) = N'L:\SQLLogs\'       -- path for .ldf files
-- ─────────────────────────────────────────────────────────────

-- ── DATABASE LIST ─────────────────────────────────────────────
DECLARE @Databases TABLE (DbName SYSNAME, BakFile NVARCHAR(260))
INSERT INTO @Databases (DbName, BakFile) VALUES
    (N'testBackup', N'testBackup_20260320_131444.bak'),
    (N'Database2',  N'Database2_20260320_131444.bak'),
    (N'Database3',  N'Database3_20260320_131444.bak'),
    (N'Database4',  N'Database4_20260320_131444.bak'),
    (N'Database5',  N'Database5_20260320_131444.bak')
-- ─────────────────────────────────────────────────────────────

-- ── INTERNALS ────────────────────────────────────────────────
SET NOCOUNT ON

DECLARE @DbName       SYSNAME
DECLARE @BakFile      NVARCHAR(260)
DECLARE @BlobUrl      NVARCHAR(1000)
DECLARE @Sql          NVARCHAR(MAX)
DECLARE @Msg          NVARCHAR(2000)
DECLARE @MoveClause   NVARCHAR(MAX)
DECLARE @LogicalName  NVARCHAR(128)
DECLARE @FileType     CHAR(1)
DECLARE @SuccessCount INT = 0
DECLARE @FailCount    INT = 0

DECLARE @Log TABLE (
    LogTime  DATETIME     DEFAULT GETDATE(),
    Level    NVARCHAR(10),
    Message  NVARCHAR(2000)
)

INSERT INTO @Log (Level, Message)
VALUES ('INFO', '===== Restore run started on ' + @@SERVERNAME + ' =====')

-- ── FILE LIST TEMP TABLE ──────────────────────────────────────
CREATE TABLE #FileList (
    LogicalName          NVARCHAR(128),
    PhysicalName         NVARCHAR(260),
    Type                 CHAR(1),
    FileGroupName        NVARCHAR(128),
    Size                 BIGINT,
    MaxSize              BIGINT,
    FileId               INT,
    CreateLSN            NUMERIC(25,0),
    DropLSN              NUMERIC(25,0),
    UniqueId             UNIQUEIDENTIFIER,
    ReadOnlyLSN          NUMERIC(25,0),
    ReadWriteLSN         NUMERIC(25,0),
    BackupSizeInBytes    BIGINT,
    SourceBlockSize      INT,
    FileGroupId          INT,
    LogGroupGUID         UNIQUEIDENTIFIER,
    DifferentialBaseLSN  NUMERIC(25,0),
    DifferentialBaseGUID UNIQUEIDENTIFIER,
    IsReadOnly           BIT,
    IsPresent            BIT,
    TDEThumbprint        VARBINARY(32),
    SnapshotUrl          NVARCHAR(360)
)

-- ── RESTORE LOOP ─────────────────────────────────────────────
DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT DbName, BakFile FROM @Databases

OPEN db_cursor
FETCH NEXT FROM db_cursor INTO @DbName, @BakFile

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @BlobUrl = @BaseUrl + N'/' + @DbName + N'/' + @BakFile

    SET @Msg = '── Processing [' + @DbName + '] from ' + @BlobUrl
    INSERT INTO @Log (Level, Message) VALUES ('INFO', @Msg)
    RAISERROR(@Msg, 0, 1) WITH NOWAIT

    -- ── Get logical file names via sp_executesql ──────────────
    TRUNCATE TABLE #FileList

    SET @Sql = N'RESTORE FILELISTONLY FROM URL = N''' + @BlobUrl + N''''

    BEGIN TRY
        INSERT INTO #FileList
        EXEC sp_executesql @Sql
        INSERT INTO @Log (Level, Message) VALUES ('INFO', 'File list retrieved OK.')
    END TRY
    BEGIN CATCH
        SET @Msg = 'FILELISTONLY failed for [' + @DbName + ']: ' + ERROR_MESSAGE()
        INSERT INTO @Log (Level, Message) VALUES ('ERROR', @Msg)
        RAISERROR(@Msg, 0, 1) WITH NOWAIT
        SET @FailCount += 1
        FETCH NEXT FROM db_cursor INTO @DbName, @BakFile
        CONTINUE
    END CATCH

    -- ── Build MOVE clauses ────────────────────────────────────
    SET @MoveClause = N''

    DECLARE file_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT LogicalName, Type FROM #FileList

    OPEN file_cursor
    FETCH NEXT FROM file_cursor INTO @LogicalName, @FileType

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @TargetFile NVARCHAR(600) =
            CASE @FileType
                WHEN 'L' THEN @LogPath  + @DbName + '_' + @LogicalName + N'.ldf'
                ELSE           @DataPath + @DbName + '_' + @LogicalName + N'.mdf'
            END

        SET @MoveClause += N', MOVE N''' + @LogicalName + N''' TO N''' + @TargetFile + N''''

        FETCH NEXT FROM file_cursor INTO @LogicalName, @FileType
    END

    CLOSE file_cursor
    DEALLOCATE file_cursor

    -- ── Log the MOVE clauses for visibility ───────────────────
    SET @Msg = 'MOVE clauses: ' + @MoveClause
    INSERT INTO @Log (Level, Message) VALUES ('INFO', @Msg)
    RAISERROR(@Msg, 0, 1) WITH NOWAIT

    -- ── Execute the restore ───────────────────────────────────
    SET @Sql = N'RESTORE DATABASE [' + @DbName + N']
                 FROM URL = N''' + @BlobUrl + N'''
                 WITH REPLACE,
                      RECOVERY,
                      STATS = 10'
             + @MoveClause + N';'

    SET @Msg = 'Restoring [' + @DbName + '] — data → ' + @DataPath + '  log → ' + @LogPath
    INSERT INTO @Log (Level, Message) VALUES ('INFO', @Msg)
    RAISERROR(@Msg, 0, 1) WITH NOWAIT

    BEGIN TRY
        EXEC sp_executesql @Sql
        SET @Msg = 'SUCCESS: [' + @DbName + '] restored.'
        INSERT INTO @Log (Level, Message) VALUES ('INFO', @Msg)
        RAISERROR(@Msg, 0, 1) WITH NOWAIT
        SET @SuccessCount += 1
    END TRY
    BEGIN CATCH
        SET @Msg = 'FAILED: [' + @DbName + '] — ' + ERROR_MESSAGE()
        INSERT INTO @Log (Level, Message) VALUES ('ERROR', @Msg)
        RAISERROR(@Msg, 0, 1) WITH NOWAIT
        SET @FailCount += 1
    END CATCH

    FETCH NEXT FROM db_cursor INTO @DbName, @BakFile
END

CLOSE db_cursor
DEALLOCATE db_cursor
DROP TABLE IF EXISTS #FileList

-- ── SUMMARY ──────────────────────────────────────────────────
DECLARE @Summary NVARCHAR(200) =
    '===== Restore complete: ' + CAST(@SuccessCount AS NVARCHAR) + ' succeeded, '
    + CAST(@FailCount AS NVARCHAR) + ' failed ====='

INSERT INTO @Log (Level, Message) VALUES ('INFO', @Summary)
RAISERROR(@Summary, 0, 1) WITH NOWAIT

SELECT LogTime, Level, Message FROM @Log ORDER BY LogTime

IF @FailCount > 0
    RAISERROR('One or more restores failed. Check the log above.', 16, 1)

-- ============================================================
--  SETUP NOTES
-- ============================================================
--
--  1. PATHS
--     @DataPath  — where .mdf files land  (e.g. D:\SQLData\)
--     @LogPath   — where .ldf files land  (e.g. L:\SQLLogs\)
--     Both folders must exist and the SQL Server service
--     account must have write access to each.
--
--  2. BAK FILE NAMES
--     Update the BakFile column in @Databases with the exact
--     filenames from your blob folders.
--
--  3. CREDENTIAL
--     Must already exist before running. To recreate:
--
--       IF EXISTS (SELECT 1 FROM sys.credentials
--                  WHERE name = 'https://sqlsa.blob.core.windows.net/backup')
--           DROP CREDENTIAL [https://sqlsa.blob.core.windows.net/backup]
--
--       CREATE CREDENTIAL [https://sqlsa.blob.core.windows.net/backup]
--       WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
--       SECRET = 'sv=2023-01-03&ss=b...'   -- NO leading ?
-- ============================================================
