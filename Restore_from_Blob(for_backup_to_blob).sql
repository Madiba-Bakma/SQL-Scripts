-- ============================================================
--  Restore-Databases-Blob.sql
--  Corresponds to Backup-Databases-Blob.sql
--  Restores databases directly from Azure Blob Storage.
--  Supports both single file and striped backups.
--
--  Per database, set UseStriping to match what was used
--  during the backup:
--    0 = single .bak file  (backup was under 200GB)
--    1 = striped .bak files (backup was over 200GB)
-- ============================================================

-- ── CONFIGURATION ────────────────────────────────────────────
DECLARE @StorageAccount NVARCHAR(200) = N'sqlsa'
DECLARE @Container      NVARCHAR(200) = N'backup'
DECLARE @CredentialName NVARCHAR(500) = N'https://sqlsa.blob.core.windows.net/backup'
DECLARE @SasToken       NVARCHAR(500) = N'?sv=2023-01-03&ss=b...'  -- include leading ?
DECLARE @DataPath       NVARCHAR(500) = N'F:\data\'    -- path for .mdf files
DECLARE @LogPath        NVARCHAR(500) = N'G:\log\'     -- path for .ldf files
DECLARE @StripeCount    INT           = 3              -- must match what was used in backup
-- ─────────────────────────────────────────────────────────────

-- ── DATABASE LIST ────────────────────────────────────────────
-- BakFile:     exact filename (without stripe suffix for striped backups)
--              e.g. Test_20260321_131939.bak
-- UseStriping: 0 = single file, 1 = striped (must match backup)
DECLARE @Databases TABLE (DbName SYSNAME, BakFile NVARCHAR(260), UseStriping BIT)
INSERT INTO @Databases (DbName, BakFile, UseStriping) VALUES
    (N'Database1', N'Database1_20260321_131939.bak', 0),  -- single file
    (N'Database2', N'Database2_20260321_131939.bak', 0),  -- single file
    (N'Database3', N'Database3_20260321_131939.bak', 0),  -- single file
    (N'Database4', N'Database4_20260321_131939.bak', 0),  -- single file
    (N'Database5', N'Database5_20260321_131939.bak', 1)   -- striped
-- ─────────────────────────────────────────────────────────────

-- ── INTERNALS — no edits needed below this line ──────────────
SET NOCOUNT ON

DECLARE @DbName       SYSNAME
DECLARE @BakFile      NVARCHAR(260)
DECLARE @UseStriping  BIT
DECLARE @BaseUrl      NVARCHAR(500)
DECLARE @BlobUrl      NVARCHAR(1000)
DECLARE @Sql          NVARCHAR(MAX)
DECLARE @Msg          NVARCHAR(2000)
DECLARE @MoveClause   NVARCHAR(MAX)
DECLARE @FromClause   NVARCHAR(MAX)
DECLARE @LogicalName  NVARCHAR(128)
DECLARE @FileType     CHAR(1)
DECLARE @SuccessCount INT = 0
DECLARE @FailCount    INT = 0
DECLARE @i            INT
DECLARE @StripeUrl    NVARCHAR(1000)
DECLARE @BakBase      NVARCHAR(260)  -- filename without .bak extension

SET @BaseUrl = N'https://' + @StorageAccount + N'.blob.core.windows.net/' + @Container

DECLARE @Log TABLE (
    LogTime  DATETIME     DEFAULT GETDATE(),
    Level    NVARCHAR(10),
    Message  NVARCHAR(2000)
)

INSERT INTO @Log (Level, Message)
VALUES ('INFO', '===== Restore run started on ' + @@SERVERNAME + ' =====')

-- ── STEP 1: CREATE SAS CREDENTIAL IF MISSING ─────────────────
IF NOT EXISTS (SELECT 1 FROM sys.credentials WHERE name = @CredentialName)
BEGIN
    SET @Msg = 'Creating SAS credential: ' + @CredentialName
    INSERT INTO @Log (Level, Message) VALUES ('INFO', @Msg)
    RAISERROR(@Msg, 0, 1) WITH NOWAIT

    SET @Sql = N'CREATE CREDENTIAL [' + @CredentialName + N']
                 WITH IDENTITY = ''SHARED ACCESS SIGNATURE'',
                 SECRET = ''' + SUBSTRING(@SasToken, 2, LEN(@SasToken)) + N''';'
    BEGIN TRY
        EXEC sp_executesql @Sql
        INSERT INTO @Log (Level, Message) VALUES ('INFO', 'Credential created.')
    END TRY
    BEGIN CATCH
        SET @Msg = 'Failed to create credential: ' + ERROR_MESSAGE()
        INSERT INTO @Log (Level, Message) VALUES ('ERROR', @Msg)
        RAISERROR(@Msg, 16, 1)
        RETURN
    END CATCH
END
ELSE
    INSERT INTO @Log (Level, Message) VALUES ('INFO', 'Credential already exists, skipping.')

-- ── STEP 2: FILE LIST TEMP TABLE ─────────────────────────────
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

-- ── STEP 3: RESTORE LOOP ─────────────────────────────────────
DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT DbName, BakFile, UseStriping FROM @Databases

OPEN db_cursor
FETCH NEXT FROM db_cursor INTO @DbName, @BakFile, @UseStriping

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @Msg = '── Processing [' + @DbName + '] (Striped: ' + CAST(@UseStriping AS CHAR(1)) + ')'
    INSERT INTO @Log (Level, Message) VALUES ('INFO', @Msg)
    RAISERROR(@Msg, 0, 1) WITH NOWAIT

    -- ── Build FROM clause ─────────────────────────────────────
    -- Single file: FROM URL = N'...DbName_date.bak'
    -- Striped:     FROM URL = N'...DbName_date_stripe_1.bak',
    --                   URL = N'...DbName_date_stripe_2.bak', ...
    IF @UseStriping = 0
    BEGIN
        SET @BlobUrl    = @BaseUrl + N'/' + @DbName + N'/' + @BakFile
        SET @FromClause = N'URL = N''' + @BlobUrl + N''''
    END
    ELSE
    BEGIN
        -- Strip .bak from filename to build stripe names
        SET @BakBase    = LEFT(@BakFile, LEN(@BakFile) - 4)
        SET @FromClause = N''
        SET @i = 1

        WHILE @i <= @StripeCount
        BEGIN
            SET @StripeUrl = @BaseUrl + N'/' + @DbName + N'/'
                           + @BakBase
                           + N'_stripe_' + CAST(@i AS NVARCHAR(3))
                           + N'.bak'

            IF @i = 1
                SET @FromClause = N'URL = N''' + @StripeUrl + N''''
            ELSE
                SET @FromClause += N',' + CHAR(13) + N'         URL = N''' + @StripeUrl + N''''

            SET @i += 1
        END

        -- Log the first stripe URL for reference
        SET @BlobUrl = @BaseUrl + N'/' + @DbName + N'/' + @BakBase + N'_stripe_1.bak'
    END

    -- ── Get logical file names ────────────────────────────────
    -- FILELISTONLY only needs the first stripe for striped backups
    TRUNCATE TABLE #FileList

    SET @Sql = N'RESTORE FILELISTONLY FROM ' + 
               CASE @UseStriping 
                   WHEN 0 THEN N'URL = N''' + @BlobUrl + N''''
                   ELSE N'URL = N''' + @BlobUrl + N''''  -- first stripe
               END

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
        FETCH NEXT FROM db_cursor INTO @DbName, @BakFile, @UseStriping
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

    -- ── Execute restore ───────────────────────────────────────
    SET @Sql = N'RESTORE DATABASE [' + @DbName + N']
                 FROM ' + @FromClause + N'
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

    FETCH NEXT FROM db_cursor INTO @DbName, @BakFile, @UseStriping
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
--  1. MATCH THE BACKUP SCRIPT
--     UseStriping and @StripeCount must match exactly what
--     was set in Backup-Databases-Blob.sql when the backup ran.
--
--  2. BAK FILE NAMES
--     Set BakFile to the base filename used during backup e.g:
--       Single : Database1_20260321_131939.bak
--       Striped: Database5_20260321_131939.bak
--                (script appends _stripe_1.bak, _stripe_2.bak etc.)
--
--  3. PATHS
--     @DataPath — where .mdf files land (e.g. F:\data\)
--     @LogPath  — where .ldf files land (e.g. G:\log\)
--     Both folders must exist and SQL Server service account
--     must have write access.
--
--  4. CREDENTIAL
--     Must already exist. SAS token needs Read permission.
--     To recreate:
--       DROP CREDENTIAL [https://sqlsa.blob.core.windows.net/backup]
--       CREATE CREDENTIAL [https://sqlsa.blob.core.windows.net/backup]
--       WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
--       SECRET = 'sv=...'   -- NO leading ?
-- ============================================================
