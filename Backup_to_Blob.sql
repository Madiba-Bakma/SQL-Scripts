-- ============================================================
--  Backup-Databases-Blob.sql
--  Backs up databases directly to Azure Blob Storage via URL.
--  Supports striping for databases whose backup exceeds 200GB.
--
--  Per database, set UseStriping:
--    0 = single .bak file  (backup under 200GB)
--    1 = striped .bak files (backup over 200GB)
--
--  Set @StripeCount to the number of stripes needed so that
--  each stripe stays under 200GB.
--  e.g. 500GB backup → 3 stripes (~167GB each)
--       800GB backup → 5 stripes (~160GB each)
-- ============================================================

-- ── CONFIGURATION ────────────────────────────────────────────
DECLARE @StorageAccount NVARCHAR(200) = N'sqlsa'
DECLARE @Container      NVARCHAR(200) = N'backup'
DECLARE @SasToken       NVARCHAR(500) = N'?sv=2023-01-03&ss=b...'  -- include leading ?
DECLARE @CredentialName NVARCHAR(500) = N'https://sqlsa.blob.core.windows.net/backup'
DECLARE @Compress       BIT           = 1    -- 1 = WITH COMPRESSION (SQL 2008+ Std/Ent)
DECLARE @StripeCount    INT           = 3    -- number of stripes for large databases
                                             -- adjust so each stripe stays under 200GB
-- ─────────────────────────────────────────────────────────────

-- ── DATABASE LIST ────────────────────────────────────────────
-- UseStriping: 0 = single file, 1 = striped across @StripeCount files
DECLARE @Databases TABLE (DbName SYSNAME, UseStriping BIT)
INSERT INTO @Databases (DbName, UseStriping) VALUES
    (N'Database1', 0),   -- under 200GB — single file
    (N'Database2', 0),   -- under 200GB — single file
    (N'Database3', 0),   -- under 200GB — single file
    (N'Database4', 0),   -- under 200GB — single file
    (N'Database5', 1)    -- over 200GB  — striped
-- ─────────────────────────────────────────────────────────────

-- ── INTERNALS — no edits needed below this line ──────────────
SET NOCOUNT ON

DECLARE @DbName       SYSNAME
DECLARE @UseStriping  BIT
DECLARE @Timestamp    NVARCHAR(20) = CONVERT(NVARCHAR(20), GETDATE(), 112)
                                   + '_'
                                   + REPLACE(CONVERT(NVARCHAR(8), GETDATE(), 108), ':', '')
DECLARE @BaseUrl      NVARCHAR(500)
DECLARE @BlobUrl      NVARCHAR(1000)
DECLARE @Sql          NVARCHAR(MAX)
DECLARE @Msg          NVARCHAR(2000)
DECLARE @SuccessCount INT = 0
DECLARE @FailCount    INT = 0
DECLARE @i            INT
DECLARE @StripeUrl    NVARCHAR(1000)
DECLARE @StripeList   NVARCHAR(MAX)

SET @BaseUrl = N'https://' + @StorageAccount + N'.blob.core.windows.net/' + @Container

DECLARE @Log TABLE (
    LogTime  DATETIME     DEFAULT GETDATE(),
    Level    NVARCHAR(10),
    Message  NVARCHAR(2000)
)

INSERT INTO @Log (Level, Message)
VALUES ('INFO', '===== Backup run started on ' + @@SERVERNAME + ' =====')

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

-- ── STEP 2: BACKUP LOOP ──────────────────────────────────────
DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT DbName, UseStriping FROM @Databases

OPEN db_cursor
FETCH NEXT FROM db_cursor INTO @DbName, @UseStriping

WHILE @@FETCH_STATUS = 0
BEGIN

    -- ── Single file backup ────────────────────────────────────
    IF @UseStriping = 0
    BEGIN
        SET @BlobUrl = @BaseUrl + N'/' + @DbName + N'/' + @DbName + N'_' + @Timestamp + N'.bak'

        SET @Msg = 'Starting single-file backup: [' + @DbName + '] → ' + @BlobUrl
        INSERT INTO @Log (Level, Message) VALUES ('INFO', @Msg)
        RAISERROR(@Msg, 0, 1) WITH NOWAIT

        BEGIN TRY
            IF @Compress = 1
                SET @Sql = N'BACKUP DATABASE [' + @DbName + N']
                             TO URL = N''' + @BlobUrl + N'''
                             WITH COMPRESSION,
                                  CHECKSUM,
                                  STATS = 10,
                                  NAME = N''' + @DbName + N' - Full Backup'';'
            ELSE
                SET @Sql = N'BACKUP DATABASE [' + @DbName + N']
                             TO URL = N''' + @BlobUrl + N'''
                             WITH CHECKSUM,
                                  STATS = 10,
                                  NAME = N''' + @DbName + N' - Full Backup'';'

            EXEC sp_executesql @Sql

            SET @Msg = 'SUCCESS: [' + @DbName + '] → ' + @BlobUrl
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
    END

    -- ── Striped backup ────────────────────────────────────────
    ELSE
    BEGIN
        -- Build a comma-separated list of stripe URLs
        -- e.g. TO URL = N'...bak_1', URL = N'...bak_2', URL = N'...bak_3'
        SET @StripeList = N''
        SET @i = 1

        WHILE @i <= @StripeCount
        BEGIN
            SET @StripeUrl = @BaseUrl + N'/' + @DbName + N'/'
                           + @DbName + N'_' + @Timestamp
                           + N'_stripe_' + CAST(@i AS NVARCHAR(3))
                           + N'.bak'

            IF @i = 1
                SET @StripeList = N'URL = N''' + @StripeUrl + N''''
            ELSE
                SET @StripeList += N',' + CHAR(13) + N'         URL = N''' + @StripeUrl + N''''

            SET @i += 1
        END

        SET @Msg = 'Starting striped backup: [' + @DbName + '] across '
                 + CAST(@StripeCount AS NVARCHAR) + ' stripes'
        INSERT INTO @Log (Level, Message) VALUES ('INFO', @Msg)
        RAISERROR(@Msg, 0, 1) WITH NOWAIT

        BEGIN TRY
            IF @Compress = 1
                SET @Sql = N'BACKUP DATABASE [' + @DbName + N']
                             TO ' + @StripeList + N'
                             WITH COMPRESSION,
                                  CHECKSUM,
                                  STATS = 10,
                                  NAME = N''' + @DbName + N' - Full Backup (Striped)'';'
            ELSE
                SET @Sql = N'BACKUP DATABASE [' + @DbName + N']
                             TO ' + @StripeList + N'
                             WITH CHECKSUM,
                                  STATS = 10,
                                  NAME = N''' + @DbName + N' - Full Backup (Striped)'';'

            EXEC sp_executesql @Sql

            SET @Msg = 'SUCCESS: [' + @DbName + '] striped backup complete.'
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
    END

    FETCH NEXT FROM db_cursor INTO @DbName, @UseStriping
END

CLOSE db_cursor
DEALLOCATE db_cursor

-- ── SUMMARY ──────────────────────────────────────────────────
DECLARE @Summary NVARCHAR(200) =
    '===== Run complete: ' + CAST(@SuccessCount AS NVARCHAR) + ' succeeded, '
    + CAST(@FailCount AS NVARCHAR) + ' failed ====='

INSERT INTO @Log (Level, Message) VALUES ('INFO', @Summary)
RAISERROR(@Summary, 0, 1) WITH NOWAIT

SELECT LogTime, Level, Message FROM @Log ORDER BY LogTime

IF @FailCount > 0
    RAISERROR('One or more database backups failed. Check the log above.', 16, 1)

-- ============================================================
--  SETUP NOTES
-- ============================================================
--
--  1. STRIPING FLAG
--     Set UseStriping = 1 for any database whose backup
--     exceeds 200GB. Set @StripeCount so each stripe is
--     comfortably under 200GB:
--       500GB backup → @StripeCount = 3  (~167GB per stripe)
--       800GB backup → @StripeCount = 5  (~160GB per stripe)
--       1.5TB DB / ~500GB backup → @StripeCount = 3
--
--     Striped files are named:
--       DbName_YYYYMMDD_HHMMSS_stripe_1.bak
--       DbName_YYYYMMDD_HHMMSS_stripe_2.bak
--       DbName_YYYYMMDD_HHMMSS_stripe_3.bak
--
--     ALL stripe files are required to restore — keep them together.
--
--  2. SAS TOKEN
--     Required permissions: Read, Write, Create.
--     Paste the full token including the leading ? into @SasToken.
--
--  3. CREDENTIAL NAME
--     Must exactly match the container URL (no trailing slash):
--     https://sqlsa.blob.core.windows.net/backup
--
--  4. COMPRESSION
--     Set @Compress = 0 if your SQL Server edition does not
--     support backup compression (e.g. Express).
--
--  5. SQL SERVER AGENT JOB
--     • New Job → Steps → New Step
--     • Type: Transact-SQL script (T-SQL)
--     • Database: master
--     • Paste this script into the command box
--     • Schedule as needed (e.g. nightly at 02:00)
-- ============================================================
