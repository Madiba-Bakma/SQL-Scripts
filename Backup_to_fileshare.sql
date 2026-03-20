-- ============================================================
--  Backup-Databases.sql
--  Backs up 5 SQL Server databases to a UNC fileshare
--  Run as a SQL Server Agent Job step (T-SQL type)
-- ============================================================

-- ── CONFIGURATION ────────────────────────────────────────────
-- Edit these variables before running

DECLARE @BackupRoot     NVARCHAR(500) = N'\\server\share\SQLBackups'  -- UNC path
DECLARE @RetentionDays  INT           = 14                            -- Days to keep old backups
DECLARE @Compress       BIT           = 1                             -- 1 = WITH COMPRESSION (SQL 2008+ Std/Ent)
-- ─────────────────────────────────────────────────────────────

-- ── DATABASE LIST ────────────────────────────────────────────
-- Add / remove your database names here
DECLARE @Databases TABLE (DbName SYSNAME)
INSERT INTO @Databases (DbName) VALUES
    (N'Database1'),
    (N'Database2'),
    (N'Database3'),
    (N'Database4'),
    (N'Database5')
-- ─────────────────────────────────────────────────────────────

-- ── INTERNALS — no edits needed below this line ──────────────
SET NOCOUNT ON

DECLARE @DbName         SYSNAME
DECLARE @Timestamp      NVARCHAR(20)  = CONVERT(NVARCHAR(20), GETDATE(), 112)   -- YYYYMMDD
        + '_'
        + REPLACE(CONVERT(NVARCHAR(8), GETDATE(), 108), ':', '')                -- HHMMSS
DECLARE @BackupPath     NVARCHAR(1000)
DECLARE @DbFolder       NVARCHAR(600)
DECLARE @Sql            NVARCHAR(2000)
DECLARE @Msg            NVARCHAR(1000)
DECLARE @SuccessCount   INT = 0
DECLARE @FailCount      INT = 0
DECLARE @ErrorMsg       NVARCHAR(2048)

-- Log table (session-scoped — visible in job output)
DECLARE @Log TABLE (
    LogTime     DATETIME        DEFAULT GETDATE(),
    Level       NVARCHAR(10),
    Message     NVARCHAR(2000)
)

INSERT INTO @Log (Level, Message)
VALUES ('INFO', '===== Backup run started on ' + @@SERVERNAME + ' =====')

-- ── BACKUP LOOP ──────────────────────────────────────────────
DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT DbName FROM @Databases

OPEN db_cursor
FETCH NEXT FROM db_cursor INTO @DbName

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Build folder and file path
    -- Note: SQL Server Agent must have write access to the UNC share,
    --       or the share must be accessible under the SQL Server service account.
    SET @DbFolder   = @BackupRoot + N'\' + @DbName
    SET @BackupPath = @DbFolder + N'\' + @DbName + N'_' + @Timestamp + N'.bak'

    -- Create the per-database subfolder via xp_cmdshell
    -- (xp_cmdshell must be enabled — see note at bottom of script)
    SET @Sql = N'EXEC xp_cmdshell ''mkdir "' + @DbFolder + N'" 2>nul'', no_output'
    BEGIN TRY
        EXEC sp_executesql @Sql
    END TRY
    BEGIN CATCH
        -- Non-fatal: folder may already exist
    END CATCH

    -- Run the backup
    SET @Msg = 'Starting backup: [' + @DbName + '] → ' + @BackupPath
    INSERT INTO @Log (Level, Message) VALUES ('INFO', @Msg)
    RAISERROR(@Msg, 0, 1) WITH NOWAIT

    BEGIN TRY
        IF @Compress = 1
            SET @Sql = N'BACKUP DATABASE [' + @DbName + N']
                         TO DISK = N''' + @BackupPath + N'''
                         WITH COMPRESSION,
                              CHECKSUM,
                              STATS = 10,
                              NAME = N''' + @DbName + N' - Full Backup'';'
        ELSE
            SET @Sql = N'BACKUP DATABASE [' + @DbName + N']
                         TO DISK = N''' + @BackupPath + N'''
                         WITH CHECKSUM,
                              STATS = 10,
                              NAME = N''' + @DbName + N' - Full Backup'';'

        EXEC sp_executesql @Sql

        SET @Msg = 'SUCCESS: [' + @DbName + ']'
        INSERT INTO @Log (Level, Message) VALUES ('INFO', @Msg)
        RAISERROR(@Msg, 0, 1) WITH NOWAIT
        SET @SuccessCount += 1

    END TRY
    BEGIN CATCH
        SET @ErrorMsg = 'FAILED: [' + @DbName + '] — ' + ERROR_MESSAGE()
        INSERT INTO @Log (Level, Message) VALUES ('ERROR', @ErrorMsg)
        RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT
        SET @FailCount += 1
    END CATCH

    FETCH NEXT FROM db_cursor INTO @DbName
END

CLOSE db_cursor
DEALLOCATE db_cursor

-- ── RETENTION CLEANUP ────────────────────────────────────────
-- Delete .bak files older than @RetentionDays using xp_cmdshell
DECLARE @CleanupCmd NVARCHAR(1000)
SET @CleanupCmd = 
    N'EXEC xp_cmdshell ''forfiles /P "' + @BackupRoot + 
    N'" /S /M *.bak /D -' + CAST(@RetentionDays AS NVARCHAR(5)) + 
    N' /C "cmd /c del @path" 2>nul'', no_output'

INSERT INTO @Log (Level, Message)
VALUES ('INFO', 'Running retention cleanup (>' + CAST(@RetentionDays AS NVARCHAR) + ' days)...')

BEGIN TRY
    EXEC sp_executesql @CleanupCmd
    INSERT INTO @Log (Level, Message) VALUES ('INFO', 'Retention cleanup complete.')
END TRY
BEGIN CATCH
    INSERT INTO @Log (Level, Message)
    VALUES ('WARN', 'Retention cleanup error: ' + ERROR_MESSAGE())
END CATCH

-- ── SUMMARY ──────────────────────────────────────────────────
DECLARE @Summary NVARCHAR(200) =
    '===== Run complete: ' + CAST(@SuccessCount AS NVARCHAR) + ' succeeded, '
    + CAST(@FailCount AS NVARCHAR) + ' failed ====='

INSERT INTO @Log (Level, Message) VALUES ('INFO', @Summary)
RAISERROR(@Summary, 0, 1) WITH NOWAIT

-- Print full log
SELECT LogTime, Level, Message FROM @Log ORDER BY LogTime

-- Fail the job step if any backup failed
IF @FailCount > 0
    RAISERROR('One or more database backups failed. Check the log above.', 16, 1)

-- ============================================================
--  SETUP NOTES
-- ============================================================
--
--  1. xp_cmdshell
--     Required for mkdir and forfiles (folder creation + cleanup).
--     Enable it once with:
--
--       EXEC sp_configure 'show advanced options', 1; RECONFIGURE;
--       EXEC sp_configure 'xp_cmdshell', 1;         RECONFIGURE;
--
--  2. UNC share permissions
--     The SQL Server service account (or Agent proxy account) must have
--     Read/Write access to \\server\share.
--     Check via:  Services → SQL Server → Log On As account.
--
--  3. SQL Server Agent Job setup
--     • New Job → Steps → New Step
--     • Type: Transact-SQL script (T-SQL)
--     • Database: master
--     • Paste this script into the command box
--     • Schedule as needed (e.g. nightly at 02:00)
--
--  4. Compression
--     Set @Compress = 0 if using SQL Server Express or an edition
--     that does not support backup compression.
-- ============================================================
