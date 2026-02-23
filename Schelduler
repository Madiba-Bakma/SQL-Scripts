USE msdb;
SET NOCOUNT ON;

--------------------------------------------------------------------------------
-- PARAMETERS (EDIT THESE)
--------------------------------------------------------------------------------
DECLARE @Role SYSNAME = N'PRIMARY';  -- 'PRIMARY' or 'SECONDARY'

-- One-time start time (SERVER local time)
DECLARE @OneTimeRunDate_YYYYMMDD INT = 20260221; -- YYYYMMDD
DECLARE @OneTimeRunTime_HHMMSS   INT = 170000;   -- HHMMSS (170000 = 5pm)

-- 1 = enable the jobs at start time
DECLARE @EnableJobs BIT = 1;

-- 1 = also start them immediately at start time (recommended for “kick off once”)
DECLARE @AlsoStartJobs BIT = 1;

-- Job name patterns (default log shipping naming)
DECLARE @PrimaryPattern SYSNAME = N'LSBackup[_]%';
DECLARE @CopyPattern    SYSNAME = N'LSCopy[_]%';
DECLARE @RestorePattern SYSNAME = N'LSRestore[_]%';

--------------------------------------------------------------------------------
-- INTERNALS
--------------------------------------------------------------------------------
DECLARE @SchedulerJobName SYSNAME =
  CASE WHEN UPPER(@Role)=N'PRIMARY'
       THEN N'ONE-TIME Start Log Shipping (Primary LSBackup)'
       ELSE N'ONE-TIME Start Log Shipping (Secondary LSCopy/LSRestore)'
  END;

DECLARE @Cmd NVARCHAR(MAX);
DECLARE @StartLine NVARCHAR(2000) = N'';
DECLARE @EnableLine NVARCHAR(2000) = N'';

IF @AlsoStartJobs = 1
  SET @StartLine = N'    EXEC msdb.dbo.sp_start_job @job_name=@job;' + CHAR(13)+CHAR(10);

IF @EnableJobs = 1
  SET @EnableLine = N'    EXEC msdb.dbo.sp_update_job @job_name=@job, @enabled=1;' + CHAR(13)+CHAR(10);

-- Build the job step command (token based: stable + readable)
IF UPPER(@Role)=N'PRIMARY'
BEGIN
  SET @Cmd = N'
/* Enable/start LSBackup jobs (PRIMARY) */
DECLARE @job SYSNAME;

DECLARE c CURSOR LOCAL FAST_FORWARD FOR
SELECT name
FROM msdb.dbo.sysjobs
WHERE name LIKE N''LSBackup[_]%'';  -- matches LSBackup_*

OPEN c;
FETCH NEXT FROM c INTO @job;

WHILE @@FETCH_STATUS = 0
BEGIN
{{ENABLELINE}}{{STARTLINE}}
    PRINT ''Processed: '' + @job;
    FETCH NEXT FROM c INTO @job;
END

CLOSE c;
DEALLOCATE c;

/* Self-delete (so this runs once only) */
EXEC msdb.dbo.sp_delete_job @job_name = N''{{SELFJOB}}'';
';
END
ELSE
BEGIN
  SET @Cmd = N'
/* Enable/start LSCopy + LSRestore jobs (SECONDARY) */
DECLARE @job SYSNAME;

DECLARE c CURSOR LOCAL FAST_FORWARD FOR
SELECT name
FROM msdb.dbo.sysjobs
WHERE name LIKE N''LSCopy[_]%''     -- matches LSCopy_*
   OR name LIKE N''LSRestore[_]%''; -- matches LSRestore_*

OPEN c;
FETCH NEXT FROM c INTO @job;

WHILE @@FETCH_STATUS = 0
BEGIN
{{ENABLELINE}}{{STARTLINE}}
    PRINT ''Processed: '' + @job;
    FETCH NEXT FROM c INTO @job;
END

CLOSE c;
DEALLOCATE c;

/* Self-delete (so this runs once only) */
EXEC msdb.dbo.sp_delete_job @job_name = N''{{SELFJOB}}'';
';
END

-- Replace tokens
SET @Cmd = REPLACE(@Cmd, N'{{ENABLELINE}}', @EnableLine);
SET @Cmd = REPLACE(@Cmd, N'{{STARTLINE}}',  @StartLine);
SET @Cmd = REPLACE(@Cmd, N'{{SELFJOB}}',    REPLACE(@SchedulerJobName,'''',''''''));

--------------------------------------------------------------------------------
-- CREATE / REPLACE THE ONE-TIME SCHEDULER JOB
--------------------------------------------------------------------------------
BEGIN TRY
  -- Idempotent: remove existing scheduler job if present
  IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = @SchedulerJobName)
  BEGIN
    EXEC msdb.dbo.sp_delete_job @job_name = @SchedulerJobName;
    PRINT 'Removed existing scheduler job: ' + @SchedulerJobName;
  END

  DECLARE @JobId UNIQUEIDENTIFIER;

  EXEC msdb.dbo.sp_add_job
      @job_name = @SchedulerJobName,
      @enabled = 1,
      @description = N'One-time job to enable/start Log Shipping jobs, then self-delete.',
      @job_id = @JobId OUTPUT;

  EXEC msdb.dbo.sp_add_jobstep
      @job_id = @JobId,
      @step_id = 1,
      @step_name = N'Enable/Start LS jobs then self-delete',
      @subsystem = N'TSQL',
      @database_name = N'msdb',
      @command = @Cmd;

  EXEC msdb.dbo.sp_add_jobschedule
      @job_id = @JobId,
      @name = N'OneTimeStart',
      @enabled = 1,
      @freq_type = 1, -- one-time
      @active_start_date = @OneTimeRunDate_YYYYMMDD,
      @active_start_time = @OneTimeRunTime_HHMMSS;

  EXEC msdb.dbo.sp_add_jobserver @job_id = @JobId;

  PRINT '===========================================================';
  PRINT 'ONE-TIME LOG SHIPPING SCHEDULER JOB CREATED';
  PRINT 'Job Name : ' + @SchedulerJobName;
  PRINT 'Role     : ' + @Role;
  PRINT 'Start    : ' + CAST(@OneTimeRunDate_YYYYMMDD AS VARCHAR(8)) + ' ' + RIGHT('000000'+CAST(@OneTimeRunTime_HHMMSS AS VARCHAR(6)),6);
  PRINT 'Enable   : ' + CASE WHEN @EnableJobs=1 THEN 'YES' ELSE 'NO' END;
  PRINT 'StartNow : ' + CASE WHEN @AlsoStartJobs=1 THEN 'YES' ELSE 'NO' END;
  PRINT 'NEXT: Do nothing. At the start time it will run once and delete itself.';
  PRINT 'OPTIONAL: Start it now manually:';
  PRINT '  EXEC msdb.dbo.sp_start_job @job_name = N''' + @SchedulerJobName + ''';';
  PRINT '===========================================================';
END TRY
BEGIN CATCH
  PRINT 'FAILED to create scheduler job.';
  PRINT ERROR_MESSAGE();
  THROW;
END CATCH;
