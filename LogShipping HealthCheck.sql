/*
===============================================================================
SQL Server Log Shipping Health Check Script
Author: DBA Production Use
Purpose:
    - Validate Log Shipping configuration
    - Detect latency issues
    - Detect restore/copy failures
    - Show last activity timestamps
    - Identify potential alerts

Safe for Production (Read-only system views only)
===============================================================================
*/

SET NOCOUNT ON;

DECLARE @CurrentTime DATETIME = GETDATE();

PRINT '===========================================================';
PRINT 'SQL SERVER LOG SHIPPING HEALTH CHECK';
PRINT 'Run Time: ' + CONVERT(VARCHAR(30), @CurrentTime, 120);
PRINT '===========================================================';


/*
===============================================================================
SECTION 1 – Primary Server Status
===============================================================================
*/
PRINT '';
PRINT '--- PRIMARY SERVER STATUS ---';

SELECT  
    p.primary_database,
    p.backup_threshold AS Backup_Threshold_Minutes,
    p.threshold_alert AS Alert_ID,
    mp.last_backup_date,
    DATEDIFF(MINUTE, mp.last_backup_date, @CurrentTime) AS Minutes_Since_Last_Backup,
    CASE 
        WHEN DATEDIFF(MINUTE, mp.last_backup_date, @CurrentTime) > p.backup_threshold
            THEN '? BACKUP THRESHOLD EXCEEDED'
        ELSE 'OK'
    END AS Backup_Status
FROM msdb.dbo.log_shipping_primary_databases p
LEFT JOIN msdb.dbo.log_shipping_monitor_primary mp
    ON p.primary_database = mp.primary_database;


/*
===============================================================================
SECTION 2 – Secondary Server Status
===============================================================================
*/
PRINT '';
PRINT '--- SECONDARY SERVER STATUS ---';

SELECT  
    s.secondary_database,
    ms.last_copied_date,
    DATEDIFF(MINUTE, ms.last_copied_date, @CurrentTime) AS Minutes_Since_Last_Copy,
    ms.last_restored_date,
    DATEDIFF(MINUTE, ms.last_restored_date, @CurrentTime) AS Minutes_Since_Last_Restore,
    s.restore_threshold AS Restore_Threshold_Minutes,
    CASE 
        WHEN DATEDIFF(MINUTE, ms.last_restored_date, @CurrentTime) > s.restore_threshold
            THEN '? RESTORE THRESHOLD EXCEEDED'
        ELSE 'OK'
    END AS Restore_Status,
    ms.last_restored_file,
    ms.last_restored_latency
FROM msdb.dbo.log_shipping_secondary_databases s
LEFT JOIN msdb.dbo.log_shipping_monitor_secondary ms
    ON s.secondary_database = ms.secondary_database;


/*
===============================================================================
SECTION 3 – Job Status (Backup, Copy, Restore Jobs)
===============================================================================
*/
PRINT '';
PRINT '--- LOG SHIPPING JOB STATUS ---';

SELECT  
    j.name AS Job_Name,
    j.enabled,
    CASE h.run_status
        WHEN 0 THEN 'FAILED'
        WHEN 1 THEN 'SUCCEEDED'
        WHEN 2 THEN 'RETRY'
        WHEN 3 THEN 'CANCELLED'
        ELSE 'UNKNOWN'
    END AS Last_Run_Status,
    msdb.dbo.agent_datetime(h.run_date, h.run_time) AS Last_Run_DateTime
FROM msdb.dbo.sysjobs j
LEFT JOIN msdb.dbo.sysjobhistory h
    ON j.job_id = h.job_id
    AND h.step_id = 0
WHERE j.name LIKE '%log shipping%'
AND h.instance_id = (
    SELECT MAX(instance_id)
    FROM msdb.dbo.sysjobhistory h2
    WHERE h2.job_id = j.job_id
    AND h2.step_id = 0
);


/*
===============================================================================
SECTION 4 – Overall Synchronization Health Summary
===============================================================================
*/

PRINT '';
PRINT '--- OVERALL SYNCHRONIZATION SUMMARY ---';

SELECT  
    secondary_database,
    last_restored_date,
    DATEDIFF(MINUTE, last_restored_date, @CurrentTime) AS Delay_Minutes,
    last_restored_latency AS Latency_Minutes,
    CASE 
        WHEN last_restored_latency > 30 THEN '? HIGH LATENCY'
        ELSE 'OK'
    END AS Latency_Status
FROM msdb.dbo.log_shipping_monitor_secondary;


/*
===============================================================================
SECTION 5 – Orphaned or Missing Log Files Check
===============================================================================
*/

PRINT '';
PRINT '--- FILE GAP CHECK (Potential Missing Log Files) ---';

SELECT  
    secondary_database,
    last_copied_file,
    last_restored_file,
    CASE 
        WHEN last_copied_file <> last_restored_file
            THEN '? COPY/RESTORE FILE MISMATCH'
        ELSE 'OK'
    END AS File_Status
FROM msdb.dbo.log_shipping_monitor_secondary;


PRINT '';
PRINT '===========================================================';
PRINT 'HEALTH CHECK COMPLETED';
PRINT 'Review any rows marked with ?';
PRINT '===========================================================';
