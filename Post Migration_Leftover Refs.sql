/*==============================================================
  FULL SWEEP FOR LEFTOVER REFERENCES (Compat-safe; no STRING_SPLIT/CONCAT)
  Environment: SQL Server 2019 (works across lower compat levels too)
  Author: Madiba 

  Terms are searched literally (curly braces included).
================================================================*/

SET NOCOUNT ON;

/*==============================================================
  0) TERMS TABLE (global for use everywhere)
==============================================================*/
IF OBJECT_ID('tempdb..##Terms') IS NOT NULL DROP TABLE ##Terms;
CREATE TABLE ##Terms (Term NVARCHAR(200) NOT NULL);

INSERT INTO ##Terms (Term) VALUES
(N''),
(N''),
(N''),
(N''),
(N''),
(N'');

PRINT N'Using your defined search terms';

/*==============================================================
  1) OBJECT DEFINITIONS (procs, views, functions, triggers) + SYNONYMS
==============================================================*/
IF OBJECT_ID('tempdb..#ObjHits') IS NOT NULL DROP TABLE #ObjHits;
CREATE TABLE #ObjHits (
    DatabaseName sysname,
    SchemaName sysname NULL,
    ObjectName sysname,
    ObjectType NVARCHAR(60),
    HitTerm NVARCHAR(200),
    ObjectDefinition NVARCHAR(MAX)
);

DECLARE @DB1 sysname, @SQL1 NVARCHAR(MAX);

DECLARE dbcur1 CURSOR LOCAL FAST_FORWARD FOR
SELECT name
FROM sys.databases
WHERE database_id > 4  -- user DBs
  AND state = 0;       -- online

OPEN dbcur1; 
FETCH NEXT FROM dbcur1 INTO @DB1;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @SQL1 = N'
    USE ' + QUOTENAME(@DB1) + N';

    /* Local copy of terms (prevents tempdb cross-db warning) */
    IF OBJECT_ID(''tempdb..#Terms'') IS NOT NULL DROP TABLE #Terms;
    CREATE TABLE #Terms (Term NVARCHAR(200) NOT NULL);
    INSERT INTO #Terms (Term) SELECT Term FROM ##Terms;

    /* Object definitions */
    INSERT INTO #ObjHits (DatabaseName, SchemaName, ObjectName, ObjectType, HitTerm, ObjectDefinition)
    SELECT
        DB_NAME() AS DatabaseName,
        s.name    AS SchemaName,
        o.name    AS ObjectName,
        o.type_desc AS ObjectType,
        t.Term    AS HitTerm,
        sm.definition AS ObjectDefinition
    FROM sys.objects AS o
    JOIN sys.sql_modules AS sm ON o.object_id = sm.object_id
    JOIN sys.schemas AS s ON o.schema_id = s.schema_id
    CROSS JOIN #Terms AS t
    WHERE sm.definition COLLATE DATABASE_DEFAULT LIKE N''%'' + t.Term COLLATE DATABASE_DEFAULT + N''%'';

    /* Synonyms (base_object_name can include server.db.schema) */
    INSERT INTO #ObjHits (DatabaseName, SchemaName, ObjectName, ObjectType, HitTerm, ObjectDefinition)
    SELECT
        DB_NAME(),
        sch.name,
        sy.name,
        N''SYNONYM'',
        t.Term,
        CAST(sy.base_object_name AS NVARCHAR(MAX))
    FROM sys.synonyms AS sy
    JOIN sys.schemas AS sch ON sy.schema_id = sch.schema_id
    CROSS JOIN #Terms AS t
    WHERE CAST(sy.base_object_name AS NVARCHAR(MAX)) COLLATE DATABASE_DEFAULT LIKE N''%'' + t.Term COLLATE DATABASE_DEFAULT + N''%'';';
    EXEC sys.sp_executesql @SQL1;

    FETCH NEXT FROM dbcur1 INTO @DB1;
END

CLOSE dbcur1; 
DEALLOCATE dbcur1;

/*==============================================================
  2) SQL AGENT JOBS (MSDB)
==============================================================*/
IF OBJECT_ID('tempdb..#JobHits') IS NOT NULL DROP TABLE #JobHits;
CREATE TABLE #JobHits (
    JobName sysname,
    StepID INT,
    StepName sysname,
    SubSystem NVARCHAR(60),
    HitTerm NVARCHAR(200),
    Command NVARCHAR(MAX)
);

USE msdb;

INSERT INTO #JobHits (JobName, StepID, StepName, SubSystem, HitTerm, Command)
SELECT 
    j.name,
    s.step_id,
    s.step_name,
    s.subsystem,
    t.Term,
    s.command
FROM msdb.dbo.sysjobsteps AS s
JOIN msdb.dbo.sysjobs AS j ON s.job_id = j.job_id
CROSS JOIN ##Terms AS t
WHERE s.command COLLATE DATABASE_DEFAULT LIKE N'%' + t.Term COLLATE DATABASE_DEFAULT + N'%';

/*==============================================================
  3) LINKED SERVERS (MASTER)
==============================================================*/
IF OBJECT_ID('tempdb..#LinkedHits') IS NOT NULL DROP TABLE #LinkedHits;
CREATE TABLE #LinkedHits (
    ServerName sysname,
    DataSource NVARCHAR(4000),
    Provider NVARCHAR(4000),
    Product NVARCHAR(4000),
    HitTerm NVARCHAR(200)
);

USE master;

INSERT INTO #LinkedHits (ServerName, DataSource, Provider, Product, HitTerm)
SELECT 
    s.name,
    s.data_source,
    s.provider,
    s.product,
    t.Term
FROM sys.servers AS s
CROSS JOIN ##Terms AS t
WHERE (ISNULL(s.data_source,N'') + N'|' + ISNULL(s.name,N'') + N'|' + ISNULL(s.product,N'')) 
      COLLATE DATABASE_DEFAULT LIKE N'%' + t.Term COLLATE DATABASE_DEFAULT + N'%';

/*==============================================================
  4) EXTERNAL OBJECTS PER DATABASE (robust to feature differences)
==============================================================*/
IF OBJECT_ID('tempdb..#ExternalHits') IS NOT NULL DROP TABLE #ExternalHits;
CREATE TABLE #ExternalHits (
    DatabaseName sysname,
    ObjectType NVARCHAR(60),
    ObjectName sysname,
    PropertyName NVARCHAR(100),
    HitValue NVARCHAR(MAX),
    HitTerm NVARCHAR(200)
);

DECLARE @DB2 sysname, @SQL2 NVARCHAR(MAX);
DECLARE dbcur2 CURSOR LOCAL FAST_FORWARD FOR
SELECT name
FROM sys.databases
WHERE database_id > 4 AND state = 0;

OPEN dbcur2; 
FETCH NEXT FROM dbcur2 INTO @DB2;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @SQL2 = N'
    USE ' + QUOTENAME(@DB2) + N';

    BEGIN TRY
        /* Local copy of terms */
        IF OBJECT_ID(''tempdb..#Terms'') IS NOT NULL DROP TABLE #Terms;
        CREATE TABLE #Terms (Term NVARCHAR(200) NOT NULL);
        INSERT INTO #Terms (Term) SELECT Term FROM ##Terms;

        /* external data sources */
        BEGIN TRY
            INSERT INTO #ExternalHits (DatabaseName, ObjectType, ObjectName, PropertyName, HitValue, HitTerm)
            SELECT DB_NAME(), N''EXTERNAL_DATA_SOURCE'', eds.name, N''location'', eds.location, t.Term
            FROM sys.external_data_sources AS eds
            CROSS JOIN #Terms AS t
            WHERE ISNULL(eds.location, N'''') COLLATE DATABASE_DEFAULT LIKE N''%'' + t.Term COLLATE DATABASE_DEFAULT + N''%'';
        END TRY BEGIN CATCH END CATCH;

        /* external file formats (handle if "format_options" is absent) */
        BEGIN TRY
            DECLARE @HasFormatOptions BIT;
            SET @HasFormatOptions = CASE 
                WHEN EXISTS (
                    SELECT 1
                    FROM sys.all_objects o
                    JOIN sys.all_columns c ON c.object_id = o.object_id
                    WHERE o.name = N''external_file_formats''
                      AND c.name = N''format_options''
                ) THEN 1 ELSE 0 END;

            IF @HasFormatOptions = 1
            BEGIN
                INSERT INTO #ExternalHits (DatabaseName, ObjectType, ObjectName, PropertyName, HitValue, HitTerm)
                SELECT DB_NAME(), N''EXTERNAL_FILE_FORMAT'', eff.name, N''format_options'', eff.format_options, t.Term
                FROM sys.external_file_formats AS eff
                CROSS JOIN #Terms AS t
                WHERE ISNULL(eff.format_options, N'''') COLLATE DATABASE_DEFAULT LIKE N''%'' + t.Term COLLATE DATABASE_DEFAULT + N''%''
                   OR eff.name COLLATE DATABASE_DEFAULT LIKE N''%'' + t.Term COLLATE DATABASE_DEFAULT + N''%'';
            END
            ELSE
            BEGIN
                INSERT INTO #ExternalHits (DatabaseName, ObjectType, ObjectName, PropertyName, HitValue, HitTerm)
                SELECT DB_NAME(), N''EXTERNAL_FILE_FORMAT'', eff.name, N''name'', eff.name, t.Term
                FROM sys.external_file_formats AS eff
                CROSS JOIN #Terms AS t
                WHERE eff.name COLLATE DATABASE_DEFAULT LIKE N''%'' + t.Term COLLATE DATABASE_DEFAULT + N''%'';
            END
        END TRY BEGIN CATCH END CATCH;

        /* database-scoped credentials (SQL 2019: column = credential_identity) */
        BEGIN TRY
            INSERT INTO #ExternalHits (DatabaseName, ObjectType, ObjectName, PropertyName, HitValue, HitTerm)
            SELECT DB_NAME(), N''DB_SCOPED_CREDENTIAL'', dsc.name, N''credential_identity'', dsc.credential_identity, t.Term
            FROM sys.database_scoped_credentials AS dsc
            CROSS JOIN #Terms AS t
            WHERE (ISNULL(dsc.name,N'''') + N''|'' + ISNULL(dsc.credential_identity,N'''')) COLLATE DATABASE_DEFAULT 
                  LIKE N''%'' + t.Term COLLATE DATABASE_DEFAULT + N''%'';
        END TRY BEGIN CATCH END CATCH;

        /* service broker routes */
        BEGIN TRY
            INSERT INTO #ExternalHits (DatabaseName, ObjectType, ObjectName, PropertyName, HitValue, HitTerm)
            SELECT DB_NAME(), N''SERVICE_BROKER_ROUTE'', r.name, N''address'', CAST(r.address AS NVARCHAR(MAX)), t.Term
            FROM sys.routes AS r
            CROSS JOIN #Terms AS t
            WHERE ISNULL(r.address, N'''') COLLATE DATABASE_DEFAULT LIKE N''%'' + t.Term COLLATE DATABASE_DEFAULT + N''%'';
        END TRY BEGIN CATCH END CATCH;

    END TRY BEGIN CATCH END CATCH;';
    EXEC sys.sp_executesql @SQL2;

    FETCH NEXT FROM dbcur2 INTO @DB2;
END

CLOSE dbcur2; 
DEALLOCATE dbcur2;

/*==============================================================
  5) SSIS PACKAGES IN MSDB (legacy SSIS package store)
     - IMAGE -> VARBINARY(MAX) -> NVARCHAR(MAX)
==============================================================*/
IF OBJECT_ID('tempdb..#SSISHits') IS NOT NULL DROP TABLE #SSISHits;
CREATE TABLE #SSISHits (
    FolderName sysname NULL,
    PackageName sysname,
    HitTerm NVARCHAR(200)
);

USE msdb;

IF EXISTS (SELECT 1 FROM msdb.sys.objects WHERE name = 'sysssispackages')
BEGIN
    INSERT INTO #SSISHits (FolderName, PackageName, HitTerm)
    SELECT 
        f.foldername AS FolderName,
        p.name       AS PackageName,
        t.Term
    FROM msdb.dbo.sysssispackages AS p
    LEFT JOIN msdb.dbo.sysssispackagefolders AS f ON p.folderid = f.folderid
    CROSS JOIN ##Terms AS t
    WHERE CONVERT(NVARCHAR(MAX), CONVERT(VARBINARY(MAX), p.packagedata)) 
              COLLATE DATABASE_DEFAULT LIKE N'%' + t.Term COLLATE DATABASE_DEFAULT + N'%';
END
ELSE
BEGIN
    PRINT 'No sysssispackages table found in msdb (skipping SSIS search).';
END

/*==============================================================
  6) STRING COLUMNS IN TABLES (generate PreviewQuery strings)
==============================================================*/
IF OBJECT_ID('tempdb..#TableSearch') IS NOT NULL DROP TABLE #TableSearch;
CREATE TABLE #TableSearch (
    DatabaseName sysname,
    SchemaName sysname,
    TableName sysname,
    ColumnName sysname,
    HitTerm NVARCHAR(200),
    PreviewQuery NVARCHAR(MAX)
);

DECLARE @DB3 sysname, @SQL3 NVARCHAR(MAX);

DECLARE dbcur3 CURSOR LOCAL FAST_FORWARD FOR
SELECT name
FROM sys.databases
WHERE database_id > 4 AND state = 0;

OPEN dbcur3; 
FETCH NEXT FROM dbcur3 INTO @DB3;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @SQL3 = N'
    USE ' + QUOTENAME(@DB3) + N';

    /* Local copy of terms */
    IF OBJECT_ID(''tempdb..#Terms'') IS NOT NULL DROP TABLE #Terms;
    CREATE TABLE #Terms (Term NVARCHAR(200) NOT NULL);
    INSERT INTO #Terms (Term) SELECT Term FROM ##Terms;

    INSERT INTO #TableSearch (DatabaseName, SchemaName, TableName, ColumnName, HitTerm, PreviewQuery)
    SELECT
        DB_NAME()                               AS DatabaseName,
        sch.name                                AS SchemaName,
        tbl.name                                AS TableName,
        col.name                                AS ColumnName,
        t.Term                                  AS HitTerm,
        /* Build a runnable preview SELECT with the term safely inlined (no CONCAT) */
        N''SELECT TOP (50) ['' + col.name + N''] AS MatchedValue, * FROM '' 
        + QUOTENAME(DB_NAME()) + N''.'' + QUOTENAME(sch.name) + N''.'' + QUOTENAME(tbl.name) 
        + N'' WHERE ['' + col.name + N''] COLLATE DATABASE_DEFAULT LIKE N''''%'' 
        + REPLACE(CAST(t.Term AS NVARCHAR(MAX)), N'''''''', N'''''''''''') 
        + N''%'''';''
    FROM sys.columns AS col
    JOIN sys.tables  AS tbl ON col.object_id = tbl.object_id
    JOIN sys.schemas AS sch ON tbl.schema_id = sch.schema_id
    CROSS JOIN #Terms AS t
    WHERE tbl.is_ms_shipped = 0
      AND col.system_type_id IN (167,175,231,239);  -- char,varchar,nchar,nvarchar
    ';
    EXEC sys.sp_executesql @SQL3;

    FETCH NEXT FROM dbcur3 INTO @DB3;
END

CLOSE dbcur3; 
DEALLOCATE dbcur3;

/*==============================================================
  7) SERVER-LEVEL EXTRAS
     - endpoints (names)
     - credentials (names)
     - proxies (msdb) 
     - extended events (session names and filename fields)
==============================================================*/
IF OBJECT_ID('tempdb..#ServerLevelHits') IS NOT NULL DROP TABLE #ServerLevelHits;
CREATE TABLE #ServerLevelHits (
    ScopeDB sysname NULL,    -- master/msdb
    ObjectType NVARCHAR(60),
    ObjectName sysname,
    PropertyName NVARCHAR(100),
    HitValue NVARCHAR(MAX),
    HitTerm NVARCHAR(200)
);

/* Endpoints (names) */
USE master;

INSERT INTO #ServerLevelHits (ScopeDB, ObjectType, ObjectName, PropertyName, HitValue, HitTerm)
SELECT DB_NAME(), N'ENDPOINT', e.name, N'name', e.name, t.Term
FROM sys.endpoints AS e
CROSS JOIN ##Terms AS t
WHERE e.name COLLATE DATABASE_DEFAULT LIKE N'%' + t.Term COLLATE DATABASE_DEFAULT + N'%';

/* Server Credentials */
INSERT INTO #ServerLevelHits (ScopeDB, ObjectType, ObjectName, PropertyName, HitValue, HitTerm)
SELECT DB_NAME(), N'CREDENTIAL', c.name, N'name', c.name, t.Term
FROM sys.credentials AS c
CROSS JOIN ##Terms AS t
WHERE c.name COLLATE DATABASE_DEFAULT LIKE N'%' + t.Term COLLATE DATABASE_DEFAULT + N'%';

/* Proxies (msdb) */
USE msdb;

INSERT INTO #ServerLevelHits (ScopeDB, ObjectType, ObjectName, PropertyName, HitValue, HitTerm)
SELECT DB_NAME(), N'PROXY', p.name, N'name', p.name, t.Term
FROM msdb.dbo.sysproxies AS p
CROSS JOIN ##Terms AS t
WHERE p.name COLLATE DATABASE_DEFAULT LIKE N'%' + t.Term COLLATE DATABASE_DEFAULT + N'%';

/* Extended Events (names + file target filenames) */
USE master;

/* XE session names */
INSERT INTO #ServerLevelHits (ScopeDB, ObjectType, ObjectName, PropertyName, HitValue, HitTerm)
SELECT DB_NAME(), N'XE_SESSION', s.name, N'name', s.name, t.Term
FROM sys.server_event_sessions AS s
CROSS JOIN ##Terms AS t
WHERE s.name COLLATE DATABASE_DEFAULT LIKE N'%' + t.Term COLLATE DATABASE_DEFAULT + N'%';

/* XE file target 'filename' (sql_variant -> NVARCHAR before COLLATE) */
INSERT INTO #ServerLevelHits (ScopeDB, ObjectType, ObjectName, PropertyName, HitValue, HitTerm)
SELECT DB_NAME(), N'XE_SESSION', s.name, N'filename',
       CAST(f.value AS NVARCHAR(4000)) AS HitValue,
       t.Term
FROM sys.server_event_sessions AS s
JOIN sys.server_event_session_fields AS f
  ON s.event_session_id = f.event_session_id
CROSS JOIN ##Terms AS t
WHERE f.name = N'filename'
  AND CAST(f.value AS NVARCHAR(4000)) COLLATE DATABASE_DEFAULT
      LIKE N'%' + t.Term COLLATE DATABASE_DEFAULT + N'%';

/*==============================================================
  OUTPUT
==============================================================*/
PRINT '========= OBJECT DEFINITIONS & SYNONYMS =========';
SELECT * FROM #ObjHits
ORDER BY DatabaseName, SchemaName, ObjectName, HitTerm;

PRINT '========= SQL AGENT JOBS =========';
SELECT * FROM #JobHits
ORDER BY JobName, StepID, HitTerm;

PRINT '========= LINKED SERVERS =========';
SELECT * FROM #LinkedHits
ORDER BY ServerName, HitTerm;

PRINT '========= EXTERNAL OBJECTS =========';
SELECT * FROM #ExternalHits
ORDER BY DatabaseName, ObjectType, ObjectName, HitTerm;

PRINT '========= SSIS PACKAGES (MSDB) =========';
SELECT * FROM #SSISHits
ORDER BY FolderName, PackageName, HitTerm;

PRINT '========= STRING COLUMNS – PREVIEW QUERIES =========';
SELECT * FROM #TableSearch
ORDER BY DatabaseName, SchemaName, TableName, ColumnName, HitTerm;

PRINT '========= SERVER-LEVEL EXTRAS =========';
SELECT * FROM #ServerLevelHits
ORDER BY ObjectType, ObjectName, HitTerm;

-- (Temp tables will be dropped when the session ends)
