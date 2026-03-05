SELECT
'USE ' + QUOTENAME(DB_NAME()) + '; ' +
'GRANT ' + dp.permission_name COLLATE DATABASE_DEFAULT +
' ON ' +
QUOTENAME(SCHEMA_NAME(o.schema_id) COLLATE DATABASE_DEFAULT) + '.' +
QUOTENAME(o.name COLLATE DATABASE_DEFAULT) +
' TO ' + 
QUOTENAME(USER_NAME(dp.grantee_principal_id) COLLATE DATABASE_DEFAULT) +
';'

FROM sys.database_permissions dp
JOIN sys.objects o
ON dp.major_id = o.object_id

WHERE dp.class = 1

--------------------------------------------------------------------------------------------------------------------------------------------
                                                            --OR--
-----------------------------------------------------------------------------------------------------------------------------------------------

SET NOCOUNT ON;

DECLARE @DBName SYSNAME;
DECLARE @SQL NVARCHAR(MAX);

DECLARE db_cursor CURSOR FOR
SELECT name
FROM sys.databases
WHERE database_id > 4
AND state_desc = 'ONLINE';

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @DBName;

WHILE @@FETCH_STATUS = 0
BEGIN

SET @SQL = '
USE ' + QUOTENAME(@DBName) + ';

SELECT
''USE ' + QUOTENAME(@DBName) + '; '' +
CASE dp.state
    WHEN ''W'' THEN ''GRANT''
    WHEN ''G'' THEN ''GRANT''
    WHEN ''D'' THEN ''DENY''
END + '' '' +
dp.permission_name COLLATE DATABASE_DEFAULT +
'' ON '' +
QUOTENAME(SCHEMA_NAME(o.schema_id) COLLATE DATABASE_DEFAULT) + ''.'' +
QUOTENAME(o.name COLLATE DATABASE_DEFAULT) +
'' TO '' +
QUOTENAME(USER_NAME(dp.grantee_principal_id) COLLATE DATABASE_DEFAULT) +
CASE WHEN dp.state = ''W'' THEN '' WITH GRANT OPTION'' ELSE '''' END +
'';''
AS PermissionScript

FROM sys.database_permissions dp
JOIN sys.objects o
ON dp.major_id = o.object_id

WHERE dp.class = 1
AND USER_NAME(dp.grantee_principal_id) IS NOT NULL;
';

EXEC sp_executesql @SQL;

FETCH NEXT FROM db_cursor INTO @DBName;

END

CLOSE db_cursor;
DEALLOCATE db_cursor;
