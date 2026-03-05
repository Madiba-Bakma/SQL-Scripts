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
