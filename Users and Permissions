/*   
This is for Scripting the Users and Permisssons with the Database Memberships of each database, mapping it to the login.
*/

SET NOCOUNT ON;

DECLARE @DBName SYSNAME
DECLARE @SQL NVARCHAR(MAX)

DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR
SELECT name
FROM sys.databases
WHERE database_id > 4
AND state_desc = 'ONLINE'

OPEN db_cursor
FETCH NEXT FROM db_cursor INTO @DBName

WHILE @@FETCH_STATUS = 0
BEGIN

SET @SQL = N'
USE ' + QUOTENAME(@DBName) + N'

PRINT ''===== DATABASE: ' + @DBName + N' =====''

-- USERS
SELECT
''CREATE USER '' +
QUOTENAME(dp.name COLLATE DATABASE_DEFAULT) +
'' FOR LOGIN '' +
QUOTENAME(sp.name COLLATE DATABASE_DEFAULT) +
'';''

FROM sys.database_principals dp
JOIN sys.server_principals sp
ON dp.sid = sp.sid

WHERE dp.principal_id > 4
AND dp.type IN (''S'',''U'')

-- ROLE MEMBERSHIP
SELECT
''ALTER ROLE '' +
QUOTENAME(r.name COLLATE DATABASE_DEFAULT) +
'' ADD MEMBER '' +
QUOTENAME(m.name COLLATE DATABASE_DEFAULT) +
'';''

FROM sys.database_role_members drm
JOIN sys.database_principals r
ON drm.role_principal_id = r.principal_id
JOIN sys.database_principals m
ON drm.member_principal_id = m.principal_id
WHERE m.principal_id > 4
'

EXEC sp_executesql @SQL

FETCH NEXT FROM db_cursor INTO @DBName

END

CLOSE db_cursor
DEALLOCATE db_cursor
