SELECT
'CREATE LOGIN [' + name + '] 
WITH PASSWORD = 0x' + CONVERT(VARCHAR(MAX), password_hash, 2) + ' HASHED,
CHECK_POLICY = OFF;' AS LoginScript
FROM sys.sql_logins
WHERE is_disabled = 0
AND name NOT LIKE '##%';
GO


--------OR----------
/*
This preserves:
password hash
SID
default database, Which prevents orphaned users.
*/

SET NOCOUNT ON;

SELECT 
'CREATE LOGIN ' + QUOTENAME(sp.name) +
CASE 
    WHEN sp.type = 'S' THEN ' WITH PASSWORD = 0x' + CONVERT(VARCHAR(MAX), sl.password_hash, 2) + ' HASHED'
    WHEN sp.type = 'U' THEN ' FROM WINDOWS'
    WHEN sp.type = 'G' THEN ' FROM WINDOWS'
END +
', SID = 0x' + CONVERT(VARCHAR(MAX), sp.sid, 2) +
', DEFAULT_DATABASE = ' + QUOTENAME(sp.default_database_name) +
', CHECK_POLICY = OFF;' AS CreateLoginScript

FROM sys.server_principals sp
LEFT JOIN sys.sql_logins sl
ON sp.principal_id = sl.principal_id

WHERE sp.type IN ('S','U','G')
AND sp.name NOT LIKE '##%'
AND sp.name <> 'sa'
AND sp.is_disabled = 0
ORDER BY sp.name;
GO
