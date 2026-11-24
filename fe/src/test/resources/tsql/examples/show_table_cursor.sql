-- Expected: table names in default, including cursor_result_table, ordered by name.
-- SHOW TABLES returns a column named name.

SET SYNC_DDL=1;
GO
TSQL
CREATE OR ALTER PROCEDURE show_table_cursor
AS
BEGIN
  DECLARE @name STRING;
  DECLARE @ins STRING;
  DECLARE c1 CURSOR FOR SHOW TABLES IN default;
  DROP TABLE IF EXISTS default.cursor_result_table;
  CREATE TABLE default.cursor_result_table (table_name STRING);
  OPEN c1;
  FETCH NEXT FROM c1 INTO @name;
  WHILE @@FETCH_STATUS = 0
  BEGIN
    SET @ins = 'INSERT INTO default.cursor_result_table VALUES ('
      + QUOTE_LITERAL(@name) + ')';
    EXEC(@ins);
    FETCH NEXT FROM c1 INTO @name;
  END
  CLOSE c1;
  DEALLOCATE c1;
  SELECT * FROM default.cursor_result_table ORDER BY 1;
END
GO
TSQL
EXEC show_table_cursor;
GO