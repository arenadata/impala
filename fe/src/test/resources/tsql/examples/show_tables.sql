-- Expected: distinct table_name values from default.cursor_result_table
-- whose dist_rows is greater than @n, after create_runtime_dict has run.
-- @n = 10000 matches the article's check. An empty list is valid on a tiny fixture.

SET SYNC_DDL=1;
GO
TSQL
CREATE OR ALTER PROCEDURE show_tables @n INT
AS
BEGIN
  EXEC create_runtime_dict;
  EXEC('DROP VIEW IF EXISTS default.v_show_table');
  DECLARE @sql STRING;
  SET @sql = 'CREATE VIEW default.v_show_table AS SELECT DISTINCT table_name FROM default.cursor_result_table WHERE dist_rows > '
    + CAST(@n AS STRING);
  EXEC(@sql);
  SELECT * FROM default.v_show_table;
END
GO
TSQL
EXEC show_tables @n = 10000;
GO