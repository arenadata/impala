-- LPSQL spelling of show_tables.
-- Expected: distinct table_name values from default.cursor_result_table
-- whose dist_rows is greater than n, after create_runtime_dict has run.

SET SYNC_DDL=1;
GO
LPSQL
CREATE OR REPLACE PROCEDURE show_tables (n INT)
BEGIN
  CALL create_runtime_dict();
  EXECUTE IMMEDIATE 'DROP VIEW IF EXISTS default.v_show_table';
  EXECUTE IMMEDIATE 'CREATE VIEW default.v_show_table AS SELECT DISTINCT table_name FROM default.cursor_result_table WHERE dist_rows > '
    || n;
  SELECT * FROM default.v_show_table;
END
GO
LPSQL
CALL show_tables(10000);
GO