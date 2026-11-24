-- LPSQL spelling of show_table_cursor.
-- Expected: table names in default, including cursor_result_table, ordered by name.
-- SHOW TABLES returns a column named name. The cursor loop reads c1.name.

SET SYNC_DDL=1;
GO
LPSQL
CREATE OR REPLACE PROCEDURE show_table_cursor ()
BEGIN
  DROP TABLE IF EXISTS default.cursor_result_table;
  CREATE TABLE default.cursor_result_table (table_name STRING);
  FOR c1 IN ('SHOW TABLES IN default') LOOP
    EXECUTE IMMEDIATE 'INSERT INTO default.cursor_result_table VALUES ('
      || QUOTE_LITERAL(c1.name) || ')';
  END LOOP;
  SELECT * FROM default.cursor_result_table ORDER BY 1;
END
GO
LPSQL
CALL show_table_cursor();
GO