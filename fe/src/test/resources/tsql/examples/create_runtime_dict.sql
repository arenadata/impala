-- Expected: no client result set. Rows land in default.cursor_result_table
-- (table_name, column_name, column_type, dist_rows).
-- SHOW COLUMN STATS order is Column, Type, #Distinct Values, ...
-- The third FETCH target is the distinct-count column. Confirm on the first VM run.

SET SYNC_DDL=1;
GO
TSQL
CREATE OR ALTER PROCEDURE create_runtime_dict
AS
BEGIN
  DECLARE @name STRING;
  DECLARE @col STRING;
  DECLARE @tp STRING;
  DECLARE @ndv INT;
  DECLARE @sql STRING;
  EXEC('DROP TABLE IF EXISTS default.cursor_result_table');
  EXEC('CREATE TABLE default.cursor_result_table (table_name STRING, column_name STRING, column_type STRING, dist_rows INT) STORED AS PARQUET');
  DECLARE c1 CURSOR FOR SHOW TABLES IN default;
  OPEN c1;
  FETCH NEXT FROM c1 INTO @name;
  WHILE @@FETCH_STATUS = 0
  BEGIN
    BEGIN TRY
      SET @sql = 'SHOW COLUMN STATS default.' + @name;
      DECLARE c2 CURSOR FOR @sql;
      OPEN c2;
      FETCH NEXT FROM c2 INTO @col, @tp, @ndv;
      WHILE @@FETCH_STATUS = 0
      BEGIN
        SET @sql = 'INSERT INTO default.cursor_result_table VALUES ('
          + QUOTE_LITERAL(@name) + ', '
          + QUOTE_LITERAL(@col) + ', '
          + QUOTE_LITERAL(@tp) + ', '
          + CAST(@ndv AS STRING) + ')';
        EXEC(@sql);
        FETCH NEXT FROM c2 INTO @col, @tp, @ndv;
      END
      CLOSE c2;
      DEALLOCATE c2;
    END TRY
    BEGIN CATCH
    END CATCH
    FETCH NEXT FROM c1 INTO @name;
  END
  CLOSE c1;
  DEALLOCATE c1;
END
GO