-- Expected: one row, result = 3 * count(default.product).
-- SET mem_limit applies only inside the procedure. The session value is unchanged after GO.
-- Submit this file with impala-shell GO mode (a line that is only GO ends the batch).

SET SYNC_DDL=1;
GO
TSQL
CREATE OR ALTER PROCEDURE demo_proc @x INT
AS
BEGIN
  DECLARE @row_nums INT;
  DECLARE @i INT = 1;
  EXEC('SET mem_limit = 100m');
  DROP TABLE IF EXISTS default.temp_table;
  CREATE TABLE default.temp_table (id BIGINT);
  WHILE @i <= @x
  BEGIN
    INSERT INTO default.temp_table SELECT id FROM default.product;
    SET @i = @i + 1;
  END
  SELECT @row_nums = COUNT(*) FROM default.temp_table;
  SELECT @row_nums AS result;
END
GO
TSQL
EXEC demo_proc @x = 3;
GO