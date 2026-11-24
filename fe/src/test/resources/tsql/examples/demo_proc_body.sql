-- M3 anonymous batch. Expected: one row, result = 3 * count(default.product).
-- The whole batch is one server request. A line that is only GO ends it.
-- After it returns, the session mem_limit is unchanged.

SET SYNC_DDL=1;
GO
TSQL
DECLARE @row_nums INT;
DECLARE @i INT = 1;
DECLARE @x INT = 3;
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
GO