-- LPSQL spelling of demo_proc. Expected: one row, result = 3 * count(default.product).
-- SET mem_limit applies only inside the procedure.
-- Submit with impala-shell GO mode (a line that is only GO ends the batch).

SET SYNC_DDL=1;
GO
LPSQL
CREATE OR REPLACE PROCEDURE demo_proc (IN x INT)
BEGIN
  DECLARE row_nums INT;
  DECLARE i INT = 1;
  EXECUTE IMMEDIATE 'SET mem_limit = 100m';
  DROP TABLE IF EXISTS default.temp_table;
  CREATE TABLE default.temp_table (id BIGINT);
  WHILE i <= x LOOP
    INSERT INTO default.temp_table SELECT id FROM default.product;
    SET i = i + 1;
  END LOOP;
  SELECT COUNT(*) INTO row_nums FROM default.temp_table;
  SELECT :row_nums AS result;
END
GO
LPSQL
CALL demo_proc(3);
GO