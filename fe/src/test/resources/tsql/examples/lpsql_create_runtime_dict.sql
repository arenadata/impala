-- LPSQL spelling of create_runtime_dict.
-- Expected: no client result set. Rows land in default.cursor_result_table
-- (table_name, column_name, column_type, dist_rows).
-- SHOW COLUMN STATS order is Column, Type, #Distinct Values.
-- A view error inside the inner cursor is swallowed and the outer loop continues.

SET SYNC_DDL=1;
GO
LPSQL
CREATE OR REPLACE PROCEDURE create_runtime_dict ()
BEGIN
  DECLARE name STRING;
  DECLARE col STRING;
  DECLARE tp STRING;
  DECLARE ndv INT;
  EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS default.cursor_result_table';
  EXECUTE IMMEDIATE 'CREATE TABLE default.cursor_result_table (table_name STRING, column_name STRING, column_type STRING, dist_rows INT) STORED AS PARQUET';
  DECLARE c1 CURSOR FOR 'SHOW TABLES IN default';
  OPEN c1;
  LOOP
    FETCH c1 INTO name;
    IF SQLCODE <> 0 THEN LEAVE; END IF;
    BEGIN
      DECLARE c2 CURSOR FOR 'SHOW COLUMN STATS default.' || name;
      OPEN c2;
      LOOP
        FETCH c2 INTO col, tp, ndv;
        IF SQLCODE <> 0 THEN LEAVE; END IF;
        EXECUTE IMMEDIATE 'INSERT INTO default.cursor_result_table VALUES ('
          || QUOTE_LITERAL(name) || ', '
          || QUOTE_LITERAL(col) || ', '
          || QUOTE_LITERAL(tp) || ', '
          || CAST(ndv AS STRING) || ')';
      END LOOP;
      CLOSE c2;
    EXCEPTION WHEN OTHERS THEN
    END;
  END LOOP;
  CLOSE c1;
END
GO
LPSQL
CALL create_runtime_dict();
GO