-- LPSQL spelling of stats_missing_proc.
-- Expected: COMPUTE STATS runs for the table only. The view raises inside the
-- column-stats cursor and EXCEPTION WHEN OTHERS swallows it. One log row is inserted.
-- Fixture: database stats_demo with table t and view v.

SET SYNC_DDL=1;
GO
LPSQL
CREATE OR REPLACE PROCEDURE stats_missing_proc (cur_schema STRING, proc_mode INT, inc_mode INT)
BEGIN
  DECLARE name STRING;
  DECLARE clmn STRING;
  DECLARE tp STRING;
  DECLARE dv INT;
  DECLARE nlls INT;

  CREATE TABLE IF NOT EXISTS default.tech_compactor_proc (tabname STRING, got_stats INT) STORED AS PARQUET;
  CREATE TABLE IF NOT EXISTS default.tech_compactor_logs (tabname STRING, query STRING, dttm TIMESTAMP) STORED AS PARQUET;
  EXECUTE IMMEDIATE 'TRUNCATE TABLE default.tech_compactor_proc';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE default.tech_compactor_logs';

  DECLARE c1 CURSOR FOR 'SHOW TABLES IN ' || cur_schema;
  OPEN c1;
  LOOP
    FETCH c1 INTO name;
    IF SQLCODE <> 0 THEN LEAVE; END IF;
    BEGIN
      DECLARE c2 CURSOR FOR 'SHOW COLUMN STATS ' || cur_schema || '.' || name;
      OPEN c2;
      LOOP
        FETCH c2 INTO clmn, tp, dv, nlls;
        IF SQLCODE <> 0 THEN LEAVE; END IF;
        IF (dv < 0 AND proc_mode = 1) OR (proc_mode = 0) THEN
          EXECUTE IMMEDIATE 'INSERT INTO default.tech_compactor_proc SELECT '
            || QUOTE_LITERAL(name) || ', ' || CAST(dv AS STRING);
        END IF;
      END LOOP;
      CLOSE c2;
    EXCEPTION WHEN OTHERS THEN
    END;
  END LOOP;
  CLOSE c1;

  IF inc_mode = 0 THEN
    DECLARE c3 CURSOR FOR SELECT DISTINCT tabname FROM default.tech_compactor_proc;
    OPEN c3;
    LOOP
      FETCH c3 INTO name;
      IF SQLCODE <> 0 THEN LEAVE; END IF;
      EXECUTE IMMEDIATE 'COMPUTE STATS ' || cur_schema || '.' || name;
      EXECUTE IMMEDIATE 'INSERT INTO default.tech_compactor_logs SELECT '
        || QUOTE_LITERAL(cur_schema || '.' || name) || ', '
        || QUOTE_LITERAL('COMPUTE STATS ' || cur_schema || '.' || name) || ', NOW()';
    END LOOP;
    CLOSE c3;
  END IF;
END
GO
LPSQL
CALL stats_missing_proc('stats_demo', 0, 0);
GO