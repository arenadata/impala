-- Expected: COMPUTE STATS runs for the table only. The view raises inside the
-- column-stats cursor and BEGIN CATCH swallows it. One log row is inserted.
-- Fixture: database stats_demo with table t and view v.

SET SYNC_DDL=1;
GO
CREATE DATABASE IF NOT EXISTS stats_demo;
DROP VIEW IF EXISTS stats_demo.v;
DROP TABLE IF EXISTS stats_demo.t;
CREATE TABLE stats_demo.t (id INT);
INSERT INTO stats_demo.t VALUES (1);
CREATE VIEW stats_demo.v AS SELECT id FROM stats_demo.t;
GO
TSQL
CREATE OR ALTER PROCEDURE stats_missing_proc
  @cur_schema STRING,
  @proc_mode INT,
  @inc_mode INT
AS
BEGIN
  DECLARE @name STRING;
  DECLARE @clmn STRING;
  DECLARE @tp STRING;
  DECLARE @dv INT;
  DECLARE @nlls INT;

  CREATE TABLE IF NOT EXISTS default.tech_compactor_proc (tabname STRING, got_stats INT) STORED AS PARQUET;
  CREATE TABLE IF NOT EXISTS default.tech_compactor_logs (tabname STRING, query STRING, dttm TIMESTAMP) STORED AS PARQUET;
  TRUNCATE TABLE default.tech_compactor_proc;
  TRUNCATE TABLE default.tech_compactor_logs;

  DECLARE c1 CURSOR FOR SHOW TABLES IN stats_demo;
  OPEN c1;
  FETCH NEXT FROM c1 INTO @name;
  WHILE @@FETCH_STATUS = 0
  BEGIN
    BEGIN TRY
      DECLARE c2 CURSOR FOR SHOW COLUMN STATS stats_demo.t;
      OPEN c2;
      FETCH NEXT FROM c2 INTO @clmn, @tp, @dv, @nlls;
      WHILE @@FETCH_STATUS = 0
      BEGIN
        IF (@dv < 0 AND @proc_mode = 1) OR (@proc_mode = 0)
        BEGIN
          EXEC('INSERT INTO default.tech_compactor_proc SELECT ' + QUOTE_LITERAL(@name) + ', ' + CAST(@dv AS STRING));
        END
        FETCH NEXT FROM c2 INTO @clmn, @tp, @dv, @nlls;
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

  IF @inc_mode = 0
  BEGIN
    DECLARE c3 CURSOR FOR SELECT DISTINCT tabname FROM default.tech_compactor_proc;
    OPEN c3;
    FETCH NEXT FROM c3 INTO @name;
    WHILE @@FETCH_STATUS = 0
    BEGIN
      EXEC('COMPUTE STATS ' + @cur_schema + '.' + @name);
      EXEC('INSERT INTO default.tech_compactor_logs SELECT '
        + QUOTE_LITERAL(@cur_schema + '.' + @name) + ', '
        + QUOTE_LITERAL('COMPUTE STATS ' + @cur_schema + '.' + @name) + ', NOW()');
      FETCH NEXT FROM c3 INTO @name;
    END
    CLOSE c3;
    DEALLOCATE c3;
  END
END
GO
TSQL
EXEC stats_missing_proc @cur_schema = 'stats_demo', @proc_mode = 0, @inc_mode = 0;
GO
SELECT tabname, query FROM default.tech_compactor_logs ORDER BY tabname;
GO