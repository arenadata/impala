-- Expected: MERGE updates id=1 (amount 10 -> 20) and inserts id=2.
-- id=1 in the target stays tech_deleted_flg = 0.
-- Run examples/00_fixtures.sql first.
-- Composite key is passed as one string, 'id, fld1'.

SET SYNC_DDL=1;
GO
TSQL
CREATE OR ALTER PROCEDURE scd1_load
  @schema_nm STRING,
  @tabname STRING,
  @pk STRING
AS
BEGIN
  DECLARE @column STRING;
  DECLARE @column_type STRING;
  DECLARE @ndv INT;
  DECLARE @upd STRING = '';
  DECLARE @ins STRING = '';
  DECLARE @eq STRING = '';
  DECLARE @pk_pred STRING = '1=1';
  DECLARE @buf STRING;
  DECLARE @counter INT = 1;
  DECLARE @tar STRING = @schema_nm + '.' + @tabname;
  DECLARE @src STRING = @schema_nm + '.int_' + @tabname;
  DECLARE @task STRING = 'tech_task_id';
  DECLARE @del STRING = 'tech_deleted_flg';
  DECLARE @ts STRING = 'tech_changed_dttm';
  DECLARE @merge STRING;

  WHILE 1 = 1
  BEGIN
    SELECT @buf = SPLIT_PART(@pk, ',', @counter);
    IF @buf = '' OR @buf IS NULL
    BEGIN
      BREAK;
    END
    SET @pk_pred = @pk_pred + ' AND old.' + @buf + ' = new.' + @buf;
    SET @counter = @counter + 1;
  END

  DECLARE c1 CURSOR FOR SHOW COLUMN STATS dds.account_tab;
  OPEN c1;
  FETCH NEXT FROM c1 INTO @column, @column_type, @ndv;
  WHILE @@FETCH_STATUS = 0
  BEGIN
    IF @column != @task AND @column != @del AND @column != @ts
    BEGIN
      SET @ins = @ins + 'new.' + @column + ', ';
      SET @upd = @upd + 'old.' + @column + ' = new.' + @column + ', ';
      SET @eq = @eq + 'old.' + @column + ' != new.' + @column + ' OR ';
    END
    FETCH NEXT FROM c1 INTO @column, @column_type, @ndv;
  END
  CLOSE c1;
  DEALLOCATE c1;

  SET @merge = 'MERGE INTO ' + @tar + ' AS old USING ' + @src + ' AS new ON ' + @pk_pred
    + ' WHEN MATCHED AND new.' + @del + ' != 1 AND (' + @eq + ' 1 = 0) THEN UPDATE SET '
    + @upd + ' old.' + @task + ' = CAST(UNIX_TIMESTAMP() AS INT), old.' + @del
    + ' = 0, old.' + @ts + ' = CURRENT_TIMESTAMP() '
    + ' WHEN NOT MATCHED AND new.' + @del + ' != 1 THEN INSERT VALUES ('
    + @ins + 'CURRENT_TIMESTAMP(), CAST(UNIX_TIMESTAMP() AS INT), new.' + @del + ')';
  EXEC(@merge);
END
GO
TSQL
EXEC scd1_load @schema_nm = 'dds', @tabname = 'account_tab', @pk = 'id, fld1';
GO
SELECT id, amount, tech_deleted_flg FROM dds.account_tab ORDER BY id;
GO