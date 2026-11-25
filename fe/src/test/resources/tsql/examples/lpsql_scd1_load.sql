-- LPSQL spelling of scd1_load.
-- Expected: MERGE updates id=1 (amount 10 -> 20) and inserts id=2.
-- Run examples/00_fixtures.sql first.
-- Composite key is passed as one string, 'id, fld1'.

SET SYNC_DDL=1;
GO
LPSQL
CREATE OR REPLACE PROCEDURE scd1_load (schema_nm STRING, tabname STRING, pk STRING)
BEGIN
  DECLARE column_name STRING;
  DECLARE column_type STRING;
  DECLARE ndv INT;
  DECLARE upd STRING = '';
  DECLARE ins STRING = '';
  DECLARE eq STRING = '';
  DECLARE pk_pred STRING = '1=1';
  DECLARE buf STRING = '';
  DECLARE counter INT = 1;
  DECLARE tar STRING = schema_nm || '.' || tabname;
  DECLARE src STRING = schema_nm || '.int_' || tabname;
  DECLARE task STRING = 'tech_task_id';
  DECLARE del STRING = 'tech_deleted_flg';
  DECLARE ts STRING = 'tech_changed_dttm';
  DECLARE merge_sql STRING;

  LOOP
    buf = SPLIT_PART(pk, ',', counter);
    IF buf = '' THEN LEAVE; END IF;
    pk_pred = pk_pred || ' AND old.' || buf || ' = new.' || buf;
    counter = counter + 1;
  END LOOP;

  DECLARE c1 CURSOR FOR 'SHOW COLUMN STATS ' || tar;
  OPEN c1;
  LOOP
    FETCH c1 INTO column_name, column_type, ndv;
    IF SQLCODE <> 0 THEN LEAVE; END IF;
    IF column_name != task AND column_name != del AND column_name != ts THEN
      ins = ins || 'new.' || column_name || ', ';
      upd = upd || 'old.' || column_name || ' = new.' || column_name || ', ';
      eq = eq || 'old.' || column_name || ' != new.' || column_name || ' OR ';
    END IF;
  END LOOP;
  CLOSE c1;

  merge_sql = 'MERGE INTO ' || tar || ' AS old USING ' || src || ' AS new ON ' || pk_pred
    || ' WHEN MATCHED AND new.' || del || ' != 1 AND (' || eq || ' 1 = 0) THEN UPDATE SET '
    || upd || ' old.' || task || ' = CAST(UNIX_TIMESTAMP() AS INT), old.' || del
    || ' = 0, old.' || ts || ' = CURRENT_TIMESTAMP() '
    || ' WHEN NOT MATCHED AND new.' || del || ' != 1 THEN INSERT VALUES ('
    || ins || 'CURRENT_TIMESTAMP(), CAST(UNIX_TIMESTAMP() AS INT), new.' || del || ')';
  EXECUTE merge_sql;
END
GO
LPSQL
CALL scd1_load('dds', 'account_tab', 'id, fld1');
GO