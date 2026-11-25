-- Fixture for scd1_load and the cursor examples.
-- Expected after the inserts: default.product has two rows (1, 'a') and (2, 'b').

CREATE DATABASE IF NOT EXISTS default;
CREATE DATABASE IF NOT EXISTS dds;
DROP TABLE IF EXISTS default.product;
CREATE TABLE default.product (id BIGINT, name STRING);
INSERT INTO default.product VALUES (1, 'a'), (2, 'b');

DROP TABLE IF EXISTS dds.account_tab;
CREATE TABLE dds.account_tab (
  id INT,
  fld1 STRING,
  amount INT,
  tech_task_id INT,
  tech_deleted_flg INT,
  tech_changed_dttm TIMESTAMP
);
DROP TABLE IF EXISTS dds.int_account_tab;
CREATE TABLE dds.int_account_tab (
  id INT,
  fld1 STRING,
  amount INT,
  tech_task_id INT,
  tech_deleted_flg INT,
  tech_changed_dttm TIMESTAMP
);
INSERT INTO dds.account_tab VALUES (1, 'k', 10, 1, 0, CURRENT_TIMESTAMP());
INSERT INTO dds.int_account_tab VALUES (1, 'k', 20, 2, 0, CURRENT_TIMESTAMP());
INSERT INTO dds.int_account_tab VALUES (2, 'n', 5, 2, 0, CURRENT_TIMESTAMP());
GO
