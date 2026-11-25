# T-SQL and LPSQL procedures

A client batch is a procedure batch only when its first keyword is `TSQL` or `LPSQL`.
The two dialects share one runtime. Send the whole batch as one string. In
impala-shell a line that is only `GO` ends the batch; interactive `TSQL` and `LPSQL`
batches are not split on semicolons.

## What is accepted

- `CREATE [OR ALTER] PROCEDURE` (T-SQL) and `CREATE [OR REPLACE] PROCEDURE` (LPSQL).
  The statement must be the only one in its batch. Names may be `database.procedure`.
  Parameters may have defaults (`@p INT = 5`, or LPSQL `p INT DEFAULT 5`).
- `EXEC`, `CALL`, `EXEC(@sql)`, `EXEC @rc = procedure`.
- `DECLARE`, `SET`, `IF`, `WHILE`, `BEGIN`/`END`, `TRY`/`CATCH`, LPSQL
  `EXCEPTION WHEN OTHERS`.
- Cursors: `DECLARE c CURSOR FOR`, `OPEN`, `FETCH`, `CLOSE`, `DEALLOCATE`.
  `CLOSE` drops the buffered rows.
- `RETURN [expr]`, `THROW`, `DROP PROCEDURE`.
- Arithmetic `+ - * / %`, comparisons, and `||` in LPSQL.
- `QUOTE_LITERAL`, `ERROR_MESSAGE()`, `ERROR_NUMBER()`, `@@FETCH_STATUS`, `@@ERROR`,
  `@@SQLCODE`.

`WITH EXECUTE AS OWNER` and `SECURITY DEFINER` are rejected. A procedure runs with
the caller's privileges. Each statement inside it is authorized on its own.
Replacing or dropping a procedure requires the owner, or `ALL` on the database when
authorization is enabled. Creating one requires `CREATE` on the database.

## Variables

Each call has its own variables and cursors. `RETURN` leaves that call.
`BREAK` and `CONTINUE` do not cross into the caller. An OUTPUT argument copies the
callee's value back. A T-SQL variable must be declared; `@idd` is an error, not NULL.
`@@ERROR` is not the same variable as `@error`.

Comparisons with NULL are unknown, so `IF @x <> 1` is not taken when `@x` is NULL.
T-SQL string comparison is case-insensitive. LPSQL string comparison is not.

Inside an LPSQL SQL statement a variable is `:name`. A bare identifier is a column,
so `INSERT INTO t SELECT id FROM s` reads the column even when a variable named `id`
exists. Procedure expressions (`IF`, `SET`, `WHILE`) still use the bare name.

## Literals

In procedure source a quote is escaped by doubling it (`O''Brien`) and a backslash
is an ordinary character (`'C:\data\'`). The text sent to Impala uses backslash
escapes. `EXEC(@sql)` is already Impala SQL and is not rewritten.

## Limits and cancellation

| Flag | Default | Meaning |
|---|---|---|
| `--enable_tsql` | true | Reject procedure batches when false. |
| `--tsql_max_steps` | 1000000 | Statements per batch. 0 is unlimited. |
| `--tsql_max_nesting` | 32 | Nested calls. |
| `--tsql_max_concurrent_batches` | 32 | Batches at once on this coordinator. |
| `--tsql_max_cursor_rows` | 100000 | Rows one cursor may buffer. |
| `--tsql_max_result_rows` | 100000 | Rows returned to the client. |

`ExecuteStatement` returns a handle while the batch is still running, so
`KILL QUERY`, the web UI, `exec_time_limit_s` and Ctrl+C in impala-shell can cancel
it. `GetResultSetMetadata` waits until the batch has produced its final schema.
A loop that runs no SQL yields often enough for cancellation to be noticed.

## Known limits

Client result columns are all STRING. `NULL` concatenated with `+` or `||` is treated
as an empty string. There is no separate EXECUTE privilege in Ranger.
