// ADH-TSQL
#include "tsql/tsql-flags.h"

#include "common/logging.h"
#include "gutil/strings/substitute.h"

DEFINE_bool(enable_tsql, true,
    "Accept T-SQL procedure batches. When false, a T-SQL batch returns an error.");
DEFINE_int32(tsql_max_cursor_rows, 100000,
    "Maximum rows a T-SQL cursor may fetch. The statement fails closed above this.");
DEFINE_int32(tsql_max_result_rows, 100000,
    "Maximum rows materialized for the client result of a T-SQL batch.");

namespace impala {

void TsqlFlagsDummy() {}

} // namespace impala
