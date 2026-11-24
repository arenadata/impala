// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
DEFINE_int64(tsql_max_steps, 1000000,
    "Maximum statements a T-SQL batch may execute. 0 means unlimited.");
DEFINE_int32(tsql_max_nesting, 32,
    "Maximum nested procedure calls. T-SQL's own limit is 32.");
DEFINE_int32(tsql_max_concurrent_batches, 32,
    "Maximum procedure batches running at once on this coordinator.");
