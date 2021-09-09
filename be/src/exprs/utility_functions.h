// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/utility_functions.h

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

#ifndef STARROCKS_BE_SRC_QUERY_EXPRS_UTILITY_FUNCTIONS_H
#define STARROCKS_BE_SRC_QUERY_EXPRS_UTILITY_FUNCTIONS_H

#include "udf/udf.h"

namespace starrocks {

class Expr;
class OpcodeRegistry;
class TupleRow;

class UtilityFunctions {
public:
    static void init();
    // Implementation of the version() function. Returns the version string.
    static starrocks_udf::StringVal version(starrocks_udf::FunctionContext* ctx);
    static starrocks_udf::StringVal current_version(starrocks_udf::FunctionContext* ctx);

    // Testing function that sleeps for the specified number of seconds. Returns true.
    static starrocks_udf::BooleanVal sleep(starrocks_udf::FunctionContext* ctx, const starrocks_udf::IntVal& seconds);

    static starrocks_udf::StringVal last_query_id(starrocks_udf::FunctionContext* ctx);
};

} // namespace starrocks

#endif
