// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/udf_builtins.h

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

#ifndef STARROCKS_BE_SRC_QUERY_EXPRS_UDF_BUILTINS_H
#define STARROCKS_BE_SRC_QUERY_EXPRS_UDF_BUILTINS_H

#include "udf/udf.h"

namespace starrocks {

// Builtins written against the UDF interface. The builtins in the other files
// should be replaced to the UDF interface as well.
// This is just to illustrate how builtins against the UDF interface will be
// implemented.

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30

class UdfBuiltins {
public:
    static starrocks_udf::DoubleVal abs(starrocks_udf::FunctionContext* context, const starrocks_udf::DoubleVal& v);
    static starrocks_udf::DecimalVal decimal_abs(starrocks_udf::FunctionContext* context,
                                                 const starrocks_udf::DecimalVal& v);
    static starrocks_udf::DecimalV2Val decimal_abs(starrocks_udf::FunctionContext* context,
                                                   const starrocks_udf::DecimalV2Val& v);
    static starrocks_udf::BigIntVal add_two_number(starrocks_udf::FunctionContext* context,
                                                   const starrocks_udf::BigIntVal& v1,
                                                   const starrocks_udf::BigIntVal& v2);
    static starrocks_udf::StringVal sub_string(starrocks_udf::FunctionContext* context,
                                               const starrocks_udf::StringVal& v1, const starrocks_udf::IntVal& begin,
                                               const starrocks_udf::IntVal& len);
    static starrocks_udf::DoubleVal pi(starrocks_udf::FunctionContext* context);

    static starrocks_udf::StringVal lower(starrocks_udf::FunctionContext* context, const starrocks_udf::StringVal&);
};

} // namespace starrocks

#endif
