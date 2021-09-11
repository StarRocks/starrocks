// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/encryption_functions.h

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

#ifndef STARROCKS_BE_SRC_QUERY_EXPRS_ENCRYPTION_FUNCTIONS_H
#define STARROCKS_BE_SRC_QUERY_EXPRS_ENCRYPTION_FUNCTIONS_H

#include <stdint.h>

#include "udf/udf.h"
#include "udf/udf_internal.h"

namespace starrocks {

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30

class Expr;
struct ExprValue;
class TupleRow;

class EncryptionFunctions {
public:
    static void init();
    static starrocks_udf::StringVal aes_encrypt(starrocks_udf::FunctionContext* context,
                                                const starrocks_udf::StringVal& val1,
                                                const starrocks_udf::StringVal& val2);
    static starrocks_udf::StringVal aes_decrypt(starrocks_udf::FunctionContext* context,
                                                const starrocks_udf::StringVal& val1,
                                                const starrocks_udf::StringVal& val2);
    static starrocks_udf::StringVal from_base64(starrocks_udf::FunctionContext* context,
                                                const starrocks_udf::StringVal& val1);
    static starrocks_udf::StringVal to_base64(starrocks_udf::FunctionContext* context,
                                              const starrocks_udf::StringVal& val1);
    static starrocks_udf::StringVal md5sum(starrocks_udf::FunctionContext* ctx, int num_args,
                                           const starrocks_udf::StringVal* args);
    static starrocks_udf::StringVal md5(starrocks_udf::FunctionContext* ctx, const starrocks_udf::StringVal& src);
};

} // namespace starrocks

#endif
