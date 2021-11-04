// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/anyval_util.cpp

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

#include "exprs/anyval_util.h"

#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"

namespace starrocks {
using starrocks_udf::BooleanVal;
using starrocks_udf::TinyIntVal;
using starrocks_udf::SmallIntVal;
using starrocks_udf::IntVal;
using starrocks_udf::BigIntVal;
using starrocks_udf::LargeIntVal;
using starrocks_udf::FloatVal;
using starrocks_udf::DoubleVal;
using starrocks_udf::DecimalVal;
using starrocks_udf::DecimalV2Val;
using starrocks_udf::DateTimeVal;
using starrocks_udf::StringVal;
using starrocks_udf::AnyVal;

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30

FunctionContext::TypeDesc AnyValUtil::column_type_to_type_desc(const TypeDescriptor& type) {
    FunctionContext::TypeDesc out;
    switch (type.type) {
    case TYPE_BOOLEAN:
        out.type = FunctionContext::TYPE_BOOLEAN;
        break;
    case TYPE_TINYINT:
        out.type = FunctionContext::TYPE_TINYINT;
        break;
    case TYPE_SMALLINT:
        out.type = FunctionContext::TYPE_SMALLINT;
        break;
    case TYPE_INT:
        out.type = FunctionContext::TYPE_INT;
        break;
    case TYPE_BIGINT:
        out.type = FunctionContext::TYPE_BIGINT;
        break;
    case TYPE_LARGEINT:
        out.type = FunctionContext::TYPE_LARGEINT;
        break;
    case TYPE_FLOAT:
        out.type = FunctionContext::TYPE_FLOAT;
        break;
    case TYPE_TIME:
    case TYPE_DOUBLE:
        out.type = FunctionContext::TYPE_DOUBLE;
        break;
    case TYPE_DATE:
        out.type = FunctionContext::TYPE_DATE;
        break;
    case TYPE_DATETIME:
        out.type = FunctionContext::TYPE_DATETIME;
        break;
    case TYPE_VARCHAR:
        out.type = FunctionContext::TYPE_VARCHAR;
        out.len = type.len;
        break;
    case TYPE_PERCENTILE:
        out.type = FunctionContext::TYPE_PERCENTILE;
        break;
    case TYPE_HLL:
        out.type = FunctionContext::TYPE_HLL;
        out.len = type.len;
        break;
    case TYPE_OBJECT:
        out.type = FunctionContext::TYPE_OBJECT;
        break;
    case TYPE_CHAR:
        out.type = FunctionContext::TYPE_CHAR;
        out.len = type.len;
        break;
    case TYPE_DECIMAL:
        out.type = FunctionContext::TYPE_DECIMAL;
        // out.precision = type.precision;
        // out.scale = type.scale;
        break;
    case TYPE_DECIMALV2:
        out.type = FunctionContext::TYPE_DECIMALV2;
        // out.precision = type.precision;
        // out.scale = type.scale;
        break;
    case TYPE_NULL:
        out.type = FunctionContext::TYPE_NULL;
        break;
    case TYPE_ARRAY:
        // NOTE: Since `TYPE_ARRAY` only supported in vectorized engine now, reaching here
        // means we are executing a vectorized built-in function and the return type is unused, so
        // here we can return any value.
        out.type = FunctionContext::TYPE_NULL;
        break;
    case TYPE_DECIMAL32:
        out.type = FunctionContext::TYPE_DECIMAL32;
        out.precision = type.precision;
        out.scale = type.scale;
        break;
    case TYPE_DECIMAL64:
        out.type = FunctionContext::TYPE_DECIMAL64;
        out.precision = type.precision;
        out.scale = type.scale;
        break;
    case TYPE_DECIMAL128:
        out.type = FunctionContext::TYPE_DECIMAL128;
        out.precision = type.precision;
        out.scale = type.scale;
        break;
    default:
        DCHECK(false) << "Unknown type: " << type;
    }
    return out;
}

} // namespace starrocks
