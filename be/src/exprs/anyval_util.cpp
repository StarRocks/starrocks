// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

FunctionContext::TypeDesc AnyValUtil::column_type_to_type_desc(const TypeDescriptor& type) {
    FunctionContext::TypeDesc out;
    switch (type.type) {
    case TYPE_BOOLEAN:
        out.type = TYPE_BOOLEAN;
        break;
    case TYPE_TINYINT:
        out.type = TYPE_TINYINT;
        break;
    case TYPE_SMALLINT:
        out.type = TYPE_SMALLINT;
        break;
    case TYPE_INT:
        out.type = TYPE_INT;
        break;
    case TYPE_BIGINT:
        out.type = TYPE_BIGINT;
        break;
    case TYPE_LARGEINT:
        out.type = TYPE_LARGEINT;
        break;
    case TYPE_FLOAT:
        out.type = TYPE_FLOAT;
        break;
    case TYPE_TIME:
    case TYPE_DOUBLE:
        out.type = TYPE_DOUBLE;
        break;
    case TYPE_DATE:
        out.type = TYPE_DATE;
        break;
    case TYPE_DATETIME:
        out.type = TYPE_DATETIME;
        break;
    case TYPE_VARCHAR:
        out.type = TYPE_VARCHAR;
        out.len = type.len;
        break;
    case TYPE_PERCENTILE:
        out.type = TYPE_PERCENTILE;
        break;
    case TYPE_HLL:
        out.type = TYPE_HLL;
        out.len = type.len;
        break;
    case TYPE_OBJECT:
        out.type = TYPE_OBJECT;
        break;
    case TYPE_CHAR:
        out.type = TYPE_CHAR;
        out.len = type.len;
        break;
    case TYPE_DECIMAL:
        out.type = TYPE_DECIMAL;
        // out.precision = type.precision;
        // out.scale = type.scale;
        break;
    case TYPE_DECIMALV2:
        out.type = TYPE_DECIMALV2;
        // out.precision = type.precision;
        // out.scale = type.scale;
        break;
    case TYPE_NULL:
        out.type = TYPE_NULL;
        break;
    case TYPE_ARRAY:
    case TYPE_MAP:
    case TYPE_STRUCT: {
        out.type = type.type;
        for (auto child : type.children) {
            out.children.emplace_back(column_type_to_type_desc(child));
        }
        break;
    }
    case TYPE_DECIMAL32:
        out.type = TYPE_DECIMAL32;
        out.precision = type.precision;
        out.scale = type.scale;
        break;
    case TYPE_DECIMAL64:
        out.type = TYPE_DECIMAL64;
        out.precision = type.precision;
        out.scale = type.scale;
        break;
    case TYPE_DECIMAL128:
        out.type = TYPE_DECIMAL128;
        out.precision = type.precision;
        out.scale = type.scale;
        break;
    case TYPE_JSON:
        out.type = TYPE_JSON;
        break;
    case TYPE_FUNCTION:
        out.type = TYPE_FUNCTION;
    case TYPE_VARBINARY:
        out.type = TYPE_VARBINARY;
        out.len = type.len;
        break;
    default:
        DCHECK(false) << "Unknown type: " << type;
    }
    return out;
}

} // namespace starrocks
