// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/hybird_set.cpp

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

#include "exprs/hybird_set.h"

namespace starrocks {

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30

HybirdSetBase* HybirdSetBase::create_set(PrimitiveType type) {
    switch (type) {
    case TYPE_BOOLEAN:
        return new (std::nothrow) HybirdSet<bool>();

    case TYPE_TINYINT:
        return new (std::nothrow) HybirdSet<int8_t>();

    case TYPE_SMALLINT:
        return new (std::nothrow) HybirdSet<int16_t>();

    case TYPE_INT:
        return new (std::nothrow) HybirdSet<int32_t>();

    case TYPE_BIGINT:
        return new (std::nothrow) HybirdSet<int64_t>();

    case TYPE_FLOAT:
        return new (std::nothrow) HybirdSet<float>();

    case TYPE_DOUBLE:
        return new (std::nothrow) HybirdSet<double>();

    case TYPE_DATE:
    case TYPE_DATETIME:
        return new (std::nothrow) HybirdSet<DateTimeValue>();

    case TYPE_DECIMAL:
        return new (std::nothrow) HybirdSet<DecimalValue>();

    case TYPE_DECIMALV2:
        return new (std::nothrow) HybirdSet<DecimalV2Value>();

    case TYPE_LARGEINT:
        return new (std::nothrow) HybirdSet<__int128>();

    case TYPE_CHAR:
    case TYPE_VARCHAR:
        return new (std::nothrow) StringValueSet();

    default:
        return NULL;
    }

    return NULL;
}

} // namespace starrocks

/* vim: set ts=4 sw=4 sts=4 tw=100 */
