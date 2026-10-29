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

#pragma once

#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks {

inline TypeDescriptor array_type(const TypeDescriptor& child_type) {
    TypeDescriptor t;
    t.type = TYPE_ARRAY;
    t.children.emplace_back(child_type);
    return t;
}

inline TypeDescriptor array_type(const LogicalType& child_type) {
    TypeDescriptor t;
    t.type = TYPE_ARRAY;
    t.children.resize(1);
    t.children[0].type = child_type;
    t.children[0].len = child_type == TYPE_VARCHAR ? 10 : child_type == TYPE_CHAR ? 10 : -1;
    return t;
}

// Common ARRAY types shared by tests.
inline const TypeDescriptor TYPE_ARRAY_BOOLEAN = array_type(TYPE_BOOLEAN);
inline const TypeDescriptor TYPE_ARRAY_TINYINT = array_type(TYPE_TINYINT);
inline const TypeDescriptor TYPE_ARRAY_SMALLINT = array_type(TYPE_SMALLINT);
inline const TypeDescriptor TYPE_ARRAY_INT = array_type(TYPE_INT);
inline const TypeDescriptor TYPE_ARRAY_BIGINT = array_type(TYPE_BIGINT);
inline const TypeDescriptor TYPE_ARRAY_LARGEINT = array_type(TYPE_LARGEINT);
inline const TypeDescriptor TYPE_ARRAY_FLOAT = array_type(TYPE_FLOAT);
inline const TypeDescriptor TYPE_ARRAY_DOUBLE = array_type(TYPE_DOUBLE);
inline const TypeDescriptor TYPE_ARRAY_VARCHAR = array_type(TYPE_VARCHAR);
inline const TypeDescriptor TYPE_ARRAY_DATE = array_type(TYPE_DATE);
inline const TypeDescriptor TYPE_ARRAY_DATETIME = array_type(TYPE_DATETIME);
inline const TypeDescriptor TYPE_ARRAY_ARRAY_INT = array_type(array_type(TYPE_INT));
inline const TypeDescriptor TYPE_ARRAY_ARRAY_VARCHAR = array_type(array_type(TYPE_VARCHAR));

} // namespace starrocks
