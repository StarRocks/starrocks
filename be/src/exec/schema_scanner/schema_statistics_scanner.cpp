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

#include "exec/schema_scanner/schema_statistics_scanner.h"

#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaStatisticsScanner::_s_cols_statistics[] = {
        //   name,       type,          size,                     is_null
        {"TABLE_CATALOG", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"TABLE_SCHEMA", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"TABLE_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"NON_UNIQUE", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"INDEX_SCHEMA", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"INDEX_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"SEQ_IN_INDEX", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"COLUMN_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"COLLATION", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"CARDINALITY", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"SUB_PART", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"PACKED", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"NULLABLE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"INDEX_TYPE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"COMMENT", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"INDEX_COMMENT", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"EXPRESSION", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
};

SchemaStatisticsScanner::SchemaStatisticsScanner()
        : SchemaScanner(_s_cols_statistics, sizeof(_s_cols_statistics) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaStatisticsScanner::~SchemaStatisticsScanner() = default;

} // namespace starrocks
