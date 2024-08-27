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

#include "exec/schema_scanner/schema_triggers_scanner.h"

#include "runtime/datetime_value.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaTriggersScanner::_s_cols_triggers[] = {
        //   name,       type,          size,                     is_null
        {"TRIGGER_CATALOG", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"TRIGGER_SCHEMA", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"TRIGGER_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"EVENT_MANIPULATION", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"EVENT_OBJECT_CATALOG", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"EVENT_OBJECT_SCHEMA", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"EVENT_OBJECT_TABLE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"ACTION_ORDER", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"ACTION_CONDITION", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"ACTION_STATEMENT", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"ACTION_ORIENTATION", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"ACTION_TIMING", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"ACTION_REFERENCE_OLD_TABLE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue),
         true},
        {"ACTION_REFERENCE_NEW_TABLE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue),
         true},
        {"ACTION_REFERENCE_OLD_ROW", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue),
         false},
        {"ACTION_REFERENCE_NEW_ROW", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue),
         false},
        {"CREATED", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"SQL_MODE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"DEFINER", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"CHARACTER_SET_CLIENT", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"COLLATION_CONNECTION", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"DATABASE_COLLATION", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
};

SchemaTriggersScanner::SchemaTriggersScanner()
        : SchemaScanner(_s_cols_triggers, sizeof(_s_cols_triggers) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaTriggersScanner::~SchemaTriggersScanner() = default;

} // namespace starrocks
