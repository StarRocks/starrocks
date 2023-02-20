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
        {"TRIGGER_CATALOG", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TRIGGER_SCHEMA", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TRIGGER_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"EVENT_MANIPULATION", TYPE_VARCHAR, sizeof(StringValue), false},
        {"EVENT_OBJECT_CATALOG", TYPE_VARCHAR, sizeof(StringValue), false},
        {"EVENT_OBJECT_SCHEMA", TYPE_VARCHAR, sizeof(StringValue), false},
        {"EVENT_OBJECT_TABLE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ACTION_ORDER", TYPE_BIGINT, sizeof(int64_t), false},
        {"ACTION_CONDITION", TYPE_VARCHAR, sizeof(StringValue), true},
        {"ACTION_STATEMENT", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ACTION_ORIENTATION", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ACTION_TIMING", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ACTION_REFERENCE_OLD_TABLE", TYPE_VARCHAR, sizeof(StringValue), true},
        {"ACTION_REFERENCE_NEW_TABLE", TYPE_VARCHAR, sizeof(StringValue), true},
        {"ACTION_REFERENCE_OLD_ROW", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ACTION_REFERENCE_NEW_ROW", TYPE_VARCHAR, sizeof(StringValue), false},
        {"CREATED", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"SQL_MODE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DEFINER", TYPE_VARCHAR, sizeof(StringValue), false},
        {"CHARACTER_SET_CLIENT", TYPE_VARCHAR, sizeof(StringValue), false},
        {"COLLATION_CONNECTION", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DATABASE_COLLATION", TYPE_VARCHAR, sizeof(StringValue), false},
};

SchemaTriggersScanner::SchemaTriggersScanner()
        : SchemaScanner(_s_cols_triggers, sizeof(_s_cols_triggers) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaTriggersScanner::~SchemaTriggersScanner() = default;

} // namespace starrocks
