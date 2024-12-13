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

#include "exec/schema_scanner/schema_events_scanner.h"

#include "runtime/datetime_value.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaEventsScanner::_s_cols_events[] = {
        //   name,       type,          size,                     is_null
<<<<<<< HEAD
        {"EVENT_CATALOG", TYPE_VARCHAR, sizeof(StringValue), false},
        {"EVENT_SCHEMA", TYPE_VARCHAR, sizeof(StringValue), false},
        {"EVENT_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DEFINER", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TIME_ZONE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"EVENT_BODY", TYPE_VARCHAR, sizeof(StringValue), false},
        {"EVENT_DEFINITION", TYPE_VARCHAR, sizeof(StringValue), false},
        {"EVENT_TYPE", TYPE_BIGINT, sizeof(StringValue), false},
        {"EXECUTE_AT", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"INTERVAL_VALUE", TYPE_VARCHAR, sizeof(StringValue), true},
        {"INTERVAL_FIELD", TYPE_VARCHAR, sizeof(StringValue), true},
        {"SQL_MODE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"STARTS", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"ENDS", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"STATUS", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ON_COMPLETION", TYPE_VARCHAR, sizeof(StringValue), false},
        {"CREATED", TYPE_DATETIME, sizeof(DateTimeValue), false},
        {"LAST_ALTERED", TYPE_DATETIME, sizeof(DateTimeValue), false},
        {"LAST_EXECUTED", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"EVENT_COMMENT", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ORIGINATOR", TYPE_VARCHAR, sizeof(StringValue), false},
        {"CHARACTER_SET_CLIENT", TYPE_VARCHAR, sizeof(StringValue), false},
        {"COLLATION_CONNECTION", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DATABASE_COLLATION", TYPE_VARCHAR, sizeof(StringValue), false},
=======
        {"EVENT_CATALOG", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"EVENT_SCHEMA", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"EVENT_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"DEFINER", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"TIME_ZONE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"EVENT_BODY", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"EVENT_DEFINITION", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"EVENT_TYPE", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(StringValue), false},
        {"EXECUTE_AT", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"INTERVAL_VALUE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"INTERVAL_FIELD", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"SQL_MODE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"STARTS", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"ENDS", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"STATUS", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"ON_COMPLETION", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"CREATED", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), false},
        {"LAST_ALTERED", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), false},
        {"LAST_EXECUTED", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"EVENT_COMMENT", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"ORIGINATOR", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"CHARACTER_SET_CLIENT", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"COLLATION_CONNECTION", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"DATABASE_COLLATION", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
};

SchemaEventsScanner::SchemaEventsScanner()
        : SchemaScanner(_s_cols_events, sizeof(_s_cols_events) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaEventsScanner::~SchemaEventsScanner() = default;

} // namespace starrocks
