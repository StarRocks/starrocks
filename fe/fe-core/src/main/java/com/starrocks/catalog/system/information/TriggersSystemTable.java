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
package com.starrocks.catalog.system.information;

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.thrift.TSchemaTableType;

import static com.starrocks.catalog.system.SystemTable.MAX_FIELD_VARCHAR_LENGTH;
import static com.starrocks.catalog.system.SystemTable.builder;

public class TriggersSystemTable {
    public static SystemTable create() {
        return new SystemTable(SystemId.TRIGGERS_ID,
                "triggers",
                Table.TableType.SCHEMA,
                builder()
                        .column("TRIGGER_CATALOG", ScalarType.createVarchar(512))
                        .column("TRIGGER_SCHEMA", ScalarType.createVarchar(64))
                        .column("TRIGGER_NAME", ScalarType.createVarchar(64))
                        .column("EVENT_MANIPULATION", ScalarType.createVarchar(6))
                        .column("EVENT_OBJECT_CATALOG", ScalarType.createVarchar(512))
                        .column("EVENT_OBJECT_SCHEMA", ScalarType.createVarchar(64))
                        .column("EVENT_OBJECT_TABLE", ScalarType.createVarchar(64))
                        .column("ACTION_ORDER", ScalarType.createType(PrimitiveType.BIGINT))
                        // TODO: Type for ACTION_CONDITION && ACTION_STATEMENT should be `longtext`, but `varchar(65535)` was set at this stage.
                        .column("ACTION_CONDITION",
                                ScalarType.createVarchar(MAX_FIELD_VARCHAR_LENGTH))
                        .column("ACTION_STATEMENT",
                                ScalarType.createVarchar(MAX_FIELD_VARCHAR_LENGTH))
                        .column("ACTION_ORIENTATION", ScalarType.createVarchar(9))
                        .column("ACTION_TIMING", ScalarType.createVarchar(6))
                        .column("ACTION_REFERENCE_OLD_TABLE", ScalarType.createVarchar(64))
                        .column("ACTION_REFERENCE_NEW_TABLE", ScalarType.createVarchar(64))
                        .column("ACTION_REFERENCE_OLD_ROW", ScalarType.createVarchar(3))
                        .column("ACTION_REFERENCE_NEW_ROW", ScalarType.createVarchar(3))
                        .column("CREATED", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("SQL_MODE", ScalarType.createVarchar(8192))
                        .column("DEFINER", ScalarType.createVarchar(77))
                        .column("CHARACTER_SET_CLIENT", ScalarType.createVarchar(32))
                        .column("COLLATION_CONNECTION", ScalarType.createVarchar(32))
                        .column("DATABASE_COLLATION", ScalarType.createVarchar(32))
                        .build(), TSchemaTableType.SCH_TRIGGERS);
    }
}
