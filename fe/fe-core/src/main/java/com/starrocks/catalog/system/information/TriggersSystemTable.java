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

import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;

import static com.starrocks.catalog.system.SystemTable.MAX_FIELD_VARCHAR_LENGTH;
import static com.starrocks.catalog.system.SystemTable.builder;

public class TriggersSystemTable {
    private static final String NAME = "triggers";

    public static SystemTable create(String catalogName) {
        return new SystemTable(
                catalogName,
                SystemId.TRIGGERS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("TRIGGER_CATALOG", TypeFactory.createVarcharType(512))
                        .column("TRIGGER_SCHEMA", TypeFactory.createVarcharType(64))
                        .column("TRIGGER_NAME", TypeFactory.createVarcharType(64))
                        .column("EVENT_MANIPULATION", TypeFactory.createVarcharType(6))
                        .column("EVENT_OBJECT_CATALOG", TypeFactory.createVarcharType(512))
                        .column("EVENT_OBJECT_SCHEMA", TypeFactory.createVarcharType(64))
                        .column("EVENT_OBJECT_TABLE", TypeFactory.createVarcharType(64))
                        .column("ACTION_ORDER", IntegerType.BIGINT)
                        // TODO: Type for ACTION_CONDITION && ACTION_STATEMENT should be `longtext`, but `varchar(65535)` was set at this stage.
                        .column("ACTION_CONDITION",
                                TypeFactory.createVarcharType(MAX_FIELD_VARCHAR_LENGTH))
                        .column("ACTION_STATEMENT",
                                TypeFactory.createVarcharType(MAX_FIELD_VARCHAR_LENGTH))
                        .column("ACTION_ORIENTATION", TypeFactory.createVarcharType(9))
                        .column("ACTION_TIMING", TypeFactory.createVarcharType(6))
                        .column("ACTION_REFERENCE_OLD_TABLE", TypeFactory.createVarcharType(64))
                        .column("ACTION_REFERENCE_NEW_TABLE", TypeFactory.createVarcharType(64))
                        .column("ACTION_REFERENCE_OLD_ROW", TypeFactory.createVarcharType(3))
                        .column("ACTION_REFERENCE_NEW_ROW", TypeFactory.createVarcharType(3))
                        .column("CREATED", DateType.DATETIME)
                        .column("SQL_MODE", TypeFactory.createVarcharType(8192))
                        .column("DEFINER", TypeFactory.createVarcharType(77))
                        .column("CHARACTER_SET_CLIENT", TypeFactory.createVarcharType(32))
                        .column("COLLATION_CONNECTION", TypeFactory.createVarcharType(32))
                        .column("DATABASE_COLLATION", TypeFactory.createVarcharType(32))
                        .build(), TSchemaTableType.SCH_TRIGGERS);
    }
}
