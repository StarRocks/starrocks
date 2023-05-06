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

public class EventsSystemTable {
    public static SystemTable create() {
        return new SystemTable(SystemId.EVENTS_ID,
                "events",
                Table.TableType.SCHEMA,
                builder()
                        .column("EVENT_CATALOG", ScalarType.createVarchar(64))
                        .column("EVENT_SCHEMA", ScalarType.createVarchar(64))
                        .column("EVENT_NAME", ScalarType.createVarchar(64))
                        .column("DEFINER", ScalarType.createVarchar(77))
                        .column("TIME_ZONE", ScalarType.createVarchar(64))
                        .column("EVENT_BODY", ScalarType.createVarchar(8))
                        // TODO: Type for EVENT_DEFINITION should be `longtext`, but `varchar(65535)` was set at this stage.
                        .column("EVENT_DEFINITION",
                                ScalarType.createVarchar(MAX_FIELD_VARCHAR_LENGTH))
                        .column("EVENT_TYPE", ScalarType.createVarchar(9))
                        .column("EXECUTE_AT", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("INTERVAL_VALUE", ScalarType.createVarchar(256))
                        .column("INTERVAL_FIELD", ScalarType.createVarchar(18))
                        .column("SQL_MODE", ScalarType.createVarchar(8192))
                        .column("STARTS", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("ENDS", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("STATUS", ScalarType.createVarchar(18))
                        .column("ON_COMPLETION", ScalarType.createVarchar(12))
                        .column("CREATED", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("LAST_ALTERED", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("LAST_EXECUTED", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("EVENT_COMMENT", ScalarType.createVarchar(64))
                        .column("ORIGINATOR", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("CHARACTER_SET_CLIENT", ScalarType.createVarchar(32))
                        .column("COLLATION_CONNECTION", ScalarType.createVarchar(32))
                        .column("DATABASE_COLLATION", ScalarType.createVarchar(32))
                        .build(), TSchemaTableType.SCH_EVENTS);
    }
}
