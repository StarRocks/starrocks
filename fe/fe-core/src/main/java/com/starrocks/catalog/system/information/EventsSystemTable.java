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

public class EventsSystemTable {
    private static final String NAME = "events";

    public static SystemTable create(String catalogName) {
        return new SystemTable(
                catalogName,
                SystemId.EVENTS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("EVENT_CATALOG", TypeFactory.createVarchar(64))
                        .column("EVENT_SCHEMA", TypeFactory.createVarchar(64))
                        .column("EVENT_NAME", TypeFactory.createVarchar(64))
                        .column("DEFINER", TypeFactory.createVarchar(77))
                        .column("TIME_ZONE", TypeFactory.createVarchar(64))
                        .column("EVENT_BODY", TypeFactory.createVarchar(8))
                        // TODO: Type for EVENT_DEFINITION should be `longtext`, but `varchar(65535)` was set at this stage.
                        .column("EVENT_DEFINITION",
                                TypeFactory.createVarchar(MAX_FIELD_VARCHAR_LENGTH))
                        .column("EVENT_TYPE", TypeFactory.createVarchar(9))
                        .column("EXECUTE_AT", DateType.DATETIME)
                        .column("INTERVAL_VALUE", TypeFactory.createVarchar(256))
                        .column("INTERVAL_FIELD", TypeFactory.createVarchar(18))
                        .column("SQL_MODE", TypeFactory.createVarchar(8192))
                        .column("STARTS", DateType.DATETIME)
                        .column("ENDS", DateType.DATETIME)
                        .column("STATUS", TypeFactory.createVarchar(18))
                        .column("ON_COMPLETION", TypeFactory.createVarchar(12))
                        .column("CREATED", DateType.DATETIME)
                        .column("LAST_ALTERED", DateType.DATETIME)
                        .column("LAST_EXECUTED", DateType.DATETIME)
                        .column("EVENT_COMMENT", TypeFactory.createVarchar(64))
                        .column("ORIGINATOR", IntegerType.BIGINT)
                        .column("CHARACTER_SET_CLIENT", TypeFactory.createVarchar(32))
                        .column("COLLATION_CONNECTION", TypeFactory.createVarchar(32))
                        .column("DATABASE_COLLATION", TypeFactory.createVarchar(32))
                        .build(), TSchemaTableType.SCH_EVENTS);
    }
}
