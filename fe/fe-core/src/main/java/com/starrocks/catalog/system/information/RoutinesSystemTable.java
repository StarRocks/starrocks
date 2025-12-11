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
import com.starrocks.type.DateType;
import com.starrocks.type.TypeFactory;

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;
import static com.starrocks.thrift.TSchemaTableType.SCH_PROCEDURES;

public class RoutinesSystemTable {
    private static final String NAME = "routines";

    public static SystemTable create(String catalogName) {
        return new SystemTable(
                catalogName,
                SystemId.ROUTINES_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("SPECIFIC_NAME", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("ROUTINE_CATALOG", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("ROUTINE_SCHEMA", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("ROUTINE_NAME", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("ROUTINE_TYPE", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("DTD_IDENTIFIER", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("ROUTINE_BODY", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("ROUTINE_DEFINITION", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("EXTERNAL_NAME", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("EXTERNAL_LANGUAGE", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("PARAMETER_STYLE", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("IS_DETERMINISTIC", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("SQL_DATA_ACCESS", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("SQL_PATH", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("SECURITY_TYPE", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("CREATED", DateType.DATETIME)
                        .column("LAST_ALTERED", DateType.DATETIME)
                        .column("SQL_MODE", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("ROUTINE_COMMENT", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("DEFINER", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("CHARACTER_SET_CLIENT", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("COLLATION_CONNECTION", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("DATABASE_COLLATION", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .build(), SCH_PROCEDURES);
    }
}
