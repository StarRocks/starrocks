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

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;
import static com.starrocks.thrift.TSchemaTableType.SCH_PROCEDURES;

public class RoutinesSystemTable {
    public static SystemTable create() {
        return new SystemTable(SystemId.ROUTINES_ID,
                "routines",
                Table.TableType.SCHEMA,
                builder()
                        .column("SPECIFIC_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("ROUTINE_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("ROUTINE_SCHEMA", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("ROUTINE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("ROUTINE_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("DTD_IDENTIFIER", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("ROUTINE_BODY", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("ROUTINE_DEFINITION", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("EXTERNAL_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("EXTERNAL_LANGUAGE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PARAMETER_STYLE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("IS_DETERMINISTIC", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("SQL_DATA_ACCESS", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("SQL_PATH", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("SECURITY_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("CREATED", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("LAST_ALTERED", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("SQL_MODE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("ROUTINE_COMMENT", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("DEFINER", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("CHARACTER_SET_CLIENT", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("COLLATION_CONNECTION", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("DATABASE_COLLATION", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .build(), SCH_PROCEDURES);
    }
}
