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

import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.thrift.TSchemaTableType;

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class TablePrivilegesSystemTable {
    public static SystemTable create(String catalogName) {
        return new SystemTable(
                catalogName,
                SystemId.TABLE_PRIVILEGES_ID,
                "table_privileges",
                Table.TableType.SCHEMA,
                builder()
                        .column("GRANTEE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TABLE_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TABLE_SCHEMA", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TABLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PRIVILEGE_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("IS_GRANTABLE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .build(), TSchemaTableType.SCH_TABLE_PRIVILEGES);
    }
}


