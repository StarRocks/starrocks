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

public class ReferentialConstraintsSystemTable {
    public static SystemTable create() {
        return new SystemTable(SystemId.REFERENTIAL_CONSTRAINTS_ID,
                "referential_constraints",
                Table.TableType.SCHEMA,
                builder()
                        .column("CONSTRAINT_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("CONSTRAINT_SCHEMA", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("CONSTRAINT_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("UNIQUE_CONSTRAINT_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("UNIQUE_CONSTRAINT_SCHEMA", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("UNIQUE_CONSTRAINT_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("MATCH_OPTION", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("UPDATE_RULE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("DELETE_RULE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TABLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("REFERENCED_TABLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .build(), TSchemaTableType.SCH_REFERENTIAL_CONSTRAINTS);
    }
}
