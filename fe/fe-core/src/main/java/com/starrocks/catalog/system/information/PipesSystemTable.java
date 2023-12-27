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

public class PipesSystemTable {

    public static final String NAME = "pipes";

    public static SystemTable create() {
        return new SystemTable(SystemId.PIPES_ID, NAME, Table.TableType.SCHEMA,
                SystemTable.builder()
                        .column("DATABASE_NAME", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .column("PIPE_ID", ScalarType.BIGINT)
                        .column("PIPE_NAME", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .column("STATE", ScalarType.createVarcharType(8))
                        .column("TABLE_NAME", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .column("LOAD_STATUS", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .column("LAST_ERROR", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .column("CREATED_TIME", ScalarType.DATETIME)
                        .build(),
                TSchemaTableType.SCH_PIPES);
    }
}
