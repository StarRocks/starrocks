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
import com.starrocks.type.JsonType;
import com.starrocks.type.TypeFactory;

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class LoadsSystemTable {
    public static final String NAME = "loads";

    public static SystemTable create() {
        return new SystemTable(SystemId.LOADS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("ID", IntegerType.BIGINT)
                        .column("LABEL", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("PROFILE_ID", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("DB_NAME", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("TABLE_NAME", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("USER", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("WAREHOUSE", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("STATE", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("PROGRESS", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("TYPE", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("PRIORITY", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("SCAN_ROWS", IntegerType.BIGINT)
                        .column("SCAN_BYTES", IntegerType.BIGINT)
                        .column("FILTERED_ROWS", IntegerType.BIGINT)
                        .column("UNSELECTED_ROWS", IntegerType.BIGINT)
                        .column("SINK_ROWS", IntegerType.BIGINT)
                        .column("RUNTIME_DETAILS", JsonType.JSON)
                        .column("CREATE_TIME", DateType.DATETIME)
                        .column("LOAD_START_TIME", DateType.DATETIME)
                        .column("LOAD_COMMIT_TIME", DateType.DATETIME)
                        .column("LOAD_FINISH_TIME", DateType.DATETIME)
                        .column("PROPERTIES", JsonType.JSON)
                        .column("ERROR_MSG", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("TRACKING_SQL", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("REJECTED_RECORD_PATH", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("JOB_ID", IntegerType.BIGINT)
                        .build(), TSchemaTableType.SCH_LOADS);
    }
}
