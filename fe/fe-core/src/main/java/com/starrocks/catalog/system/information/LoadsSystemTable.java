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

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class LoadsSystemTable {
    public static SystemTable create() {
        return new SystemTable(SystemId.LOADS_ID,
                "loads",
                Table.TableType.SCHEMA,
                builder()
                        .column("JOB_ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("LABEL", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("DATABASE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("STATE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PROGRESS", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PRIORITY", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("SCAN_ROWS", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("FILTERED_ROWS", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("UNSELECTED_ROWS", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("SINK_ROWS", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("ETL_INFO", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TASK_INFO", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("CREATE_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("ETL_START_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("ETL_FINISH_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("LOAD_START_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("LOAD_FINISH_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("JOB_DETAILS", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("ERROR_MSG", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TRACKING_URL", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TRACKING_SQL", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("REJECTED_RECORD_PATH", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .build(), TSchemaTableType.SCH_LOADS);
    }
}
