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

public class RoutineLoadJobsSystemTable {
    public static SystemTable create() {
        return new SystemTable(SystemId.ROUTINE_LOAD_JOBS_ID,
                "routine_load_jobs",
                Table.TableType.SCHEMA,
                builder()
                        .column("ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("CREATE_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("PAUSE_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("END_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("DB_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TABLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("STATE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("DATA_SOURCE_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("CURRENT_TASK_NUM", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("JOB_PROPERTIES", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("DATA_SOURCE_PROPERTIES", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("CUSTOM_PROPERTIES", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("STATISTICS", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PROGRESS", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("REASONS_OF_STATE_CHANGED", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("ERROR_LOG_URLS", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TRACKING_SQL", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("OTHER_MSG", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .build(), TSchemaTableType.SCH_ROUTINE_LOAD_JOBS);
    }
}
