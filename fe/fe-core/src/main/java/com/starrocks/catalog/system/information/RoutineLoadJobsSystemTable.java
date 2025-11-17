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

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class RoutineLoadJobsSystemTable {
    public static final String NAME = "routine_load_jobs";

    public static SystemTable create() {
        return new SystemTable(SystemId.ROUTINE_LOAD_JOBS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("ID", IntegerType.BIGINT)
                        .column("NAME", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("CREATE_TIME", DateType.DATETIME)
                        .column("PAUSE_TIME", DateType.DATETIME)
                        .column("END_TIME", DateType.DATETIME)
                        .column("DB_NAME", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("TABLE_NAME", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("STATE", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("DATA_SOURCE_TYPE", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("CURRENT_TASK_NUM", IntegerType.BIGINT)
                        .column("JOB_PROPERTIES", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("DATA_SOURCE_PROPERTIES", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("CUSTOM_PROPERTIES", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("STATISTICS", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("PROGRESS", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("REASONS_OF_STATE_CHANGED", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("ERROR_LOG_URLS", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("TRACKING_SQL", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("OTHER_MSG", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("LATEST_SOURCE_POSITION", TypeFactory.createJsonType())
                        .column("OFFSET_LAG", TypeFactory.createJsonType())
                        .column("TIMESTAMP_PROGRESS", TypeFactory.createJsonType())
                        .build(), TSchemaTableType.SCH_ROUTINE_LOAD_JOBS);
    }
}
