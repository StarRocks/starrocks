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
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class FeTabletSchedulesSystemTable {
    public static final String NAME = "fe_tablet_schedules";

    public static SystemTable create() {
        return new SystemTable(SystemId.FE_SCHEDULES_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("TABLET_ID", IntegerType.BIGINT)
                        .column("TABLE_ID", IntegerType.BIGINT)
                        .column("PARTITION_ID", IntegerType.BIGINT)
                        .column("TYPE", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("STATE", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("SCHEDULE_REASON", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("MEDIUM", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("PRIORITY", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("ORIG_PRIORITY", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("LAST_PRIORITY_ADJUST_TIME", DateType.DATETIME)
                        .column("VISIBLE_VERSION", IntegerType.BIGINT)
                        .column("COMMITTED_VERSION", IntegerType.BIGINT)
                        .column("SRC_BE_ID", IntegerType.BIGINT)
                        .column("SRC_PATH", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("DEST_BE_ID", IntegerType.BIGINT)
                        .column("DEST_PATH", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("TIMEOUT", IntegerType.BIGINT)
                        .column("CREATE_TIME", DateType.DATETIME)
                        .column("SCHEDULE_TIME", DateType.DATETIME)
                        .column("FINISH_TIME", DateType.DATETIME)
                        .column("CLONE_BYTES", IntegerType.BIGINT)
                        .column("CLONE_DURATION", FloatType.DOUBLE)
                        .column("CLONE_RATE", FloatType.DOUBLE)
                        .column("FAILED_SCHEDULE_COUNT", IntegerType.INT)
                        .column("FAILED_RUNNING_COUNT", IntegerType.INT)
                        .column("MSG", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .build(), TSchemaTableType.SCH_FE_TABLET_SCHEDULES);
    }
}
