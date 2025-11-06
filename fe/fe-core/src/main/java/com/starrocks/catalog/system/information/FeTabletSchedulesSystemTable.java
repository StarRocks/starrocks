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
import com.starrocks.type.PrimitiveType;
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
                        .column("TABLET_ID", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("TABLE_ID", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("PARTITION_ID", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("TYPE", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("STATE", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("SCHEDULE_REASON", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("MEDIUM", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("PRIORITY", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("ORIG_PRIORITY", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("LAST_PRIORITY_ADJUST_TIME", TypeFactory.createType(PrimitiveType.DATETIME))
                        .column("VISIBLE_VERSION", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("COMMITTED_VERSION", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("SRC_BE_ID", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("SRC_PATH", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("DEST_BE_ID", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("DEST_PATH", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("TIMEOUT", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("CREATE_TIME", TypeFactory.createType(PrimitiveType.DATETIME))
                        .column("SCHEDULE_TIME", TypeFactory.createType(PrimitiveType.DATETIME))
                        .column("FINISH_TIME", TypeFactory.createType(PrimitiveType.DATETIME))
                        .column("CLONE_BYTES", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("CLONE_DURATION", TypeFactory.createType(PrimitiveType.DOUBLE))
                        .column("CLONE_RATE", TypeFactory.createType(PrimitiveType.DOUBLE))
                        .column("FAILED_SCHEDULE_COUNT", TypeFactory.createType(PrimitiveType.INT))
                        .column("FAILED_RUNNING_COUNT", TypeFactory.createType(PrimitiveType.INT))
                        .column("MSG", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .build(), TSchemaTableType.SCH_FE_TABLET_SCHEDULES);
    }
}
