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

public class FeTabletSchedulesSystemTable {
    public static SystemTable create() {
        return new SystemTable(SystemId.FE_SCHEDULES_ID,
                "fe_tablet_schedules",
                Table.TableType.SCHEMA,
                builder()
                        .column("TABLE_ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("PARTITION_ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("TABLET_ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PRIORITY", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("STATE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TABLET_STATUS", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("CREATE_TIME", ScalarType.createType(PrimitiveType.DOUBLE))
                        .column("SCHEDULE_TIME", ScalarType.createType(PrimitiveType.DOUBLE))
                        .column("FINISH_TIME", ScalarType.createType(PrimitiveType.DOUBLE))
                        .column("CLONE_SRC", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("CLONE_DEST", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("CLONE_BYTES", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("CLONE_DURATION", ScalarType.createType(PrimitiveType.DOUBLE))
                        .column("MSG", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .build(), TSchemaTableType.SCH_FE_TABLET_SCHEDULES);
    }
}
