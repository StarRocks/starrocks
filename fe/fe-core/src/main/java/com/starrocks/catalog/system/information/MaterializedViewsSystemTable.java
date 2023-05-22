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

import static com.starrocks.catalog.system.SystemTable.MAX_FIELD_VARCHAR_LENGTH;
import static com.starrocks.catalog.system.SystemTable.builder;

public class MaterializedViewsSystemTable {
    public static SystemTable create() {
        return new SystemTable(SystemId.MATERIALIZED_VIEWS_ID,
                "materialized_views",
                Table.TableType.SCHEMA,
                builder()
                        .column("MATERIALIZED_VIEW_ID", ScalarType.createVarchar(50))
                        .column("TABLE_SCHEMA", ScalarType.createVarchar(20))
                        .column("TABLE_NAME", ScalarType.createVarchar(50))
                        .column("REFRESH_TYPE", ScalarType.createVarchar(20))
                        .column("IS_ACTIVE", ScalarType.createVarchar(10))
                        .column("INACTIVE_REASON", ScalarType.createVarcharType(1024))
                        .column("PARTITION_TYPE", ScalarType.createVarchar(16))
                        .column("TASK_ID", ScalarType.createVarchar(20))
                        .column("TASK_NAME", ScalarType.createVarchar(50))
                        .column("LAST_REFRESH_START_TIME", ScalarType.createVarchar(20))
                        .column("LAST_REFRESH_FINISHED_TIME", ScalarType.createVarchar(20))
                        .column("LAST_REFRESH_DURATION", ScalarType.createVarchar(20))
                        .column("LAST_REFRESH_STATE", ScalarType.createVarchar(20))
                        .column("LAST_REFRESH_FORCE_REFRESH", ScalarType.createVarchar(8))
                        .column("LAST_REFRESH_START_PARTITION", ScalarType.createVarchar(1024))
                        .column("LAST_REFRESH_END_PARTITION", ScalarType.createVarchar(1024))
                        .column("LAST_REFRESH_BASE_REFRESH_PARTITIONS", ScalarType.createVarchar(1024))
                        .column("LAST_REFRESH_MV_REFRESH_PARTITIONS", ScalarType.createVarchar(1024))
                        .column("LAST_REFRESH_ERROR_CODE", ScalarType.createVarchar(20))
                        .column("LAST_REFRESH_ERROR_MESSAGE", ScalarType.createVarchar(1024))
                        .column("TABLE_ROWS", ScalarType.createVarchar(50))
                        .column("MATERIALIZED_VIEW_DEFINITION",
                                ScalarType.createVarchar(MAX_FIELD_VARCHAR_LENGTH))
                        .build(), TSchemaTableType.SCH_MATERIALIZED_VIEWS);
    }
}
