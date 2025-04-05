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

import static com.starrocks.catalog.system.SystemTable.FN_REFLEN;
import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class WarehouseMetricsSystemTable {
    public static final String NAME = "warehouse_metrics";

    public static SystemTable create() {
        return new SystemTable(
                SystemId.WAREHOUSE_METRICS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("WAREHOUSE_ID", ScalarType.createVarchar(FN_REFLEN))
                        .column("WAREHOUSE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("QUEUE_PENDING_LENGTH", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("QUEUE_RUNNING_LENGTH", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("MAX_PENDING_LENGTH", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("MAX_PENDING_TIME_SECOND", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("EARLIEST_QUERY_WAIT_TIME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("MAX_REQUIRED_SLOTS", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("SUM_REQUIRED_SLOTS", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("REMAIN_SLOTS", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("MAX_SLOTS", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .build(), TSchemaTableType.SCH_WAREHOUSE_METRICS);
    }
}
