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

public class BackendSystemTable {
    public static SystemTable create() {
        return new SystemTable(SystemId.BACKENDS_ID,
                "backends",
                Table.TableType.SCHEMA,
                builder()
                        .column("BE_ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("VERSION", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("IP", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("HEARTBEAT_PORT", ScalarType.createType(PrimitiveType.INT))
                        .column("BE_PORT", ScalarType.createType(PrimitiveType.INT))
                        .column("HTTP_PORT", ScalarType.createType(PrimitiveType.INT))
                        .column("BRPC_PORT", ScalarType.createType(PrimitiveType.INT))
                        .column("ALIVE", ScalarType.createType(PrimitiveType.BOOLEAN))
                        .column("DECOMMISSIONED", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("DATA_USED_CAPACITY", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("AVAIL_CAPACITY", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("TOTAL_CAPACITY", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("DATA_TOTAL_CAPACITY", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("CPU_CORES", ScalarType.createType(PrimitiveType.INT))
                        .build(), TSchemaTableType.SCH_BACKENDS);
    }
}

