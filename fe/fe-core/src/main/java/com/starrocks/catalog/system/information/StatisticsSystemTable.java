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

import static com.starrocks.catalog.system.SystemTable.builder;

public class StatisticsSystemTable {
    public static SystemTable create() {
        return new SystemTable(SystemId.STATISTICS_ID,
                "statistics",
                Table.TableType.SCHEMA,
                builder()
                        .column("TABLE_CATALOG", ScalarType.createVarchar(512))
                        .column("TABLE_SCHEMA", ScalarType.createVarchar(64))
                        .column("TABLE_NAME", ScalarType.createVarchar(64))
                        .column("NON_UNIQUE", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("INDEX_SCHEMA", ScalarType.createVarchar(64))
                        .column("INDEX_NAME", ScalarType.createVarchar(64))
                        .column("SEQ_IN_INDEX", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("COLUMN_NAME", ScalarType.createVarchar(64))
                        .column("COLLATION", ScalarType.createVarchar(1))
                        .column("CARDINALITY", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("SUB_PART", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("PACKED", ScalarType.createVarchar(10))
                        .column("NULLABLE", ScalarType.createVarchar(3))
                        .column("INDEX_TYPE", ScalarType.createVarchar(16))
                        .column("COMMENT", ScalarType.createVarchar(16))
                        .column("INDEX_COMMENT", ScalarType.createVarchar(1024))
                        .build(), TSchemaTableType.SCH_STATISTICS);
    }
}
