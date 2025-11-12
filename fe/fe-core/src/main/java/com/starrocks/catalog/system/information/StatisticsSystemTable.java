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

import static com.starrocks.catalog.system.SystemTable.MAX_FIELD_VARCHAR_LENGTH;
import static com.starrocks.catalog.system.SystemTable.builder;

public class StatisticsSystemTable {
    private static final String NAME = "statistics";

    public static SystemTable create(String catalogName) {
        return new SystemTable(
                catalogName,
                SystemId.STATISTICS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("TABLE_CATALOG", TypeFactory.createVarchar(512))
                        .column("TABLE_SCHEMA", TypeFactory.createVarchar(64))
                        .column("TABLE_NAME", TypeFactory.createVarchar(64))
                        .column("NON_UNIQUE", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("INDEX_SCHEMA", TypeFactory.createVarchar(64))
                        .column("INDEX_NAME", TypeFactory.createVarchar(64))
                        .column("SEQ_IN_INDEX", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("COLUMN_NAME", TypeFactory.createVarchar(64))
                        .column("COLLATION", TypeFactory.createVarchar(1))
                        .column("CARDINALITY", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("SUB_PART", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("PACKED", TypeFactory.createVarchar(10))
                        .column("NULLABLE", TypeFactory.createVarchar(3))
                        .column("INDEX_TYPE", TypeFactory.createVarchar(16))
                        .column("COMMENT", TypeFactory.createVarchar(16))
                        .column("INDEX_COMMENT", TypeFactory.createVarchar(1024))
                        .column("EXPRESSION", TypeFactory.createVarcharType(MAX_FIELD_VARCHAR_LENGTH))
                        .build(), TSchemaTableType.SCH_STATISTICS);
    }
}
