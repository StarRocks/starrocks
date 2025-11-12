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

import static com.starrocks.catalog.system.SystemTable.builder;

public class ColumnsSystemTable {
    private static final String NAME = "columns";

    public static SystemTable create(String catalogName) {
        return new SystemTable(
                catalogName,
                SystemId.COLUMNS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("TABLE_CATALOG", TypeFactory.createVarchar(512))
                        .column("TABLE_SCHEMA", TypeFactory.createVarchar(64))
                        .column("TABLE_NAME", TypeFactory.createVarchar(64))
                        .column("COLUMN_NAME", TypeFactory.createVarchar(64))
                        .column("ORDINAL_POSITION", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("COLUMN_DEFAULT", TypeFactory.createVarchar(1024))
                        .column("IS_NULLABLE", TypeFactory.createVarchar(3))
                        .column("DATA_TYPE", TypeFactory.createVarchar(64))
                        .column("CHARACTER_MAXIMUM_LENGTH",
                                TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("CHARACTER_OCTET_LENGTH",
                                TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("NUMERIC_PRECISION", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("NUMERIC_SCALE", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("DATETIME_PRECISION", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("CHARACTER_SET_NAME", TypeFactory.createVarchar(32))
                        .column("COLLATION_NAME", TypeFactory.createVarchar(32))
                        .column("COLUMN_TYPE", TypeFactory.createVarchar(32))
                        .column("COLUMN_KEY", TypeFactory.createVarchar(3))
                        .column("EXTRA", TypeFactory.createVarchar(27))
                        .column("PRIVILEGES", TypeFactory.createVarchar(80))
                        .column("COLUMN_COMMENT", TypeFactory.createVarchar(255))
                        .column("COLUMN_SIZE", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("DECIMAL_DIGITS", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("GENERATION_EXPRESSION", TypeFactory.createVarchar(64))
                        .column("SRS_ID", TypeFactory.createType(PrimitiveType.BIGINT))
                        .build(), TSchemaTableType.SCH_COLUMNS);
    }
}
