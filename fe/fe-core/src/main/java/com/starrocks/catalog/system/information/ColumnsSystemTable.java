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

public class ColumnsSystemTable {
    public static SystemTable create(String catalogName) {
        return new SystemTable(
                catalogName,
                SystemId.COLUMNS_ID,
                "columns",
                Table.TableType.SCHEMA,
                builder()
                        .column("TABLE_CATALOG", ScalarType.createVarchar(512))
                        .column("TABLE_SCHEMA", ScalarType.createVarchar(64))
                        .column("TABLE_NAME", ScalarType.createVarchar(64))
                        .column("COLUMN_NAME", ScalarType.createVarchar(64))
                        .column("ORDINAL_POSITION", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("COLUMN_DEFAULT", ScalarType.createVarchar(1024))
                        .column("IS_NULLABLE", ScalarType.createVarchar(3))
                        .column("DATA_TYPE", ScalarType.createVarchar(64))
                        .column("CHARACTER_MAXIMUM_LENGTH",
                                ScalarType.createType(PrimitiveType.BIGINT))
                        .column("CHARACTER_OCTET_LENGTH",
                                ScalarType.createType(PrimitiveType.BIGINT))
                        .column("NUMERIC_PRECISION", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("NUMERIC_SCALE", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("DATETIME_PRECISION", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("CHARACTER_SET_NAME", ScalarType.createVarchar(32))
                        .column("COLLATION_NAME", ScalarType.createVarchar(32))
                        .column("COLUMN_TYPE", ScalarType.createVarchar(32))
                        .column("COLUMN_KEY", ScalarType.createVarchar(3))
                        .column("EXTRA", ScalarType.createVarchar(27))
                        .column("PRIVILEGES", ScalarType.createVarchar(80))
                        .column("COLUMN_COMMENT", ScalarType.createVarchar(255))
                        .column("COLUMN_SIZE", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("DECIMAL_DIGITS", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("GENERATION_EXPRESSION", ScalarType.createVarchar(64))
                        .column("SRS_ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .build(), TSchemaTableType.SCH_COLUMNS);
    }
}
