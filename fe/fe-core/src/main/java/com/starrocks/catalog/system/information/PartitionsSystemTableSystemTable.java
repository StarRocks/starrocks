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

import static com.starrocks.catalog.system.SystemTable.FN_REFLEN;
import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class PartitionsSystemTableSystemTable {
    public static SystemTable create() {
        return new SystemTable(
                SystemId.PARTITIONS_ID,
                "partitions",
                Table.TableType.SCHEMA,
                builder()
                        .column("TABLE_CATALOG", ScalarType.createVarchar(FN_REFLEN))
                        .column("TABLE_SCHEMA", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TABLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PARTITION_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("SUBPARTITION_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PARTITION_ORDINAL_POSITION", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("PARTITION_METHOD", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("SUBPARTITION_METHOD", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PARTITION_EXPRESSION", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("SUBPARTITION_EXPRESSION", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PARTITION_DESCRIPTION", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TABLE_ROWS", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("AVG_ROW_LENGTH", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("DATA_LENGTH", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("MAX_DATA_LENGTH", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("INDEX_LENGTH", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("DATA_FREE", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("CREATE_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("UPDATE_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("CHECK_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("CHECKSUM", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("PARTITION_COMMENT", ScalarType.createVarchar(2048))
                        .column("NODEGROUP", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TABLESPACE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .build(), TSchemaTableType.SCH_PARTITIONS);
    }
}
