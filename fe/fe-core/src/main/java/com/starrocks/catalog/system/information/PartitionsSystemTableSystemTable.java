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

import static com.starrocks.catalog.system.SystemTable.FN_REFLEN;
import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class PartitionsSystemTableSystemTable {
    private static final String NAME = "partitions";

    public static SystemTable create(String catalogName) {
        return new SystemTable(
                catalogName,
                SystemId.PARTITIONS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("TABLE_CATALOG", TypeFactory.createVarchar(FN_REFLEN))
                        .column("TABLE_SCHEMA", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("TABLE_NAME", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("PARTITION_NAME", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("SUBPARTITION_NAME", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("PARTITION_ORDINAL_POSITION", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("PARTITION_METHOD", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("SUBPARTITION_METHOD", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("PARTITION_EXPRESSION", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("SUBPARTITION_EXPRESSION", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("PARTITION_DESCRIPTION", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("TABLE_ROWS", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("AVG_ROW_LENGTH", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("DATA_LENGTH", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("MAX_DATA_LENGTH", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("INDEX_LENGTH", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("DATA_FREE", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("CREATE_TIME", TypeFactory.createType(PrimitiveType.DATETIME))
                        .column("UPDATE_TIME", TypeFactory.createType(PrimitiveType.DATETIME))
                        .column("CHECK_TIME", TypeFactory.createType(PrimitiveType.DATETIME))
                        .column("CHECKSUM", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("PARTITION_COMMENT", TypeFactory.createVarchar(2048))
                        .column("NODEGROUP", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("TABLESPACE_NAME", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("SUBPARTITION_ORDINAL_POSITION", TypeFactory.createType(PrimitiveType.BIGINT))
                        .build(), TSchemaTableType.SCH_PARTITIONS);
    }
}
