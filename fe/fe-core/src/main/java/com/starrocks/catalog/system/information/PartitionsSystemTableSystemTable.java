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
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
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
                        .column("TABLE_CATALOG", TypeFactory.createVarcharType(FN_REFLEN))
                        .column("TABLE_SCHEMA", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("TABLE_NAME", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("PARTITION_NAME", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("SUBPARTITION_NAME", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("PARTITION_ORDINAL_POSITION", IntegerType.BIGINT)
                        .column("PARTITION_METHOD", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("SUBPARTITION_METHOD", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("PARTITION_EXPRESSION", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("SUBPARTITION_EXPRESSION", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("PARTITION_DESCRIPTION", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("TABLE_ROWS", IntegerType.BIGINT)
                        .column("AVG_ROW_LENGTH", IntegerType.BIGINT)
                        .column("DATA_LENGTH", IntegerType.BIGINT)
                        .column("MAX_DATA_LENGTH", IntegerType.BIGINT)
                        .column("INDEX_LENGTH", IntegerType.BIGINT)
                        .column("DATA_FREE", IntegerType.BIGINT)
                        .column("CREATE_TIME", DateType.DATETIME)
                        .column("UPDATE_TIME", DateType.DATETIME)
                        .column("CHECK_TIME", DateType.DATETIME)
                        .column("CHECKSUM", IntegerType.BIGINT)
                        .column("PARTITION_COMMENT", TypeFactory.createVarcharType(2048))
                        .column("NODEGROUP", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("TABLESPACE_NAME", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("SUBPARTITION_ORDINAL_POSITION", IntegerType.BIGINT)
                        .build(), TSchemaTableType.SCH_PARTITIONS);
    }
}
