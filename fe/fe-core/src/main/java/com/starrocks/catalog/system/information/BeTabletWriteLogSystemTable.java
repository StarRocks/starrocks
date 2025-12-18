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

import static com.starrocks.catalog.system.SystemTable.builder;

public class BeTabletWriteLogSystemTable {
    public static final String NAME = "be_tablet_write_log";

    public static SystemTable create() {
        return new SystemTable(SystemId.BE_TABLET_WRITE_LOG_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("BE_ID", IntegerType.BIGINT)
                        .column("BEGIN_TIME", DateType.DATETIME)
                        .column("FINISH_TIME", DateType.DATETIME)
                        .column("TXN_ID", IntegerType.BIGINT)
                        .column("TABLET_ID", IntegerType.BIGINT)
                        .column("TABLE_ID", IntegerType.BIGINT)
                        .column("PARTITION_ID", IntegerType.BIGINT)
                        .column("LOG_TYPE", TypeFactory.createVarcharType(64))
                        .column("INPUT_ROWS", IntegerType.BIGINT)
                        .column("INPUT_BYTES", IntegerType.BIGINT)
                        .column("OUTPUT_ROWS", IntegerType.BIGINT)
                        .column("OUTPUT_BYTES", IntegerType.BIGINT)
                        .column("INPUT_SEGMENTS", IntegerType.INT)
                        .column("OUTPUT_SEGMENTS", IntegerType.INT)
                        .column("LABEL", TypeFactory.createVarcharType(1024))
                        .column("COMPACTION_SCORE", IntegerType.BIGINT)
                        .column("COMPACTION_TYPE", TypeFactory.createVarcharType(64))
                        .build(),
                TSchemaTableType.SCH_BE_TABLET_WRITE_LOG);
    }
}

