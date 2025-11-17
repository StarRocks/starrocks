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
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;

import static com.starrocks.catalog.system.SystemTable.builder;

public class BeCloudNativeCompactionsSystemTable {
    private static final String NAME = "be_cloud_native_compactions";

    public static SystemTable create() {
        return new SystemTable(SystemId.BE_CLOUD_NATIVE_COMPACTIONS,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("BE_ID", IntegerType.BIGINT)
                        .column("TXN_ID", IntegerType.BIGINT)
                        .column("TABLET_ID", IntegerType.BIGINT)
                        .column("VERSION", IntegerType.BIGINT)
                        .column("SKIPPED", BooleanType.BOOLEAN)
                        .column("RUNS", IntegerType.INT)
                        .column("START_TIME", DateType.DATETIME)
                        .column("FINISH_TIME", DateType.DATETIME)
                        .column("PROGRESS", IntegerType.INT)
                        .column("STATUS", VarcharType.VARCHAR)
                        .column("PROFILE", VarcharType.VARCHAR)
                        .build(), TSchemaTableType.SCH_BE_CLOUD_NATIVE_COMPACTIONS);
    }
}
