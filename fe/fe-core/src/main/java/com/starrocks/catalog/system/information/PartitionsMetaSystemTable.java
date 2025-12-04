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
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class PartitionsMetaSystemTable {
    public static final String NAME = "partitions_meta";

    public static SystemTable create() {
        return new SystemTable(SystemId.PARTITIONS_META_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("DB_NAME", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("TABLE_NAME", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("PARTITION_NAME", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("PARTITION_ID", IntegerType.BIGINT)
                        .column("COMPACT_VERSION", IntegerType.BIGINT)
                        .column("VISIBLE_VERSION", IntegerType.BIGINT)
                        .column("VISIBLE_VERSION_TIME", DateType.DATETIME)
                        .column("NEXT_VERSION", IntegerType.BIGINT)
                        .column("DATA_VERSION", IntegerType.BIGINT)
                        .column("VERSION_EPOCH", IntegerType.BIGINT)
                        .column("VERSION_TXN_TYPE", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("PARTITION_KEY", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        // corresponding to `Range` or `List` in `SHOW PARTITIONS FROM XXX`
                        .column("PARTITION_VALUE", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("DISTRIBUTION_KEY", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("BUCKETS", IntegerType.INT)
                        .column("REPLICATION_NUM", IntegerType.INT)
                        .column("STORAGE_MEDIUM", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("COOLDOWN_TIME", DateType.DATETIME)
                        .column("LAST_CONSISTENCY_CHECK_TIME", DateType.DATETIME)
                        .column("IS_IN_MEMORY", BooleanType.BOOLEAN)
                        .column("IS_TEMP", BooleanType.BOOLEAN)
                        .column("DATA_SIZE", IntegerType.BIGINT)
                        .column("ROW_COUNT", IntegerType.BIGINT)
                        .column("ENABLE_DATACACHE", BooleanType.BOOLEAN)
                        .column("AVG_CS", FloatType.DOUBLE)
                        .column("P50_CS", FloatType.DOUBLE)
                        .column("MAX_CS", FloatType.DOUBLE)
                        .column("STORAGE_PATH", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("STORAGE_SIZE", IntegerType.BIGINT)
                        .column("TABLET_BALANCED", BooleanType.BOOLEAN)
                        .column("METADATA_SWITCH_VERSION", IntegerType.BIGINT)
                        .build(), TSchemaTableType.SCH_PARTITIONS_META);
    }
}
