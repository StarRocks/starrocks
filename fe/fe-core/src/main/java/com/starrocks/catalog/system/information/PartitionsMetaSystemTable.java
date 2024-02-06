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

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class PartitionsMetaSystemTable {
    public static SystemTable create() {
        return new SystemTable(SystemId.PARTITIONS_META_ID,
                "partitions_meta",
                Table.TableType.SCHEMA,
                builder()
                        .column("DB_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TABLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PARTITION_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PARTITION_ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("COMPACT_VERSION", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("VISIBLE_VERSION", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("VISIBLE_VERSION_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("NEXT_VERSION", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("PARTITION_KEY", ScalarType.createVarchar(NAME_CHAR_LEN))
                        // corresponding to `Range` or `List` in `SHOW PARTITIONS FROM XXX`
                        .column("PARTITION_VALUE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("DISTRIBUTION_KEY", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("BUCKETS", ScalarType.createType(PrimitiveType.INT))
                        .column("REPLICATION_NUM", ScalarType.createType(PrimitiveType.INT))
                        .column("STORAGE_MEDIUM", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("COOLDOWN_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("LAST_CONSISTENCY_CHECK_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("IS_IN_MEMORY", ScalarType.createType(PrimitiveType.BOOLEAN))
                        .column("IS_TEMP", ScalarType.createType(PrimitiveType.BOOLEAN))
                        .column("DATA_SIZE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("ROW_COUNT", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("ENABLE_DATACACHE", ScalarType.createType(PrimitiveType.BOOLEAN))
                        .column("AVG_CS", ScalarType.createType(PrimitiveType.DOUBLE))
                        .column("P50_CS", ScalarType.createType(PrimitiveType.DOUBLE))
                        .column("MAX_CS", ScalarType.createType(PrimitiveType.DOUBLE))
                        .column("STORAGE_PATH", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .build(), TSchemaTableType.SCH_PARTITIONS_META);
    }
}
