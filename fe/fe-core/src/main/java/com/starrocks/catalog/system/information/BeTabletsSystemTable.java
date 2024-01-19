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

public class BeTabletsSystemTable {
    public static SystemTable create() {
        return new SystemTable(SystemId.BE_TABLETS_ID,
                "be_tablets",
                Table.TableType.SCHEMA,
                builder()
                        .column("BE_ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("TABLE_ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("PARTITION_ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("TABLET_ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("NUM_VERSION", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("MAX_VERSION", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("MIN_VERSION", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("NUM_ROWSET", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("NUM_ROW", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("DATA_SIZE", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("INDEX_MEM", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("CREATE_TIME", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("STATE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("DATA_DIR", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("SHARD_ID", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("SCHEMA_HASH", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("INDEX_DISK", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("MEDIUM_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("NUM_SEGMENT", ScalarType.createType(PrimitiveType.BIGINT))
                        .build(), TSchemaTableType.SCH_BE_TABLETS);
    }
}
