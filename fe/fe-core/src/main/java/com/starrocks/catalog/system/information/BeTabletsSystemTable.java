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

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class BeTabletsSystemTable {
    public static final String NAME = "be_tablets";

    public static SystemTable create() {
        return new SystemTable(SystemId.BE_TABLETS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("BE_ID", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("TABLE_ID", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("PARTITION_ID", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("TABLET_ID", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("NUM_VERSION", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("MAX_VERSION", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("MIN_VERSION", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("NUM_ROWSET", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("NUM_ROW", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("DATA_SIZE", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("INDEX_MEM", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("CREATE_TIME", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("STATE", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("TYPE", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("DATA_DIR", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("SHARD_ID", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("SCHEMA_HASH", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("INDEX_DISK", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("MEDIUM_TYPE", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("NUM_SEGMENT", TypeFactory.createType(PrimitiveType.BIGINT))
                        .build(), TSchemaTableType.SCH_BE_TABLETS);
    }
}
