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
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class BeTxnsSystemTable {
    private static final String NAME = "be_txns";

    public static SystemTable create() {
        return new SystemTable(SystemId.BE_TXNS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("BE_ID", IntegerType.BIGINT)
                        .column("LOAD_ID", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("TXN_ID", IntegerType.BIGINT)
                        .column("PARTITION_ID", IntegerType.BIGINT)
                        .column("TABLET_ID", IntegerType.BIGINT)
                        .column("CREATE_TIME", IntegerType.BIGINT)
                        .column("COMMIT_TIME", IntegerType.BIGINT)
                        .column("PUBLISH_TIME", IntegerType.BIGINT)
                        .column("ROWSET_ID", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("NUM_SEGMENT", IntegerType.BIGINT)
                        .column("NUM_DELFILE", IntegerType.BIGINT)
                        .column("NUM_ROW", IntegerType.BIGINT)
                        .column("DATA_SIZE", IntegerType.BIGINT)
                        .column("VERSION", IntegerType.BIGINT)
                        .build(), TSchemaTableType.SCH_BE_TXNS);
    }
}
