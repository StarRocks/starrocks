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

public class BeTxnsSystemTable {
    private static final String NAME = "be_txns";

    public static SystemTable create() {
        return new SystemTable(SystemId.BE_TXNS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("BE_ID", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("LOAD_ID", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("TXN_ID", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("PARTITION_ID", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("TABLET_ID", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("CREATE_TIME", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("COMMIT_TIME", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("PUBLISH_TIME", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("ROWSET_ID", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("NUM_SEGMENT", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("NUM_DELFILE", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("NUM_ROW", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("DATA_SIZE", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("VERSION", TypeFactory.createType(PrimitiveType.BIGINT))
                        .build(), TSchemaTableType.SCH_BE_TXNS);
    }
}
