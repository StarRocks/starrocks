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

public class StreamLoadsSystemTable {
    public static final String NAME = "stream_loads";

    public static SystemTable create() {
        return new SystemTable(SystemId.STREAM_LOADS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("LABEL", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("ID", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("LOAD_ID", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("TXN_ID", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("DB_NAME", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("TABLE_NAME", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("STATE", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("ERROR_MSG", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("TRACKING_URL", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("CHANNEL_NUM", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("PREPARED_CHANNEL_NUM", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("NUM_ROWS_NORMAL", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("NUM_ROWS_AB_NORMAL", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("NUM_ROWS_UNSELECTED", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("NUM_LOAD_BYTES", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("TIMEOUT_SECOND", TypeFactory.createType(PrimitiveType.BIGINT))
                        .column("CREATE_TIME_MS", TypeFactory.createType(PrimitiveType.DATETIME))
                        .column("BEFORE_LOAD_TIME_MS", TypeFactory.createType(PrimitiveType.DATETIME))
                        .column("START_LOADING_TIME_MS", TypeFactory.createType(PrimitiveType.DATETIME))
                        .column("START_PREPARING_TIME_MS", TypeFactory.createType(PrimitiveType.DATETIME))
                        .column("FINISH_PREPARING_TIME_MS", TypeFactory.createType(PrimitiveType.DATETIME))
                        .column("END_TIME_MS", TypeFactory.createType(PrimitiveType.DATETIME))
                        .column("CHANNEL_STATE", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("TYPE", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("TRACKING_SQL", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .build(), TSchemaTableType.SCH_STREAM_LOADS);
    }
}
