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
import com.starrocks.type.TypeFactory;

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class FeThreadsSystemTable {
    public static final String NAME = "fe_threads";

    public static SystemTable create() {
        return new SystemTable(SystemId.FE_THREADS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("FE_ID", TypeFactory.createVarchar(NAME_CHAR_LEN), false)
                        .column("CONNECTION_ID", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("USER", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("HOST", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("DB", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("COMMAND", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("CONNECTION_START_TIME", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("TIME", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("STATE", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("INFO", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("IS_PENDING", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("WAREHOUSE", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("CNGROUP", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("CATALOG", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .column("QUERY_ID", TypeFactory.createVarchar(NAME_CHAR_LEN))
                        .build(), TSchemaTableType.SCH_FE_THREADS);
    }
}
