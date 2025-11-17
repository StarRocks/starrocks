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

import static com.starrocks.catalog.system.SystemTable.FN_REFLEN;
import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class TemporaryTablesTable {
    public static final int MY_CS_NAME_SIZE = 32;
    public static final String NAME = "temp_tables";

    public static SystemTable create() {
        return new SystemTable(SystemId.TEMP_TABLES_ID, NAME, Table.TableType.SCHEMA, builder()
                .column("TABLE_CATALOG", TypeFactory.createVarchar(FN_REFLEN))
                .column("TABLE_SCHEMA", TypeFactory.createVarchar(NAME_CHAR_LEN))
                .column("TABLE_NAME", TypeFactory.createVarchar(NAME_CHAR_LEN))
                .column("TABLE_TYPE", TypeFactory.createVarchar(NAME_CHAR_LEN))
                .column("ENGINE", TypeFactory.createVarchar(NAME_CHAR_LEN))
                .column("VERSION", IntegerType.BIGINT)
                .column("ROW_FORMAT", TypeFactory.createVarchar(10))
                .column("TABLE_ROWS", IntegerType.BIGINT)
                .column("AVG_ROW_LENGTH", IntegerType.BIGINT)
                .column("DATA_LENGTH", IntegerType.BIGINT)
                .column("MAX_DATA_LENGTH", IntegerType.BIGINT)
                .column("INDEX_LENGTH", IntegerType.BIGINT)
                .column("DATA_FREE", IntegerType.BIGINT)
                .column("AUTO_INCREMENT", IntegerType.BIGINT)
                .column("CREATE_TIME", DateType.DATETIME)
                .column("UPDATE_TIME", DateType.DATETIME)
                .column("CHECK_TIME", DateType.DATETIME)
                .column("TABLE_COLLATION", TypeFactory.createVarchar(MY_CS_NAME_SIZE))
                .column("CHECKSUM", IntegerType.BIGINT)
                .column("CREATE_OPTIONS", TypeFactory.createVarchar(255))
                .column("TABLE_COMMENT", TypeFactory.createVarchar(2048))
                .column("SESSION", TypeFactory.createVarchar(128))
                .column("TABLE_ID", IntegerType.BIGINT)
                .build(), TSchemaTableType.SCH_TEMP_TABLES);
    }
}
