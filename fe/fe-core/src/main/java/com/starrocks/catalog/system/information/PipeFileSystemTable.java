// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog.system.information;

import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.thrift.TSchemaTableType;

public class PipeFileSystemTable {

    public static final String NAME = "pipe_files";

    public static SystemTable create() {
        return new SystemTable(SystemId.PIPE_FILES_ID, NAME, Table.TableType.SCHEMA,
                SystemTable.builder()
                        .column("DATABASE_NAME", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .column("PIPE_ID", ScalarType.BIGINT)
                        .column("PIPE_NAME", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))

                        .column("FILE_NAME", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .column("FILE_VERSION", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .column("FILE_ROWS", ScalarType.BIGINT)
                        .column("FILE_SIZE", ScalarType.BIGINT)
                        .column("LAST_MODIFIED", ScalarType.createVarcharType(16))

                        .column("LOAD_STATE", ScalarType.createVarcharType(8))
                        .column("STAGED_TIME", ScalarType.createVarcharType(16))
                        .column("START_LOAD_TIME", ScalarType.createVarcharType(16))
                        .column("FINISH_LOAD_TIME", ScalarType.createVarcharType(16))

                        .column("ERROR_MSG", ScalarType.createVarcharType(512))
                        .column("ERROR_COUNT", ScalarType.BIGINT)
                        .column("ERROR_LINE", ScalarType.BIGINT)
                        .build(),
                TSchemaTableType.SCH_PIPE_FILES);
    }
}
