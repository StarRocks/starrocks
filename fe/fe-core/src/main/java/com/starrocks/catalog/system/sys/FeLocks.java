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

package com.starrocks.catalog.system.sys;

import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.thrift.TSchemaTableType;

public class FeLocks {

    public static final String NAME = "fe_locks";

    public static SystemTable create() {
        return new SystemTable(SystemId.FE_LOCKS_ID, NAME,
                Table.TableType.SCHEMA,
                SystemTable.builder()
                        .column("lock_type", ScalarType.createVarcharType(64))
                        .column("lock_object", ScalarType.createVarcharType(64))
                        .column("lock_mode", ScalarType.createVarcharType(64))
                        .column("lock_granted", ScalarType.createVarcharType(64))
                        .column("thread_info", ScalarType.createVarcharType(64))
                        .column("wait_start", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .column("waiter_list", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .build(),
                TSchemaTableType.STARROCKS_OBJECT_DEPENDENCIES);
    }

}
