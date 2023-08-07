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

import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.thrift.TSchemaTableType;

import static com.starrocks.catalog.system.SystemTable.MAX_FIELD_VARCHAR_LENGTH;
import static com.starrocks.catalog.system.SystemTable.builder;

public class ViewsSystemTable {
    public static SystemTable create() {
        return new SystemTable(SystemId.VIEWS_ID,
                "views",
                Table.TableType.SCHEMA,
                builder()
                        .column("TABLE_CATALOG", ScalarType.createVarchar(512))
                        .column("TABLE_SCHEMA", ScalarType.createVarchar(64))
                        .column("TABLE_NAME", ScalarType.createVarchar(64))
                        // TODO: Type for EVENT_DEFINITION should be `longtext`, but `varchar(65535)` was set at this stage.
                        .column("VIEW_DEFINITION",
                                ScalarType.createVarchar(MAX_FIELD_VARCHAR_LENGTH))
                        .column("CHECK_OPTION", ScalarType.createVarchar(8))
                        .column("IS_UPDATABLE", ScalarType.createVarchar(3))
                        .column("DEFINER", ScalarType.createVarchar(77))
                        .column("SECURITY_TYPE", ScalarType.createVarchar(7))
                        .column("CHARACTER_SET_CLIENT", ScalarType.createVarchar(32))
                        .column("COLLATION_CONNECTION", ScalarType.createVarchar(32))
                        .build(), TSchemaTableType.SCH_VIEWS);
    }
}
