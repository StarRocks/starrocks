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

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class ApplicableRolesSystemTable {
    private static final String NAME = "applicable_roles";

    public static SystemTable create(String catalogName) {
        return new SystemTable(
                catalogName,
                SystemId.APPLICABLE_ROLES_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("USER", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("HOST", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("GRANTEE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("GRANTEE_HOST", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("ROLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("ROLE_HOST", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("IS_GRANTABLE", ScalarType.createVarchar(3))
                        .column("IS_DEFAULT", ScalarType.createVarchar(3))
                        .column("IS_MANDATORY", ScalarType.createVarchar(3))
                        .build(), TSchemaTableType.SCH_APPLICABLE_ROLES);
    }
}
