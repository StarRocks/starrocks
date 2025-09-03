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

package com.starrocks.authorization;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.TableName;

public class AllowAllAccessController extends ExternalAccessController {
    @Override
    public void checkDbAction(ConnectContext context, String catalogName, String db, PrivilegeType privilegeType) {
        // Allow all DB actions
    }

    @Override
    public void checkAnyActionOnDb(ConnectContext context, String catalogName, String db) {
        // Allow any action on any DB
    }

    @Override
    public void checkTableAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType) {
        // Allow all table actions
    }

    @Override
    public void checkAnyActionOnTable(ConnectContext context, TableName tableName) {
        // Allow any action on any table
    }

    @Override
    public void checkAnyActionOnAnyTable(ConnectContext context, String catalog, String db) {
        // Allow any action on any table in any DB
    }

    @Override
    public void checkColumnAction(ConnectContext context, TableName tableName, String column, PrivilegeType privilegeType) {
        // Allow all column actions
    }

    @Override
    public void checkViewAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType) {
        // Allow all view actions
    }

    @Override
    public void checkAnyActionOnView(ConnectContext context, TableName tableName) {
        // Allow any action on any view
    }

    @Override
    public void checkAnyActionOnAnyView(ConnectContext context, String db) {
        // Allow any action on any view in DB
    }

    @Override
    public void checkActionInDb(ConnectContext context, String db, PrivilegeType privilegeType) {
        // Allow any action in DB
    }
}
