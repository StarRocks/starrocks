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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Mysql schema used for MySQL compatible.
public class SysDb extends Database {
    public static final String DATABASE_NAME = "sys";

    public SysDb() {
        super(SystemId.SYS_DB_ID, DATABASE_NAME);
        super.createTable(RoleEdges.create());
        super.createTable(GrantsTo.createGrantsToRoles());
        super.createTable(GrantsTo.createGrantsToUsers());
    }

    @Override
    public boolean createTableWithLock(Table table, boolean isReplay) {
        return false;
    }

    @Override
    public boolean createTable(Table table) {
        // Do nothing.
        return false;
    }

    @Override
    public void dropTableWithLock(String name) {
        // Do nothing.
    }

    @Override
    public void dropTable(String name) {
        // Do nothing.
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // Do nothing
    }

    public void readFields(DataInput in) throws IOException {
        throw new IOException("Not support.");
    }

    @Override
    public Table getTable(String name) {
        return super.getTable(name.toLowerCase());
    }
}
