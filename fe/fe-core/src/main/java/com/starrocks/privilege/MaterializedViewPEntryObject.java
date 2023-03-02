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

package com.starrocks.privilege;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;

public class MaterializedViewPEntryObject extends TablePEntryObject {

    protected MaterializedViewPEntryObject(String dbUUID, String tblUUID) {
        super(dbUUID, tblUUID);
    }

    public static MaterializedViewPEntryObject generate(GlobalStateMgr mgr, List<String> tokens)
            throws PrivilegeException {
        if (tokens.size() != 2) {
            throw new PrivilegeException("invalid object tokens, should have two: " + tokens);
        }
        String dbUUID;
        String tblUUID;

        if (tokens.get(0).equals("*")) {
            dbUUID = ALL_DATABASES_UUID;
            tblUUID = ALL_TABLES_UUID;
        } else {
            Database database = mgr.getDb(tokens.get(0));
            if (database == null) {
                throw new PrivObjNotFoundException("cannot find db: " + tokens.get(0));
            }
            dbUUID = database.getUUID();

            if (tokens.get(1).equals("*")) {
                tblUUID = ALL_TABLES_UUID;
            } else {
                Table table = database.getTable(tokens.get(1));
                if (table == null || !table.isMaterializedView()) {
                    throw new PrivObjNotFoundException(
                            "cannot find materialized view " + tokens.get(1) + " in db " + tokens.get(0));
                }
                tblUUID = table.getUUID();
            }
        }

        return new MaterializedViewPEntryObject(dbUUID, tblUUID);
    }

    @Override
    public String toString() {
        return toStringImpl("MATERIALIZED_VIEWS");
    }
}