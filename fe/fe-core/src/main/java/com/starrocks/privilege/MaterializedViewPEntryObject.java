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
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Objects;

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

        if (Objects.equals(tokens.get(0), "*")) {
            dbUUID = PrivilegeBuiltinConstants.ALL_DATABASES_UUID;
            tblUUID = PrivilegeBuiltinConstants.ALL_TABLES_UUID;
        } else {
            Database database = mgr.getMetadataMgr().getDb(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, tokens.get(0));
            if (database == null) {
                throw new PrivObjNotFoundException("cannot find db: " + tokens.get(0));
            }
            dbUUID = database.getUUID();

            if (Objects.equals(tokens.get(1), "*")) {
                tblUUID = PrivilegeBuiltinConstants.ALL_TABLES_UUID;
            } else {
                Table table = mgr.getMetadataMgr().getTable(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        database.getFullName(), tokens.get(1));
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
        return toStringImpl("MATERIALIZED VIEWS");
    }
}