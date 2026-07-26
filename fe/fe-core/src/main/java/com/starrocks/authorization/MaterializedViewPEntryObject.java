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

import com.starrocks.catalog.Table;

import java.util.List;

public class MaterializedViewPEntryObject extends TablePEntryObject {

    protected MaterializedViewPEntryObject(long catalogId, String dbUUID, String tblUUID) {
        super(catalogId, dbUUID, tblUUID);
    }

    protected MaterializedViewPEntryObject(String dbUUID, String tblUUID) {
        super(dbUUID, tblUUID);
    }

    public static MaterializedViewPEntryObject generate(List<String> tokens)
            throws PrivilegeException {
        return (MaterializedViewPEntryObject) generateTableLikeObject(tokens, MaterializedViewPEntryObject::new,
                (catalogName, dbToken, mvToken) ->
                        resolveObjectUUID(catalogName, dbToken, mvToken, Table::isMaterializedView, "materialized view"));
    }

    @Override
    public String toString() {
        return toStringImpl("MATERIALIZED VIEWS");
    }

    @Override
    public MaterializedViewPEntryObject clone() {
        return new MaterializedViewPEntryObject(getCatalogId(), this.databaseUUID, this.tableUUID);
    }
}