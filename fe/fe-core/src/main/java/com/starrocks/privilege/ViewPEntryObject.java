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

import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Objects;

/**
 * View is a subclass of table, only the table type is different
 */
public class ViewPEntryObject extends TablePEntryObject {

    protected ViewPEntryObject(long catalogId, String dbUUID, String tblUUID) {
        super(catalogId, dbUUID, tblUUID);
    }

    public static ViewPEntryObject generate(GlobalStateMgr mgr, List<String> tokens) throws PrivilegeException {
        String catalogName = null;
        long catalogId;
        if (tokens.size() == 3) {
            // This is true only when we are initializing built-in roles like root and db_admin
            if (tokens.get(0).equals("*")) {
                return new ViewPEntryObject(PrivilegeBuiltinConstants.ALL_CATALOGS_ID,
                        PrivilegeBuiltinConstants.ALL_DATABASES_UUID, PrivilegeBuiltinConstants.ALL_TABLES_UUID);
            }
            catalogName = tokens.get(0);

            // It's very trick here.
            // Because of the historical legacy code, create external table belongs to internal catalog from the grammatical level
            // . In terms of code implementation, the catalog obtained from TableName turned out to be external catalog!
            // As a result, the authorization grant stmt is authorized according to the internal catalog,
            // but the external catalog is used for authentication.
            if (CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog(catalogName)) {
                catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
            }

            tokens = tokens.subList(1, tokens.size());
        } else if (tokens.size() != 2) {
            throw new PrivilegeException("invalid object tokens, should have two: " + tokens);
        }

        // Default to internal_catalog when no catalog explicitly selected.
        if (catalogName == null || CatalogMgr.isInternalCatalog(catalogName)) {
            catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
            catalogId = InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID;
        } else {
            Catalog catalog = mgr.getCatalogMgr().getCatalogByName(catalogName);
            if (catalog == null) {
                throw new PrivObjNotFoundException("cannot find catalog: " + catalogName);
            }
            catalogId = catalog.getId();
        }

        String dbUUID;
        String tblUUID;

        if (Objects.equals(tokens.get(0), "*")) {
            dbUUID = PrivilegeBuiltinConstants.ALL_DATABASES_UUID;
            tblUUID = PrivilegeBuiltinConstants.ALL_TABLES_UUID;
        } else {
            Database database = mgr.getMetadataMgr().getDb(catalogName, tokens.get(0));
            if (database == null) {
                throw new PrivObjNotFoundException("cannot find db: " + tokens.get(0));
            }
            dbUUID = database.getUUID();

            if (Objects.equals(tokens.get(1), "*")) {
                tblUUID = PrivilegeBuiltinConstants.ALL_TABLES_UUID;
            } else {
                Table table = mgr.getMetadataMgr().getTable(catalogName, database.getFullName(), tokens.get(1));
                if (table == null || !table.isView()) {
                    throw new PrivObjNotFoundException("cannot find view " + tokens.get(1) + " in db " + tokens.get(0));
                }
                tblUUID = table.getUUID();
            }
        }

        return new ViewPEntryObject(catalogId, dbUUID, tblUUID);
    }

    @Override
    public String toString() {
        return toStringImpl("VIEWS");
    }
}
