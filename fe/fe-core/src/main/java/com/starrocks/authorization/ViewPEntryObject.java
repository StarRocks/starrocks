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

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.connector.exception.StarRocksConnectorException;
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

    protected ViewPEntryObject(String dbUUID, String tblUUID) {
        super(dbUUID, tblUUID);
    }

    public static ViewPEntryObject generate(List<String> tokens) throws PrivilegeException {
        String catalogName = null;
        long catalogId;
        if (tokens.size() == 3) {
            if (tokens.get(0).equals("*")) {
                return new ViewPEntryObject(PrivilegeBuiltinConstants.ALL_CATALOGS_ID,
                        PrivilegeBuiltinConstants.ALL_DATABASES_UUID, PrivilegeBuiltinConstants.ALL_TABLES_UUID);
            }
            catalogName = tokens.get(0);
            if (CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog(catalogName)) {
                catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
            }
            tokens = tokens.subList(1, tokens.size());
        } else if (tokens.size() != 2) {
            throw new PrivilegeException(
                    "invalid object tokens, should have two or three elements, current: " + tokens);
        }

        if (catalogName == null || CatalogMgr.isInternalCatalog(catalogName)) {
            catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
            catalogId = InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID;
        } else {
            Catalog catalog = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogByName(catalogName);
            if (catalog == null) {
                throw new PrivObjNotFoundException("cannot find catalog: " + catalogName);
            }
            catalogId = catalog.getId();
        }

        if (Objects.equals(tokens.get(0), "*")) {
            return new ViewPEntryObject(
                    catalogId,
                    PrivilegeBuiltinConstants.ALL_DATABASES_UUID,
                    PrivilegeBuiltinConstants.ALL_TABLES_UUID);
        }

        String dbUUID = DbPEntryObject.getDatabaseUUID(catalogName, tokens.get(0));
        String tblUUID = getViewUUID(catalogName, tokens.get(0), tokens.get(1));
        return new ViewPEntryObject(catalogId, dbUUID, tblUUID);
    }

    private static String getViewUUID(String catalogName, String dbToken, String viewToken)
            throws PrivObjNotFoundException {
        Preconditions.checkArgument(!dbToken.equals("*"));
        if (viewToken.equals("*")) {
            return PrivilegeBuiltinConstants.ALL_TABLES_UUID;
        }

        if (CatalogMgr.isInternalCatalog(catalogName)) {
            Table table;
            try {
                table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(dbToken, viewToken);
            } catch (StarRocksConnectorException e) {
                throw new PrivObjNotFoundException("cannot find view " +
                        viewToken + " in db " + dbToken + ", msg: " + e.getMessage());
            }
            if (table == null || !table.isOlapView()) {
                throw new PrivObjNotFoundException("cannot find view " + viewToken + " in db " + dbToken);
            }
            return table.getUUID();
        }

        return viewToken;
    }

    @Override
    public String toString() {
        return toStringImpl("VIEWS");
    }

    @Override
    public ViewPEntryObject clone() {
        return new ViewPEntryObject(getCatalogId(), this.databaseUUID, this.tableUUID);
    }
}
