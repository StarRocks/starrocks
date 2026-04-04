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

public class MaterializedViewPEntryObject extends TablePEntryObject {

    protected MaterializedViewPEntryObject(long catalogId, String dbUUID, String tblUUID) {
        super(catalogId, dbUUID, tblUUID);
    }

    protected MaterializedViewPEntryObject(String dbUUID, String tblUUID) {
        super(dbUUID, tblUUID);
    }

    public static MaterializedViewPEntryObject generate(List<String> tokens)
            throws PrivilegeException {
        String catalogName = null;
        long catalogId;
        if (tokens.size() == 3) {
            if (tokens.get(0).equals("*")) {
                return new MaterializedViewPEntryObject(PrivilegeBuiltinConstants.ALL_CATALOGS_ID,
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
            return new MaterializedViewPEntryObject(
                    catalogId,
                    PrivilegeBuiltinConstants.ALL_DATABASES_UUID,
                    PrivilegeBuiltinConstants.ALL_TABLES_UUID);
        }

        String dbUUID = DbPEntryObject.getDatabaseUUID(catalogName, tokens.get(0));
        String tblUUID = getMaterializedViewUUID(catalogName, tokens.get(0), tokens.get(1));
        return new MaterializedViewPEntryObject(catalogId, dbUUID, tblUUID);
    }

    private static String getMaterializedViewUUID(String catalogName, String dbToken, String mvToken)
            throws PrivObjNotFoundException {
        Preconditions.checkArgument(!dbToken.equals("*"));
        if (mvToken.equals("*")) {
            return PrivilegeBuiltinConstants.ALL_TABLES_UUID;
        }

        if (CatalogMgr.isInternalCatalog(catalogName)) {
            Table table;
            try {
                table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(dbToken, mvToken);
            } catch (StarRocksConnectorException e) {
                throw new PrivObjNotFoundException("cannot find materialized view " +
                        mvToken + " in db " + dbToken + ", msg: " + e.getMessage());
            }
            if (table == null || !table.isMaterializedView()) {
                throw new PrivObjNotFoundException(
                        "cannot find materialized view " + mvToken + " in db " + dbToken);
            }
            return table.getUUID();
        }

        return mvToken;
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