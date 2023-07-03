// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.Objects;

public class DbUID {
    @SerializedName(value = "ci")
    private long catalogId;
    @SerializedName(value = "i")
    private String uuid;

    protected DbUID(long catalogId, String uuid) {
        this.catalogId = catalogId;
        this.uuid = uuid;
    }

    protected DbUID(String uuid) {
        this.catalogId = InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID;
        this.uuid = uuid;
    }

    public static DbUID generate(String catalogName, String dbName) {
        long catalogId;
        if (catalogName == null || CatalogMgr.isInternalCatalog(catalogName)) {
            catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
            catalogId = InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID;
        } else {
            Catalog catalog = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogByName(catalogName);
            if (catalog == null) {
                throw new SemanticException("cannot find catalog: " + catalogName);
            }
            catalogId = catalog.getId();
        }

        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalogName, dbName);
        if (database == null) {
            throw new SemanticException("cannot find db: " + dbName);
        }

        return new DbUID(catalogId, database.getUUID());
    }

    public String getUUID() {
        return uuid;
    }

    public long getCatalogId() {
        return catalogId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DbUID dbUID = (DbUID) o;
        return catalogId == dbUID.catalogId && Objects.equals(uuid, dbUID.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogId, uuid);
    }
}
