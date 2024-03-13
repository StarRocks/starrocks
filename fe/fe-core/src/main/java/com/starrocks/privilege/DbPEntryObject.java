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

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalCatalog;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaNotFoundException;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.starrocks.catalog.ExternalCatalog.getCompatibleDbUUID;

public class DbPEntryObject implements PEntryObject {
    @SerializedName(value = "ci")
    private long catalogId;
    @SerializedName(value = "i")
    private String uuid;

    protected DbPEntryObject(long catalogId, String uuid) {
        this.catalogId = catalogId;
        this.uuid = uuid;
    }

    protected DbPEntryObject(String uuid) {
        this.catalogId = InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID;
        this.uuid = uuid;
    }

    public String getUUID() {
        return uuid;
    }

    public long getCatalogId() {
        return catalogId;
    }

    public static DbPEntryObject generate(GlobalStateMgr mgr, List<String> tokens) throws PrivilegeException {
        String catalogName = null;
        long catalogId;
        if (tokens.size() == 2) {
            // This is true only when we are initializing built-in roles like root and db_admin
            if (tokens.get(0).equals("*")) {
                return new DbPEntryObject(PrivilegeBuiltinConstants.ALL_CATALOGS_ID,
                        PrivilegeBuiltinConstants.ALL_DATABASES_UUID);
            }
            catalogName = tokens.get(0);
            tokens = tokens.subList(1, tokens.size());
        } else if (tokens.size() != 1) {
            throw new PrivilegeException(
                    "invalid object tokens, should have one, current: " + tokens);
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

        if (Objects.equals(tokens.get(0), "*")) {
            return new DbPEntryObject(catalogId, PrivilegeBuiltinConstants.ALL_DATABASES_UUID);
        }

        return new DbPEntryObject(catalogId, getDatabaseUUID(mgr, catalogName, tokens.get(0)));
    }

    /**
     * for internal database, use {@link Database#getUUID()} as privilege id.
     * for external database, use database name as privilege id.
     */
    public static String getDatabaseUUID(GlobalStateMgr mgr, String catalogName, String dbToken) throws PrivObjNotFoundException {
        checkArgument(!dbToken.equals("*"));
        if (CatalogMgr.isInternalCatalog(catalogName)) {
            Database database = mgr.getMetadataMgr().getDb(catalogName, dbToken);
            if (database == null) {
                throw new PrivObjNotFoundException("cannot find db: " + dbToken);
            }
            return database.getUUID();
        }

        // for database in external catalog, return database name directly without validation
        return dbToken;
    }

    /**
     * if the current db matches other db, including fuzzy matching.
     * <p>
     * this(db1), other(db1) -> true
     * this(db1), other(ALL) -> true
     * this(ALL), other(db1) -> false
     */
    @Override
    public boolean match(Object obj) {
        if (!(obj instanceof DbPEntryObject)) {
            return false;
        }
        DbPEntryObject other = (DbPEntryObject) obj;
        if (other.catalogId == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
            return true;
        }
        if (Objects.equals(other.uuid, PrivilegeBuiltinConstants.ALL_DATABASES_UUID)) {
            return this.catalogId == other.catalogId;
        }
        return this.catalogId == other.catalogId &&
                Objects.equals(getCompatibleDbUUID(this.uuid), getCompatibleDbUUID(other.uuid));
    }

    @Override
    public boolean isFuzzyMatching() {
        return PrivilegeBuiltinConstants.ALL_CATALOGS_ID == catalogId
                || PrivilegeBuiltinConstants.ALL_DATABASES_UUID.equals(uuid);
    }

    @Override
    public boolean validate(GlobalStateMgr globalStateMgr) {
        if (catalogId == InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID) {
            return globalStateMgr.getLocalMetastore().getDbIncludeRecycleBin(Long.parseLong(this.uuid)) != null;
        }
        // do not validate privilege of external database
        return true;
    }

    @Override
    public PEntryObject clone() {
        return new DbPEntryObject(catalogId, uuid);
    }

    @Override
    public int compareTo(PEntryObject obj) {
        if (!(obj instanceof DbPEntryObject)) {
            throw new ClassCastException("cannot cast " + obj.getClass().toString() + " to " + this.getClass());
        }
        DbPEntryObject o = (DbPEntryObject) obj;
        if (this.catalogId == o.catalogId) {
            // Always put the fuzzy matching object at the front of the privilege entry list
            // when sorting in ascendant order.
            if (Objects.equals(this.uuid, o.uuid)) {
                return 0;
            } else if (Objects.equals(this.uuid, PrivilegeBuiltinConstants.ALL_DATABASES_UUID)) {
                return -1;
            } else if (Objects.equals(o.uuid, PrivilegeBuiltinConstants.ALL_DATABASES_UUID)) {
                return 1;
            } else {
                return this.uuid.compareTo(o.uuid);
            }
        } else if (this.catalogId == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
            return -1;
        } else if (o.catalogId == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
            return 1;
        } else {
            return (int) (this.catalogId - o.catalogId);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DbPEntryObject that = (DbPEntryObject) o;
        return this.catalogId == that.catalogId && Objects.equals(uuid, that.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogId, uuid);
    }

    @Override
    public String toString() {
        if (uuid.equalsIgnoreCase(PrivilegeBuiltinConstants.ALL_DATABASES_UUID)) {
            return "ALL DATABASES";
        } else {
            if (CatalogMgr.isInternalCatalog(catalogId)) {
                Database database = GlobalStateMgr.getCurrentState().getDb(Long.parseLong(uuid));
                if (database == null) {
                    throw new MetaNotFoundException("Can't find database : " + uuid);
                }
                return database.getFullName();
            } else {
                return ExternalCatalog.getDbNameFromUUID(uuid);
            }
        }
    }
}
