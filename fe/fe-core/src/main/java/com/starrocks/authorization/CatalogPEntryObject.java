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

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaNotFoundException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Currently there is only one internal catalog, i.e. `default_catalog`,
 * and we may support grant some privilege on some catalog, but user cannot
 * actually execute the operation based on the capability of the catalog,
 * for example we can grant create_database on catalog external_catalog1, but we
 * don't support create database under external catalog for now.
 */
public class CatalogPEntryObject implements PEntryObject {
    @SerializedName(value = "i")
    private long id;

    public long getId() {
        return id;
    }

    public static CatalogPEntryObject generate(List<String> tokens) throws PrivilegeException {
        if (tokens.size() != 1) {
            throw new PrivilegeException("invalid object tokens, should have only one, token: " + tokens);
        }
        String name = tokens.get(0);
        if (name.equals("*")) {
            return new CatalogPEntryObject(PrivilegeBuiltinConstants.ALL_CATALOGS_ID);
        }

        long id;
        if (CatalogMgr.isInternalCatalog(name)) {
            id = InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID;
        } else {
            Catalog catalog = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogByName(name);
            if (catalog == null) {
                throw new PrivObjNotFoundException("cannot find catalog: " + name);
            }
            id = catalog.getId();
        }
        return new CatalogPEntryObject(id);
    }

    protected CatalogPEntryObject(long id) {
        this.id = id;
    }

    /**
     * if the current catalog matches other catalog, including fuzzy matching.
     * <p>
     * this(catalog1), other(catalog1) -> true<p>
     * this(catalog1), other(ALL) -> true<p>
     * this(ALL), other(catalog1) -> false
     */
    @Override
    public boolean match(Object obj) {
        if (!(obj instanceof CatalogPEntryObject)) {
            return false;
        }
        CatalogPEntryObject other = (CatalogPEntryObject) obj;
        if (other.id == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
            return true;
        }
        return other.id == id;
    }

    @Override
    public boolean isFuzzyMatching() {
        return PrivilegeBuiltinConstants.ALL_CATALOGS_ID == id;
    }

    @Override
    public boolean validate() {
        if (id == InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID) {
            return true;
        } else {
            return GlobalStateMgr.getCurrentState().getCatalogMgr().checkCatalogExistsById(id);
        }
    }

    @Override
    public int compareTo(PEntryObject obj) {
        if (!(obj instanceof CatalogPEntryObject)) {
            throw new ClassCastException("cannot cast " + obj.getClass().toString() + " to " + this.getClass());
        }
        CatalogPEntryObject o = (CatalogPEntryObject) obj;
        return Long.compare(this.id, o.id);
    }

    @Override
    public PEntryObject clone() {
        return new CatalogPEntryObject(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CatalogPEntryObject that = (CatalogPEntryObject) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        if (id == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
            return "ALL CATALOGS";
        } else {
            if (id == InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID) {
                return InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
            }

            List<Catalog> catalogs =
                    new ArrayList<>(GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogs().values());
            Optional<Catalog> catalogOptional = catalogs.stream().filter(
                    catalog -> catalog.getId() == id
            ).findFirst();
            if (!catalogOptional.isPresent()) {
                throw new MetaNotFoundException("Can't find catalog : " + id);
            }
            Catalog catalog = catalogOptional.get();
            return catalog.getName();
        }
    }
}
