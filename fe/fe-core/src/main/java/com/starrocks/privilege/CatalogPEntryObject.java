// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Objects;

/**
 * Currently there is only one internal catalog, i.e. `default_catalog`,
 * and we may support grant some privilege on some catalog, but user cannot
 * actually execute the operation based on the capability of the catalog,
 * for example we can grant create_database on catalog external_catalog1, but we
 * don't support create database under external catalog for now.
 */
public class CatalogPEntryObject implements PEntryObject {
    protected static final long ALL_CATALOG_ID = -1; // -1 represent all
    @SerializedName(value = "i")
    private long id;

    public static CatalogPEntryObject generate(GlobalStateMgr mgr, List<String> tokens) throws PrivilegeException {
        if (tokens.size() != 1) {
            throw new PrivilegeException("invalid object tokens, should have only one, token: " + tokens);
        }
        String name = tokens.get(0);
        long id;
        if (CatalogMgr.isInternalCatalog(name)) {
            id = InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID;
        } else {
            Catalog catalog = mgr.getCatalogMgr().getCatalogByName(name);
            if (catalog == null) {
                throw new PrivilegeException("cannot find catalog: " + name);
            }
            id = catalog.getId();
        }
        return new CatalogPEntryObject(id);
    }

    public static CatalogPEntryObject generate(
            List<String> allTypes, String restrictType, String restrictName) throws PrivilegeException {
        if (allTypes.size() != 1 || restrictType != null || restrictName != null) {
            throw new PrivilegeException("invalid ALL statement for catalogs! only support ON ALL CATALOGS");
        }
        return new CatalogPEntryObject(ALL_CATALOG_ID);
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
        if (other.id == ALL_CATALOG_ID) {
            return true;
        }
        return other.id == id;
    }

    @Override
    public boolean isFuzzyMatching() {
        return ALL_CATALOG_ID == id;
    }

    @Override
    public boolean validate(GlobalStateMgr globalStateMgr) {
        if (id == InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID) {
            return true;
        } else {
            return globalStateMgr.getCatalogMgr().checkCatalogExistsById(id);
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
}
