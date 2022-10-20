// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Objects;

public class DbPEntryObject implements PEntryObject {
    protected static final long ALL_DATABASE_ID = -2; // -2 represent all
    @SerializedName(value = "i")
    private long id;

    public static DbPEntryObject generate(GlobalStateMgr mgr, List<String> tokens) throws PrivilegeException {
        if (tokens.size() != 1) {
            throw new PrivilegeException("invalid object tokens, should have one: " + tokens);
        }
        Database database = mgr.getDb(tokens.get(0));
        if (database == null) {
            throw new PrivilegeException("cannot find db: " + tokens.get(0));
        }
        return new DbPEntryObject(database.getId());
    }

    public static DbPEntryObject generate(
            List<String> allTypes, String restrictType, String restrictName) throws PrivilegeException {
        // only support ON ALL DATABASE
        if (allTypes.size() != 1 || restrictType != null || restrictName != null) {
            throw new PrivilegeException("invalid ALL statement for databases! only support ON ALL DATABASES");
        }
        return new DbPEntryObject(ALL_DATABASE_ID);
    }

    protected DbPEntryObject(long dbId) {
        id = dbId;
    }

    @Override
    public boolean match(Object obj) {
        if (!(obj instanceof DbPEntryObject)) {
            return false;
        }
        if (id == ALL_DATABASE_ID) {
            return true;
        }
        DbPEntryObject other = (DbPEntryObject) obj;
        return other.id == id;
    }

    @Override
    public boolean isFuzzyMatching() {
        return ALL_DATABASE_ID == id;
    }

    @Override
    public boolean validate(GlobalStateMgr globalStateMgr) {
        return globalStateMgr.getDbIncludeRecycleBin(this.id) != null;
    }

    @Override
    public PEntryObject clone() {
        return new DbPEntryObject(id);
    }

    @Override
    public int compareTo(PEntryObject obj) {
        if (!(obj instanceof DbPEntryObject)) {
            throw new ClassCastException("cannot cast " + obj.getClass().toString() + " to " + this.getClass());
        }
        DbPEntryObject o = (DbPEntryObject) obj;
        return Long.compare(this.id, o.id);
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
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
