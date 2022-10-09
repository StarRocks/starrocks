// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.starrocks.catalog.Database;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;

public class DbPEntryObject extends PEntryObject {

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

    protected DbPEntryObject(long dbId) {
        super(dbId);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DbPEntryObject)) {
            return false;
        }
        DbPEntryObject other = (DbPEntryObject) obj;
        return other.id == id;
    }

    @Override
    public boolean validate(GlobalStateMgr globalStateMgr) {
        return globalStateMgr.getDbIncludeRecycleBin(this.id) != null;
    }
}
