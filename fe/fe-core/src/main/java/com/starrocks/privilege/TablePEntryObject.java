// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class TablePEntryObject extends PEntryObject {
    @SerializedName(value = "d")
    protected long databaseId;
    private static final long ALL_DATABASE_ID = -2; // -2 represent all databases
    private static final long ALL_TABLES_ID = -3; // -3 represent all tables

    public static TablePEntryObject generate(GlobalStateMgr mgr, List<String> tokens) throws PrivilegeException {
        if (tokens.size() != 2) {
            throw new PrivilegeException("invalid object tokens, should have two: " + tokens);
        }
        Database database = mgr.getDb(tokens.get(0));
        if (database == null) {
            throw new PrivilegeException("cannot find db: " + tokens.get(0));
        }
        Table table = database.getTable(tokens.get(1));
        if (table == null) {
            throw new PrivilegeException("cannot find table " + tokens.get(1) + " in db " + tokens.get(0));
        }
        return new TablePEntryObject(database.getId(), table.getId());
    }
    public static TablePEntryObject generate(
            GlobalStateMgr mgr, List<String> allTypes, String restrictType, String restrictName)
            throws PrivilegeException {
        if (allTypes.size() == 1) {
            if (StringUtils.isEmpty(restrictType)
                    || !restrictType.equals(PrivilegeTypes.DATABASE.toString())
                    || StringUtils.isEmpty(restrictName)) {
                throw new PrivilegeException("ALL TABLES must be restricted with database!");
            }

            Database database = mgr.getDb(restrictName);
            if (database == null) {
                throw new PrivilegeException("cannot find db: " + restrictName);
            }
            return new TablePEntryObject(database.getId(), ALL_TABLES_ID);
        } else if (allTypes.size() == 2) {
            if (allTypes.get(1).equals(PrivilegeTypes.DATABASE.getPlural())) {
                throw new PrivilegeException(
                        "ALL TABLES must be restricted with ALL DATABASES instead of ALL " + allTypes.get(1));
            }
            return new TablePEntryObject(ALL_DATABASE_ID, ALL_TABLES_ID);
        } else {
            throw new PrivilegeException("invalid ALL statement for tables!");
        }
    }

    protected TablePEntryObject(long databaseId, long tableId) {
        super(tableId);
        this.databaseId = databaseId;
    }

    @Override
    public boolean match(Object obj) {
        if (!(obj instanceof TablePEntryObject)) {
            return false;
        }
        TablePEntryObject other = (TablePEntryObject) obj;
        if (databaseId == ALL_DATABASE_ID) {
            return id == ALL_TABLES_ID || other.id == id;
        }
        return other.databaseId == databaseId && other.id == id;
    }

    @Override
    public boolean validate(GlobalStateMgr globalStateMgr) {
        Database db = globalStateMgr.getDbIncludeRecycleBin(this.databaseId);
        if (db == null) {
            return false;
        }
        return globalStateMgr.getTableIncludeRecycleBin(db, this.id) != null;
    }
}
