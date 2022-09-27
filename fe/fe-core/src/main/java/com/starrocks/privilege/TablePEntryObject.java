// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;

public class TablePEntryObject extends PEntryObject {
    @SerializedName(value = "d")
    private long databaseId;

    public static TablePEntryObject generate(GlobalStateMgr mgr, List<String> tokens) throws PrivilegeException {
        if (tokens.size() != 2) {
            throw new PrivilegeException("invalid object tokens, should have two: " + tokens);
        }
        Database database = mgr.getDb(tokens.get(0));
        if (database == null) {
            throw new PrivilegeException("invalid db in " + tokens);
        }
        Table table = database.getTable(tokens.get(1));
        if (table == null) {
            throw new PrivilegeException("invalid table in " + tokens);
        }
        return new TablePEntryObject(database.getId(), table.getId());
    }

    public TablePEntryObject(long databaseId, long tableId) {
        super(tableId);
        this.databaseId = databaseId;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TablePEntryObject)) {
            return false;
        }
        TablePEntryObject other = (TablePEntryObject) obj;
        return other.databaseId == databaseId && other.id == id;
    }
}
