// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.Relation;

// Show WorkGroups
// 1. Show WorkGroup specified by name
//  SHOW RESOURCE_GROUP <name>
// 2. Show all WorkGroups
//  SHOW RESOURCE_GROUPS ALL
// 3. Show all of WorkGroups that visible to current user
//  SHOW RESOURCE_GROUPS

public class ShowWorkGroupStmt extends ShowStmt {
    private String name;
    private boolean listAll;

    public ShowWorkGroupStmt(String name, boolean listAll) {
        this.name = name;
        this.listAll = listAll;
    }

    public boolean isListAll() {
        return listAll;
    }

    public String getName() {
        return name;
    }

    public Relation analyze() {
        return null;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return null;
    }
}
