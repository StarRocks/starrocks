// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.catalog.WorkGroup;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;

// Show WorkGroups
// 1. Show WorkGroup specified by name
//  SHOW RESOURCE GROUP <name>
// 2. Show all WorkGroups
//  SHOW RESOURCE GROUPS ALL
// 3. Show all of WorkGroups that visible to current user
//  SHOW RESOURCE GROUPS

public class ShowWorkGroupStmt extends ShowStmt {
    private final String name;
    private final boolean listAll;

    public ShowWorkGroupStmt(String name, boolean listAll) {
        this.name = name;
        this.listAll = listAll;
    }

    public boolean isListAll() {
        return listAll;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return WorkGroup.META_DATA;
    }

    public String getName() {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowWorkGroupStmt(this, context);
    }
}
