// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.catalog.ResourceGroup;
import com.starrocks.qe.ShowResultSetMetaData;

// Show ResourceGroups
// 1. Show ResourceGroup specified by name
//  SHOW RESOURCE GROUP <name>
// 2. Show all ResourceGroups
//  SHOW RESOURCE GROUPS ALL
// 3. Show all of ResourceGroups that visible to current user
//  SHOW RESOURCE GROUPS

public class ShowResourceGroupStmt extends ShowStmt {
    private final String name;
    private final boolean listAll;

    public ShowResourceGroupStmt(String name, boolean listAll) {
        this.name = name;
        this.listAll = listAll;
    }

    public boolean isListAll() {
        return listAll;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return ResourceGroup.META_DATA;
    }

    public String getName() {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowResourceGroupStatement(this, context);
    }
}
