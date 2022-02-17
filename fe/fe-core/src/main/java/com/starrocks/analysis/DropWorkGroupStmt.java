// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.analysis;


import com.starrocks.sql.ast.Relation;

// Drop WorkGroup specified by name
// DROP RESOURCE_GROUP <name>
public class DropWorkGroupStmt extends DdlStmt {
    private String name;

    public DropWorkGroupStmt(String name) {
        this.name = name;
    }

    public Relation analyze() {
        return null;
    }

    public String getName() {
        return name;
    }
}
