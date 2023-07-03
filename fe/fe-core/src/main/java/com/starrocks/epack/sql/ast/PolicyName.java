// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.parser.NodePosition;

public class PolicyName implements ParseNode {
    @SerializedName(value = "c")
    private String catalog;
    @SerializedName(value = "d")
    private String dbName;
    @SerializedName(value = "n")
    private final String name;

    private final NodePosition pos;

    public PolicyName(String catalog, String dbName, String name, NodePosition pos) {
        this.catalog = catalog;
        this.dbName = dbName;
        this.name = name;
        this.pos = pos;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getName() {
        return name;
    }

    public NodePosition getPos() {
        return pos;
    }

    @Override
    public String toString() {
        return name;
    }
}
