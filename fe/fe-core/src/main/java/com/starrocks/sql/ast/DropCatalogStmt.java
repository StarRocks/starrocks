// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.DdlStmt;

// ToDo(zhuodong): to support internal catalog in the future
public class DropCatalogStmt extends DdlStmt {

    private final String name;

    public DropCatalogStmt(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropCatalogStatement(this, context);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP CATALOG ");
        sb.append("\'" + name + "\'");
        return sb.toString();
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
