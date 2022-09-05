// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.common.util.PrintableMap;
import com.starrocks.sql.ast.AstVisitor;

import java.util.Map;

public class AlterResourceStmt extends DdlStmt {
    private final String resourceName;
    private final Map<String, String> properties;

    public AlterResourceStmt(String resourceName, Map<String, String> properties) {
        this.resourceName = resourceName;
        this.properties = properties;
    }

    public String getResourceName() {
        return resourceName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER ");
        sb.append("RESOURCE '").append(resourceName).append("' ");
        sb.append("SET PROPERTIES(").append(new PrintableMap<>(properties, "=", true, false)).append(")");
        return sb.toString();
    }
    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterResourceStatement(this, context);
    }
    @Override
    public String toString() {
        return toSql();
    }
}

