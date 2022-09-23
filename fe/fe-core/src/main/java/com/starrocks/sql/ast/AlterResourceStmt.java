// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

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

    public boolean isSupportNewPlanner() {
        return true;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterResourceStatement(this, context);
    }
}

