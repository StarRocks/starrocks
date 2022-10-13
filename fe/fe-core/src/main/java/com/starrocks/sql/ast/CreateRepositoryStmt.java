// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import java.util.Map;

public class CreateRepositoryStmt extends DdlStmt {
    private final boolean isReadOnly;
    private final String name;
    private final String brokerName;
    private final String location;
    private final Map<String, String> properties;
    private final boolean hasBroker;

    public CreateRepositoryStmt(boolean isReadOnly, String name, String brokerName, String location,
                                Map<String, String> properties) {
        this.isReadOnly = isReadOnly;
        this.name = name;
        this.brokerName = brokerName;
        this.location = location;
        this.properties = properties;
        if (brokerName == null) {
            hasBroker = false;
        } else {
            hasBroker = true;
        }
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public String getName() {
        return name;
    }

    public boolean hasBroker() {
        return hasBroker;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public String getLocation() {
        return location;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateRepositoryStatement(this, context);
    }
}
