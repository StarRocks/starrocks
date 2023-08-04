// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class CreateSecurityIntegrationStatement extends DdlStmt {
    private final String name;
    private final Map<String, String> propertyMap;

    public CreateSecurityIntegrationStatement(String name, Map<String, String> propertyMap, NodePosition pos) {
        super(pos);
        this.name = name;
        this.propertyMap = propertyMap;
    }

    public Map<String, String> getPropertyMap() {
        return propertyMap;
    }

    public String getName() {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateSecurityIntegrationStatement(this, context);
    }
}