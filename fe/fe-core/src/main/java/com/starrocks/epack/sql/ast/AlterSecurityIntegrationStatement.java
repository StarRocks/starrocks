// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;
import javax.annotation.Nonnull;

// clause which is used to alter security integration
public class AlterSecurityIntegrationStatement extends DdlStmt {
    private final String name;
    private final Map<String, String> properties;

    public AlterSecurityIntegrationStatement(String name, Map<String, String> properties) {
        this(name, properties, NodePosition.ZERO);
    }

    public AlterSecurityIntegrationStatement(String name, Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.name = name;
        this.properties = properties;
    }

    public String getName() {
        return name;
    }

    @Nonnull
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterSecurityIntegrationStatement(this, context);
    }
}
