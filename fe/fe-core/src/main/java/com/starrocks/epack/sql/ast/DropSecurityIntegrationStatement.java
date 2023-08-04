// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.parser.NodePosition;

public class DropSecurityIntegrationStatement extends DdlStmt {
    private final String name;

    public DropSecurityIntegrationStatement(String name) {
        this(name, NodePosition.ZERO);
    }

    public DropSecurityIntegrationStatement(String name, NodePosition pos) {
        super(pos);
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropSecurityIntegrationStatement(this, context);
    }

}
