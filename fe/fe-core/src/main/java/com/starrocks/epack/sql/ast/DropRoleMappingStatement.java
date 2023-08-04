// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.parser.NodePosition;

public class DropRoleMappingStatement extends DdlStmt {
    private final String name;

    public DropRoleMappingStatement(String name) {
        this(name, NodePosition.ZERO);
    }

    public DropRoleMappingStatement(String name, NodePosition pos) {
        super(pos);
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropRoleMappingStatement(this, context);
    }

}
