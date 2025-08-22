// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.sql.ast.AlterTableClause;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

public class RevokeMaskingPolicyClause extends AlterTableClause {
    private final String maskingColumn;

    public RevokeMaskingPolicyClause(String maskingColumn, NodePosition nodePosition) {
        super(nodePosition);
        this.maskingColumn = maskingColumn;
    }

    public String getMaskingColumn() {
        return maskingColumn;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        if (visitor instanceof AstVisitorEPack) {
            return ((AstVisitorEPack<R, C>) visitor).visitRevokeMaskingPolicyClause(this, context);
        } else {
            return null;
        }
    }
}
