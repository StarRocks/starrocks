// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.alter.AlterOpType;
import com.starrocks.sql.ast.AlterTableClause;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

public class RevokeMaskingPolicyClause extends AlterTableClause {
    private final String maskingColumn;

    public RevokeMaskingPolicyClause(String maskingColumn, NodePosition nodePosition) {
        super(AlterOpType.REVOKE_COLUMN_MASKING_POLICY, nodePosition);
        this.maskingColumn = maskingColumn;
    }

    public String getMaskingColumn() {
        return maskingColumn;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRevokeMaskingPolicyClause(this, context);
    }
}
