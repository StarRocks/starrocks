// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.alter.AlterOpType;
import com.starrocks.sql.ast.AlterTableClause;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

public class ApplyMaskingPolicyClause extends AlterTableClause {
    private final String maskingColumn;
    private final WithColumnMaskingPolicy withColumnMaskingPolicy;

    public ApplyMaskingPolicyClause(String maskingColumn, WithColumnMaskingPolicy withColumnMaskingPolicy,
                                    NodePosition nodePosition) {
        super(AlterOpType.APPLY_COLUMN_MASKING_POLICY, nodePosition);
        this.maskingColumn = maskingColumn;
        this.withColumnMaskingPolicy = withColumnMaskingPolicy;
    }

    public String getMaskingColumn() {
        return maskingColumn;
    }

    public WithColumnMaskingPolicy getWithColumnMaskingPolicy() {
        return withColumnMaskingPolicy;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitApplyMaskingPolicyClause(this, context);
    }
}