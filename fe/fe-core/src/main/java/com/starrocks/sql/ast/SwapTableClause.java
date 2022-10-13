// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;

// clause which is used to swap table
// eg:
// ALTER TABLE tbl SWAP WITH TABLE tbl2;
public class SwapTableClause extends AlterTableClause {
    private final String tblName;

    public SwapTableClause(String tblName) {
        super(AlterOpType.SWAP);
        this.tblName = tblName;
    }

    public String getTblName() {
        return tblName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSwapTableClause(this, context);
    }
}
