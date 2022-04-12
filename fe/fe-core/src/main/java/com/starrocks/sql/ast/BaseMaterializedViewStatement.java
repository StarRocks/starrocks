// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.analysis.DdlStmt;

public class BaseMaterializedViewStatement extends DdlStmt {

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBaseMaterializedViewStatement(this, context);
    }

}