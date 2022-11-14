// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.RedirectStatus;

public class ImportWhereStmt extends StatementBase {
    private final Expr expr;

    public ImportWhereStmt(Expr expr) {
        this.expr = expr;
    }

    public Expr getExpr() {
        return expr;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return null;
    }
}
