// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.TableRef;

// TRUNCATE TABLE tbl [PARTITION(p1, p2, ...)]
public class TruncateTableStmt extends DdlStmt {

    private final TableRef tblRef;

    public TruncateTableStmt(TableRef tblRef) {
        this.tblRef = tblRef;
    }

    public TableRef getTblRef() {
        return tblRef;
    }

    public String getDbName() {
        return tblRef.getName().getDb();
    }

    public void setDbName(String dbName) {
        this.tblRef.getName().setDb(dbName);
    }

    public String getTblName() {
        return tblRef.getName().getTbl();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTruncateTableStatement(this, context);
    }
}
