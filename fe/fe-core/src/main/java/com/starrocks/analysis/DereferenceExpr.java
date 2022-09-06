// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.thrift.TExprNode;

public class DereferenceExpr extends SlotRef {

    private final QualifiedName qualifiedName;

    private boolean isParsed;

    public DereferenceExpr(QualifiedName qualifiedName) {
        super(null, null, null);
        this.qualifiedName = qualifiedName;
        isParsed = false;
    }

    public DereferenceExpr(DereferenceExpr expr) {
        super(expr);
        this.qualifiedName = QualifiedName.of(expr.qualifiedName.getParts());
        this.isParsed = expr.isParsed;
    }


    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        Preconditions.checkState(isParsed());
        super.analyzeImpl(analyzer);
    }

    @Override
    public String toSqlImpl() {
        if (!isParsed()) {
            return qualifiedName.toSqlImpl();
        } else {
            return super.toSqlImpl();
        }
    }

    @Override
    protected void toThrift(TExprNode msg) {
        Preconditions.checkState(isParsed());
        super.toThrift(msg);
    }

    @Override
    public Expr clone() {
        return new DereferenceExpr(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return visitor.visitDereferenceExpr(this, context);
    }

    public SlotRef getSlotRef() {
        checkSlotRefParsed();
        return this;
    }

    private void checkSlotRefParsed() {
        Preconditions.checkState(isParsed, "SlotRef has not been parsed, you can't use it.");
    }

    public void setSlotRef(TableName tblName, String col, String label) {
        this.tblName = tblName;
        this.col = col;
        this.label = label;
        isParsed = true;
    }

    public QualifiedName getQualifiedName() {
        return qualifiedName;
    }

    public boolean isParsed() {
        return isParsed;
    }

    @Override
    public int hashCode() {
        if (!isParsed) {
            return qualifiedName.hashCode();
        } else {
            return super.hashCode();
        }
    }

    @Override
    public String getColumnName() {
        if (!isParsed) {
            return qualifiedName.getProbablyColumnName();
        } else {
            return super.getColumnName();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!super.equals(obj)) {
            return false;
        }

        DereferenceExpr expr = (DereferenceExpr) obj;
        if (!isParsed) {
            return qualifiedName.equals(expr.qualifiedName);
        } else {
            return qualifiedName.equals(expr.qualifiedName) && super.equals(expr.getSlotRef());
        }
    }
}
