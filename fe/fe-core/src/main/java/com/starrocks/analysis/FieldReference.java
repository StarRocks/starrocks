// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.ExprVisitor;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.thrift.TExprNode;

import java.util.Objects;

/**
 * The special "SlotRef" does not store the column name.
 * Only store the offset. Used in the analysis of star
 * eg. "select * from (select count(*) from table) t"
 * will store field reference 0 in inner queryblock
 */
public class FieldReference extends Expr {
    private final int fieldIndex;
    private final TableName tblName;

    public FieldReference(int fieldIndex, TableName tableName) {
        this.fieldIndex = fieldIndex;
        this.tblName = tableName;
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitFieldReference(this, context);
    }

    public int getFieldIndex() {
        return fieldIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FieldReference that = (FieldReference) o;
        return fieldIndex == that.fieldIndex && tblName.equals(that.tblName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fieldIndex, tblName);
    }

    @Override
    public Expr clone() {
        return new FieldReference(fieldIndex, tblName);
    }

    @Override
    protected String toSqlImpl() {
        return "FieldReference(" + fieldIndex + ")";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        throw new StarRocksPlannerException("FieldReference not implement toThrift", ErrorType.INTERNAL_ERROR);
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        throw new StarRocksPlannerException("FieldReference not implement toThrift", ErrorType.INTERNAL_ERROR);
    }
}
