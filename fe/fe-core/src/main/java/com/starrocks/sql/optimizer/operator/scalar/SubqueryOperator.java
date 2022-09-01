// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.AST2SQL;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SubqueryOperator extends ScalarOperator {

    // mark work way
    private boolean useSemiAnti;
    private final QueryStatement queryStatement;

    public SubqueryOperator(boolean useSemiAnti, QueryStatement queryStatement) {
        super(OperatorType.SUBQUERY, Type.SUBQUERY_TYPE);
        this.useSemiAnti = useSemiAnti;
        this.queryStatement = queryStatement;
    }

    public boolean isUseSemiAnti() {
        return useSemiAnti;
    }

    public void setUseSemiAnti(boolean useSemiAnti) {
        this.useSemiAnti = useSemiAnti;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public List<ScalarOperator> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public ScalarOperator getChild(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setChild(int index, ScalarOperator child) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "(" + AST2SQL.toString(queryStatement) + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(useSemiAnti, queryStatement);
    }

    @Override
    public boolean equals(Object other) {
        return this == other;
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitSubqueryOperator(this, context);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        return new ColumnRefSet();
    }
}
