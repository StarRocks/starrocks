// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.AST2SQL;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.transformer.OptExprBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SubqueryOperator extends ScalarOperator {

    private final QueryStatement queryStatement;
    private final LogicalApplyOperator applyOperator;
    private final OptExprBuilder rootBuilder;

    public SubqueryOperator(Type type, QueryStatement queryStatement,
                            LogicalApplyOperator applyOperator, OptExprBuilder rootBuilder) {
        super(OperatorType.SUBQUERY, type);
        this.queryStatement = queryStatement;
        this.applyOperator = applyOperator;
        this.rootBuilder = rootBuilder;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    public LogicalApplyOperator getApplyOperator() {
        return applyOperator;
    }

    public OptExprBuilder getRootBuilder() {
        return rootBuilder;
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
        return Objects.hash(queryStatement, applyOperator);
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
