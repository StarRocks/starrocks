// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.AstToStringBuilder;
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
        return "(" + AstToStringBuilder.toString(queryStatement) + ")";
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
