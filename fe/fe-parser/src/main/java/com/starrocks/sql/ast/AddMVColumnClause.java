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

package com.starrocks.sql.ast;

import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.Collections;
import java.util.List;

/**
 * Clause which is used to add a column with aggregate expression to a materialized view.
 * Syntax: ALTER MATERIALIZED VIEW mv_name ADD COLUMN column_name AS aggregate_expr [COMMENT 'comment']
 * 
 * Example:
 * ALTER MATERIALIZED VIEW mv1 ADD COLUMN new_metric_sum AS SUM(new_metric_sum)
 */
public class AddMVColumnClause extends AlterTableColumnClause {
    private final String columnName;
    private final Expr aggregateExpression;
    private final String comment;
    private ColumnDef columnDef;

    public String getColumnName() {
        return columnName;
    }

    public Expr getAggregateExpression() {
        return aggregateExpression;
    }

    public String getComment() {
        return comment;
    }

    public ColumnDef getColumnDef() {
        return columnDef;
    }

    public void setColumnDef(ColumnDef columnDef) {
        this.columnDef = columnDef;
    }

    public AddMVColumnClause(String columnName, Expr aggregateExpression, String comment) {
        this(columnName, aggregateExpression, comment, NodePosition.ZERO);
    }

    public AddMVColumnClause(String columnName, Expr aggregateExpression, String comment, NodePosition pos) {
        super(null, pos);
        this.columnName = columnName;
        this.aggregateExpression = aggregateExpression;
        this.comment = comment;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAddMVColumnClause(this, context);
    }

    public AddColumnsClause toAddColumnsClause() {
        return new AddColumnsClause(List.of(columnDef), null, Collections.emptyMap(), getPos());
    }
}
