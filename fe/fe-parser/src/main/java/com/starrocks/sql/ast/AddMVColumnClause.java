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
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.Type;

import java.util.Collections;
import java.util.List;

/**
 * Clause which is used to add a column with aggregate expression to a materialized view.
 * Syntax: ALTER MATERIALIZED VIEW mv_name ADD COLUMN column_name AS aggregate_expr [DEFAULT default_value]
 *         [COMMENT 'comment']
 * 
 * Example:
 * ALTER MATERIALIZED VIEW mv1 ADD COLUMN new_metric_sum AS SUM(new_metric_sum)
 */
public class AddMVColumnClause extends AlterTableColumnClause {
    private final String columnName;
    private final Expr aggregateExpression;
    private final ColumnDef.DefaultValueDef defaultValueDef;
    private final String comment;

    public String getColumnName() {
        return columnName;
    }

    public Expr getAggregateExpression() {
        return aggregateExpression;
    }

    public ColumnDef.DefaultValueDef getDefaultValueDef() {
        return defaultValueDef;
    }

    public String getComment() {
        return comment;
    }

    public AddMVColumnClause(String columnName, Expr aggregateExpression, String comment) {
        this(columnName, aggregateExpression, ColumnDef.DefaultValueDef.NULL_DEFAULT_VALUE, comment, NodePosition.ZERO);
    }

    public AddMVColumnClause(String columnName, Expr aggregateExpression,
                             ColumnDef.DefaultValueDef defaultValueDef, String comment) {
        this(columnName, aggregateExpression, defaultValueDef, comment, NodePosition.ZERO);
    }

    public AddMVColumnClause(String columnName, Expr aggregateExpression, String comment, NodePosition pos) {
        this(columnName, aggregateExpression, ColumnDef.DefaultValueDef.NULL_DEFAULT_VALUE, comment, pos);
    }

    public AddMVColumnClause(String columnName, Expr aggregateExpression,
                             ColumnDef.DefaultValueDef defaultValueDef, String comment, NodePosition pos) {
        super(null, pos);
        this.columnName = columnName;
        this.aggregateExpression = aggregateExpression;
        this.defaultValueDef = defaultValueDef == null
                ? ColumnDef.DefaultValueDef.NULL_DEFAULT_VALUE
                : defaultValueDef;
        this.comment = comment;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAddMVColumnClause(this, context);
    }

    public AddColumnsClause toAddColumnsClause(Type columnType) {
        ColumnDef columnDef = new ColumnDef(columnName, new TypeDef(columnType, getPos()), null,
                false, null, null, true,
                defaultValueDef,
                null, null, comment, getPos());
        return new AddColumnsClause(List.of(columnDef), null, Collections.emptyMap(), getPos());
    }
}
