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

package com.starrocks.sql.ast.expression;

import com.google.common.base.Preconditions;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.formatter.AST2SQLVisitor;
import com.starrocks.sql.formatter.ExprExplainVisitor;
import com.starrocks.sql.formatter.ExprVerboseVisitor;
import com.starrocks.sql.formatter.FormatOptions;

public class ExprToSql {
    /**
     * toSql is an obsolete interface, because of historical reasons, the implementation of toSql is not rigorous enough.
     * Newly developed code should use AstToSQLBuilder::toSQL instead
     */
    public static String toSql(Expr expr) {
        ExprExplainVisitor explain = new ExprExplainVisitor();
        return explain.visit(expr);
    }

    /**
     * `toSqlWithoutTbl` will return sql without table name for column name, so it can be easier to compare two expr.
     */
    public static String toSqlWithoutTbl(Expr expr) {
        return AST2SQLVisitor.withOptions(
                FormatOptions.allEnable().setColumnSimplifyTableName(false).setColumnWithTableName(false)
                        .setEnableDigest(false)).visit(expr);
    }

    public static String explain(Expr expr) {
        ExprVerboseVisitor explain = new ExprVerboseVisitor();
        return explain.visit(expr);
    }

    public static String toMySql(Expr expr) {
        if (expr instanceof SlotRef slotRef) {
            if (slotRef.getLabel() == null) {
                throw new IllegalArgumentException(
                        "should set label for cols in MySQLScanNode. SlotRef: " + slotRef.debugString());
            }
            return slotRef.getLabel();
        } else {
            return toSql(expr);
        }
    }

    public static String toSql(OrderByElement orderByElement) {
        Expr expr = orderByElement.getExpr();
        boolean isAsc = orderByElement.getIsAsc();
        Boolean nullsFirstParam = orderByElement.getNullsFirstParam();

        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(ExprToSql.toSql(expr));
        strBuilder.append(isAsc ? " ASC" : " DESC");

        if (nullsFirstParam != null) {
            if (isAsc && !nullsFirstParam) {
                strBuilder.append(" NULLS LAST");
            } else if (!isAsc && nullsFirstParam) {
                strBuilder.append(" NULLS FIRST");
            }
        }
        return strBuilder.toString();
    }

    public static String explain(OrderByElement orderByElement) {
        Expr expr = orderByElement.getExpr();
        boolean isAsc = orderByElement.getIsAsc();
        Boolean nullsFirstParam = orderByElement.getNullsFirstParam();

        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(ExprToSql.explain(expr));
        strBuilder.append(isAsc ? " ASC" : " DESC");
        if (nullsFirstParam != null) {
            if (isAsc && !nullsFirstParam) {
                strBuilder.append(" NULLS LAST");
            } else if (!isAsc && nullsFirstParam) {
                strBuilder.append(" NULLS FIRST");
            }
        }
        return strBuilder.toString();
    }

    public static String toSql(AnalyticWindow window) {
        Preconditions.checkNotNull(window);

        StringBuilder sb = new StringBuilder();
        sb.append(window.getType().toString()).append(" ");
        AnalyticWindowBoundary rightBoundary = window.getRightBoundary();
        if (rightBoundary == null) {
            sb.append(toSql(window.getLeftBoundary()));
        } else {
            sb.append("BETWEEN ").append(toSql(window.getLeftBoundary())).append(" AND ");
            sb.append(toSql(rightBoundary));
        }

        return sb.toString();
    }

    public static String toSql(AnalyticWindowBoundary boundary) {
        StringBuilder sb = new StringBuilder();

        if (boundary.getExpr() != null) {
            sb.append(ExprToSql.toSql(boundary.getExpr())).append(" ");
        }

        sb.append(boundary.getBoundaryType().toString());
        return sb.toString();
    }
}
