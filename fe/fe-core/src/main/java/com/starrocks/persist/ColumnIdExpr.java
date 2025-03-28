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

package com.starrocks.persist;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.parser.SqlParser;

import java.util.List;
import java.util.Map;

public class ColumnIdExpr {
    private final Expr expr;

    private ColumnIdExpr(Expr expr) {
        this.expr = expr;
    }

    public static ColumnIdExpr create(Map<String, Column> nameToColumn, Expr expr) {
        setColumnId(nameToColumn, expr);
        return new ColumnIdExpr(expr);
    }

    public static ColumnIdExpr create(List<Column> schema, Expr expr) {
        setColumnId(MetaUtils.buildNameToColumn(schema), expr);
        return new ColumnIdExpr(expr);
    }

    // Only used on create table, you should make sure that no columns in expr have been renamed.
    public static ColumnIdExpr create(Expr expr) {
        setColumnIdByColumnName(expr);
        return new ColumnIdExpr(expr);
    }

    public Expr convertToColumnNameExpr(Map<ColumnId, Column> idToColumn) {
        setColumnName(idToColumn, expr);
        return expr;
    }

    public Expr convertToColumnNameExpr(List<Column> schema) {
        setColumnName(MetaUtils.buildIdToColumn(schema), expr);
        return expr;
    }

    public Expr getExpr() {
        return expr;
    }

    public String toSql() {
        return new ExprSerializeVisitor().visit(expr);
    }

    public static ColumnIdExpr fromSql(String sql) {
        Expr expr = SqlParser.parseSqlToExpr(sql, SqlModeHelper.MODE_DEFAULT);
        setColumnIdByColumnName(expr);
        return new ColumnIdExpr(expr);
    }

    private void setColumnName(Map<ColumnId, Column> idToColumn, Expr expr) {
        if (expr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) expr;
            Column column = idToColumn.get(slotRef.getColumnId());
            if (column == null) {
                throw new SemanticException(String.format("can not get column by column id: %s", slotRef.getColumnId()));
            }
            if (!slotRef.getColumnName().equalsIgnoreCase(column.getName())) {
                slotRef.setColumnName(column.getName());
                slotRef.setLabel("`" + column.getName() + "`");
            }
        }

        for (Expr child : expr.getChildren()) {
            setColumnName(idToColumn, child);
        }
    }

    private static void setColumnId(Map<String, Column> nameToColumn, Expr expr) {
        if (expr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) expr;
            Column column = nameToColumn.get(slotRef.getColumnName());
            if (column == null) {
                throw new SemanticException(String.format("can not get column by name : %s", slotRef.getColumnName()));
            }
            slotRef.setColumnId(column.getColumnId());
        }

        for (Expr child : expr.getChildren()) {
            setColumnId(nameToColumn, child);
        }
    }

    private static void setColumnIdByColumnName(Expr expr) {
        if (expr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) expr;
            slotRef.setColumnId(ColumnId.create(slotRef.getColumnName()));
        }

        for (Expr child : expr.getChildren()) {
            setColumnIdByColumnName(child);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj.getClass() != ColumnIdExpr.class) {
            return false;
        }

        return this.expr.equals(((ColumnIdExpr) obj).expr);
    }

    private static class ExprSerializeVisitor extends AstToStringBuilder.AST2StringBuilderVisitor {
        @Override
        public String visitSlot(SlotRef node, Void context) {
            if (node.getTblNameWithoutAnalyzed() != null) {
                return node.getTblNameWithoutAnalyzed().toSql() + "." + ParseUtil.backquote(node.getColumnId().getId());
            } else {
                return node.getColumnId().toSql(node.isBackQuoted());
            }
        }
    }
}
