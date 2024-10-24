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

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.parser.NodePosition;

public class ImportColumnDesc implements ParseNode {
    private String columnName;
    private Expr expr;

    // for table function table
    // if the column is in select list, isMaterialized is true, else false
    // isMaterialized always is true in broker load
    private final boolean isMaterialized;

    private final NodePosition pos;

    public ImportColumnDesc(String column) {
        this(column, null, NodePosition.ZERO);
    }

    public ImportColumnDesc(String column, Expr expr) {
        this(column, expr, NodePosition.ZERO);
    }

    public ImportColumnDesc(String column, Expr expr, NodePosition pos) {
        this(column, expr, true, pos);
    }

    // for table function table
    public ImportColumnDesc(String column, boolean isMaterialized) {
        this(column, null, isMaterialized, NodePosition.ZERO);
    }

    public ImportColumnDesc(String column, Expr expr, boolean isMaterialized, NodePosition pos) {
        this.pos = pos;
        this.columnName = column;
        this.expr = expr;
        this.isMaterialized = isMaterialized;
    }

    public void reset(String column, Expr expr) {
        this.columnName = column;
        this.expr = expr;
    }

    public String getColumnName() {
        return columnName;
    }

    public Expr getExpr() {
        return expr;
    }

    public boolean isColumn() {
        return expr == null;
    }

    public boolean isMaterialized() {
        return isMaterialized;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(columnName);
        if (expr != null) {
            sb.append("=").append(expr.toSql());
        }
        return sb.toString();
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
