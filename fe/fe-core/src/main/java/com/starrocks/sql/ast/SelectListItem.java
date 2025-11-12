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

import com.google.common.base.Preconditions;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
public class SelectListItem implements ParseNode {
    private Expr expr;
    // for "[name.]*"
    private final TableName tblName;
    private final boolean isStar;
    private String alias;

    private final NodePosition pos;

    private final List<String> excludedColumns;

    public SelectListItem(Expr expr, String alias) {
        this(expr, alias, NodePosition.ZERO);
    }

    public SelectListItem(TableName tblName) {
        this(tblName, NodePosition.ZERO, Collections.emptyList());
    }

    public SelectListItem(Expr expr, String alias, NodePosition pos) {
        Preconditions.checkNotNull(expr);
        this.pos = pos;
        this.expr = expr;
        this.alias = alias;
        this.tblName = null;
        this.isStar = false;
        this.excludedColumns = Collections.emptyList();
    }

    public SelectListItem(TableName tblName, NodePosition pos, List<String> excludedColumns) {
        this.pos = pos;
        this.expr = null;
        this.tblName = tblName;
        this.isStar = true;
        this.excludedColumns = excludedColumns;
    }

    protected SelectListItem(SelectListItem other) {
        pos = other.pos;
        if (other.expr == null) {
            expr = null;
        } else {
            expr = other.expr.clone().reset();
        }
        tblName = other.tblName;
        isStar = other.isStar;
        alias = other.alias;
        this.excludedColumns = new ArrayList<>(other.excludedColumns);
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    @Override
    public SelectListItem clone() {
        return new SelectListItem(this);
    }

    public boolean isStar() {
        return isStar;
    }

    public TableName getTblName() {
        return tblName;
    }

    public Expr getExpr() {
        return expr;
    }

    public void setExpr(Expr expr) {
        this.expr = expr;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public List<String> getExcludedColumns() {
        return excludedColumns;
    }

}
