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
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.util.List;
import java.util.stream.Collectors;

public class MapExpr extends Expr {
    private boolean explicitType = false;

    public MapExpr(Type type, List<Expr> items) {
        super();
        this.type = type;
        this.children = Expr.cloneList(items);
        this.explicitType = this.type != null;
    }

    public MapExpr(MapExpr other) {
        super(other);
    }

    public Expr getKeyExpr() {
        Preconditions.checkState(children.size() > 1);
        return children.get(0);
    }

    public Expr getValueExpr() {
        Preconditions.checkState(children.size() > 1);
        return children.get(1);
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    }

    public boolean isExplicitType() {
        return explicitType;
    }

    @Override
    protected String toSqlImpl() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        sb.append(children.stream().map(Expr::toSql).collect(Collectors.joining(",")));
        sb.append(']');
        return sb.toString();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.setNode_type(TExprNodeType.MAP_EXPR);
    }

    @Override
    public Expr clone() {
        return new MapExpr(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitMapExpr(this, context);
    }
}
