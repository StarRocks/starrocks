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


package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;

/**
 * ArrowExpr: col->'xxx'
 * Mostly used in JSON expression
 */
public class ArrowExpr extends Expr {

    public ArrowExpr(Expr left, Expr right) {
        this(left, right, NodePosition.ZERO);
    }

    public ArrowExpr(Expr left, Expr right, NodePosition pos) {
        super(pos);
        this.children.add(left);
        this.children.add(right);
    }

    public ArrowExpr(ArrowExpr rhs) {
        super(rhs);
    }

    public Expr getItem() {
        Preconditions.checkState(getChildren().size() == 2);
        return getChild(0);
    }

    public Expr getKey() {
        Preconditions.checkState(getChildren().size() == 2);
        return getChild(1);
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        Preconditions.checkState(false, "unreachable");
    }

    @Override
    protected String toSqlImpl() {
        return String.format("%s->%s", getItem().toSqlImpl(), getKey().toSqlImpl());
    }

    @Override
    protected void toThrift(TExprNode msg) {
        throw new StarRocksPlannerException("not support", ErrorType.INTERNAL_ERROR);
    }

    @Override
    public Expr clone() {
        return new ArrowExpr(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitArrowExpr(this, context);
    }
}
