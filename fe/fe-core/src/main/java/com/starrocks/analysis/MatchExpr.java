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
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.util.Objects;

public class MatchExpr extends Expr {
    public MatchExpr(Expr e1, Expr e2) {
        this(e1, e2, NodePosition.ZERO);
    }

    public MatchExpr(Expr e1, Expr e2, NodePosition pos) {
        super(pos);
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkNotNull(e2);
        children.add(e2);
        setType(Type.BOOLEAN);
    }

    protected MatchExpr(MatchExpr other) {
        super(other);
        this.children.clear();
        for (Expr child : other.getChildren()) {
            Preconditions.checkNotNull(child);
            this.children.add(child.clone());
        }
        setType(Type.BOOLEAN);
    }

    @Override
    public Expr clone() {
        return new MatchExpr(this);
    }

    @Override
    public String toSqlImpl() {
        return getChild(0).toSql() + " MATCH " + getChild(1).toSql();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.MATCH_EXPR;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode());
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return visitor.visitMatchExpr(this, context);
    }
}
