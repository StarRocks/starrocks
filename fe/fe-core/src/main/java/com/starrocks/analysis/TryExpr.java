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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Objects;

public class TryExpr extends Expr {
    private static final Logger LOG = LogManager.getLogger(TryExpr.class);

    public TryExpr(Expr child) {
        this(child, NodePosition.ZERO);
    }

    public TryExpr(Expr child, NodePosition pos) {
        super(pos);
        this.addChild(child);
        setType(child.getType());
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    }

    @Override
    public Type getType() {
        return getChild(0).getType();
    }

    @Override
    public void setType(Type type) {
        super.setType(type);
        getChild(0).setType(type);
    }

    @Override
    protected String toSqlImpl() {
        return "TRY(" + getChild(0).toSqlImpl() + ")";
    }

    @Override
    protected String explainImpl() {
        return "try(" + getChild(0).explain() + ")";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.TRY_EXPR;
        msg.setOpcode(opcode);
        msg.setOutput_column(outputColumn);
        if (getChild(0).getType().isComplexType()) {
            msg.setChild_type_desc(getChild(0).getType().toThrift());
        } else {
            msg.setChild_type(getChild(0).getType().getPrimitiveType().toThrift());
        }
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public Expr clone() {
        return new TryExpr(getChild(0).clone(), pos);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTryExpr(this, context);
    }
}
