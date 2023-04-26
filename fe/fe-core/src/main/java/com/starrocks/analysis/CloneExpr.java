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

import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

public class CloneExpr extends Expr {
    public CloneExpr(Expr child) {
        super();
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
        return "clone(" + getChild(0).toSqlImpl() + ")";
    }

    @Override
    protected String explainImpl() {
        return "clone(" + getChild(0).explain() + ")";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.setNode_type(TExprNodeType.CLONE_EXPR);
    }

    @Override
    public Expr clone() {
        return new CloneExpr(getChild(0));
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCloneExpr(this, context);
    }
}
