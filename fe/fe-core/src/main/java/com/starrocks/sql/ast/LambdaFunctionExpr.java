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
import com.starrocks.analysis.SlotRef;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.util.List;
import java.util.Map;

public class LambdaFunctionExpr extends Expr {
    private int commonSubOperatorNum = 0;

    // the arguments are lambda expr, lambda arg1, lambda arg2...
    public LambdaFunctionExpr(List<Expr> arguments) {
        this(arguments, NodePosition.ZERO);
    }

    public LambdaFunctionExpr(List<Expr> arguments, NodePosition pos) {
        super(pos);
        this.children.addAll(arguments);
    }

    // transformed from operators before pushing down to BE.
    public LambdaFunctionExpr(List<Expr> arguments, Map<SlotRef, Expr> commonSubOperatorMap) {
        this.children.addAll(arguments);
        this.commonSubOperatorNum = commonSubOperatorMap.size();
        if (!commonSubOperatorMap.isEmpty()) { // flatten commonSubOperatorMap's slot_id and sub_expr to children
            children.addAll(commonSubOperatorMap.keySet());
            children.addAll(commonSubOperatorMap.values());
        }
    }


    public LambdaFunctionExpr(LambdaFunctionExpr rhs) {
        super(rhs);
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        Preconditions.checkState(false, "unreachable");
    }

    @Override
    protected String toSqlImpl() {
        String names = getChild(1).toSql();
        int realChildrenNum = getChildren().size() - 2 * commonSubOperatorNum;
        if (realChildrenNum > 2) {
            names = "(" + getChild(1).toSql();
            for (int i = 2; i < realChildrenNum; ++i) {
                names = names + ", " + getChild(i).toSql();
            }
            names = names + ")";
        }
        StringBuilder commonSubOp = new StringBuilder();
        if (commonSubOperatorNum > 0) {
            commonSubOp.append("\n        lambda common expressions:");
        }
        for (int i = realChildrenNum; i < realChildrenNum + commonSubOperatorNum; ++i) {
            commonSubOp.append("{").append(getChild(i).toSql()).append(" <-> ")
                    .append(getChild(i + commonSubOperatorNum).toSql()).append("}");
        }
        if (commonSubOperatorNum > 0) {
            commonSubOp.append("\n        ");
        }
        return String.format("%s -> %s%s", names, getChild(0).toSql(), commonSubOp);
    }

    @Override
    public String explainImpl() {
        String names = getChild(1).explain();
        int realChildrenNum = getChildren().size() - 2 * commonSubOperatorNum;
        if (realChildrenNum > 2) {
            names = "(" + getChild(1).explain();
            for (int i = 2; i < realChildrenNum; ++i) {
                names = names + ", " + getChild(i).explain();
            }
            names = names + ")";
        }
        StringBuilder commonSubOp = new StringBuilder();
        if (commonSubOperatorNum > 0) {
            commonSubOp.append("\n        lambda common expressions:");
        }
        for (int i = realChildrenNum; i < realChildrenNum + commonSubOperatorNum; ++i) {
            commonSubOp.append("{").append(getChild(i).explain()).append(" <-> ")
                    .append(getChild(i + commonSubOperatorNum).explain()).append("}");
        }
        if (commonSubOperatorNum > 0) {
            commonSubOp.append("\n        ");
        }
        return String.format("%s -> %s%s", names, getChild(0).explain(), commonSubOp);
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.setNode_type(TExprNodeType.LAMBDA_FUNCTION_EXPR);
        msg.setOutput_column(commonSubOperatorNum);
    }

    @Override
    public Expr clone() {
        return new LambdaFunctionExpr(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLambdaFunctionExpr(this, context);
    }

}
