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

package com.starrocks.planner.expression;

import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.type.Type;

import java.util.List;

/**
 * Execution-plan lambda function expression.
 */
public class ExecLambdaFunction extends ExecExpr {
    private final int commonSubOperatorNum;
    private final boolean isNondeterministic;

    public ExecLambdaFunction(Type type, int commonSubOperatorNum, boolean isNondeterministic,
                              List<ExecExpr> children) {
        super(type, children);
        this.commonSubOperatorNum = commonSubOperatorNum;
        this.isNondeterministic = isNondeterministic;
    }

    private ExecLambdaFunction(ExecLambdaFunction other) {
        super(other.type, other.cloneChildren());
        this.commonSubOperatorNum = other.commonSubOperatorNum;
        this.isNondeterministic = other.isNondeterministic;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    public int getCommonSubOperatorNum() {
        return commonSubOperatorNum;
    }

    public boolean isNondeterministic() {
        return isNondeterministic;
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public TExprNodeType getNodeType() {
        return TExprNodeType.LAMBDA_FUNCTION_EXPR;
    }

    @Override
    public void toThrift(TExprNode node) {
        node.setOutput_column(commonSubOperatorNum);
        node.setIs_nondeterministic(isNondeterministic);
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecLambdaFunction(this, context);
    }

    @Override
    public ExecLambdaFunction clone() {
        return new ExecLambdaFunction(this);
    }
}
