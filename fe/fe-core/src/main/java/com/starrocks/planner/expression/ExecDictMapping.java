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
 * Execution-plan dictionary mapping expression.
 */
public class ExecDictMapping extends ExecExpr {

    public ExecDictMapping(Type type, List<ExecExpr> children) {
        super(type, children);
    }

    private ExecDictMapping(ExecDictMapping other) {
        super(other.type, other.cloneChildren());
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public TExprNodeType getNodeType() {
        return TExprNodeType.DICT_EXPR;
    }

    @Override
    public void toThrift(TExprNode node) {
        // DICT_EXPR has no extra fields
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecDictMapping(this, context);
    }

    @Override
    public ExecDictMapping clone() {
        return new ExecDictMapping(this);
    }
}
