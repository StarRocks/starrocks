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

import com.starrocks.catalog.Function;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TFunction;
import com.starrocks.type.BooleanType;

import java.util.List;

/**
 * Execution-plan LIKE / REGEXP predicate.
 * Serialized as a FUNCTION_CALL with the resolved built-in function.
 */
public class ExecLikePredicate extends ExecExpr {
    private final boolean isRegexp;
    private final Function fn;

    public ExecLikePredicate(boolean isRegexp, Function fn, List<ExecExpr> children) {
        super(BooleanType.BOOLEAN, children);
        this.isRegexp = isRegexp;
        this.fn = fn;
    }

    private ExecLikePredicate(ExecLikePredicate other) {
        super(other.type, other.cloneChildren());
        this.isRegexp = other.isRegexp;
        this.fn = other.fn;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    public boolean isRegexp() {
        return isRegexp;
    }

    public Function getFn() {
        return fn;
    }

    @Override
    public boolean isNullable() {
        return hasNullableChild();
    }

    @Override
    public TExprNodeType getNodeType() {
        return TExprNodeType.FUNCTION_CALL;
    }

    @Override
    public void toThrift(TExprNode node) {
        if (fn != null) {
            TFunction tfn = fn.toThrift();
            tfn.setIgnore_nulls(false);
            node.setFn(tfn);
            if (fn.hasVarArgs()) {
                node.setVararg_start_idx(fn.getNumArgs() - 1);
            }
        }
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecLikePredicate(this, context);
    }

    @Override
    public ExecLikePredicate clone() {
        return new ExecLikePredicate(this);
    }
}
