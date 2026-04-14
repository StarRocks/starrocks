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
import com.starrocks.catalog.FunctionName;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TFunction;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.InvalidType;
import com.starrocks.type.Type;

import java.util.List;

/**
 * Execution-plan IS NULL / IS NOT NULL predicate.
 * Serialized as a FUNCTION_CALL with is_null_pred / is_not_null_pred built-in function.
 */
public class ExecIsNullPredicate extends ExecExpr {
    private static final Function IS_NULL_FN = buildNullPredicateFn("is_null_pred");
    private static final Function IS_NOT_NULL_FN = buildNullPredicateFn("is_not_null_pred");

    private final boolean isNotNull;

    public ExecIsNullPredicate(boolean isNotNull, ExecExpr child) {
        super(BooleanType.BOOLEAN, List.of(child));
        this.isNotNull = isNotNull;
    }

    private ExecIsNullPredicate(ExecIsNullPredicate other) {
        super(other.type, other.cloneChildren());
        this.isNotNull = other.isNotNull;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    private static Function buildNullPredicateFn(String name) {
        Function fn = new Function(new FunctionName(name),
                new Type[] {InvalidType.INVALID}, BooleanType.BOOLEAN, false);
        fn.setBinaryType(TFunctionBinaryType.BUILTIN);
        return fn;
    }

    public boolean isNotNull() {
        return isNotNull;
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public TExprNodeType getNodeType() {
        return TExprNodeType.FUNCTION_CALL;
    }

    @Override
    public void toThrift(TExprNode node) {
        Function fn = isNotNull ? IS_NOT_NULL_FN : IS_NULL_FN;
        TFunction tfn = fn.toThrift();
        tfn.setIgnore_nulls(false);
        node.setFn(tfn);
        if (fn.hasVarArgs()) {
            node.setVararg_start_idx(fn.getNumArgs() - 1);
        }
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecIsNullPredicate(this, context);
    }

    @Override
    public ExecIsNullPredicate clone() {
        return new ExecIsNullPredicate(this);
    }
}
