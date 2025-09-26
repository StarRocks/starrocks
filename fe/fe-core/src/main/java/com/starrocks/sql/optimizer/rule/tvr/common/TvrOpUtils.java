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
// limitations under the License

package com.starrocks.sql.optimizer.rule.tvr.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.combinator.AggStateUtils;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;

/**
 * Utility class for TVR operations.
 */
public class TvrOpUtils {
    public static final String COLUMN_ROW_ID = "__ROW_ID__";
    public static final String COLUMN_AGG_STATE_PREFIX = "__AGG_STATE";

    public static String getTvrAggStateColumnName(FunctionCallExpr functionCallExpr) {
        // TODO: format functionCallExpr to a more readable name
        // agg_state column name is like __AGG_STATE_<agg_func_name>
        String exprFuncName = AstToStringBuilder.getAliasName(functionCallExpr, false,
                false);
        return String.format("%s_%s", COLUMN_AGG_STATE_PREFIX, exprFuncName);
    }

    /**
     * Build the row id function expression for IVM.
     * - For multi unique keys, use ENCODE_SORT_KEY to encode them to a varbinary and use FROM_BINARY to convert it into
     * varchar:
     *  `FROM_BINARY(ENCODE_SORT_KEY(k1, k2, ..., kn), 'encode64')
     * TODO: Since binary type is not supported to be used for primary keys, we can remove from_binary in the future.
     */
    public static FunctionCallExpr buildRowIdFuncExpr(List<Expr> uniqueKeys) {
        // This method is a placeholder for the actual implementation of building a row ID function.
        // The implementation would typically create a FunctionCallExpr that represents the row ID function
        // used in incremental view maintenance (IVM).
        FunctionCallExpr encodeSortKeyFunc = new FunctionCallExpr(FunctionSet.ENCODE_SORT_KEY, uniqueKeys);
        List<Expr> fromBinaryArgs = Lists.newArrayList(encodeSortKeyFunc, new StringLiteral("encode64"));
        FunctionCallExpr fromBinaryFunc = new FunctionCallExpr(FunctionSet.FROM_BINARY, fromBinaryArgs);
        return fromBinaryFunc;
    }

    public static ScalarOperator buildRowIdEqBinaryPredicateOp(ColumnRefOperator aggStateRowIdScalarOp,
                                                               List<ScalarOperator> uniqueKeys) {
        // build row id operator for agg state table
        ScalarOperator deltaInputRowIdScalarOp = buildRowIdColumnOperator(uniqueKeys);
        BinaryPredicateOperator eqBinaryPredicateOperator =
                new BinaryPredicateOperator(BinaryType.EQ, aggStateRowIdScalarOp, deltaInputRowIdScalarOp);
        return eqBinaryPredicateOperator;
    }

    public static ScalarOperator buildRowIdColumnOperator(List<ScalarOperator> uniqueKeys) {
        // build row id operator for agg state table
        Type[] argTypes = uniqueKeys.stream()
                .map(ScalarOperator::getType)
                .toArray(Type[]::new);
        Function newFunc = Expr.getBuiltinFunction(FunctionSet.ENCODE_SORT_KEY, argTypes,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        if (newFunc == null) {
            throw new IllegalArgumentException("Function " + FunctionSet.ENCODE_SORT_KEY + " not found");
        }
        ScalarOperator rowIdScalarOp = new CallOperator(FunctionSet.ENCODE_SORT_KEY, Type.VARBINARY, uniqueKeys, newFunc);
        // varbinary to varchar
        return fromBinaryToVarchar(rowIdScalarOp);
    }

    private static ScalarOperator fromBinaryToVarchar(ScalarOperator rowIdScalarOp) {
        Type[] argTypes = new Type[] { rowIdScalarOp.getType(), Type.VARCHAR };
        Function newFunc = Expr.getBuiltinFunction(FunctionSet.FROM_BINARY, argTypes,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        if (newFunc == null) {
            throw new IllegalArgumentException("Function " + FunctionSet.FROM_BINARY + " not found");
        }
        List<ScalarOperator> args = Lists.newArrayList(rowIdScalarOp, ConstantOperator.createVarchar("encode64"));
        return new CallOperator(FunctionSet.FROM_BINARY, Type.VARCHAR, args, newFunc);
    }

    public static ScalarOperator buildStateUnionScalarOperator(CallOperator aggFunc,
                                                               ScalarOperator intermediateAggScalarOp,
                                                               ScalarOperator aggStateAggStateColumnRef) {
        Preconditions.checkArgument(intermediateAggScalarOp.getType().equals(aggStateAggStateColumnRef.getType()),
                "The type of intermediateAggScalarOp and aggStateTableRowIdScalarOp must be the same");
        // build row id operator for agg state table
        Type[] argTypes = new Type[] { intermediateAggScalarOp.getType(), aggStateAggStateColumnRef.getType() };
        // get the state union function name
        String origAggFuncName = AggStateUtils.getAggFuncNameOfCombinator(aggFunc.getFnName());
        String stateUnionFunctionName = AggStateUtils.stateUnionFunctionName(origAggFuncName);
        Function newFunc = Expr.getBuiltinFunction(stateUnionFunctionName, argTypes,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        if (newFunc == null) {
            throw new IllegalArgumentException("Function " + stateUnionFunctionName + " not found");
        }
        return new CallOperator(stateUnionFunctionName, intermediateAggScalarOp.getType(),
                List.of(intermediateAggScalarOp, aggStateAggStateColumnRef), newFunc);
    }
}
