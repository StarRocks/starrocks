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

package com.starrocks.sql.optimizer.rule.ivm.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.combinator.AggStateUtils;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.AggStateDesc;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;

import java.util.List;

/**
 * Utility class for IVM (Incremental View Maintenance) operations.
 *
 * <p>Contains helpers for:
 * <ul>
 *   <li>Aggregate state column naming ({@code __AGG_STATE_*})</li>
 *   <li>Row-id encoding ({@code encode_sort_key} / {@code encode_fingerprint_sha256})</li>
 *   <li>Row-id equality predicate and state_union scalar operator construction</li>
 * </ul>
 */
public class IvmOpUtils {
    public static final String COLUMN_ROW_ID = "__ROW_ID__";
    public static final String COLUMN_AGG_STATE_PREFIX = "__AGG_STATE";
    public static final ImmutableMap<Integer, String> ENCODE_ROW_ID_FUNCTION_MAP =
            new ImmutableMap.Builder<Integer, String>()
                    .put(0, FunctionSet.ENCODE_SORT_KEY)
                    .put(1, FunctionSet.ENCODE_FINGERPRINT_SHA256)
                    .build();

    private IvmOpUtils() {
    }

    public static String getIvmAggStateColumnName(FunctionCallExpr functionCallExpr) {
        // agg_state column name is like __AGG_STATE_<agg_func_name>
        String exprFuncName = AstToStringBuilder.getAliasName(functionCallExpr, false, false);
        return String.format("%s_%s", COLUMN_AGG_STATE_PREFIX, exprFuncName);
    }

    /**
     * encode_row_id(...) -> encode_sort_key(...) if all args are fixed-length and total size <= 32 bytes
     *                    -> encode_fingerprint_sha256(...) otherwise
     */
    public static int deduceEncodeRowIdVersion(List<Expr> children) {
        boolean allTypesAvailable = true;
        int totalSize = 0;

        for (Expr child : children) {
            if (!child.isAnalyzed() || child.getType() == null || !child.getType().isValid()) {
                allTypesAvailable = false;
                break;
            }

            if (child.getType().isScalarType()) {
                ScalarType scalarType = (ScalarType) child.getType();
                if (scalarType.getPrimitiveType().isVariableLengthType()) {
                    allTypesAvailable = false;
                    break;
                }
                totalSize += scalarType.getPrimitiveType().getTypeSize();
            } else if (child.getType().isComplexType()) {
                allTypesAvailable = false;
                break;
            }
        }

        if (allTypesAvailable && totalSize > 0 && totalSize <= 32) {
            return 0;
        }
        return 1;
    }

    public static String getEncodeRowIdFunctionNameChecked(List<Expr> children) {
        int encodeRowIdVersion = deduceEncodeRowIdVersion(children);
        return getEncodeRowIdFunctionNameChecked(encodeRowIdVersion);
    }

    public static String getEncodeRowIdFunctionNameChecked(int encodeRowIdVersion) {
        final String encodeRowIdFuncName = ENCODE_ROW_ID_FUNCTION_MAP.get(encodeRowIdVersion);
        if (encodeRowIdFuncName == null) {
            throw new IllegalArgumentException("Unsupported encode row id function version: " + encodeRowIdVersion);
        }
        return encodeRowIdFuncName;
    }

    /**
     * Build the row id function expression for IVM.
     * For multi unique keys, use ENCODE_ROW_ID to encode them to a varbinary and use FROM_BINARY to convert it into
     * varchar: {@code FROM_BINARY(ENCODE_ROW_ID(k1, k2, ..., kn), 'encode64')}
     */
    public static FunctionCallExpr buildRowIdFuncExpr(int encodeRowIdVersion, List<Expr> uniqueKeys) {
        final String encodeRowIdFuncName = getEncodeRowIdFunctionNameChecked(encodeRowIdVersion);
        FunctionCallExpr encodeSortKeyFunc = new FunctionCallExpr(encodeRowIdFuncName, uniqueKeys);
        List<Expr> fromBinaryArgs = Lists.newArrayList(encodeSortKeyFunc, new StringLiteral("encode64"));
        return new FunctionCallExpr(FunctionSet.FROM_BINARY, fromBinaryArgs);
    }

    public static ScalarOperator buildRowIdEqBinaryPredicateOp(int encodeRowIdVersion,
                                                                ColumnRefOperator aggStateRowIdScalarOp,
                                                                List<ScalarOperator> uniqueKeys) {
        ScalarOperator deltaInputRowIdScalarOp = buildRowIdColumnOperator(encodeRowIdVersion, uniqueKeys);
        return new BinaryPredicateOperator(BinaryType.EQ, aggStateRowIdScalarOp, deltaInputRowIdScalarOp);
    }

    public static ScalarOperator buildRowIdColumnOperator(int encodeRowIdVersion,
                                                           List<ScalarOperator> uniqueKeys) {
        Type[] argTypes = uniqueKeys.stream().map(ScalarOperator::getType).toArray(Type[]::new);
        final String encodeRowIdFuncName = getEncodeRowIdFunctionNameChecked(encodeRowIdVersion);
        Function newFunc = ExprUtils.getBuiltinFunction(encodeRowIdFuncName, argTypes,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        if (newFunc == null) {
            throw new IllegalArgumentException("Function " + encodeRowIdFuncName + " not found");
        }
        ScalarOperator rowIdScalarOp =
                new CallOperator(encodeRowIdFuncName, VarbinaryType.VARBINARY, uniqueKeys, newFunc);
        return fromBinaryToVarchar(rowIdScalarOp);
    }

    private static ScalarOperator fromBinaryToVarchar(ScalarOperator rowIdScalarOp) {
        Type[] argTypes = new Type[] {rowIdScalarOp.getType(), VarcharType.VARCHAR};
        Function newFunc = ExprUtils.getBuiltinFunction(FunctionSet.FROM_BINARY, argTypes,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        if (newFunc == null) {
            throw new IllegalArgumentException("Function " + FunctionSet.FROM_BINARY + " not found");
        }
        List<ScalarOperator> args = Lists.newArrayList(rowIdScalarOp, ConstantOperator.createVarchar("encode64"));
        return new CallOperator(FunctionSet.FROM_BINARY, VarcharType.VARCHAR, args, newFunc);
    }

    public static ScalarOperator buildStateUnionScalarOperator(CallOperator aggFunc,
                                                                ScalarOperator intermediateAggScalarOp,
                                                                ScalarOperator aggStateAggStateColumnRef) {
        Preconditions.checkArgument(
                typesCompatibleForStateUnion(intermediateAggScalarOp.getType(), aggStateAggStateColumnRef.getType()),
                "intermediateAggScalarOp type %s is incompatible with MV state column type %s",
                intermediateAggScalarOp.getType(), aggStateAggStateColumnRef.getType());
        Type[] argTypes = new Type[] {intermediateAggScalarOp.getType(), aggStateAggStateColumnRef.getType()};
        String origAggFuncName = AggStateUtils.getAggFuncNameOfCombinator(aggFunc.getFnName());
        String stateUnionFunctionName = AggStateUtils.stateUnionFunctionName(origAggFuncName);
        Function newFunc = ExprUtils.getBuiltinFunction(stateUnionFunctionName, argTypes,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        if (newFunc == null) {
            throw new IllegalArgumentException("Function " + stateUnionFunctionName + " not found");
        }
        // <agg>_state_union overloads share one signature (varbinary, varbinary), so FunctionSet only
        // keeps the first; its AggStateDesc may not match the real input type. The AGG_STATE column
        // type carries the typed AggStateDesc from AggStateCombineCombinator.of — use it to specialize.
        AggStateDesc inputAggStateDesc = intermediateAggScalarOp.getType().getAggStateDesc();
        Preconditions.checkState(inputAggStateDesc != null,
                "intermediateAggScalarOp's type must carry an AggStateDesc, got: %s",
                intermediateAggScalarOp.getType());
        Function specializedFunc = newFunc.copy();
        specializedFunc.setAggStateDesc(inputAggStateDesc.clone());
        return new CallOperator(stateUnionFunctionName, intermediateAggScalarOp.getType(),
                List.of(intermediateAggScalarOp, aggStateAggStateColumnRef), specializedFunc);
    }

    @VisibleForTesting
    static boolean typesCompatibleForStateUnion(Type intermediate, Type mvColumn) {
        if (intermediate.equals(mvColumn)) {
            return true;
        }
        // MaterializedViewAnalyzer caps VARCHAR length at OlapMaxVarcharLength (65533)
        // when persisting MV column types, so the delta side's narrower VARCHAR(N) is
        // semantically compatible with the MV side's wider VARCHAR(65533). Same logic
        // applies to other string types sharing a primitive kind.
        if (intermediate.isStringType() && mvColumn.isStringType()
                && intermediate.getPrimitiveType() == mvColumn.getPrimitiveType()) {
            return true;
        }
        return false;
    }
}
