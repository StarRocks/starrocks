// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.DecimalV3FunctionAnalyzer;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;

import static com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF;
import static com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter.DEFAULT_TYPE_CAST_RULE;

public class ScalarOperatorUtil {
    public static CallOperator buildMultiCountDistinct(CallOperator oldFunctionCall) {
        Function searchDesc = new Function(new FunctionName(FunctionSet.MULTI_DISTINCT_COUNT),
                oldFunctionCall.getFunction().getArgs(), Type.INVALID, false);
        Function fn = GlobalStateMgr.getCurrentState().getFunction(searchDesc, IS_NONSTRICT_SUPERTYPE_OF);

        ScalarOperatorRewriter scalarOpRewriter = new ScalarOperatorRewriter();
        return (CallOperator) scalarOpRewriter.rewrite(
                new CallOperator(FunctionSet.MULTI_DISTINCT_COUNT, fn.getReturnType(), oldFunctionCall.getChildren(),
                        fn),
                DEFAULT_TYPE_CAST_RULE);
    }

    public static CallOperator buildSum(ColumnRefOperator arg) {
        Preconditions.checkArgument(arg.getType() == Type.BIGINT);
        Function searchDesc = new Function(new FunctionName(FunctionSet.SUM),
                new Type[] {arg.getType()}, arg.getType(), false);
        Function fn = GlobalStateMgr.getCurrentState().getFunction(searchDesc, IS_NONSTRICT_SUPERTYPE_OF);
        ScalarOperatorRewriter scalarOpRewriter = new ScalarOperatorRewriter();
        return (CallOperator) scalarOpRewriter.rewrite(
                new CallOperator(FunctionSet.SUM, fn.getReturnType(), Lists.newArrayList(arg), fn),
                DEFAULT_TYPE_CAST_RULE);
    }

    public static CallOperator buildMultiSumDistinct(CallOperator oldFunctionCall) {
        Function multiDistinctSum = DecimalV3FunctionAnalyzer.convertSumToMultiDistinctSum(
                oldFunctionCall.getFunction(), oldFunctionCall.getChild(0).getType());
        ScalarOperatorRewriter scalarOpRewriter = new ScalarOperatorRewriter();
        return (CallOperator) scalarOpRewriter.rewrite(
                new CallOperator(
                        FunctionSet.MULTI_DISTINCT_SUM, multiDistinctSum.getReturnType(),
                        oldFunctionCall.getChildren(), multiDistinctSum), DEFAULT_TYPE_CAST_RULE);
    }
}
