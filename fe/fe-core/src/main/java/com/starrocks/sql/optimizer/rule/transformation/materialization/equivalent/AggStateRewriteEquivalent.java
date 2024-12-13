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
package com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.combinator.AggStateCombinator;
import com.starrocks.catalog.combinator.AggStateUnionCombinator;
import com.starrocks.catalog.combinator.AggStateUtils;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Arrays;
import java.util.List;

import static com.starrocks.catalog.Function.CompareMode.IS_IDENTICAL;
import static com.starrocks.catalog.FunctionSet.AGG_STATE_SUFFIX;

public class AggStateRewriteEquivalent extends IAggregateRewriteEquivalent {
    public static IAggregateRewriteEquivalent INSTANCE = new AggStateRewriteEquivalent();

    public AggStateRewriteEquivalent() {}

    @Override
    public RewriteEquivalentContext prepare(ScalarOperator op) {
        if (op == null || !(op instanceof CallOperator)) {
            return null;
        }
        CallOperator aggFunc = (CallOperator) op;
        String aggFuncName = aggFunc.getFnName();
        if (aggFuncName == null || !aggFuncName.endsWith(FunctionSet.AGG_STATE_UNION_SUFFIX)) {
            return null;
        }
        // agg function must be agg_state_union
        Function callAggFunc = aggFunc.getFunction();
        if (!(callAggFunc instanceof AggStateUnionCombinator)) {
            return null;
        }
        String realAggFuncName = AggStateUtils.getAggFuncNameOfCombinator(aggFuncName);

        ScalarOperator arg0 = aggFunc.getChild(0);
        if (arg0 == null || !(arg0 instanceof CallOperator)) {
            return null;
        }

        CallOperator argCall0 = (CallOperator) arg0;
        Function argFunc0 = argCall0.getFunction();
        // arg0 must agg_state function
        if (argFunc0 == null || !(argFunc0 instanceof AggStateCombinator)) {
            return null;
        }
        String argStateName = argCall0.getFnName();
        if (argStateName == null || !argStateName.endsWith(AGG_STATE_SUFFIX)) {
            return null;
        }
        if (argCall0.getType().getAggStateDesc() == null) {
            return null;
        }
        if (!realAggFuncName.equalsIgnoreCase(AggStateUtils.getAggFuncNameOfCombinator(argStateName))) {
            return null;
        }
        if (argCall0.getChildren().isEmpty()) {
            return null;
        }
        ScalarOperator eqChild = argCall0.getChild(0);
        // use agg_state's arg0 as equivalent, and save agg_state's expression as original expression
        return new RewriteEquivalentContext(eqChild, argCall0);
    }

    @Override
    public boolean isSupportPushDownRewrite(CallOperator aggFunc) {
        return true;
    }

    @Override
    public ScalarOperator rewrite(RewriteEquivalentContext eqContext,
                                  EquivalentShuttleContext shuttleContext,
                                  ColumnRefOperator replace,
                                  ScalarOperator newInput) {
        if (newInput == null || !(newInput instanceof CallOperator)) {
            return null;
        }

        // input
        CallOperator aggFunc = (CallOperator) newInput;
        String aggFuncName = aggFunc.getFnName();

        CallOperator eqAggState = (CallOperator) eqContext.getInput();
        String realAggFuncName = AggStateUtils.getAggFuncNameOfCombinator(eqAggState.getFnName());
        if (!aggFuncName.equalsIgnoreCase(realAggFuncName) &&
                !aggFuncName.equalsIgnoreCase(FunctionSet.getAggStateMergeName(realAggFuncName))) {
            return null;
        }

        List<ScalarOperator> eqArgs = eqAggState.getChildren();
        if (aggFuncName.equalsIgnoreCase(realAggFuncName)) {
            // query's agg function, mv: avg_union(avg_state(x)), query: avg(x)
            // check all input arguments are the same.
            if (aggFunc.getChildren().size() != eqArgs.size()) {
                return null;
            }
            for (int i = 0; i < eqArgs.size(); i++) {
                if (!eqArgs.get(i).equals(aggFunc.getChild(i))) {
                    return null;
                }
            }
        } else {
            // query's agg_merge function, mv: avg_union(avg_state(x)), query: avg_merge(avg_state(x))
            ScalarOperator queryArg0 = aggFunc.getChild(0);
            if (queryArg0 == null || !(queryArg0 instanceof CallOperator)) {
                return null;
            }
            CallOperator queryArg0Call = (CallOperator) queryArg0;
            if (!(queryArg0Call.getFunction() instanceof AggStateCombinator)) {
                return null;
            }
            if (!realAggFuncName.equalsIgnoreCase(AggStateUtils.getAggFuncNameOfCombinator(queryArg0Call.getFnName()))) {
                return null;
            }
            List<ScalarOperator> queryArgs = queryArg0Call.getChildren();
            // check all input arguments are the same.
            if (queryArgs.size() != eqArgs.size()) {
                return null;
            }
            for (int i = 0; i < eqArgs.size(); i++) {
                if (!eqArgs.get(i).equals(queryArgs.get(i))) {
                    return null;
                }
            }
        }

        if (shuttleContext.isRollup()) {
            return makeAggStateRollupFunc(replace, realAggFuncName, aggFunc.getType());
        } else {
            return makeAggStateMergeFunc(replace, realAggFuncName, aggFunc.getType());
        }
    }

    private CallOperator makeAggStateRollupFunc(ScalarOperator replace,
                                                String realAggFuncName,
                                                Type aggReturnType) {
        CallOperator aggUnionRollup = makeAggStateUnionFunc(replace, realAggFuncName);
        return makeAggStateMergeFunc(aggUnionRollup, realAggFuncName, aggReturnType);
    }

    private CallOperator makeAggStateUnionFunc(ScalarOperator replace,
                                               String realAggFuncName) {
        String aggUnionFuncName = FunctionSet.getAggStateUnionName(realAggFuncName);

        // agg_state_union(input), its type is the same as input
        Type intermediateType = replace.getType();
        Function aggUnionFn = Expr.getBuiltinFunction(aggUnionFuncName, new Type[] { intermediateType },
                IS_IDENTICAL);
        Preconditions.checkState(aggUnionFn != null);
        return new CallOperator(aggUnionFuncName, intermediateType, Arrays.asList(replace), aggUnionFn);
    }

    private CallOperator makeAggStateMergeFunc(ScalarOperator input,
                                               String realAggFuncName,
                                               Type aggReturnType) {
        Type intermediateType = input.getType();
        // agg_state_merge(agg_state_union(input)), its type is the same as input
        String aggMergeFuncName = FunctionSet.getAggStateMergeName(realAggFuncName);
        Function aggMergeFn = Expr.getBuiltinFunction(aggMergeFuncName,
                new Type[] { intermediateType }, Function.CompareMode.IS_IDENTICAL);
        Preconditions.checkState(aggMergeFn != null);
        return new CallOperator(aggMergeFuncName, aggReturnType,
                Arrays.asList(input), aggMergeFn);
    }

    @Override
    public ScalarOperator rewriteRollupAggregateFunc(EquivalentShuttleContext shuttleContext,
                                                     CallOperator aggFunc,
                                                     ColumnRefOperator replace) {
        String realAggFuncName = AggStateUtils.getAggFuncNameOfCombinator(aggFunc.getFnName());
        return makeAggStateUnionFunc(replace, realAggFuncName);
    }

    @Override
    public ScalarOperator rewriteAggregateFunc(EquivalentShuttleContext shuttleContext,
                                               CallOperator aggFunc,
                                               ColumnRefOperator replace) {
        String realAggFuncName = AggStateUtils.getAggFuncNameOfCombinator(aggFunc.getFnName());
        return makeAggStateMergeFunc(replace, realAggFuncName, aggFunc.getType());
    }

    @Override
    public Pair<CallOperator, CallOperator> rewritePushDownRollupAggregateFunc(EquivalentShuttleContext shuttleContext,
                                                                               CallOperator aggFunc,
                                                                               ColumnRefOperator replace) {
        String realAggFuncName = AggStateUtils.getAggFuncNameOfCombinator(aggFunc.getFnName());
        CallOperator partialFn = makeAggStateUnionFunc(replace, realAggFuncName);
        CallOperator finalFn = makeAggStateMergeFunc(replace, realAggFuncName, aggFunc.getType());
        return Pair.create(partialFn, finalFn);
    }
}