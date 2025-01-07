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
import com.google.common.collect.Lists;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.combinator.AggStateCombinator;
import com.starrocks.catalog.combinator.AggStateMergeCombinator;
import com.starrocks.catalog.combinator.AggStateUnionCombinator;
import com.starrocks.catalog.combinator.AggStateUtils;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.FunctionAnalyzer;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.parser.NodePosition;

import java.util.Arrays;
import java.util.List;

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
        AggregateFunction aggregateFunction = (AggregateFunction) aggFunc.getFunction();
        List<Type> argTypes = aggFunc.getChildren().stream().map(ScalarOperator::getType).toList();
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

            return rewriteImpl(shuttleContext, aggFunc, replace);
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

            return rewriteImpl(shuttleContext, aggFunc, replace);
        }
    }

    private CallOperator makeAggStateUnionFunc(ScalarOperator replace,
                                               String realAggFuncName,
                                               List<Type> realArgTypes) {
        // agg_state_union(input), its type is the same as input
        Function aggFn = getAnalyzedFunction(realAggFuncName, realArgTypes);
        Preconditions.checkArgument(aggFn instanceof AggregateFunction);
        return AggStateUnionCombinator.of((AggregateFunction) aggFn)
                .map(aggUnionFunc -> new CallOperator(aggUnionFunc.functionName(),
                        aggUnionFunc.getReturnType(), Arrays.asList(replace), aggUnionFunc))
                .orElse(null);
    }

    private CallOperator makeAggStateMergeFunc(ScalarOperator input,
                                               String realAggFuncName,
                                               List<Type> realArgTypes) {
        // agg_state_merge(agg_state_union(input)), its type is the same as input
        Function aggFn = getAnalyzedFunction(realAggFuncName, realArgTypes);
        Preconditions.checkArgument(aggFn instanceof AggregateFunction);
        return AggStateMergeCombinator.of((AggregateFunction) aggFn)
                .map(aggMergeFunc -> new CallOperator(aggMergeFunc.functionName(),
                        aggMergeFunc.getReturnType(), Arrays.asList(input), aggMergeFunc))
                .orElse(null);
    }

    private Function getAnalyzedFunction(String functionName,
                                         List<Type> argTypes) {
        ConnectContext connectContext = ConnectContext.get() != null ? ConnectContext.get() : new ConnectContext();
        FunctionParams params = new FunctionParams(false, Lists.newArrayList());
        Type[] argumentTypes = argTypes.toArray(Type[]::new);
        Boolean[] isArgumentConstants = argTypes.stream().map(x -> false).toArray(Boolean[]::new);
        Function aggFn = FunctionAnalyzer.getAnalyzedAggregateFunction(connectContext, functionName,
                params, argumentTypes, isArgumentConstants, NodePosition.ZERO);
        Preconditions.checkState(aggFn != null);
        return aggFn;
    }

    @Override
    public ScalarOperator rewriteRollupAggregateFunc(EquivalentShuttleContext shuttleContext,
                                                     CallOperator aggFunc,
                                                     ColumnRefOperator replace) {
        List<Type> argTypes = aggFunc.getChildren().stream().map(ScalarOperator::getType).toList();
        String realAggFuncName = AggStateUtils.getAggFuncNameOfCombinator(aggFunc.getFnName());

        AggregateFunction aggregateFunction = (AggregateFunction) aggFunc.getFunction();
        if (aggregateFunction instanceof AggStateUnionCombinator) {
            return makeAggStateUnionFunc(replace, realAggFuncName, argTypes);
        } else if (aggregateFunction instanceof AggStateMergeCombinator) {
            return makeAggStateMergeFunc(replace, realAggFuncName, argTypes);
        } else {
            return makeAggStateMergeFunc(replace, realAggFuncName, argTypes);
        }
    }

    @Override
    public ScalarOperator rewriteAggregateFuncWithoutRollup(EquivalentShuttleContext shuttleContext,
                                                            CallOperator aggFunc,
                                                            ColumnRefOperator replace) {
        // List<Type> argTypes = aggFunc.getChildren().stream().map(ScalarOperator::getType).toList();
        // String realAggFuncName = AggStateUtils.getAggFuncNameOfCombinator(aggFunc.getFnName());
        // return makeAggStateMergeFunc(replace, realAggFuncName, argTypes);
        return null;
    }

    @Override
    public Pair<CallOperator, CallOperator> rewritePushDownRollupAggregateFunc(EquivalentShuttleContext shuttleContext,
                                                                               CallOperator aggFunc,
                                                                               ColumnRefOperator replace) {
        String realAggFuncName = AggStateUtils.getAggFuncNameOfCombinator(aggFunc.getFnName());
        List<Type> argTypes = aggFunc.getChildren().stream().map(ScalarOperator::getType).toList();
        CallOperator partialFn = makeAggStateUnionFunc(replace, realAggFuncName, argTypes);
        CallOperator finalFn = makeAggStateMergeFunc(replace, realAggFuncName, Lists.newArrayList(partialFn.getType()));
        return Pair.create(partialFn, finalFn);
    }
}