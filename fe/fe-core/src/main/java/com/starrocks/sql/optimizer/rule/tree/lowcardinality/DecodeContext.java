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

package com.starrocks.sql.optimizer.rule.tree.lowcardinality;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictMappingOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.statistics.ColumnDict;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * DecodeContext is used to store the information needed for decoding
 * 1. Record the original string column information(Expressions, Define) used for low cardinality
 * 2. Record the global dictionary
 * 3. Generate new dictionary column:
 *  a. Generate corresponding dictionary ref based on the string ref
 *  b. Generate the define expression of global dictionary column
 *  b. Rewrite string expressions
 *  c. Rewrite string aggregate expressions
 *  e. Generate the global dictionary expression
 */
class DecodeContext {
    private final ColumnRefFactory factory;

    // Global DictCache
    // string ColumnRefId -> ColumnDict
    Map<Integer, ColumnDict> stringRefToDicts = Maps.newHashMap();

    // all support dict string columns
    Set<Integer> allStringColumns = new LinkedHashSet<>();

    // string column -> define expression
    // e.g.
    // Project Node:
    // | a : upper(c)
    // | b : b
    // a is string column, upper(b) is his define
    // b is string column, b is his define
    Map<Integer, ScalarOperator> stringRefToDefineExprMap = Maps.newHashMap();

    // string column -> all relation expression
    // e.g.
    // select upper(a) from t0 where lower(a) = 'tt'
    // a is string column
    // upper(a), lower(a) is relation expression
    Map<Integer, List<ScalarOperator>> stringExprsMap = Maps.newHashMap();

    // all string aggregate expressions
    List<CallOperator> stringAggregateExprs = Lists.newArrayList();

    // The string columns used by the operator
    // IdentityHashMap: use object == object, operator equals is not enough
    Map<Operator, DecodeInfo> operatorDecodeInfo = Maps.newIdentityHashMap();

    // global dict expressions
    Map<Integer, ScalarOperator> globalDictsExpr = Maps.newHashMap();

    Map<ColumnRefOperator, ColumnRefOperator> stringRefToDictRefMap = Maps.newHashMap();

    Map<ColumnRefOperator, ScalarOperator> dictRefToDefineExprMap = Maps.newHashMap();

    Map<ScalarOperator, ScalarOperator> stringExprToDictExprMap = Maps.newHashMap();

    DecodeContext(ColumnRefFactory factory) {
        this.factory = factory;
    }

    public void initRewriteExpressions() {
        rewriteStringRefToDictRef();
        rewriteGlobalDict();
        rewriteStringExpressions();
        rewriteStringAggregations();
    }

    private void rewriteStringRefToDictRef() {
        // rewrite string column to dict column
        for (Integer stringId : allStringColumns) {
            if (!stringRefToDefineExprMap.containsKey(stringId)) {
                continue;
            }
            ColumnRefOperator stringRef = factory.getColumnRef(stringId);
            ColumnRefOperator dictRef = createNewDictColumn(stringRef);
            stringRefToDictRefMap.put(stringRef, dictRef);
            ScalarOperator defineExpr = new DictMappingOperator(dictRef, stringRef.clone(), stringRef.getType());
            stringExprToDictExprMap.put(stringRef, defineExpr);
        }

        // rewrite string column define expression
        for (Integer stringId : stringRefToDefineExprMap.keySet()) {
            ScalarOperator stringDefineExpr = stringRefToDefineExprMap.get(stringId);
            ColumnRefOperator stringRef = factory.getColumnRef(stringId);
            ColumnRefOperator dictRef = stringRefToDictRefMap.get(stringRef);

            // may be a: b (just rename in projection)
            if (stringDefineExpr.isColumnRef() && stringRef.equals(stringDefineExpr)) {
                dictRefToDefineExprMap.put(dictRef, new DictMappingOperator(dictRef, stringRef.clone(), dictRef.getType()));
            } else {
                List<ColumnRefOperator> useStringRefs = stringDefineExpr.getColumnRefs();
                if (useStringRefs.stream().distinct().count() != 1) {
                    // unsupported define expression use multi column
                    // noinspection DataFlowIssue
                    Preconditions.checkState(false);
                }
                ColumnRefOperator useStringRef = useStringRefs.get(0);
                ColumnRefOperator useDictRef = stringRefToDictRefMap.get(useStringRef);
                // return type is dict
                ScalarOperator dictExpr = new DictMappingOperator(useDictRef, stringDefineExpr.clone(), dictRef.getType());
                dictRefToDefineExprMap.put(dictRef, dictExpr);
            }
        }
    }

    private void rewriteGlobalDict() {
        GlobalDictRewriter dictRewriter = new GlobalDictRewriter();
        for (Integer stringId : stringRefToDefineExprMap.keySet()) {
            if (stringRefToDicts.containsKey(stringId)) {
                continue;
            }

            // rewrite global dict expression
            // rewrite to dictMapping(originDictColumn, fold_expression)
            // e.g. A : upper(B)
            //      B : lower(C)
            // decode A: dictMapping(B, upper(B)) -> dictMapping(C, upper(lower(C)))
            ColumnRefOperator dictRef = stringRefToDictRefMap.get(factory.getColumnRef(stringId));
            ScalarOperator stringDefineExpr = dictRefToDefineExprMap.get(dictRef);
            globalDictsExpr.put(dictRef.getId(), stringDefineExpr.accept(dictRewriter, null));
        }
    }

    private void rewriteStringExpressions() {
        // rewrite string expression
        for (Integer stringId : stringExprsMap.keySet()) {
            ColumnRefOperator stringRef = factory.getColumnRef(stringId);
            ColumnRefOperator dictRef = stringRefToDictRefMap.get(stringRef);
            for (ScalarOperator stringExpr : stringExprsMap.getOrDefault(stringId, Collections.emptyList())) {
                if (stringExprToDictExprMap.containsKey(stringExpr)) {
                    continue;
                }
                // return type is string, different as define expression
                ScalarOperator dictExpr =
                        new DictMappingOperator(dictRef, stringExpr.clone(), stringExpr.getType());
                stringExprToDictExprMap.put(stringExpr, dictExpr);
            }
        }
    }

    private void rewriteStringAggregations() {
        // rewrite string aggregate expression
        FunctionRewriter rewriter = new FunctionRewriter();
        for (CallOperator aggFn : stringAggregateExprs) {
            CallOperator new1stAggFn = (CallOperator) (aggFn.accept(rewriter, null));
            stringExprToDictExprMap.put(aggFn, new1stAggFn);
        }
    }

    // create a new dictionary column and assign the same property except for the type and column id
    // the input column maybe a dictionary column or a string column
    private ColumnRefOperator createNewDictColumn(ColumnRefOperator column) {
        if (column.getType().isArrayType()) {
            return factory.create(column.getName(), Type.ARRAY_INT, column.isNullable());
        } else {
            return factory.create(column.getName(), Type.INT, column.isNullable());
        }
    }

    private class FunctionRewriter extends BaseScalarOperatorShuttle {
        @Override
        public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void ignore) {
            return stringRefToDictRefMap.getOrDefault(variable, variable);
        }

        @Override
        public ScalarOperator visitCall(CallOperator call, Void ignore) {
            boolean[] hasChange = new boolean[1];
            List<ScalarOperator> newChildren = visitList(call.getChildren(), hasChange);

            if (call.getFunction() instanceof AggregateFunction) {
                Type[] argTypes = new Type[] {Type.INT};
                Function fn = Expr.getBuiltinFunction(call.getFnName(), argTypes, Function.CompareMode.IS_SUPERTYPE_OF);
                // min/max function: will rewrite all stage, return type is dict type
                if (FunctionSet.MAX.equals(call.getFnName()) || FunctionSet.MIN.equals(call.getFnName())) {
                    return new CallOperator(call.getFnName(), fn.getReturnType(), newChildren, fn, call.isDistinct());
                } else if (FunctionSet.COUNT.equals(call.getFnName()) ||
                        FunctionSet.MULTI_DISTINCT_COUNT.equals(call.getFnName()) ||
                        FunctionSet.APPROX_COUNT_DISTINCT.equals(call.getFnName())) {
                    // count, count_distinct, approx_count_distinct:
                    // return type don't update
                    return new CallOperator(call.getFnName(), call.getType(), newChildren, fn, call.isDistinct());
                }
            }

            if (!hasChange[0]) {
                return call;
            }

            Type[] argTypes = new Type[newChildren.size()];
            for (int i = 0; i < newChildren.size(); i++) {
                argTypes[i] = newChildren.get(i).getType();
            }
            Function fn = Expr.getBuiltinFunction(call.getFnName(), argTypes, Function.CompareMode.IS_SUPERTYPE_OF);
            return new CallOperator(call.getFnName(), fn.getReturnType(), newChildren, fn, call.isDistinct());
        }
    }

    private class GlobalDictRewriter extends BaseScalarOperatorShuttle {
        @Override
        public ScalarOperator visitDictMappingOperator(DictMappingOperator operator, Void context) {
            ScalarOperator so = operator.getOriginScalaOperator().accept(this, context);
            List<ColumnRefOperator> ll = so.getColumnRefs();
            Preconditions.checkState(ll.stream().distinct().count() == 1);
            ColumnRefOperator dictRef = stringRefToDictRefMap.get(ll.get(0));
            return new DictMappingOperator(dictRef, so, operator.getType());
        }

        @Override
        public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void ignore) {
            ScalarOperator res = stringRefToDefineExprMap.get(variable.getId());
            if (res.isColumnRef() && variable.getId() == ((ColumnRefOperator) res).getId()) {
                return stringRefToDefineExprMap.get(((ColumnRefOperator) res).getId());
            }
            return res.accept(this, null);
        }

        @Override
        public ScalarOperator visitCall(CallOperator call, Void context) {
            if (call.getFunction() instanceof AggregateFunction) {
                return call.getChild(0).accept(this, context);
            }
            return super.visitCall(call, context);
        }
    }
}
