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
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictMappingOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.statistics.ColumnDict;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.starrocks.sql.optimizer.rule.tree.lowcardinality.DecodeCollector.LOW_CARD_ARRAY_FUNCTIONS;

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
        DictExprRewrite exprRewriter = new DictExprRewrite();
        for (Integer stringId : allStringColumns) {
            if (!stringRefToDefineExprMap.containsKey(stringId)) {
                continue;
            }
            ColumnRefOperator stringRef = factory.getColumnRef(stringId);
            ColumnRefOperator dictRef = createNewDictColumn(stringRef);
            stringRefToDictRefMap.put(stringRef, dictRef);
            stringExprToDictExprMap.put(stringRef, exprRewriter.decode(dictRef, stringRef, stringRef));
        }

        // rewrite string column define expression
        for (Integer stringId : stringRefToDefineExprMap.keySet()) {
            ScalarOperator stringDefineExpr = stringRefToDefineExprMap.get(stringId);
            ColumnRefOperator stringRef = factory.getColumnRef(stringId);
            ColumnRefOperator dictRef = stringRefToDictRefMap.get(stringRef);

            // may be a: b (just rename in projection)
            ColumnRefOperator useStringRef;
            if (stringDefineExpr.isColumnRef()) {
                useStringRef = (ColumnRefOperator) stringDefineExpr;
            } else {
                List<ColumnRefOperator> useStringRefs = stringDefineExpr.getColumnRefs();
                Preconditions.checkState(useStringRefs.stream().distinct().count() == 1);
                useStringRef = useStringRefs.get(0);
            }
            // return type is dict
            ScalarOperator dictExpr = exprRewriter.define(dictRef.getType(), useStringRef, stringDefineExpr);
            dictRefToDefineExprMap.put(dictRef, dictExpr);
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
            ScalarOperator stringDefineExpr = stringRefToDefineExprMap.get(stringId);

            ScalarOperator defineExpr = stringDefineExpr.accept(dictRewriter, null);
            List<ColumnRefOperator> defineUsedStringRef = defineExpr.getColumnRefs();
            Preconditions.checkState(!defineUsedStringRef.isEmpty());

            ColumnRefOperator defineUsedDictRef = stringRefToDictRefMap.get(defineUsedStringRef.get(0));
            ScalarOperator globalDictExpr = new DictMappingOperator(defineUsedDictRef, defineExpr, dictRef.getType());
            globalDictsExpr.put(dictRef.getId(), globalDictExpr);
        }
    }

    private void rewriteStringExpressions() {
        // rewrite string expression
        DictExprRewrite exprRewriter = new DictExprRewrite();
        for (Integer stringId : stringExprsMap.keySet()) {
            ColumnRefOperator stringRef = factory.getColumnRef(stringId);
            ColumnRefOperator dictRef = stringRefToDictRefMap.get(stringRef);
            for (ScalarOperator stringExpr : stringExprsMap.getOrDefault(stringId, Collections.emptyList())) {
                if (stringExprToDictExprMap.containsKey(stringExpr)) {
                    continue;
                }
                // return type is string, different as define expression
                ScalarOperator dictExpr = exprRewriter.decode(dictRef, stringRef, stringExpr);
                stringExprToDictExprMap.put(stringExpr, dictExpr);
            }
        }
    }

    private void rewriteStringAggregations() {
        // rewrite string aggregate expression
        AggregateRewriter rewriter = new AggregateRewriter();
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

    // define mode: means the result column is dict, DictExpr should return int/array<int> type
    // decode mode: means the result column is string, DictExpr should return string/array<string> type
    private class DictExprRewrite extends BaseScalarOperatorShuttle {
        // to mark special array expression: array_min/array_max/array[x]
        // their return type is string, but use low cardinality optimization, we need execute them first
        private Optional<ScalarOperator> array2StringAnchor;

        public ScalarOperator decode(ColumnRefOperator useDictRef, ColumnRefOperator useStringRef,
                                     ScalarOperator expression) {
            if (useStringRef.getType().isVarchar()) {
                return new DictMappingOperator(useDictRef, expression.clone(), expression.getType());
            }

            Preconditions.checkState(Type.ARRAY_VARCHAR.matchesType(useStringRef.getType()));
            array2StringAnchor = Optional.empty();
            ScalarOperator result = expression.accept(this, null);
            if (result.isColumnRef() && array2StringAnchor.isEmpty()) {
                // decode array-column-ref
                return new DictMappingOperator(useDictRef, result, expression.getType());
            } else if (result instanceof CallOperator &&
                    (FunctionSet.ARRAY_LENGTH.equalsIgnoreCase(((CallOperator) result).getFnName()) ||
                            FunctionSet.CARDINALITY.equalsIgnoreCase(((CallOperator) result).getFnName()))) {
                Preconditions.checkState(array2StringAnchor.isEmpty());
                return result;
            }
            result = processArrayAnchor(result);
            return new DictMappingOperator(expression.getType(), useDictRef, result, array2StringAnchor.get());
        }

        public ScalarOperator define(Type type, ColumnRefOperator useStringRef, ScalarOperator expression) {
            ColumnRefOperator useDictRef = stringRefToDictRefMap.get(useStringRef);
            if (useStringRef.getType().isVarchar()) {
                return new DictMappingOperator(useDictRef, expression.clone(), useDictRef.getType());
            }
            Preconditions.checkState(Type.ARRAY_VARCHAR.matchesType(useStringRef.getType()));
            array2StringAnchor = Optional.empty();
            ScalarOperator result = expression.accept(this, null);

            if (array2StringAnchor.isPresent()) {
                if (!result.isColumnRef()) {
                    // e.g. upper(array_column[0])), need define string-expr by dict-expr
                    return new DictMappingOperator(type, useDictRef, result, array2StringAnchor.get());
                } else {
                    // e.g. array_column[0], need define to string
                    return array2StringAnchor.get();
                }
            }

            return result;
        }

        // array rewrite
        @Override
        public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
            return stringRefToDictRefMap.getOrDefault(variable, variable);
        }

        @Override
        public ScalarOperator visitCall(CallOperator call, Void context) {
            if (!isSupportedArrayFunction(call)) {
                return super.visitCall(call, context);
            }
            boolean[] hasChange = new boolean[1];
            List<ScalarOperator> newChildren = visitList(call.getChildren(), hasChange);
            if (!hasChange[0]) {
                return call;
            }

            Type[] argTypes = newChildren.stream().map(ScalarOperator::getType).toArray(Type[]::new);
            Function arrayFn = Expr.getBuiltinFunction(call.getFnName(), argTypes,
                    Function.CompareMode.IS_SUPERTYPE_OF);
            ScalarOperator result = new CallOperator(call.getFnName(), arrayFn.getReturnType(), newChildren, arrayFn);

            if (FunctionSet.ARRAY_MAX.equalsIgnoreCase(call.getFnName()) ||
                    FunctionSet.ARRAY_MIN.equalsIgnoreCase(call.getFnName())) {
                return processArrayAnchor(result);
            }
            return result;
        }

        @Override
        public ScalarOperator visitCollectionElement(CollectionElementOperator collectionElementOp, Void context) {
            boolean[] hasChange = new boolean[1];
            List<ScalarOperator> newChildren = visitList(collectionElementOp.getChildren(), hasChange);
            if (!hasChange[0]) {
                return collectionElementOp;
            }
            Preconditions.checkState(newChildren.get(0).getType().isArrayType());
            ScalarOperator result = new CollectionElementOperator(((ArrayType) newChildren.get(0)
                    .getType()).getItemType(), newChildren.get(0), newChildren.get(1));

            return processArrayAnchor(result);
        }

        private ScalarOperator processArrayAnchor(ScalarOperator expr) {
            if (array2StringAnchor.isPresent()) {
                return expr;
            }

            // e.g. DictExpr(useDictColumn, array_distinct(array_column)[0])
            // we need compute array_distinct(x)[0] first, then decode to string on the result
            array2StringAnchor = Optional.of(expr);
            // mock use column ref, only type is used, ScalarOperatorToExpr will rewrite it
            // @todo: rewrite ScalarOperatorToExpr process when v1 is deprecated
            List<ColumnRefOperator> usedColumns = expr.getColumnRefs();
            Preconditions.checkState(!usedColumns.isEmpty());
            ColumnRefOperator usedRef = usedColumns.get(0);
            return new ColumnRefOperator(usedRef.getId(), expr.getType(), usedRef.getName(), usedRef.isNullable());
        }
    }

    private class AggregateRewriter extends BaseScalarOperatorShuttle {
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
        public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void ignore) {
            // string dict expression use origin string column
            ScalarOperator res = stringRefToDefineExprMap.get(variable.getId());
            if (res.isColumnRef() && variable.getId() == ((ColumnRefOperator) res).getId()) {
                // mock to string column
                return new ColumnRefOperator(variable.getId(), variable.getType(), variable.getName(),
                        variable.isNullable());
            }
            return res.accept(this, null);
        }

        @Override
        public ScalarOperator visitCollectionElement(CollectionElementOperator collectionElementOp, Void context) {
            return collectionElementOp.getChild(0).accept(this, context);
        }

        @Override
        public ScalarOperator visitCall(CallOperator call, Void context) {
            if (call.getFunction() instanceof AggregateFunction) {
                return call.getChild(0).accept(this, context);
            }
            if (isSupportedArrayFunction(call)) {
                return call.getChild(0).accept(this, context);
            }
            return super.visitCall(call, context);
        }
    }

    private boolean isSupportedArrayFunction(CallOperator call) {
        // Array Function may has same name with String Function
        return LOW_CARD_ARRAY_FUNCTIONS.contains(call.getFnName()) &&
                Arrays.stream(call.getFunction().getArgs()).anyMatch(Type::isArrayType);
    }
}
