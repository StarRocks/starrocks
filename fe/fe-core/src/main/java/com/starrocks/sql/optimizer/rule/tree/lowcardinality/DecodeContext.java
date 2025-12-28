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
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictMappingOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.sql.optimizer.rule.tree.lowcardinality.DecodeCollector.LOW_CARD_ARRAY_FUNCTIONS;
import static com.starrocks.sql.optimizer.rule.tree.lowcardinality.DecodeCollector.LOW_CARD_STRUCT_FUNCTIONS;
import static com.starrocks.sql.optimizer.rule.tree.lowcardinality.DecodeCollector.LOW_CARD_WINDOW_FUNCTIONS;

/*
 * DecodeContext is used to store the information needed for decoding
 * 1. Record the original string column information(Expressions, Define) used for low cardinality
 * 2. Record the global dictionary
 * 3. Generate new dictionary column:
 *  a. Generate corresponding dictionary ref based on the string ref
 *  b. Generate the define expression of global dictionary column
 *  c. Rewrite string expressions
 *  d. Rewrite string aggregate expressions
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

    // Maintains a mapping from struct ColumnRefs to field ColumnRefs to use for encoded fields.
    // Currently only fields which correspond to a ColumnRef can be encoded. All field ColumnRefOperators should have
    // STRING or ARRAY<STRING> types. Nested structs are not supported.
    Map<Integer, Map<String, ColumnRefOperator>> structRefToFieldUseStringRefMap = Maps.newHashMap();

    // Maintains a mapping from struct operators to field ColumnRefs to use for encoded fields.
    // Currently only fields which correspond to a ColumnRef can be encoded. All field ColumnRefOperators should have
    // STRING or ARRAY<STRING> types. Nested structs are not supported.
    Map<ScalarOperator, Map<String, ColumnRefOperator>> structOpToFieldUseStringRefMap =
            Maps.newIdentityHashMap();

    DecodeContext(ColumnRefFactory factory) {
        this.factory = factory;
    }

    public void initRewriteExpressions() {
        rewriteStringRefToDictRef();
        rewriteGlobalDict();
        rewriteStringExpressions();
        rewriteStringAggregations();
    }

    Map<String, ColumnRefOperator> getFieldUseStringRefMap(ScalarOperator operator) {
        if (operator.isColumnRef()) {
            ColumnRefOperator c = operator.cast();
            return structRefToFieldUseStringRefMap.get(c.getId());
        }
        return structOpToFieldUseStringRefMap.get(operator);

    }

    ColumnRefOperator getUseStringRef(ScalarOperator operator) {
        if (operator.isColumnRef()) {
            return (ColumnRefOperator) operator;
        }
        if (operator instanceof SubfieldOperator) {
            SubfieldOperator subfieldOperator = operator.cast();
            Map<String, ColumnRefOperator> fieldsUseRefMap = getFieldUseStringRefMap(subfieldOperator.getChild(0));
            Preconditions.checkNotNull(fieldsUseRefMap);
            Preconditions.checkState(subfieldOperator.getFieldNames().size() == 1
                    && fieldsUseRefMap.containsKey(subfieldOperator.getFieldNames().get(0)));
            return getUseStringRef(fieldsUseRefMap.get(subfieldOperator.getFieldNames().get(0)));
        }
        List<ColumnRefOperator> columnRefs = Lists.newArrayList();
        for (ScalarOperator child : operator.getChildren()) {
            ColumnRefOperator ref = getUseStringRef(child);
            if (ref != null) {
                columnRefs.add(ref);
            }
        }
        if (columnRefs.isEmpty()) {
            return null;
        }
        Preconditions.checkState(columnRefs.stream().distinct().count() == 1);
        return columnRefs.get(0);
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
            ColumnRefOperator useStringRef = stringRef.getType().isStructType() ? stringRef
                    : getUseStringRef(stringDefineExpr);
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
            if (dictRef.getType().isStructType()) {
                continue;
            }
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
        if (column.getType().isStringArrayType()) {
            return factory.create(column.getName(), ArrayType.ARRAY_INT, column.isNullable());
        } else if (column.getType().isStringType()) {
            return factory.create(column.getName(), IntegerType.INT, column.isNullable());
        } else if (column.getType().isStructType()) {
            Map<String, ColumnRefOperator> fieldsData = getFieldUseStringRefMap(column);
            Preconditions.checkNotNull(fieldsData);
            List<StructField> structFields = Lists.newArrayList();
            StructType type = (StructType) column.getType();
            for (int i = 0; i < type.getFields().size(); ++i) {
                if (fieldsData.containsKey(type.getField(i).getName())) {
                    // DecodeCollector ensures all ColumnRefOperators in this map have STRING or ARRAY<STRING> type.
                    Preconditions.checkState(type.getField(i).getType().isStringArrayType()
                            || type.getField(i).getType().isStringType());
                    structFields.add(new StructField(type.getField(i).getName(),
                            type.getField(i).getType().isArrayType() ? ArrayType.ARRAY_INT : IntegerType.INT));
                } else {
                    structFields.add(type.getField(i));
                }
            }
            StructType dictType = new StructType(structFields, type.isNamed());
            return factory.create(column.getName(), dictType, column.isNullable());
        } else {
            throw new IllegalArgumentException("Unsupported dictified type: " +  column.getType());
        }
    }

    // define mode: means the result column is dict, DictExpr should return int/array<int> type
    // decode mode: means the result column is string, DictExpr should return string/array<string> type
    private class DictExprRewrite extends BaseScalarOperatorShuttle {
        // to mark special array expression: array_min/array_max/array[x]
        // their return type is string, but use low cardinality optimization, we need execute them first
        private ScalarOperator anchorOp;
        private ColumnRefOperator anchorUseDictRef;

        public ScalarOperator decodeStruct(ScalarOperator dictExpression,
                                           ScalarOperator expression,
                                           ColumnRefOperator useStringRef) {
            Preconditions.checkState(dictExpression.getType().isStructType());
            Preconditions.checkState(useStringRef.getType().isStructType());
            StructType exprType =  (StructType) expression.getType();
            StructType dictType = (StructType) dictExpression.getType();
            List<ScalarOperator> newFields = Lists.newArrayList();
            Map<String, ColumnRefOperator> fieldsStringRefMap = getFieldUseStringRefMap(useStringRef);
            Preconditions.checkNotNull(fieldsStringRefMap);
            for (int i = 0; i < exprType.getFields().size(); ++i) {
                String fieldName = exprType.getField(i).getName();
                newFields.add(ConstantOperator.createVarchar(fieldName));
                Type fieldOriginalType =  exprType.getField(i).getType();
                Type fieldDictType = dictType.getField(i).getType();
                ScalarOperator fieldExpr =  new SubfieldOperator(dictExpression, fieldDictType, List.of(fieldName));
                if (fieldOriginalType.matchesType(fieldDictType)) {
                    newFields.add(fieldExpr);
                } else {
                    ColumnRefOperator fieldStringRef = fieldsStringRefMap.get(fieldName);
                    Preconditions.checkNotNull(fieldStringRef);
                    ColumnRefOperator fieldDictRef = stringRefToDictRefMap.get(fieldStringRef);
                    newFields.add(new DictMappingOperator(
                            fieldOriginalType,
                            fieldDictRef,
                            new ColumnRefOperator(
                                    fieldDictRef.getId(),
                                    fieldExpr.getType(),
                                    fieldDictRef.getName(),
                                    fieldExpr.isNullable()),
                            fieldExpr));
                }
            }
            Type[] argTypes = newFields.stream().map(ScalarOperator::getType).toArray(Type[]::new);
            Function fn = ExprUtils.getBuiltinFunction(
                    FunctionSet.NAMED_STRUCT, argTypes, Function.CompareMode.IS_SUPERTYPE_OF).copy();
            fn.setRetType(exprType);
            return new CallOperator(FunctionSet.NAMED_STRUCT, fn.getReturnType(), newFields, fn);
        }

        public ScalarOperator decode(ColumnRefOperator useDictRef, ColumnRefOperator useStringRef,
                                     ScalarOperator expression) {
            anchorOp = null;
            anchorUseDictRef = null;
            ScalarOperator result = expression.accept(this, null);
            if (useStringRef.getType().isVarchar() && anchorOp == null) {
                return new DictMappingOperator(useDictRef, expression.clone(), expression.getType());
            }
            if (result.getType().isStructType()) {
                Preconditions.checkState(anchorOp == null);
                return decodeStruct(result, expression, useStringRef);
            }
            if (result.isColumnRef() && anchorOp == null) {
                // decode array-column-ref
                return new DictMappingOperator(useDictRef, result, expression.getType());
            } else if (result instanceof CallOperator &&
                    (FunctionSet.ARRAY_LENGTH.equalsIgnoreCase(((CallOperator) result).getFnName()) ||
                            FunctionSet.CARDINALITY.equalsIgnoreCase(((CallOperator) result).getFnName()))) {
                Preconditions.checkState(anchorOp == null);
                return result;
            } else if (result instanceof SubfieldOperator) {
                Preconditions.checkState(expression instanceof SubfieldOperator);
                SubfieldOperator subfieldOperator = expression.cast();
                if (subfieldOperator.getFieldNames().size() != 1 ||
                        !getFieldUseStringRefMap(expression.getChild(0))
                                .containsKey(subfieldOperator.getFieldNames().get(0))) {
                    // Getting non dictified field from a struct
                    return result;
                }
            }
            result = processAnchor(result);
            return new DictMappingOperator(expression.getType(), anchorUseDictRef, result, anchorOp);
        }

        public ScalarOperator define(Type type, ColumnRefOperator useStringRef, ScalarOperator expression) {
            ColumnRefOperator useDictRef = stringRefToDictRefMap.get(useStringRef);
            anchorOp = null;
            anchorUseDictRef = null;
            ScalarOperator result = expression.accept(this, null);
            if (useStringRef.getType().isVarchar() && anchorOp == null) {
                return new DictMappingOperator(useDictRef, expression.clone(), useDictRef.getType());

            }
            if (anchorOp != null) {
                if (!result.isColumnRef()) {
                    // e.g. upper(array_column[0])), need define string-expr by dict-expr
                    return new DictMappingOperator(type, anchorUseDictRef, result, anchorOp);
                } else {
                    // e.g. array_column[0], need define to string
                    return anchorOp;
                }
            }

            return result;
        }

        // array rewrite
        @Override
        public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
            return stringRefToDictRefMap.getOrDefault(variable, variable);
        }

        private static Function buildFunction(String fnName, List<ScalarOperator> args) {
            Type[] argTypes = args.stream().map(ScalarOperator::getType).toArray(Type[]::new);
            Function fn = ExprUtils.getBuiltinFunction(fnName, argTypes, Function.CompareMode.IS_SUPERTYPE_OF);
            if (!fnName.equals(FunctionSet.NAMED_STRUCT)) {
                return fn;
            }
            fn = fn.copy();
            List<StructField> fields = Lists.newArrayList();
            for (int i = 0; i < args.size(); i += 2) {
                fields.add(new StructField(((ConstantOperator) args.get(i)).getVarchar(), argTypes[i + 1]));
            }
            fn.setRetType(new StructType(fields, true));
            return fn;
        }

        @Override
        public ScalarOperator visitCall(CallOperator call, Void context) {
            if (!isSupportedArrayFunction(call) && !LOW_CARD_STRUCT_FUNCTIONS.contains(call.getFnName())) {
                return super.visitCall(call, context);
            }
            boolean[] hasChange = new boolean[1];
            List<ScalarOperator> newChildren = visitList(call.getChildren(), hasChange);
            if (!hasChange[0]) {
                return call;
            }

            Function fn = buildFunction(call.getFnName(), newChildren);
            ScalarOperator result = new CallOperator(call.getFnName(), fn.getReturnType(), newChildren, fn);

            if (FunctionSet.ARRAY_MAX.equalsIgnoreCase(call.getFnName()) ||
                    FunctionSet.ARRAY_MIN.equalsIgnoreCase(call.getFnName())) {
                return processAnchor(result);
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
                    .getType()).getItemType(), newChildren.get(0), newChildren.get(1), collectionElementOp.isCheckOutOfBounds());

            return processAnchor(result);
        }

        @Override
        public ScalarOperator visitSubfield(SubfieldOperator operator, Void context) {
            ScalarOperator newChild = operator.getChild(0).accept(this, context);
            if (newChild == operator.getChild(0)) {
                return operator;
            }
            StructType originalType = (StructType) operator.getChild(0).getType();
            StructType newType = (StructType) newChild.getType();
            ScalarOperator result = new SubfieldOperator(
                    newChild,
                    newType.getField(operator.getFieldNames().get(0)).getType(),
                    operator.getFieldNames(),
                    operator.getCopyFlag());
            if (!originalType.getField(operator.getFieldNames().get(0)).getType().matchesType(
                    newType.getField(operator.getFieldNames().get(0)).getType())) {
                Map<String, ColumnRefOperator> useFieldRefMap = getFieldUseStringRefMap(operator.getChild(0));
                Preconditions.checkNotNull(useFieldRefMap);
                ColumnRefOperator fieldUseStringRef = useFieldRefMap.get(operator.getFieldNames().get(0));
                Preconditions.checkNotNull(fieldUseStringRef);
                ColumnRefOperator childDictRef = stringRefToDictRefMap.get(fieldUseStringRef);
                Preconditions.checkNotNull(childDictRef);
                anchorUseDictRef = childDictRef;
                if (operator.getType().isStringType()) {
                    return processAnchor(result);
                } else {
                    return result;
                }
            }
            return result;
        }

        private ScalarOperator processAnchor(ScalarOperator expr) {
            if (anchorOp != null && !anchorOp.equals(expr)) {
                return expr;
            }

            // e.g. DictExpr(useDictColumn, array_distinct(array_column)[0])
            // we need compute array_distinct(x)[0] first, then decode to string on the result
            anchorOp = expr;
            // mock use column ref, only type is used, ScalarOperatorToExpr will rewrite it
            // @todo: rewrite ScalarOperatorToExpr process when v1 is deprecated
            if (anchorUseDictRef == null) {
                List<ColumnRefOperator> usedColumns = expr.getColumnRefs();
                Preconditions.checkState(!usedColumns.isEmpty());
                anchorUseDictRef = usedColumns.get(0);
            }

            return new ColumnRefOperator(anchorUseDictRef.getId(), expr.getType(),
                    anchorUseDictRef.getName(), anchorUseDictRef.isNullable());
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
                Type argType = newChildren.get(0).getType().isStructType() ? newChildren.get(0).getType()
                        : newChildren.get(0).getType().isArrayType() ? new ArrayType(IntegerType.INT) : IntegerType.INT;
                Type[] argTypes = new Type[] {argType};
                Function fn = ExprUtils.getBuiltinFunction(call.getFnName(), argTypes, Function.CompareMode.IS_SUPERTYPE_OF);
                // min/max function: will rewrite all stage, return type is dict type
                if (FunctionSet.MAX.equals(call.getFnName()) || FunctionSet.MIN.equals(call.getFnName())
                        || FunctionSet.ANY_VALUE.equals((call.getFnName()))) {
                    return new CallOperator(call.getFnName(), fn.getReturnType(), newChildren, fn,
                            call.isDistinct(), call.isRemovedDistinct());
                } else if (FunctionSet.COUNT.equals(call.getFnName()) ||
                        FunctionSet.MULTI_DISTINCT_COUNT.equals(call.getFnName()) ||
                        FunctionSet.APPROX_COUNT_DISTINCT.equals(call.getFnName())) {
                    // count, count_distinct, approx_count_distinct:
                    // return type don't update
                    return new CallOperator(call.getFnName(), call.getType(), newChildren, fn,
                            call.isDistinct(), call.isRemovedDistinct());
                } else if (LOW_CARD_WINDOW_FUNCTIONS.contains(call.getFnName())) {
                    CallOperator newCall = new CallOperator(call.getFnName(), fn.getReturnType(), newChildren, fn,
                            call.isDistinct(), call.isRemovedDistinct());
                    //encoded window functions should set ignore null flag
                    newCall.setIgnoreNulls(call.getIgnoreNulls());
                    return newCall;
                }
            }

            if (!hasChange[0]) {
                return call;
            }

            Type[] argTypes = new Type[newChildren.size()];
            for (int i = 0; i < newChildren.size(); i++) {
                argTypes[i] = newChildren.get(i).getType();
            }
            Function fn = ExprUtils.getBuiltinFunction(call.getFnName(), argTypes, Function.CompareMode.IS_SUPERTYPE_OF);
            return new CallOperator(call.getFnName(), fn.getReturnType(), newChildren, fn,
                    call.isDistinct(), call.isRemovedDistinct());
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

        @Override
        public ScalarOperator visitSubfield(SubfieldOperator operator, Void context) {
            Preconditions.checkState(operator.getFieldNames().size() == 1);
            Map<String, ColumnRefOperator> fieldData = getFieldUseStringRefMap(operator.getChild(0));
            Preconditions.checkNotNull(fieldData);
            ColumnRefOperator useStringRef = fieldData.get(operator.getFieldNames().get(0));
            Preconditions.checkNotNull(useStringRef);
            return useStringRef.accept(this, context);
        }
    }

    private boolean isSupportedArrayFunction(CallOperator call) {
        // Array Function may has same name with String Function
        return LOW_CARD_ARRAY_FUNCTIONS.contains(call.getFnName()) &&
                Arrays.stream(call.getFunction().getArgs()).anyMatch(Type::isArrayType);
    }
}
