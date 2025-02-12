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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.TableFunction;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDecodeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import org.apache.commons.collections4.CollectionUtils;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/*
 * Rewrite the whole plan using the dict column by from bottom-up
 */
public class DecodeRewriter extends OptExpressionVisitor<OptExpression, ColumnRefSet> {
    private final ColumnRefFactory factory;

    private final DecodeContext context;

    public DecodeRewriter(ColumnRefFactory factory, DecodeContext context) {
        this.factory = factory;
        this.context = context;
    }

    public OptExpression rewrite(OptExpression optExpression) {
        if (context.allStringColumns.isEmpty()) {
            return optExpression;
        }
        context.initRewriteExpressions();
        // check output need decode
        DecodeInfo decodeInfo = context.operatorDecodeInfo.getOrDefault(optExpression.getOp(), DecodeInfo.EMPTY);
        // compute the fragment used dict expr
        optExpression = rewriteImpl(optExpression, new ColumnRefSet());
        if (!decodeInfo.outputStringColumns.isEmpty()) {
            // decode the output dict column
            return insertDecodeNode(optExpression, decodeInfo.outputStringColumns, decodeInfo.outputStringColumns);
        }

        return optExpression;
    }

    // fragmentUseDictExprs: record the dict columns used in this fragment, to
    // compute which expressions & dict should save in the fragment
    private OptExpression rewriteImpl(OptExpression optExpression, ColumnRefSet fragmentUsedDictExprs) {
        // should get DecodeInfo before rewrite operator
        DecodeInfo decodeInfo = context.operatorDecodeInfo.getOrDefault(optExpression.getOp(), DecodeInfo.EMPTY);

        fragmentUsedDictExprs.union(decodeInfo.outputStringColumns);
        fragmentUsedDictExprs.union(decodeInfo.usedStringColumns);
        ColumnRefSet childFragmentUsedDictExpr = optExpression.getOp() instanceof PhysicalDistributionOperator ?
                new ColumnRefSet() : fragmentUsedDictExprs;

        for (int i = 0; i < optExpression.arity(); i++) {
            OptExpression child = optExpression.inputAt(i);

            DecodeInfo childDecodeInfo = context.operatorDecodeInfo.getOrDefault(child.getOp(), DecodeInfo.EMPTY);
            child = rewriteImpl(child, childFragmentUsedDictExpr);
            if (decodeInfo.decodeStringColumns.isIntersect(childDecodeInfo.outputStringColumns)) {
                // if child's output dict column required decode, insert decode node
                child = insertDecodeNode(child, childDecodeInfo.outputStringColumns, decodeInfo.decodeStringColumns);
            }
            optExpression.setChild(i, child);
        }
        // some string column need rewrite
        boolean hasDictInput = !decodeInfo.inputStringColumns.isEmpty();
        boolean hasDictOutput = !decodeInfo.outputStringColumns.isEmpty();

        if (hasDictInput || hasDictOutput) {
            return optExpression.getOp().accept(this, optExpression, fragmentUsedDictExprs);
        }
        return optExpression;
    }

    private OptExpression insertDecodeNode(OptExpression child, ColumnRefSet inputIds, ColumnRefSet decodeIds) {
        Map<ColumnRefOperator, ColumnRefOperator> dictRefToStringRefMap = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> dictRefToDictExprMap = Maps.newHashMap();
        for (Integer stringId : decodeIds.getColumnIds()) {
            if (!inputIds.contains(stringId)) {
                continue;
            }
            ColumnRefOperator stringRef = factory.getColumnRef(stringId);
            ColumnRefOperator dictRef = context.stringRefToDictRefMap.get(stringRef);
            dictRefToStringRefMap.put(dictRef, stringRef);
        }

        PhysicalDecodeOperator decodeOperator =
                new PhysicalDecodeOperator(ImmutableMap.copyOf(dictRefToStringRefMap), dictRefToDictExprMap);

        LogicalProperty property = new LogicalProperty(child.getLogicalProperty());
        ColumnRefSet outputColumns = child.getLogicalProperty().getOutputColumns();

        final ColumnRefSet rewriteOutputColumns = new ColumnRefSet();
        // rewrite dict column -> string column
        outputColumns.getStream().map(factory::getColumnRef).map(c -> dictRefToStringRefMap.getOrDefault(c, c))
                .forEach(rewriteOutputColumns::union);
        property.setOutputColumns(rewriteOutputColumns);

        // use child's info
        return OptExpression.builder().with(child).setOp(decodeOperator).setLogicalProperty(property)
                .setInputs(Lists.newArrayList(child)).build();
    }

    @Override
    public OptExpression visit(OptExpression optExpression, ColumnRefSet fragmentUseDictExprs) {
        PhysicalOperator op = optExpression.getOp().cast();
        DecodeInfo info = context.operatorDecodeInfo.getOrDefault(op, DecodeInfo.EMPTY);
        op.setPredicate(rewritePredicate(op.getPredicate(), info.inputStringColumns));
        op.setProjection(rewriteProjection(op.getProjection(), info.inputStringColumns));
        return rewriteOptExpression(optExpression, op, info.outputStringColumns);
    }

    @Override
    public OptExpression visitPhysicalHashAggregate(OptExpression optExpression, ColumnRefSet fragmentUseDictExprs) {
        // rewrite multi-stage aggregate
        PhysicalHashAggregateOperator aggregate = optExpression.getOp().cast();
        DecodeInfo info = context.operatorDecodeInfo.getOrDefault(aggregate, DecodeInfo.EMPTY);
        ColumnRefSet inputStringRefs = new ColumnRefSet();
        inputStringRefs.union(info.inputStringColumns);

        List<ColumnRefOperator> groupBys = aggregate.getGroupBys().stream()
                .map(c -> inputStringRefs.contains(c) ? context.stringRefToDictRefMap.getOrDefault(c, c) : c)
                .collect(Collectors.toList());
        List<ColumnRefOperator> partitions = aggregate.getPartitionByColumns().stream()
                .map(c -> inputStringRefs.contains(c) ? context.stringRefToDictRefMap.getOrDefault(c, c) : c)
                .collect(Collectors.toList());

        Map<ColumnRefOperator, CallOperator> aggregations = Maps.newLinkedHashMap();
        for (ColumnRefOperator aggRef : aggregate.getAggregations().keySet()) {
            CallOperator aggFn = aggregate.getAggregations().get(aggRef);
            if (!context.stringExprToDictExprMap.containsKey(aggFn)) {
                aggregations.put(aggRef, aggFn);
                continue;
            }

            // merge stage is different from update stage
            if (FunctionSet.MAX.equals(aggFn.getFnName()) || FunctionSet.MIN.equals(aggFn.getFnName())) {
                ColumnRefOperator newAggRef = context.stringRefToDictRefMap.getOrDefault(aggRef, aggRef);
                aggregations.put(newAggRef, context.stringExprToDictExprMap.get(aggFn).cast());
                inputStringRefs.union(aggRef.getId());
            } else {
                aggregations.put(aggRef, context.stringExprToDictExprMap.get(aggFn).cast());
            }
        }

        ScalarOperator predicate = rewritePredicate(aggregate.getPredicate(), inputStringRefs);
        Projection projection = rewriteProjection(aggregate.getProjection(), inputStringRefs);
        PhysicalHashAggregateOperator op =
                new PhysicalHashAggregateOperator(aggregate.getType(), groupBys, partitions, aggregations,
                        aggregate.isSplit(), aggregate.getLimit(), predicate,
                        projection);
        op.setMergedLocalAgg(aggregate.isMergedLocalAgg());
        op.setUseSortAgg(aggregate.isUseSortAgg());
        op.setUsePerBucketOptmize(aggregate.isUsePerBucketOptmize());
        return rewriteOptExpression(optExpression, op, info.outputStringColumns);
    }

    @Override
    public OptExpression visitPhysicalDistribution(OptExpression optExpression, ColumnRefSet fragmentUseDictExprs) {
        PhysicalDistributionOperator exchange = optExpression.getOp().cast();
        if (!context.operatorDecodeInfo.containsKey(exchange)) {
            return optExpression;
        }
        DecodeInfo info = context.operatorDecodeInfo.get(exchange);
        // compute the dicts and expressions used by in the fragment
        Map<Integer, ColumnDict> dictMap = Maps.newHashMap();
        for (int sid : info.inputStringColumns.getColumnIds()) {
            ColumnRefOperator stringRef = factory.getColumnRef(sid);
            if (!context.stringRefToDictRefMap.containsKey(stringRef)) {
                // count/count distinct
                continue;
            }
            ColumnRefOperator dictRef = context.stringRefToDictRefMap.get(stringRef);
            if (context.stringRefToDicts.containsKey(sid)) {
                dictMap.put(dictRef.getId(), context.stringRefToDicts.get(sid));
            } else {
                // compute expressions depend-on dict
                for (ColumnRefOperator useRefs : context.globalDictsExpr.get(dictRef.getId()).getColumnRefs()) {

                    if (context.stringRefToDicts.containsKey(useRefs.getId())) {
                        dictMap.put(context.stringRefToDictRefMap.get(useRefs).getId(),
                                context.stringRefToDicts.get(useRefs.getId()));
                    }
                }
            }
        }
        List<Pair<Integer, ColumnDict>> dicts = Lists.newArrayList();
        dictMap.forEach((k, v) -> dicts.add(new Pair<>(k, v)));
        exchange.setGlobalDicts(dicts);
        exchange.setGlobalDictsExpr(computeDictExpr(fragmentUseDictExprs));

        if (!(exchange.getDistributionSpec() instanceof HashDistributionSpec)) {
            return optExpression;
        }
        HashDistributionSpec spec = (HashDistributionSpec) exchange.getDistributionSpec();
        List<DistributionCol> shuffledColumns = Lists.newArrayList();
        for (DistributionCol column : spec.getHashDistributionDesc().getDistributionCols()) {
            if (!info.outputStringColumns.contains(column.getColId())) {
                shuffledColumns.add(column);
                continue;
            }
            ColumnRefOperator stringRef = factory.getColumnRef(column.getColId());
            ColumnRefOperator dictRef = context.stringRefToDictRefMap.getOrDefault(stringRef, stringRef);
            shuffledColumns.add(new DistributionCol(dictRef.getId(), column.isNullStrict()));
        }
        exchange.setDistributionSpec(new HashDistributionSpec(
                new HashDistributionDesc(shuffledColumns, spec.getHashDistributionDesc().getSourceType())));
        return rewriteOptExpression(optExpression, exchange, info.outputStringColumns);
    }

    @Override
    public OptExpression visitPhysicalTopN(OptExpression optExpression, ColumnRefSet fragmentUseDictExprs) {
        PhysicalTopNOperator topN = optExpression.getOp().cast();
        DecodeInfo info = context.operatorDecodeInfo.get(topN);

        List<Ordering> newOrdering = Lists.newArrayList();
        for (Ordering orderDesc : topN.getOrderSpec().getOrderDescs()) {
            if (!info.inputStringColumns.contains(orderDesc.getColumnRef().getId()) ||
                    !context.stringRefToDictRefMap.containsKey(orderDesc.getColumnRef())) {
                newOrdering.add(orderDesc);
                continue;
            }
            ColumnRefOperator dictRef = context.stringRefToDictRefMap.get(orderDesc.getColumnRef());
            newOrdering.add(new Ordering(dictRef, orderDesc.isAscending(), orderDesc.isNullsFirst()));
        }

        List<ColumnRefOperator> newPartitionByColumns = null;

        if (topN.getPartitionByColumns() != null) {
            newPartitionByColumns =
                    topN.getPartitionByColumns().stream().map(c -> context.stringRefToDictRefMap.getOrDefault(c, c))
                            .collect(Collectors.toList());
        }

        ScalarOperator predicate = rewritePredicate(topN.getPredicate(), info.inputStringColumns);
        Projection projection = rewriteProjection(topN.getProjection(), info.inputStringColumns);
        PhysicalTopNOperator newOp =
                PhysicalTopNOperator.builder().withOperator(topN).setOrderSpec(new OrderSpec(newOrdering))
                        .setPartitionByColumns(newPartitionByColumns).setPredicate(predicate).setProjection(projection)
                        .build();
        return rewriteOptExpression(optExpression, newOp, info.outputStringColumns);
    }

    @Override
    public OptExpression visitPhysicalTableFunction(OptExpression optExpression, ColumnRefSet fragmentUseDictExprs) {
        PhysicalTableFunctionOperator tableFunc = optExpression.getOp().cast();
        DecodeInfo info = context.operatorDecodeInfo.get(tableFunc);
        ColumnRefSet inputStringRefs = new ColumnRefSet();
        inputStringRefs.union(info.inputStringColumns);

        List<ColumnRefOperator> outers = tableFunc.getOuterColRefs().stream()
                .map(c -> inputStringRefs.contains(c) ? context.stringRefToDictRefMap.getOrDefault(c, c) : c)
                .collect(Collectors.toList());

        List<ColumnRefOperator> fnInputs = tableFunc.getFnParamColumnRefs();
        List<ColumnRefOperator> fnOutputs = tableFunc.getFnResultColRefs();
        TableFunction function = tableFunc.getFn();
        if (FunctionSet.UNNEST.equalsIgnoreCase(tableFunc.getFn().getFunctionName().getFunction()) &&
                inputStringRefs.containsAny(fnInputs)) {
            for (int i = 0; i < fnInputs.size(); i++) {
                if (!inputStringRefs.contains(fnInputs.get(i))) {
                    continue;
                }

                inputStringRefs.union(fnOutputs.get(i));
                fnInputs.set(i, context.stringRefToDictRefMap.getOrDefault(fnInputs.get(i), fnInputs.get(i)));
                fnOutputs.set(i, context.stringRefToDictRefMap.getOrDefault(fnOutputs.get(i), fnOutputs.get(i)));
            }
            function = (TableFunction) Expr.getBuiltinFunction(FunctionSet.UNNEST,
                    fnInputs.stream().map(ScalarOperator::getType).toArray(Type[]::new), function.getArgNames(),
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        }

        ScalarOperator predicate = rewritePredicate(tableFunc.getPredicate(), inputStringRefs);
        Projection projection = rewriteProjection(tableFunc.getProjection(), inputStringRefs);

        PhysicalTableFunctionOperator op = new PhysicalTableFunctionOperator(fnOutputs, function, fnInputs,
                outers, tableFunc.getLimit(), predicate, projection);
        return rewriteOptExpression(optExpression, op, info.outputStringColumns);
    }

    @Override
    public OptExpression visitPhysicalOlapScan(OptExpression optExpression, ColumnRefSet fragmentUseDictExprs) {
        PhysicalOlapScanOperator scanOperator = (PhysicalOlapScanOperator) optExpression.getOp();
        DecodeInfo info = context.operatorDecodeInfo.get(scanOperator);

        Map<ColumnRefOperator, Column> newRefToMetaMap = Maps.newHashMap();
        List<Pair<Integer, ColumnDict>> dicts = Lists.newArrayList();
        for (ColumnRefOperator ref : scanOperator.getColRefToColumnMetaMap().keySet()) {
            Column meta = scanOperator.getColRefToColumnMetaMap().get(ref);

            if (!info.inputStringColumns.contains(ref.getId())) {
                newRefToMetaMap.put(ref, meta);
                continue;
            }

            ColumnRefOperator dictRef = context.stringRefToDictRefMap.get(ref);
            Column newMeta = new Column(meta);
            newMeta.setType(dictRef.getType());
            newRefToMetaMap.put(dictRef, newMeta);
            dicts.add(new Pair<>(dictRef.getId(), context.stringRefToDicts.get(ref.getId())));
        }

        PhysicalOlapScanOperator.Builder builder = PhysicalOlapScanOperator.builder();
        builder.withOperator(scanOperator);

        builder.setColRefToColumnMetaMap(newRefToMetaMap);
        builder.setGlobalDicts(dicts);
        builder.setGlobalDictsExpr(computeDictExpr(fragmentUseDictExprs));

        builder.setPredicate(rewritePredicate(scanOperator.getPredicate(), info.inputStringColumns));
        if (CollectionUtils.isNotEmpty(scanOperator.getPrunedPartitionPredicates())) {
            List<ScalarOperator> prunedPredicates = scanOperator.getPrunedPartitionPredicates().stream()
                    .map(p -> rewritePredicate(p, info.inputStringColumns)).collect(Collectors.toList());
            builder.setPrunedPartitionPredicates(prunedPredicates);
        }
        builder.setProjection(rewriteProjection(scanOperator.getProjection(), info.inputStringColumns));
        return rewriteOptExpression(optExpression, builder.build(), info.outputStringColumns);
    }

    @Override
    public OptExpression visitPhysicalHiveScan(OptExpression optExpression, ColumnRefSet fragmentUseDictExprs) {
        PhysicalHiveScanOperator scanOperator = (PhysicalHiveScanOperator) optExpression.getOp();
        DecodeInfo info = context.operatorDecodeInfo.get(scanOperator);

        Map<ColumnRefOperator, Column> newRefToMetaMap = Maps.newHashMap();
        List<Pair<Integer, ColumnDict>> dicts = Lists.newArrayList();
        for (ColumnRefOperator ref : scanOperator.getColRefToColumnMetaMap().keySet()) {
            Column meta = scanOperator.getColRefToColumnMetaMap().get(ref);

            if (!info.inputStringColumns.contains(ref.getId())) {
                newRefToMetaMap.put(ref, meta);
                continue;
            }

            ColumnRefOperator dictRef = context.stringRefToDictRefMap.get(ref);
            Column newMeta = new Column(meta);
            newMeta.setType(dictRef.getType());
            newRefToMetaMap.put(dictRef, newMeta);
            dicts.add(new Pair<>(dictRef.getId(), context.stringRefToDicts.get(ref.getId())));
        }

        PhysicalHiveScanOperator.Builder builder = PhysicalHiveScanOperator.builder();
        builder.withOperator(scanOperator);
        builder.setColRefToColumnMetaMap(newRefToMetaMap);
        builder.setScanPredicates(
                rewriteScanPredicate(
                        scanOperator.getScanOperatorPredicates(), newRefToMetaMap, info.inputStringColumns));
        builder.setGlobalDicts(dicts).setGlobalDictsExpr(computeDictExpr(fragmentUseDictExprs));

        builder.setPredicate(rewritePredicate(scanOperator.getPredicate(), info.inputStringColumns));
        builder.setProjection(rewriteProjection(scanOperator.getProjection(), info.inputStringColumns));
        return rewriteOptExpression(optExpression, builder.build(), info.outputStringColumns);
    }

    @NotNull
    private Map<Integer, ScalarOperator> computeDictExpr(ColumnRefSet fragmentUseDictExprs) {
        Map<Integer, ScalarOperator> dictExprs = Maps.newHashMap();
        for (int sid : fragmentUseDictExprs.getColumnIds()) {
            ColumnRefOperator strRef = factory.getColumnRef(sid);
            if (!context.stringRefToDictRefMap.containsKey(strRef)) {
                // count/count distinct
                continue;
            }
            ColumnRefOperator dictRef = context.stringRefToDictRefMap.get(strRef);
            if (context.globalDictsExpr.containsKey(dictRef.getId())) {
                dictExprs.put(dictRef.getId(), context.globalDictsExpr.get(dictRef.getId()));
            }
        }
        return dictExprs;
    }

    private ScanOperatorPredicates rewriteScanPredicate(ScanOperatorPredicates predicates,
                                                        Map<ColumnRefOperator, Column> newmap, ColumnRefSet inputs) {
        ScanOperatorPredicates newPredicates = predicates.clone();
        newPredicates.getNoEvalPartitionConjuncts().clear();
        newPredicates.getNoEvalPartitionConjuncts().addAll(
                predicates.getNoEvalPartitionConjuncts().stream().map(x -> rewritePredicate(x, inputs))
                        .collect(Collectors.toList()));
        newPredicates.getNonPartitionConjuncts().clear();
        newPredicates.getNonPartitionConjuncts().addAll(
                predicates.getNonPartitionConjuncts().stream().map(x -> rewritePredicate(x, inputs))
                        .collect(Collectors.toList()));
        newPredicates.getMinMaxConjuncts().clear();
        newPredicates.getMinMaxConjuncts().addAll(
                predicates.getMinMaxConjuncts().stream().map(x -> rewritePredicate(x, inputs))
                        .collect(Collectors.toList()));

        newPredicates.getMinMaxColumnRefMap().clear();
        for (Map.Entry<ColumnRefOperator, Column> kv : predicates.getMinMaxColumnRefMap().entrySet()) {
            if (context.stringRefToDictRefMap.containsKey(kv.getKey())) {
                newPredicates.getMinMaxColumnRefMap().put(context.stringRefToDictRefMap.get(kv.getKey()),
                        newmap.get(context.stringRefToDictRefMap.get(kv.getKey())));
            } else {
                newPredicates.getMinMaxColumnRefMap().put(kv.getKey(), kv.getValue());
            }
        }
        return newPredicates;
    }

    private ScalarOperator rewritePredicate(ScalarOperator predicate, ColumnRefSet inputs) {
        if (predicate == null) {
            return null;
        }

        // replace string predicate to dict predicate
        ExprReplacer replacer = new ExprReplacer(context.stringExprToDictExprMap, inputs);
        return predicate.accept(replacer, null);
    }

    private Projection rewriteProjection(Projection projection, ColumnRefSet inputs) {
        if (projection == null) {
            return null;
        }

        ExprReplacer replacer = new ExprReplacer(context.stringExprToDictExprMap, inputs);
        Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = Maps.newHashMap();
        for (ColumnRefOperator key : projection.getColumnRefMap().keySet()) {
            ScalarOperator value = projection.getColumnRefMap().get(key);

            if (!context.stringRefToDictRefMap.containsKey(key)) {
                newColumnRefMap.put(key, value.accept(replacer, null));
                continue;
            }
            if (!inputs.containsAll(value.getUsedColumns())) {
                newColumnRefMap.put(key, value);
                continue;
            }
            ColumnRefOperator dictRef = context.stringRefToDictRefMap.get(key);
            if (key.equals(value)) {
                // a: a
                newColumnRefMap.put(dictRef, dictRef);
            } else {
                // a: abs(b)
                newColumnRefMap.put(dictRef, context.dictRefToDefineExprMap.get(dictRef));
            }
        }

        return new Projection(newColumnRefMap);
    }

    private OptExpression rewriteOptExpression(OptExpression optExpression, Operator newOp, ColumnRefSet outputs) {
        // rewrite logical property, update output columns
        LogicalProperty property = optExpression.getLogicalProperty();
        if (outputs.containsAny(property.getOutputColumns())) {
            LogicalProperty newProperty = new LogicalProperty(property);
            ColumnRefSet outputColumns = property.getOutputColumns();
            final ColumnRefSet rewritesOutputColumns = new ColumnRefSet();
            // For string column rewrite to dictionary column, other columns remain unchanged
            outputColumns.getStream().map(factory::getColumnRef)
                    .map(c -> outputs.contains(c) ? context.stringRefToDictRefMap.getOrDefault(c, c) : c)
                    .forEach(rewritesOutputColumns::union);
            newProperty.setOutputColumns(rewritesOutputColumns);
            property = newProperty;
        }

        return OptExpression.builder().with(optExpression).setOp(newOp).setLogicalProperty(property).build();
    }

    private static class ExprReplacer extends BaseScalarOperatorShuttle {
        private final Map<ScalarOperator, ScalarOperator> exprMapping;
        private final ColumnRefSet supportColumns;

        public ExprReplacer(Map<ScalarOperator, ScalarOperator> exprMapping, ColumnRefSet supportColumns) {
            this.exprMapping = exprMapping;
            this.supportColumns = supportColumns;
        }

        @Override
        public Optional<ScalarOperator> preprocess(ScalarOperator scalarOperator) {
            if (exprMapping.containsKey(scalarOperator)
                    && supportColumns.containsAll(scalarOperator.getUsedColumns())) {
                return Optional.of(exprMapping.get(scalarOperator));
            }
            return Optional.empty();
        }
    }
}
