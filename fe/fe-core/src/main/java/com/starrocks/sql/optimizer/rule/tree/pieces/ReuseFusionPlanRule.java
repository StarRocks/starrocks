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

package com.starrocks.sql.optimizer.rule.tree.pieces;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.combinator.AggStateIf;
import com.starrocks.cdc.CDCPlanHelper;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.FunctionAnalyzer;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.FunctionParams;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.CTEContext;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarRangePredicateExtractor;
import com.starrocks.sql.optimizer.rule.tree.TreeRewriteRule;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.BooleanType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ReuseFusionPlanRule implements TreeRewriteRule {
    // original piece id -> fusion piece
    private final Map<Integer, QueryPiecesPlan> originPlan2FusionPiecsMap = Maps.newHashMap();

    private final List<QueryPiecesPlan> fusionPieces = Lists.newArrayList();

    private ColumnRefFactory factory;

    private ConnectContext ctx;

    public boolean hasRewrite() {
        return !fusionPieces.isEmpty();
    }

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        if (CDCPlanHelper.containsChangesScan(root)) {
            return root;
        }
        factory = taskContext.getOptimizerContext().getColumnRefFactory();
        ctx = taskContext.getOptimizerContext().getConnectContext();
        PiecesPlanTransformer transformer = new PiecesPlanTransformer(factory);
        root = transformer.transformSPJGPieces(root);

        recommendFusionCTE(transformer.getPlanPieces());
        Preconditions.checkState(root.getOp().getOpType() == OperatorType.LOGICAL);

        if (!fusionPieces.isEmpty()) {
            OptExpression anchor = generateCTEPlan(taskContext.getOptimizerContext().getCteContext(), root.inputAt(0));
            root.setChild(0, anchor);
        }
        return transformer.transformPlan(root);
    }

    private OptExpression generateCTEPlan(CTEContext cteContext, OptExpression root) {
        OptExpression start = root;
        for (QueryPiecesPlan plan : fusionPieces) {
            int cteId = cteContext.getNextCteId();
            plan.planId = cteId;
            OptExpression producerChild = ensureProducerOutputsForCTE(plan.toOptExpression(), plan);
            OptExpression produce = OptExpression.create(new LogicalCTEProduceOperator(cteId), producerChild);
            root = OptExpression.create(new LogicalCTEAnchorOperator(cteId), produce, root);
            cteContext.addForceCTE(cteId);
        }
        rewritePlan(start);
        return root;
    }

    private void deriveLogicalProperty(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            deriveLogicalProperty(child);
        }

        if (root.getLogicalProperty() == null) {
            ExpressionContext context = new ExpressionContext(root);
            context.deriveLogicalProperty();
            root.setLogicalProperty(context.getRootProperty());
        }
    }

    private OptExpression ensureProducerOutputsForCTE(OptExpression producerChild, QueryPiecesPlan plan) {
        if (plan.pieceIdToRowCountRef == null || plan.pieceIdToRowCountRef.isEmpty()) {
            return producerChild;
        }

        deriveLogicalProperty(producerChild);
        ColumnRefSet childOutputs = producerChild.getOutputColumns();
        boolean needProject = false;
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        for (ColumnRefOperator ref : childOutputs.getColumnRefOperators(factory)) {
            projectMap.put(ref, ref);
        }
        for (ColumnRefOperator hitRef : plan.pieceIdToRowCountRef.values()) {
            if (!childOutputs.contains(hitRef)) {
                projectMap.put(hitRef, hitRef);
                needProject = true;
            }
        }
        if (needProject) {
            return OptExpression.create(new LogicalProjectOperator(projectMap), producerChild);
        }
        return producerChild;
    }

    private OptExpression rewritePlan(OptExpression root) {
        if (root.getOp().getOpType() != OperatorType.LOGICAL_SPJG_PIECES) {
            for (int i = 0; i < root.arity(); i++) {
                root.setChild(i, rewritePlan(root.inputAt(i)));
            }
            return root;
        }

        LogicalPiecesOperator pieces = (LogicalPiecesOperator) root.getOp();
        if (!originPlan2FusionPiecsMap.containsKey(pieces.getPiece().planId)) {
            return pieces.getPlan();
        }

        ColumnRefSet outputs = pieces.getPlan().getOutputColumns();
        Map<ColumnRefOperator, ColumnRefOperator> cteRefsMapping = Maps.newHashMap();

        QueryPiecesPlan fusionPlan = originPlan2FusionPiecsMap.get(pieces.getPiece().planId);
        for (ColumnRefOperator originRef : outputs.getColumnRefOperators(factory)) {
            cteRefsMapping.put(originRef, fusionPlan.columnRefConverter.convertRef(originRef));
        }

        ScalarOperator consumerPredicate = buildPerPieceGatingPredicate(pieces, fusionPlan, cteRefsMapping);

        LogicalCTEConsumeOperator consumer = LogicalCTEConsumeOperator.builder().setCteId(fusionPlan.planId)
                .setCteOutputColumnRefMap(cteRefsMapping)
                .setPredicate(consumerPredicate)
                .build();
        return OptExpression.create(consumer);
    }

    private ScalarOperator buildPerPieceGatingPredicate(LogicalPiecesOperator pieces,
                                                        QueryPiecesPlan fusionPlan,
                                                        Map<ColumnRefOperator, ColumnRefOperator> cteRefsMapping) {
        ScalarOperator basePredicate = pieces.getPredicate();
        if (fusionPlan.pieceIdToRowCountRef == null ||
                !fusionPlan.pieceIdToRowCountRef.containsKey(pieces.getPiece().planId)) {
            return basePredicate;
        }

        ColumnRefOperator fusedHitRef = fusionPlan.pieceIdToRowCountRef.get(pieces.getPiece().planId);
        Boolean hasGroupBy = fusionPlan.pieceIdToHasGroupBy.getOrDefault(pieces.getPiece().planId, Boolean.TRUE);
        if (!hasGroupBy) {
            return basePredicate;
        }

        ColumnRefOperator originHitRef = null;
        for (Map.Entry<ColumnRefOperator, ColumnRefOperator> e : cteRefsMapping.entrySet()) {
            if (e.getValue().equals(fusedHitRef)) {
                originHitRef = e.getKey();
                break;
            }
        }
        if (originHitRef == null) {
            originHitRef = factory.create(fusedHitRef.getName(), fusedHitRef.getType(), false);
            cteRefsMapping.put(originHitRef, fusedHitRef);
        }

        ScalarOperator gate = (fusedHitRef.getType() == BooleanType.BOOLEAN)
                ? new IsNullPredicateOperator(true, originHitRef)
                : BinaryPredicateOperator.gt(originHitRef, ConstantOperator.createBigint(0));

        return basePredicate == null ? gate : Utils.compoundAnd(basePredicate, gate);
    }

    private void recommendFusionCTE(List<QueryPiecesPlan> piecesPlans) {
        Collectors.groupingBy(QueryPiecesPlan::planIdentifier);

        Map<String, List<QueryPiecesPlan>> groups =
                piecesPlans.stream().collect(Collectors.groupingBy(QueryPiecesPlan::planIdentifier));

        for (Map.Entry<String, List<QueryPiecesPlan>> entry : groups.entrySet()) {
            List<QueryPiecesPlan> pieces = entry.getValue();

            if (pieces.size() < 2) {
                continue;
            }

            PiecesFusion fusion = new PiecesFusion(factory, ctx, pieces);
            Optional<QueryPiecesPlan> p = fusion.fusion();
            p.ifPresent(queryPiecesPlan -> {
                fusionPieces.add(queryPiecesPlan);
                pieces.forEach(pi -> originPlan2FusionPiecsMap.put(pi.planId, queryPiecesPlan));
            });
        }
    }

    // top-down generator new plan
    private static class PiecesFusion extends OperatorVisitor<Optional<QueryPieces>, List<QueryPieces>> {

        private final ScalarOperatorConverter converter;

        private final ColumnRefFactory factory;

        private final List<QueryPiecesPlan> piecePlans;

        private final List<JoinOperator> allJoinTypes;

        // align with piecePlans
        private final List<List<ScalarOperator>> fusionScanFilters;

        private ConnectContext ctx;

        public PiecesFusion(ColumnRefFactory factory, ConnectContext ctx, List<QueryPiecesPlan> piecePlans) {
            this.factory = factory;
            this.piecePlans = piecePlans;

            this.converter = new ScalarOperatorConverter();
            this.converter.disableCreateRef();

            this.allJoinTypes = Lists.newArrayList();
            this.fusionScanFilters = Lists.newArrayList();
            piecePlans.forEach(p -> this.fusionScanFilters.add(Lists.newArrayList()));
        }

        public Optional<QueryPiecesPlan> fusion() {
            List<QueryPieces> qp = piecePlans.stream().map(p -> p.root).collect(Collectors.toList());
            if (qp.stream().map(p -> p.op.getOpType()).distinct().count() > 1) {
                return Optional.empty();
            }
            Optional<QueryPieces> plan = qp.get(0).op.accept(this, qp);
            return plan.map(p -> {
                QueryPiecesPlan newPlan = new QueryPiecesPlan(-1, converter);
                newPlan.root = p;
                for (QueryPiecesPlan pp : piecePlans) {
                    if (pp.pieceIdToRowCountRef != null) {
                        newPlan.pieceIdToRowCountRef.putAll(pp.pieceIdToRowCountRef);
                    }
                }
                return newPlan;
            });
        }

        private Optional<QueryPieces> visitChild(List<QueryPieces> pieces, int childIndex) {
            List<Integer> childSize = pieces.stream().map(p -> p.inputs.size()).distinct().collect(Collectors.toList());
            if (childSize.size() > 1 && childSize.get(0) <= childIndex) {
                return Optional.empty();
            }

            List<QueryPieces> children = pieces.stream().map(p -> p.inputs.get(childIndex))
                    .collect(Collectors.toList());
            List<OperatorType> ops = children.stream().map(p -> p.op.getOpType()).distinct()
                    .collect(Collectors.toList());
            if (ops.size() > 1) {
                return Optional.empty();
            }
            return children.get(0).op.accept(this, children);
        }

        @Override
        public Optional<QueryPieces> visitLogicalProject(LogicalProjectOperator node, List<QueryPieces> context) {
            Optional<QueryPieces> child = visitChild(context, 0);
            if (child.isEmpty()) {
                return child;
            }

            Map<ColumnRefOperator, ScalarOperator> project = Maps.newHashMap();
            for (QueryPieces pieces : context) {
                LogicalProjectOperator p = pieces.op.cast();
                p.getColumnRefMap().forEach((k, v) -> {
                    if (converter.contains(k)) {
                        project.put(converter.convertRef(k), converter.convert(v));
                    } else {
                        ColumnRefOperator newRef = factory.create(k.getName(), k.getType(), k.isNullable());
                        project.put(newRef, converter.convert(v));
                        converter.add(k, newRef);
                    }
                });
            }

            for (ColumnRefOperator ref : child.get().filterUsedRefs) {
                project.put(ref, ref);
            }
            return QueryPieces.of(new LogicalProjectOperator(project), child.get().filterUsedRefs, child.get());
        }

        @Override
        public Optional<QueryPieces> visitLogicalAggregation(LogicalAggregationOperator node,
                                                             List<QueryPieces> context) {
            Optional<QueryPieces> child = visitChild(context, 0);
            if (child.isEmpty()) {
                return Optional.empty();
            }

            List<ScalarOperator> pieceFilters = fusionScanFilters.stream()
                    .map(Utils::compoundAnd).toList();

            long filterDistinct = pieceFilters.stream().distinct().count();
            if (filterDistinct != 1 && !allJoinTypes.isEmpty() &&
                    allJoinTypes.stream().anyMatch(p -> p != JoinOperator.INNER_JOIN)) {
                return Optional.empty();
            }

            List<ColumnRefOperator> groupBys = node.getGroupingKeys().stream().map(converter::convertRef)
                    .collect(Collectors.toList());
            List<ColumnRefOperator> partitions = node.getPartitionByColumns().stream().map(converter::convertRef)
                    .collect(Collectors.toList());
            Map<CallOperator, ColumnRefOperator> aggToRefs = Maps.newHashMap();
            Map<ScalarOperator, ColumnRefOperator> aggFilterProject = Maps.newHashMap();
            List<ScalarOperator> havingPredicates = Lists.newArrayList();
            // extra aggregation outputs that must be preserved by upper projects (e.g., row_hit/row_count_if)
            List<ColumnRefOperator> extraOutputRefs = Lists.newArrayList();

            Preconditions.checkState(context.size() == pieceFilters.size());
            for (int i = 0; i < context.size(); i++) {
                LogicalAggregationOperator aggregate = context.get(i).op.cast();

                // check group by
                if (aggregate.getGroupingKeys().stream().map(converter::convertRef)
                        .anyMatch(c -> !groupBys.contains(c))) {
                    return Optional.empty();
                }

                // check aggregate
                boolean onlyGroupBy = aggregate.getAggregations().isEmpty();
                boolean isMultiParamsAggFunc =
                        aggregate.getAggregations().values().stream().anyMatch(v -> v.getChildren().size() > 1);
                if (filterDistinct > 1 && (onlyGroupBy || isMultiParamsAggFunc)) {
                    return Optional.empty();
                }
                boolean hasDistinctAgg =
                        aggregate.getAggregations().values().stream().anyMatch(CallOperator::isDistinct);


                ScalarOperator filter = filterDistinct > 1 ? pieceFilters.get(i) : null;
                QueryPiecesPlan originalPiece = piecePlans.get(i);
                originalPiece.pieceIdToHasGroupBy.put(originalPiece.planId, !aggregate.getGroupingKeys().isEmpty());

                // track if this piece already has a safe row-count aggregation (COUNT(*)) after rewrite
                final ColumnRefOperator[] pieceRowCountRef = {null};
                aggregate.getAggregations().forEach((ref, call) -> {
                    CallOperator newCall = addFilterAggCall((CallOperator) converter.convert(call), filter,
                            aggFilterProject, hasDistinctAgg);

                    if (aggToRefs.containsKey(newCall)) {
                        converter.add(ref, aggToRefs.get(newCall));
                    } else {
                        ColumnRefOperator newRef = factory.create(ref.getName(), ref.getType(), ref.isNullable());
                        aggToRefs.put(newCall, newRef);
                        converter.add(ref, newRef);
                    }

                    // if original is COUNT(*) (non-distinct), reuse its filtered form as row_count_if
                    if (pieceRowCountRef[0] == null && filter != null) {
                        if (FunctionSet.COUNT.equalsIgnoreCase(call.getFnName())
                                && call.getChildren().isEmpty()
                                && !call.isDistinct()) {
                            pieceRowCountRef[0] = aggToRefs.get(newCall);
                        }
                    }
                });

                havingPredicates.add(converter.convert(aggregate.getPredicate()));

                if (filter != null && !aggregate.getGroupingKeys().isEmpty()) {
                    if (pieceRowCountRef[0] != null) {
                        originalPiece.pieceIdToRowCountRef.put(originalPiece.planId, pieceRowCountRef[0]);
                        if (!extraOutputRefs.contains(pieceRowCountRef[0])) {
                            extraOutputRefs.add(pieceRowCountRef[0]);
                        }
                    } else {
                        Function anyFn = ExprUtils.getBuiltinFunction(FunctionSet.ANY_VALUE,
                                new Type[] {BooleanType.BOOLEAN}, Function.CompareMode.IS_IDENTICAL);
                        CallOperator anyTrue = new CallOperator(FunctionSet.ANY_VALUE, BooleanType.BOOLEAN,
                                List.of(ConstantOperator.createBoolean(true)), anyFn);
                        CallOperator anyIfCall = addFilterAggCall(anyTrue, filter, aggFilterProject, false);
                        ColumnRefOperator hitRef;
                        if (aggToRefs.containsKey(anyIfCall)) {
                            hitRef = aggToRefs.get(anyIfCall);
                        } else {
                            hitRef = factory.create("row_hit", BooleanType.BOOLEAN, true);
                            aggToRefs.put(anyIfCall, hitRef);
                        }
                        originalPiece.pieceIdToRowCountRef.put(originalPiece.planId, hitRef);
                        if (!extraOutputRefs.contains(hitRef)) {
                            extraOutputRefs.add(hitRef);
                        }
                    }
                }
            }

            Optional<QueryPieces> childPieces = child;
            if (!aggFilterProject.isEmpty()) {
                Map<ColumnRefOperator, ScalarOperator> filterProject = aggFilterProject.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
                groupBys.forEach(k -> filterProject.put(k, k));
                aggToRefs.keySet().forEach(k -> {
                    List<ColumnRefOperator> input = k.getColumnRefs();
                    input.forEach(v -> filterProject.putIfAbsent(v, v));
                });
                childPieces = QueryPieces.of(new LogicalProjectOperator(filterProject),
                        Collections.emptyList(), childPieces.get());
            }

            ScalarOperator havingPredicate =
                    havingPredicates.stream().distinct().count() == 1 ? havingPredicates.get(0) : null;

            Map<ColumnRefOperator, CallOperator> aggs = aggToRefs.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
            Operator op = LogicalAggregationOperator.builder().withOperator(node)
                    .setAggregations(aggs)
                    .setGroupingKeys(groupBys)
                    .setPartitionByColumns(partitions)
                    .setPredicate(havingPredicate)
                    .build();
            Preconditions.checkState(childPieces.isPresent());
            return QueryPieces.of(op, extraOutputRefs, childPieces.get());
        }

        private CallOperator addFilterAggCall(CallOperator call, ScalarOperator filter,
                                              Map<ScalarOperator, ColumnRefOperator> filterProject, boolean hasDistinct) {
            if (filter == null) {
                return call;
            }

            Preconditions.checkState(call.getChildren().size() <= 1);

            ScalarOperator child;
            Function aggFunc;
            if (call.getChildren().isEmpty()) {
                // count(*)
                Preconditions.checkState(FunctionSet.COUNT.equalsIgnoreCase(call.getFnName()));
                child = ConstantOperator.createInt(1);
                aggFunc = ExprUtils.getBuiltinFunction(call.getFunction().getFunctionName().getFunction(),
                        new Type[] {IntegerType.INT}, Function.CompareMode.IS_IDENTICAL);
            } else {
                child = call.getChild(0);
                aggFunc = call.getFunction();
            }

            java.util.function.Function<CallOperator, CallOperator> fallbackAggIfBuilder = aggCall -> {
                Function f = ExprUtils.getBuiltinFunction(FunctionSet.IF,
                        new Type[] {BooleanType.BOOLEAN, child.getType(), child.getType()}, Function.CompareMode.IS_IDENTICAL);
                CallOperator ifNull = new CallOperator("if", child.getType(),
                        List.of(filter, child, ConstantOperator.createNull(child.getType())), f);
                if (!filterProject.containsKey(ifNull)) {
                    filterProject.put(ifNull, factory.create(ifNull, child.getType(), true));
                }
                return new CallOperator(call.getFnName(), call.getType(), List.of(filterProject.get(ifNull)),
                        aggFunc, call.isDistinct());
            };
            // since agg_If doens't support distinct right now
            if (hasDistinct) {
                return fallbackAggIfBuilder.apply(call);
            } else {
                Preconditions.checkState(aggFunc instanceof AggregateFunction);
                Type[] argTypes = new Type[] {child.getType(), BooleanType.BOOLEAN};
                Function aggStateIf = ExprUtils.getBuiltinFunction(aggFunc.functionName() + FunctionSet.AGG_STATE_IF_SUFFIX,
                        argTypes, Function.CompareMode.IS_IDENTICAL);
                // set precision and scale for decimal
                if (aggStateIf != null && argTypes[0].isDecimalV3()) {
                    aggStateIf = rectifyAggregationFunctionForDecimal(aggFunc.functionName(), child.getType());
                }
                // If the found aggStateIf has a polymorphic intermediate type (e.g., ANY_STRUCT for array_agg_if),
                // create a properly resolved AggStateIf using the already-analyzed aggFunc, which has concrete types.
                // Otherwise, SplitAggregateRule would use the polymorphic ANY_STRUCT as the intermediate column type,
                // causing getTypeSize() to throw when deriving statistics.
                if (aggStateIf instanceof AggregateFunction &&
                        ((AggregateFunction) aggStateIf).getIntermediateTypeOrReturnType().isPseudoType()) {
                    aggStateIf = AggStateIf.of((AggregateFunction) aggFunc).orElse((AggStateIf) aggStateIf);
                }

                if (aggStateIf == null) {
                    return fallbackAggIfBuilder.apply(call);
                }

                if (!filterProject.containsKey(filter)) {
                    filterProject.put(filter, factory.create(filter, BooleanType.BOOLEAN, true));
                }

                if (!filterProject.containsKey(child) && !child.isConstant()) {
                    filterProject.put(child, factory.create(child, child.getType(), child.isNullable()));
                }

                ScalarOperator childOutput = child.isConstant() ? child : filterProject.get(child);

                return new CallOperator(aggStateIf.functionName(), call.getType(),
                        List.of(childOutput, filterProject.get(filter)), aggStateIf,
                        call.isDistinct());
            }
        }

        Function rectifyAggregationFunctionForDecimal(String fnWithoutIfName, Type argType) {
            FunctionParams argParams = new FunctionParams(false, Lists.newArrayList());
            Boolean[] argArgumentConstants = new Boolean[] {false};
            NodePosition position = new NodePosition(0, 0);

            Function fnWithoutIf =
                    FunctionAnalyzer.getAnalyzedAggregateFunction(ctx, fnWithoutIfName, argParams,
                            new Type[] {argType}, argArgumentConstants, position);
            if (fnWithoutIf == null) {
                return null;
            }
            if (!(fnWithoutIf instanceof AggregateFunction aggFuncWithoutIf)) {
                return null;
            }

            Optional<? extends Function> finalAggIfFunc = AggStateIf.of(aggFuncWithoutIf);
            if (finalAggIfFunc.isPresent()) {
                return finalAggIfFunc.get();
            } else {
                return null;
            }
        }

        @Override
        public Optional<QueryPieces> visitLogicalJoin(LogicalJoinOperator node, List<QueryPieces> context) {
            if (context.stream().map(p -> ((LogicalJoinOperator) p.op).getJoinType()).distinct().count() > 1) {
                return Optional.empty();
            }

            allJoinTypes.add(node.getJoinType());
            Optional<QueryPieces> left = visitChild(context, 0);
            if (left.isEmpty()) {
                return Optional.empty();
            }
            Optional<QueryPieces> right = visitChild(context, 1);
            if (right.isEmpty()) {
                return Optional.empty();
            }

            List<ColumnRefOperator> filterRefs = Lists.newArrayList(left.get().filterUsedRefs);
            filterRefs.addAll(right.get().filterUsedRefs);

            LogicalJoinOperator.Builder builder = LogicalJoinOperator.builder()
                    .withOperator(node)
                    .setOnPredicate(converter.convert(node.getOnPredicate()));
            return QueryPieces.of(builder.build(), filterRefs, left.get(), right.get());
        }

        @Override
        public Optional<QueryPieces> visitLogicalTableScan(LogicalScanOperator node, List<QueryPieces> context) {
            if (context.stream().map(p -> ((LogicalScanOperator) p.op).getTable()).distinct().count() > 1) {
                return Optional.empty();
            }
            if (context.stream().map(p -> p.op.getLimit()).distinct().count() > 1) {
                return Optional.empty();
            }

            Map<Column, ColumnRefOperator> columnMetaToColRefMap = Maps.newHashMap();
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap = Maps.newHashMap();

            int scanId = factory.getNextRelationId();
            node.getTable().getColumns().stream().sorted(Comparator.comparing(Column::getName)).forEach(c -> {
                ColumnRefOperator newRef = factory.create(c.getName(), c.getType(), c.isAllowNull());
                columnMetaToColRefMap.put(c, newRef);
                colRefToColumnMetaMap.put(newRef, c);
                factory.updateColumnToRelationIds(newRef.getId(), scanId);
                factory.updateColumnRefToColumns(newRef, c, node.getTable());
            });

            LogicalScanOperator.Builder builder = OperatorBuilderFactory.build(node);
            builder.withOperator(node);
            builder.setColRefToColumnMetaMap(colRefToColumnMetaMap);
            builder.setColumnMetaToColRefMap(columnMetaToColRefMap);
            builder.setTable(node.getTable());
            builder.setPredicate(null);

            List<ScalarOperator> fusionPredicates = Lists.newArrayList();
            Set<ScalarOperator> fusionPredicateSet = Sets.newHashSet();
            for (QueryPieces piece : context) {
                LogicalScanOperator scan = piece.op.cast();
                scan.getColumnMetaToColRefMap().forEach((c, ref) -> {
                    ColumnRefOperator newRef = columnMetaToColRefMap.get(c);
                    converter.add(ref, newRef);
                });
                ScalarOperator newPredicate = converter.convert(scan.getPredicate());
                fusionPredicates.add(newPredicate);
                fusionPredicateSet.add(newPredicate);
            }

            List<ColumnRefOperator> filterUsedRefs = Lists.newArrayList();
            if (fusionPredicateSet.size() > 1) {
                // same predicate doesn't need do again
                Preconditions.checkState(fusionPredicates.size() == this.fusionScanFilters.size());
                for (int i = 0; i < fusionPredicates.size(); i++) {
                    ScalarOperator filter = fusionPredicates.get(i);
                    if (filter != null) {
                        this.fusionScanFilters.get(i).add(filter);
                        filter.getColumnRefs(filterUsedRefs);
                    }
                }
            }
            if (fusionPredicateSet.stream().allMatch(Objects::nonNull)) {
                ScalarOperator predicate = Utils.compoundOr(fusionPredicateSet);
                ScalarRangePredicateExtractor extractor = new ScalarRangePredicateExtractor();
                predicate = extractor.rewriteOnlyColumn(predicate);
                builder.setPredicate(predicate);
            }

            // rewrite hash distribution info for olap table scan operator
            if (node instanceof LogicalOlapScanOperator) {
                if (((LogicalOlapScanOperator) node).getDistributionSpec().getType()
                        == DistributionSpec.DistributionType.SHUFFLE) {
                    OlapTable olapTable = ((OlapTable) node.getTable());
                    DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();

                    List<Integer> hashDistributeColumns = new ArrayList<>();
                    HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                    List<Column> distributedColumns =
                            MetaUtils.getColumnsByColumnIds(olapTable, hashDistributionInfo.getDistributionColumns());
                    for (Column distributedColumn : distributedColumns) {
                        Preconditions.checkState(columnMetaToColRefMap.containsKey(distributedColumn));
                        hashDistributeColumns.add(columnMetaToColRefMap.get(distributedColumn).getId());
                    }
                    HashDistributionDesc hashDistributionDesc =
                            new HashDistributionDesc(hashDistributeColumns, HashDistributionDesc.SourceType.LOCAL);
                    ((LogicalOlapScanOperator.Builder) builder).setDistributionSpec(
                            DistributionSpec.createHashDistributionSpec(hashDistributionDesc));
                }
            }
            return QueryPieces.of(builder.build(), filterUsedRefs);
        }

        @Override
        public Optional<QueryPieces> visitOperator(Operator node, List<QueryPieces> context) {
            return Optional.empty();
        }
    }
}
