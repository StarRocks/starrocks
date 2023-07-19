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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/AnalyticEvalNode.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.planner;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.AnalyticWindow;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.common.UserException;
import com.starrocks.thrift.TAnalyticNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TNormalAnalyticNode;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;

import java.util.List;
import java.util.Optional;

public class AnalyticEvalNode extends PlanNode {
    private List<Expr> analyticFnCalls;

    // Partitioning exprs from the AnalyticInfo
    private final List<Expr> partitionExprs;

    // TODO: Remove when the BE uses partitionByLessThan rather than the exprs
    private List<Expr> substitutedPartitionExprs;
    private List<OrderByElement> orderByElements;

    private final AnalyticWindow analyticWindow;

    private final boolean useHashBasedPartition;

    // Physical tuples used/produced by this analytic node.
    private final TupleDescriptor intermediateTupleDesc;
    private final TupleDescriptor outputTupleDesc;

    // predicates constructed from partitionExprs_/orderingExprs_ to
    // compare input to buffered tuples
    private final Expr partitionByEq;
    private final Expr orderByEq;
    private final TupleDescriptor bufferedTupleDesc;

    public AnalyticEvalNode(
            PlanNodeId id, PlanNode input, List<Expr> analyticFnCalls,
            List<Expr> partitionExprs, List<OrderByElement> orderByElements,
            AnalyticWindow analyticWindow,
            boolean useHashBasedPartition,
            TupleDescriptor intermediateTupleDesc,
            TupleDescriptor outputTupleDesc,
            Expr partitionByEq, Expr orderByEq, TupleDescriptor bufferedTupleDesc) {
        super(id, input.getTupleIds(), "ANALYTIC");
        Preconditions.checkState(!tupleIds.contains(outputTupleDesc.getId()));
        // we're materializing the input row augmented with the analytic output tuple
        tupleIds.add(outputTupleDesc.getId());
        this.analyticFnCalls = analyticFnCalls;
        this.partitionExprs = partitionExprs;
        this.orderByElements = orderByElements;
        this.analyticWindow = analyticWindow;
        this.useHashBasedPartition = useHashBasedPartition;
        this.intermediateTupleDesc = intermediateTupleDesc;
        this.outputTupleDesc = outputTupleDesc;
        this.partitionByEq = partitionByEq;
        this.orderByEq = orderByEq;
        this.bufferedTupleDesc = bufferedTupleDesc;
        children.add(input);
        nullableTupleIds = Sets.newHashSet(input.getNullableTupleIds());
    }

    public List<Expr> getAnalyticFnCalls() {
        return analyticFnCalls;
    }

    public List<Expr> getPartitionExprs() {
        return partitionExprs;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
    }

    @Override
    protected void computeStats(Analyzer analyzer) {
    }

    @Override
    protected String debugString() {
        List<String> orderByElementStrs = Lists.newArrayList();

        for (OrderByElement element : orderByElements) {
            orderByElementStrs.add(element.toSql());
        }

        return MoreObjects.toStringHelper(this)
                .add("analyticFnCalls", Expr.debugString(analyticFnCalls))
                .add("partitionExprs", Expr.debugString(partitionExprs))
                .add("subtitutedPartitionExprs", Expr.debugString(substitutedPartitionExprs))
                .add("orderByElements", Joiner.on(", ").join(orderByElementStrs))
                .add("window", analyticWindow)
                .add("useHashBasedPartition", useHashBasedPartition)
                .add("intermediateTid", intermediateTupleDesc.getId())
                .add("intermediateTid", outputTupleDesc.getId())
                .add("outputTid", outputTupleDesc.getId())
                .add("partitionByEq",
                        partitionByEq != null ? partitionByEq.debugString() : "null")
                .add("orderByEq",
                        orderByEq != null ? orderByEq.debugString() : "null")
                .addValue(super.debugString())
                .toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.ANALYTIC_EVAL_NODE;
        msg.analytic_node = new TAnalyticNode();
        if (intermediateTupleDesc != null) {
            msg.analytic_node.setIntermediate_tuple_id(intermediateTupleDesc.getId().asInt());
        }
        msg.analytic_node.setOutput_tuple_id(outputTupleDesc.getId().asInt());
        msg.analytic_node.setPartition_exprs(Expr.treesToThrift(substitutedPartitionExprs));
        StringBuilder sqlPartitionKeysBuilder = new StringBuilder();
        for (Expr e : substitutedPartitionExprs) {
            if (sqlPartitionKeysBuilder.length() > 0) {
                sqlPartitionKeysBuilder.append(", ");
            }
            sqlPartitionKeysBuilder.append(e.toSql());
        }
        if (sqlPartitionKeysBuilder.length() > 0) {
            msg.analytic_node.setSql_partition_keys(sqlPartitionKeysBuilder.toString());
        }
        msg.analytic_node.setOrder_by_exprs(
                Expr.treesToThrift(OrderByElement.getOrderByExprs(orderByElements)));
        msg.analytic_node.setAnalytic_functions(Expr.treesToThrift(analyticFnCalls));
        StringBuilder sqlAggFuncBuilder = new StringBuilder();
        // only serialize agg exprs that are being materialized
        for (Expr e : analyticFnCalls) {
            if (!(e instanceof FunctionCallExpr)) {
                continue;
            }
            if (sqlAggFuncBuilder.length() > 0) {
                sqlAggFuncBuilder.append(", ");
            }
            sqlAggFuncBuilder.append(e.toSql());
        }
        if (sqlAggFuncBuilder.length() > 0) {
            msg.analytic_node.setSql_aggregate_functions(sqlAggFuncBuilder.toString());
        }

        if (analyticWindow == null) {
            if (!orderByElements.isEmpty()) {
                msg.analytic_node.setWindow(AnalyticWindow.DEFAULT_WINDOW.toThrift());
            }
        } else {
            // TODO: Window boundaries should have range_offset_predicate set
            msg.analytic_node.setWindow(analyticWindow.toThrift());
        }

        if (partitionByEq != null) {
            msg.analytic_node.setPartition_by_eq(partitionByEq.treeToThrift());
        }

        if (orderByEq != null) {
            msg.analytic_node.setOrder_by_eq(orderByEq.treeToThrift());
        }

        if (useHashBasedPartition) {
            msg.analytic_node.setUse_hash_based_partition(useHashBasedPartition);
        }

        if (bufferedTupleDesc != null) {
            msg.analytic_node.setBuffered_tuple_id(bufferedTupleDesc.getId().asInt());
        }
        msg.analytic_node.setHas_outer_join_child(hasNullableGenerateChild);
    }

    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("functions: ");
        List<String> strings = Lists.newArrayList();

        for (Expr fnCall : analyticFnCalls) {
            strings.add("[");
            if (detailLevel.equals(TExplainLevel.NORMAL)) {
                strings.add(fnCall.toSql());
            } else {
                strings.add(fnCall.explain());
            }
            strings.add("]");
        }

        output.append(Joiner.on(", ").join(strings));
        output.append("\n");

        if (!partitionExprs.isEmpty()) {
            output.append(prefix).append("partition by: ");
            strings.clear();

            for (Expr partitionExpr : partitionExprs) {
                if (detailLevel.equals(TExplainLevel.NORMAL)) {
                    strings.add(partitionExpr.toSql());
                } else {
                    strings.add(partitionExpr.explain());
                }
            }

            output.append(Joiner.on(", ").join(strings));
            output.append("\n");
        }

        if (!orderByElements.isEmpty()) {
            output.append(prefix).append("order by: ");
            strings.clear();

            for (OrderByElement element : orderByElements) {
                if (detailLevel.equals(TExplainLevel.NORMAL)) {
                    strings.add(element.toSql());
                } else {
                    strings.add(element.explain());
                }
            }

            output.append(Joiner.on(", ").join(strings));
            output.append("\n");
        }

        if (analyticWindow != null) {
            output.append(prefix).append("window: ");
            output.append(analyticWindow.toSql());
            output.append("\n");
        }

        if (useHashBasedPartition) {
            output.append(prefix).append("useHashBasedPartition").append("\n");
        }

        return output.toString();
    }

    public void setSubstitutedPartitionExprs(List<Expr> substitutedPartitionExprs) {
        this.substitutedPartitionExprs = substitutedPartitionExprs;
    }

    @Override
    public Optional<List<Expr>> candidatesOfSlotExpr(Expr expr) {
        if (!expr.isBoundByTupleIds(getTupleIds())) {
            return Optional.empty();
        }
        if (!(expr instanceof SlotRef)) {
            return Optional.empty();
        }
        List<Expr> newSlotExprs = Lists.newArrayList();
        for (Expr pExpr : partitionExprs) {
            // push down only when both of them are slot ref and slot id match.
            if ((pExpr instanceof SlotRef) &&
                    (((SlotRef) pExpr).getSlotId().asInt() == ((SlotRef) expr).getSlotId().asInt())) {
                newSlotExprs.add(pExpr);
            }
        }
        return newSlotExprs.size() > 0 ? Optional.of(newSlotExprs) : Optional.empty();
    }

    @Override
    public boolean pushDownRuntimeFilters(DescriptorTable descTbl, RuntimeFilterDescription description, Expr probeExpr,
                                          List<Expr> partitionByExprs) {
        if (!canPushDownRuntimeFilter()) {
            return false;
        }

        if (!probeExpr.isBoundByTupleIds(getTupleIds())) {
            return false;
        }

        return pushdownRuntimeFilterForChildOrAccept(descTbl, description, probeExpr, candidatesOfSlotExpr(probeExpr),
                partitionByExprs, candidatesOfSlotExprs(partitionByExprs), 0, true);
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return getChildren().stream().allMatch(PlanNode::canUseRuntimeAdaptiveDop);
    }

    @Override
    public boolean extractConjunctsToNormalize(FragmentNormalizer normalizer) {
        List<Expr> conjuncts = normalizer.getConjunctsByPlanNodeId(this);
        normalizer.filterOutPartColRangePredicates(getId(), conjuncts, FragmentNormalizer.getSlotIdSet(partitionExprs));
        return false;
    }

    @Override
    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        TNormalAnalyticNode analyticNode = new TNormalAnalyticNode();
        analyticNode.setPartition_exprs(normalizer.normalizeOrderedExprs(substitutedPartitionExprs));
        analyticNode.setOrder_by_exprs(
                normalizer.normalizeOrderedExprs(OrderByElement.getOrderByExprs(orderByElements)));
        analyticNode.setAnalytic_functions(normalizer.normalizeExprs(analyticFnCalls));
        if (analyticWindow != null) {
            analyticNode.setWindow(analyticWindow.toThrift());
        }
        if (intermediateTupleDesc != null) {
            analyticNode.setIntermediate_tuple_id(normalizer.remapTupleId(intermediateTupleDesc.getId()).asInt());
        }
        if (outputTupleDesc != null) {
            analyticNode.setOutput_tuple_id(normalizer.remapTupleId(outputTupleDesc.getId()).asInt());
        }
        if (bufferedTupleDesc != null) {
            analyticNode.setBuffered_tuple_id(normalizer.remapTupleId(bufferedTupleDesc.getId()).asInt());
        }
        if (partitionByEq != null) {
            analyticNode.setPartition_by_eq(normalizer.normalizeExpr(partitionByEq));
        }
        if (orderByEq != null) {
            analyticNode.setOrder_by_eq(normalizer.normalizeExpr(orderByEq));
        }
        analyticNode.setHas_outer_join_child(hasNullableGenerateChild);
        planNode.setAnalytic_node(analyticNode);
        planNode.setNode_type(TPlanNodeType.ANALYTIC_EVAL_NODE);
        normalizeConjuncts(normalizer, planNode, conjuncts);
    }
}
