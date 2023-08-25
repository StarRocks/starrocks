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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/AggregationNode.java

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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.AggregateInfo;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.thrift.TAggregationNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TNormalAggregationNode;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TStreamingPreaggregationMode;
import org.apache.commons.collections.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.starrocks.qe.SessionVariableConstants.FORCE_PREAGGREGATION;
import static com.starrocks.qe.SessionVariableConstants.FORCE_STREAMING;
import static com.starrocks.qe.SessionVariableConstants.LIMITED;

public class AggregationNode extends PlanNode {
    private final AggregateInfo aggInfo;

    // Set to true if this aggregation node needs to run the Finalize step. This
    // node is the root node of a distributed aggregation.
    private boolean needsFinalize;

    // If true, use streaming preaggregation algorithm. Not valid if this is a merge agg.
    private boolean useStreamingPreagg;

    private String streamingPreaggregationMode = "auto";

    private boolean useSortAgg = false;

    private boolean withLocalShuffle = false;

    // identicallyDistributed meanings the PlanNode above OlapScanNode are cases as follows:
    // 1. bucket shuffle join,
    // 2. colocate join,
    // 3. one-phase agg,
    // 4. 1st phaes of three-phase-agg(2nd phase of four-phase agg eliminated).
    // OlapScanNode and these PlanNodes have the same data partition policy.
    private boolean identicallyDistributed = false;

    /**
     * Create an agg node that is not an intermediate node.
     * isIntermediate is true if it is a slave node in a 2-part agg plan.
     */
    public AggregationNode(PlanNodeId id, PlanNode input, AggregateInfo aggInfo) {
        super(id, aggInfo.getOutputTupleId().asList(), "AGGREGATE");
        this.aggInfo = aggInfo;
        this.children.add(input);
        this.needsFinalize = true;
        updateplanNodeName();
    }

    // Unsets this node as requiring finalize. Only valid to call this if it is
    // currently marked as needing finalize.
    public void unsetNeedsFinalize() {
        Preconditions.checkState(needsFinalize);
        needsFinalize = false;
        updateplanNodeName();
    }

    /**
     * Sets this node as a preaggregation. Only valid to call this if it is not marked
     * as a preaggregation
     */
    public void setIsPreagg(boolean useStreamingPreAgg) {
        useStreamingPreagg = useStreamingPreAgg;
    }

    public AggregateInfo getAggInfo() {
        return aggInfo;
    }

    public boolean isNeedsFinalize() {
        return needsFinalize;
    }

    /**
     * Have this node materialize the aggregation's intermediate tuple instead of
     * the output tuple.
     */
    public void setIntermediateTuple() {
        Preconditions.checkState(!tupleIds.isEmpty());
        Preconditions.checkState(tupleIds.get(0).equals(aggInfo.getOutputTupleId()));
        tupleIds.clear();
        tupleIds.add(aggInfo.getIntermediateTupleId());
    }

    public void setWithLocalShuffle(boolean withLocalShuffle) {
        this.withLocalShuffle = withLocalShuffle;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
    }

    public void setStreamingPreaggregationMode(String mode) {
        this.streamingPreaggregationMode = mode;
    }

    public void setUseSortAgg(boolean useSortAgg) {
        this.useSortAgg = useSortAgg;
    }

    @Override
    public void computeStats(Analyzer analyzer) {
    }

    private void updateplanNodeName() {
        StringBuilder sb = new StringBuilder();
        sb.append("AGGREGATE");
        sb.append(" (");
        if (aggInfo.isMerge()) {
            sb.append("merge");
        } else {
            sb.append("update");
        }
        if (needsFinalize) {
            sb.append(" finalize");
        } else {
            sb.append(" serialize");
        }
        sb.append(")");
        setPlanNodeName(sb.toString());
    }

    public void setIdenticallyDistributed(boolean identicallyDistributed) {
        this.identicallyDistributed = identicallyDistributed;
    }

    public boolean isIdenticallyDistributed() {
        return identicallyDistributed;
    }

    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this).add("aggInfo", aggInfo.debugString()).addValue(
                super.debugString()).toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.AGGREGATION_NODE;

        List<TExpr> aggregateFunctions = Lists.newArrayList();
        StringBuilder sqlAggFuncBuilder = new StringBuilder();
        // only serialize agg exprs that are being materialized
        for (FunctionCallExpr e : aggInfo.getMaterializedAggregateExprs()) {
            aggregateFunctions.add(e.treeToThrift());
            if (sqlAggFuncBuilder.length() > 0) {
                sqlAggFuncBuilder.append(", ");
            }
            sqlAggFuncBuilder.append(e.toSql());
        }

        msg.agg_node =
                new TAggregationNode(
                        aggregateFunctions,
                        aggInfo.getIntermediateTupleId().asInt(),
                        aggInfo.getOutputTupleId().asInt(), needsFinalize);
        msg.agg_node.setUse_streaming_preaggregation(useStreamingPreagg);
        if (sqlAggFuncBuilder.length() > 0) {
            msg.agg_node.setSql_aggregate_functions(sqlAggFuncBuilder.toString());
        }
        msg.agg_node.setUse_sort_agg(useSortAgg);

        List<Expr> groupingExprs = aggInfo.getGroupingExprs();
        if (groupingExprs != null) {
            msg.agg_node.setGrouping_exprs(Expr.treesToThrift(groupingExprs));
            StringBuilder sqlGroupingKeysBuilder = new StringBuilder();
            for (Expr e : groupingExprs) {
                if (sqlGroupingKeysBuilder.length() > 0) {
                    sqlGroupingKeysBuilder.append(", ");
                }
                sqlGroupingKeysBuilder.append(e.toSql());
            }
            if (sqlGroupingKeysBuilder.length() > 0) {
                msg.agg_node.setSql_grouping_keys(sqlGroupingKeysBuilder.toString());
            }
        }

        List<Expr> intermediateAggrExprs = aggInfo.getIntermediateAggrExprs();
        if (intermediateAggrExprs != null && !intermediateAggrExprs.isEmpty()) {
            msg.agg_node.setIntermediate_aggr_exprs(Expr.treesToThrift(intermediateAggrExprs));
        }

        msg.agg_node.setHas_outer_join_child(hasNullableGenerateChild);
        if (streamingPreaggregationMode.equalsIgnoreCase(FORCE_STREAMING)) {
            msg.agg_node.setStreaming_preaggregation_mode(TStreamingPreaggregationMode.FORCE_STREAMING);
        } else if (streamingPreaggregationMode.equalsIgnoreCase(FORCE_PREAGGREGATION)) {
            msg.agg_node.setStreaming_preaggregation_mode(TStreamingPreaggregationMode.FORCE_PREAGGREGATION);
        } else if (streamingPreaggregationMode.equalsIgnoreCase(LIMITED)) {
            msg.agg_node.setStreaming_preaggregation_mode(TStreamingPreaggregationMode.LIMITED_MEM);
        } else {
            msg.agg_node.setStreaming_preaggregation_mode(TStreamingPreaggregationMode.AUTO);
        }

        msg.agg_node.setAgg_func_set_version(FeConstants.AGG_FUNC_VERSION);
        msg.agg_node.setInterpolate_passthrough(
                useStreamingPreagg && ConnectContext.get().getSessionVariable().isInterpolatePassthrough());
    }

    protected String getDisplayLabelDetail() {
        if (useStreamingPreagg) {
            return "STREAMING";
        }
        return null;
    }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        String nameDetail = getDisplayLabelDetail();
        if (nameDetail != null) {
            output.append(detailPrefix).append(nameDetail).append("\n");
        }
        if (aggInfo.getAggregateExprs() != null && aggInfo.getMaterializedAggregateExprs().size() > 0) {
            if (detailLevel == TExplainLevel.VERBOSE) {
                output.append(detailPrefix).append("aggregate: ");
            } else {
                output.append(detailPrefix).append("output: ");
            }
            output.append(getVerboseExplain(aggInfo.getAggregateExprs(), detailLevel)).append("\n");
        }
        // TODO: unify them
        if (detailLevel == TExplainLevel.VERBOSE) {
            if (CollectionUtils.isNotEmpty(aggInfo.getGroupingExprs())) {
                output.append(detailPrefix).append("group by: ").append(
                        getVerboseExplain(aggInfo.getGroupingExprs(), detailLevel)).append("\n");
            }
        } else {
            output.append(detailPrefix).append("group by: ").append(
                    getVerboseExplain(aggInfo.getGroupingExprs(), detailLevel)).append("\n");
        }

        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("having: ").append(getVerboseExplain(conjuncts, detailLevel))
                    .append("\n");
        }
        if (useSortAgg) {
            output.append(detailPrefix).append("sorted streaming: true\n");
        }

        if (withLocalShuffle) {
            output.append(detailPrefix).append("withLocalShuffle: true\n");
        }

        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return children.get(0).getNumInstances();
    }

    @Override
    public Optional<List<Expr>> candidatesOfSlotExpr(Expr expr, Function<Expr, Boolean> couldBound) {
        if (!couldBound.apply(expr)) {
            return Optional.empty();
        }
        if (!(expr instanceof SlotRef)) {
            return Optional.empty();
        }
        List<Expr> newSlotExprs = Lists.newArrayList();
        for (Expr gexpr : aggInfo.getGroupingExprs()) {
            if (!(gexpr instanceof SlotRef)) {
                continue;
            }
            if (((SlotRef) gexpr).getSlotId().asInt() == ((SlotRef) expr).getSlotId().asInt()) {
                newSlotExprs.add(gexpr);
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

        if (!couldBound(probeExpr, description, descTbl)) {
            return false;
        }

        Function<Expr, Boolean> couldBoundChecker = couldBound(description, descTbl);
        return pushdownRuntimeFilterForChildOrAccept(descTbl, description, probeExpr,
                candidatesOfSlotExpr(probeExpr, couldBoundChecker),
                partitionByExprs, candidatesOfSlotExprs(partitionByExprs, couldBoundForPartitionExpr()), 0, true);
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return getChildren().stream().allMatch(PlanNode::canUseRuntimeAdaptiveDop);
    }

    private void disableCacheIfHighCardinalityGroupBy(FragmentNormalizer normalizer) {
        if (ConnectContext.get() == null || getCardinality() == -1) {
            return;
        }
        long cardinalityLimit = ConnectContext.get().getSessionVariable().getQueryCacheAggCardinalityLimit();
        long cardinality = getCardinality();
        if (cardinality < cardinalityLimit || aggInfo.getGroupingExprs().isEmpty()) {
            return;
        }
        List<Expr> groupByExprs = aggInfo.getGroupingExprs();
        if (groupByExprs.size() > 3) {
            normalizer.setUncacheable(true);
        }
        List<SlotRef> slotRefs = groupByExprs.stream().filter(e -> e instanceof SlotRef && e.getType().isStringType())
                .map(e -> (SlotRef) e).collect(Collectors.toList());
        // we assume that if there exists a very high cardinality of string-typed group-by columns whose average length is
        // greater than 24 bytes(it is equivalent to three bigint-typed group-by columns), then cache populating penalty
        // is unacceptable.
        List<ColumnStatistic> stringColumnStatistics = slotRefs.stream()
                .map(slot -> columnStatistics.get(new ColumnRefOperator(slot.getSlotId().asInt(),
                        ScalarType.UNKNOWN_TYPE, "key", false)))
                .filter(stat -> stat != null && !stat.isUnknown() &&
                        stat.getAverageRowSize() * stat.getDistinctValuesCount() > 24 * cardinalityLimit)
                .collect(Collectors.toList());
        if (!stringColumnStatistics.isEmpty()) {
            normalizer.setUncacheable(true);
        }
    }

    @Override
    public boolean extractConjunctsToNormalize(FragmentNormalizer normalizer) {
        List<Expr> conjuncts = normalizer.getConjunctsByPlanNodeId(this);
        normalizer.filterOutPartColRangePredicates(getId(), conjuncts,
                FragmentNormalizer.getSlotIdSet(aggInfo.getGroupingExprs()));
        return false;
    }

    @Override
    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        disableCacheIfHighCardinalityGroupBy(normalizer);
        TNormalAggregationNode aggrNode = new TNormalAggregationNode();
        TupleId tupleId = needsFinalize ? aggInfo.getOutputTupleId() : aggInfo.getIntermediateTupleId();
        List<SlotId> slotIds = normalizer.getExecPlan().getDescTbl().getTupleDesc(tupleId).getSlots()
                .stream().map(SlotDescriptor::getId).collect(Collectors.toList());

        List<Expr> groupingExprs = aggInfo.getGroupingExprs();
        Map<SlotId, Expr> slotIdsAndGroupingExprs = Maps.newHashMap();
        int numGroupingExprs = (groupingExprs == null || groupingExprs.isEmpty()) ? 0 : groupingExprs.size();

        IntStream.range(0, numGroupingExprs).forEach(i ->
                slotIdsAndGroupingExprs.put(slotIds.get(i), groupingExprs.get(i)));
        Pair<List<Integer>, List<ByteBuffer>> remappedGroupExprs =
                normalizer.normalizeSlotIdsAndExprs(slotIdsAndGroupingExprs);
        aggrNode.setGrouping_exprs(remappedGroupExprs.second);

        Map<SlotId, Expr> slotIdsAndAggExprs = Maps.newHashMap();
        List<FunctionCallExpr> aggExprs = aggInfo.getMaterializedAggregateExprs();
        int numAggExprs = (aggExprs == null || aggExprs.isEmpty()) ? 0 : aggExprs.size();
        IntStream.range(0, numAggExprs).forEach(i ->
                slotIdsAndAggExprs.put(slotIds.get(i + numGroupingExprs), aggExprs.get(i)));

        normalizer.addSlotsUseAggColumns(slotIdsAndAggExprs);
        normalizer.disableMultiversionIfExprsUseAggColumns(groupingExprs);

        Pair<List<Integer>, List<ByteBuffer>> remappedAggExprs =
                normalizer.normalizeSlotIdsAndExprs(slotIdsAndAggExprs);
        aggrNode.setAggregate_functions(remappedAggExprs.second);

        aggrNode.setIntermediate_tuple_id(normalizer.remapTupleId(aggInfo.getIntermediateTupleId()).asInt());
        aggrNode.setOutput_tuple_id(normalizer.remapTupleId(aggInfo.getOutputTupleId()).asInt());
        aggrNode.setNeed_finalize(needsFinalize);
        aggrNode.setUse_streaming_preaggregation(useStreamingPreagg);
        aggrNode.setHas_outer_join_child(hasNullableGenerateChild);
        if (streamingPreaggregationMode.equalsIgnoreCase("force_streaming")) {
            aggrNode.setStreaming_preaggregation_mode(TStreamingPreaggregationMode.FORCE_STREAMING);
        } else if (streamingPreaggregationMode.equalsIgnoreCase("force_preaggregation")) {
            aggrNode.setStreaming_preaggregation_mode(TStreamingPreaggregationMode.FORCE_PREAGGREGATION);
        } else {
            aggrNode.setStreaming_preaggregation_mode(TStreamingPreaggregationMode.AUTO);
        }
        aggrNode.setAgg_func_set_version(FeConstants.AGG_FUNC_VERSION);
        planNode.setNode_type(TPlanNodeType.AGGREGATION_NODE);
        planNode.setAgg_node(aggrNode);
        normalizeConjuncts(normalizer, planNode, conjuncts);
    }

    @Override
    public List<SlotId> getOutputSlotIds(DescriptorTable descriptorTable) {
        final List<Expr> groupingExprs = aggInfo.getGroupingExprs();
        final List<FunctionCallExpr> aggExprs = aggInfo.getMaterializedAggregateExprs();
        int numGroupingExprs = groupingExprs != null ? groupingExprs.size() : 0;
        int numAggExprs = aggExprs != null ? aggExprs.size() : 0;
        TupleId tupleId = needsFinalize ? aggInfo.getOutputTupleId() : aggInfo.getIntermediateTupleId();
        return descriptorTable.getTupleDesc(tupleId).getSlots().subList(0, numGroupingExprs + numAggExprs)
                .stream().map(SlotDescriptor::getId).collect(Collectors.toList());
    }
}
