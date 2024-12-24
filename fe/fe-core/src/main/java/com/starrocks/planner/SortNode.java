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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/SortNode.java

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
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.SortInfo;
import com.starrocks.common.IdGenerator;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TNormalSortInfo;
import com.starrocks.thrift.TNormalSortNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TRuntimeFilterDescription;
import com.starrocks.thrift.TSortInfo;
import com.starrocks.thrift.TSortNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class SortNode extends PlanNode implements RuntimeFilterBuildNode {

    private static final Logger LOG = LogManager.getLogger(SortNode.class);
    private final SortInfo info;
    private final boolean useTopN;
    private final boolean isDefaultLimit;

    private TopNType topNType = TopNType.ROW_NUMBER;

    private long offset;
    // if SortNode(TopNNode in BE) is followed by AnalyticNode with partition_exprs, this partition_exprs is
    // also added to TopNNode to hint that local shuffle operator is prepended to TopNNode in
    // order to eliminate merging operation in pipeline execution engine.
    private List<Expr> analyticPartitionExprs = Collections.emptyList();
    private boolean analyticPartitionSkewed = false;

    // info_.sortTupleSlotExprs_ substituted with the outputSmap_ for materialized slots in init().
    public List<Expr> resolvedTupleExprs;

    private final List<RuntimeFilterDescription> buildRuntimeFilters = Lists.newArrayList();
    private boolean withRuntimeFilters = false;

    private List<Expr> preAggFnCalls;

    private List<SlotId> preAggOutputColumnId;

    public void setAnalyticPartitionExprs(List<Expr> exprs) {
        this.analyticPartitionExprs = exprs;
    }

    public void setAnalyticPartitionSkewed(boolean isSkewed) {
        analyticPartitionSkewed = isSkewed;
    }

    private DataPartition inputPartition;

    public SortNode(PlanNodeId id, PlanNode input, SortInfo info, boolean useTopN,
                    boolean isDefaultLimit, long offset) {
        super(id, useTopN ? "TOP-N" : (info.getPartitionExprs().isEmpty() ? "SORT" : "PARTITION-TOP-N"));
        this.info = info;
        this.useTopN = useTopN;
        this.isDefaultLimit = isDefaultLimit;
        this.tupleIds.addAll(Lists.newArrayList(info.getSortTupleDescriptor().getId()));
        this.nullableTupleIds.addAll(input.getNullableTupleIds());
        this.children.add(input);
        this.offset = offset;
        if (info.getPreAggTupleDesc_() != null && !info.getPreAggTupleDesc_().getSlots().isEmpty()) {
            this.tupleIds.addAll(Lists.newArrayList(info.getPreAggTupleDesc_().getId()));
        }
        Preconditions.checkArgument(info.getOrderingExprs().size() == info.getIsAscOrder().size());
    }

    public TopNType getTopNType() {
        return topNType;
    }

    public void setTopNType(TopNType topNType) {
        this.topNType = topNType;
    }

    public boolean isUseTopN() {
        return useTopN;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public SortInfo getSortInfo() {
        return info;
    }

    public void setPreAggFnCalls(List<Expr> preAggFnCalls) {
        this.preAggFnCalls = preAggFnCalls;
    }

    @Override
    protected void computeStats(Analyzer analyzer) {
    }

    @Override
    public List<RuntimeFilterDescription> getBuildRuntimeFilters() {
        return buildRuntimeFilters;
    }

    @Override
    public void buildRuntimeFilters(IdGenerator<RuntimeFilterId> generator, DescriptorTable descTbl,
                                    ExecGroupSets execGroupSets) {
        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        // only support the runtime filter in TopN when limit > 0
        if (limit < 0 || !sessionVariable.getEnableTopNRuntimeFilter() ||
                getSortInfo().getOrderingExprs().isEmpty()) {
            return;
        }

        // RuntimeFilter only works for the first column
        Expr orderBy = getSortInfo().getOrderingExprs().get(0);

        RuntimeFilterDescription rf = new RuntimeFilterDescription(sessionVariable);
        rf.setFilterId(generator.getNextId().asInt());
        rf.setBuildPlanNodeId(getId().asInt());
        rf.setExprOrder(0);
        rf.setJoinMode(JoinNode.DistributionMode.BROADCAST);
        rf.setOnlyLocal(true);
        rf.setSortInfo(getSortInfo());
        rf.setBuildExpr(orderBy);
        rf.setRuntimeFilterType(RuntimeFilterDescription.RuntimeFilterType.TOPN_FILTER);
        RuntimeFilterPushDownContext rfPushDownCtx = new RuntimeFilterPushDownContext(rf, descTbl, execGroupSets);
        for (PlanNode child : children) {
            if (child.pushDownRuntimeFilters(rfPushDownCtx, orderBy, Lists.newArrayList())) {
                this.buildRuntimeFilters.add(rf);
            }
        }
        withRuntimeFilters = !buildRuntimeFilters.isEmpty();
    }

    @Override
    public void clearBuildRuntimeFilters() {
        buildRuntimeFilters.clear();
    }

    public void setPreAggOutputColumnId(List<SlotId> preAggOutputColumnId) {
        this.preAggOutputColumnId = preAggOutputColumnId;
    }

    @Override
    protected String debugString() {
        List<String> strings = Lists.newArrayList();
        for (Boolean isAsc : info.getIsAscOrder()) {
            strings.add(isAsc ? "a" : "d");
        }
        return MoreObjects.toStringHelper(this).add("ordering_exprs",
                Expr.debugString(info.getOrderingExprs())).add("is_asc",
                "[" + Joiner.on(" ").join(strings) + "]").addValue(super.debugString()).toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.SORT_NODE;
        TSortInfo sortInfo = new TSortInfo(
                Expr.treesToThrift(info.getOrderingExprs()),
                info.getIsAscOrder(),
                info.getNullsFirst());
        sortInfo.setSort_tuple_slot_exprs(Expr.treesToThrift(resolvedTupleExprs));

        msg.sort_node = new TSortNode(sortInfo, useTopN);
        msg.sort_node.setOffset(offset);
        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        SessionVariable defaultVariable = GlobalStateMgr.getCurrentState().getVariableMgr().getDefaultSessionVariable();
        if (sessionVariable.getFullSortMaxBufferedBytes() != defaultVariable.getFullSortMaxBufferedBytes()) {
            msg.sort_node.setMax_buffered_bytes(sessionVariable.getFullSortMaxBufferedBytes());
        }
        if (sessionVariable.getFullSortMaxBufferedRows() != defaultVariable.getFullSortMaxBufferedRows()) {
            msg.sort_node.setMax_buffered_rows(sessionVariable.getFullSortMaxBufferedRows());
        }

        msg.sort_node.setLate_materialization(sessionVariable.isFullSortLateMaterialization());
        msg.sort_node.setEnable_parallel_merge(sessionVariable.isEnableParallelMerge());

        if (info.getPartitionExprs() != null) {
            msg.sort_node.setPartition_exprs(Expr.treesToThrift(info.getPartitionExprs()));
            msg.sort_node.setPartition_limit(info.getPartitionLimit());
        }
        msg.sort_node.setTopn_type(topNType.toThrift());
        // TODO(lingbin): remove blew codes, because it is duplicate with TSortInfo
        msg.sort_node.setOrdering_exprs(Expr.treesToThrift(info.getOrderingExprs()));
        msg.sort_node.setIs_asc_order(info.getIsAscOrder());
        msg.sort_node.setNulls_first(info.getNullsFirst());
        msg.sort_node.setAnalytic_partition_exprs(Expr.treesToThrift(analyticPartitionExprs));
        msg.sort_node.setAnalytic_partition_skewed(analyticPartitionSkewed);
        if (info.getSortTupleSlotExprs() != null) {
            msg.sort_node.setSort_tuple_slot_exprs(Expr.treesToThrift(info.getSortTupleSlotExprs()));
        }
        msg.sort_node.setHas_outer_join_child(hasNullableGenerateChild);
        // For profile printing `SortKeys`
        Iterator<Expr> expr = info.getOrderingExprs().iterator();
        Iterator<Boolean> direction = info.getIsAscOrder().iterator();
        StringBuilder sqlSortKeysBuilder = new StringBuilder();
        while (expr.hasNext()) {
            if (sqlSortKeysBuilder.length() > 0) {
                sqlSortKeysBuilder.append(", ");
            }
            sqlSortKeysBuilder.append(expr.next().toSql().replaceAll("<slot\\s[0-9]+>\\s+", "")).append(" ");
            sqlSortKeysBuilder.append(direction.next() ? "ASC" : "DESC");
        }
        if (sqlSortKeysBuilder.length() > 0) {
            msg.sort_node.setSql_sort_keys(sqlSortKeysBuilder.toString());
        }

        if (!buildRuntimeFilters.isEmpty()) {
            List<TRuntimeFilterDescription> tRuntimeFilterDescriptions =
                    RuntimeFilterDescription.toThriftRuntimeFilterDescriptions(buildRuntimeFilters);
            msg.sort_node.setBuild_runtime_filters(tRuntimeFilterDescriptions);
        }

        if (preAggFnCalls != null && !preAggFnCalls.isEmpty()) {
            msg.sort_node.setPre_agg_exprs(Expr.treesToThrift(preAggFnCalls));
            msg.sort_node.setPre_agg_insert_local_shuffle(
                    ConnectContext.get().getSessionVariable().isInsertLocalShuffleForWindowPreAgg());
        }
        if (preAggOutputColumnId != null && !preAggOutputColumnId.isEmpty()) {
            List<Integer> outputColumnsId = preAggOutputColumnId.stream().map(slotId -> slotId.asInt()).collect(
                    Collectors.toList());
            msg.sort_node.setPre_agg_output_slot_id(outputColumnsId);
        }
    }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        if (!TopNType.ROW_NUMBER.equals(topNType)) {
            output.append(detailPrefix).append("type: ").append(topNType.toString()).append("\n");
        }
        Iterator<Expr> partitionExpr = info.getPartitionExprs().iterator();
        boolean start = true;
        while (partitionExpr.hasNext()) {
            if (start) {
                start = false;
                output.append(detailPrefix).append("partition by: ");
            } else {
                output.append(", ");
            }
            if (detailLevel.equals(TExplainLevel.NORMAL)) {
                output.append(partitionExpr.next().toSql()).append(" ");
            } else {
                output.append(partitionExpr.next().explain()).append(" ");
            }
        }
        if (!start) {
            output.append("\n");
            output.append(detailPrefix).append("partition limit: ").append(info.getPartitionLimit()).append("\n");
        }
        output.append(detailPrefix).append("order by: ");
        Iterator<Expr> orderExpr = info.getOrderingExprs().iterator();
        Iterator<Boolean> isAsc = info.getIsAscOrder().iterator();
        start = true;
        while (orderExpr.hasNext()) {
            if (start) {
                start = false;
            } else {
                output.append(", ");
            }
            if (detailLevel.equals(TExplainLevel.NORMAL)) {
                output.append(orderExpr.next().toSql()).append(" ");
            } else {
                output.append(orderExpr.next().explain()).append(" ");
            }
            output.append(isAsc.next() ? "ASC" : "DESC");
        }
        output.append("\n");

        if (preAggFnCalls != null && !preAggFnCalls.isEmpty()) {
            output.append(detailPrefix).append("pre agg functions: ");
            List<String> strings = Lists.newArrayList();

            for (Expr fnCall : preAggFnCalls) {
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
        }

        if (!analyticPartitionExprs.isEmpty()) {
            output.append(detailPrefix).append("analytic partition by: ");
            start = true;
            for (Expr expr : analyticPartitionExprs) {
                if (start) {
                    start = false;
                } else {
                    output.append(", ");
                }
                if (detailLevel.equals(TExplainLevel.NORMAL)) {
                    output.append(expr.toSql());
                } else {
                    output.append(expr.explain());
                }
            }
            output.append("\n");
        }

        if (detailLevel == TExplainLevel.VERBOSE) {
            if (!buildRuntimeFilters.isEmpty()) {
                output.append(detailPrefix).append("build runtime filters:\n");
                for (RuntimeFilterDescription rf : buildRuntimeFilters) {
                    output.append(detailPrefix).append("- ").append(rf.toExplainString(-1)).append("\n");
                }
            }
        }
        output.append(detailPrefix).append("offset: ").append(offset).append("\n");
        return output.toString();
    }

    public void init(Analyzer analyzer) throws UserException {
        // Compute the memory layout for the generated tuple.
        computeStats(analyzer);
        // createDefaultSmap(analyzer);
        // // populate resolvedTupleExprs and outputSmap_
        // List<SlotDescriptor> sortTupleSlots = info.getSortTupleDescriptor().getSlots();
        // List<Expr> slotExprs = info.getSortTupleSlotExprs_();
        // Preconditions.checkState(sortTupleSlots.size() == slotExprs.size());

        // populate resolvedTupleExprs_ and outputSmap_
        List<SlotDescriptor> sortTupleSlots = info.getSortTupleDescriptor().getSlots();
        List<Expr> slotExprs = info.getSortTupleSlotExprs();
        Preconditions.checkState(sortTupleSlots.size() == slotExprs.size());

        resolvedTupleExprs = Lists.newArrayList();
        outputSmap = new ExprSubstitutionMap();

        for (int i = 0; i < slotExprs.size(); ++i) {
            if (!sortTupleSlots.get(i).isMaterialized()) {
                continue;
            }
            resolvedTupleExprs.add(slotExprs.get(i));
            outputSmap.put(slotExprs.get(i), new SlotRef(sortTupleSlots.get(i)));
        }

        ExprSubstitutionMap childSmap = getCombinedChildSmap();
        resolvedTupleExprs = Expr.substituteList(resolvedTupleExprs, childSmap, analyzer, false);

        // Remap the ordering exprs to the tuple materialized by this sort node. The mapping
        // is a composition of the childSmap and the outputSmap_ because the child node may
        // have also remapped its input (e.g., as in a a series of (sort->analytic)* nodes).
        // Parent nodes have have to do the same so set the composition as the outputSmap_.
        outputSmap = ExprSubstitutionMap.compose(childSmap, outputSmap, analyzer);
        info.substituteOrderingExprs(outputSmap, analyzer);

        hasNullableGenerateChild = checkHasNullableGenerateChild();

        if (LOG.isDebugEnabled()) {
            LOG.debug("sort id " + tupleIds.get(0).toString() + " smap: "
                    + outputSmap.debugString());
            LOG.debug("sort input exprs: " + Expr.debugString(resolvedTupleExprs));
        }
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return !withRuntimeFilters && getChildren().stream().allMatch(PlanNode::canUseRuntimeAdaptiveDop);
    }

    @Override
    public boolean canPushDownRuntimeFilter() {
        return !useTopN;
    }

    @Override
    public boolean extractConjunctsToNormalize(FragmentNormalizer normalizer) {
        if (!useTopN) {
            return super.extractConjunctsToNormalize(normalizer);
        }
        return false;
    }

    @Override
    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        TNormalSortNode sortNode = new TNormalSortNode();
        TNormalSortInfo sortInfo = new TNormalSortInfo();
        sortInfo.setOrdering_exprs(normalizer.normalizeOrderedExprs(info.getOrderingExprs()));
        sortInfo.setIs_asc_order(info.getIsAscOrder());
        sortInfo.setNulls_first(info.getNullsFirst());
        if (info.getSortTupleSlotExprs() != null) {
            sortInfo.setSort_tuple_slot_exprs(normalizer.normalizeOrderedExprs(info.getSortTupleSlotExprs()));
        }
        sortNode.setSort_info(sortInfo);
        sortNode.setUse_top_n(useTopN);
        sortNode.setOffset(offset);
        if (info.getPartitionExprs() != null) {
            sortNode.setPartition_exprs(normalizer.normalizeOrderedExprs(info.getPartitionExprs()));
            sortNode.setPartition_limit(info.getPartitionLimit());
        }
        sortNode.setTopn_type(topNType.toThrift());
        if (analyticPartitionExprs != null) {
            sortNode.setAnalytic_partition_exprs(normalizer.normalizeOrderedExprs(analyticPartitionExprs));
        }
        sortNode.setHas_outer_join_child(hasNullableGenerateChild);
        planNode.setSort_node(sortNode);
        planNode.setNode_type(TPlanNodeType.SORT_NODE);
        normalizeConjuncts(normalizer, planNode, conjuncts);
    }
}
