// This file is made available under Elastic License 2.0.
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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.SortInfo;
import com.starrocks.common.UserException;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TSortInfo;
import com.starrocks.thrift.TSortNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class SortNode extends PlanNode {

    private static final Logger LOG = LogManager.getLogger(SortNode.class);
    private final SortInfo info;
    private final boolean useTopN;
    private final boolean isDefaultLimit;

    private long offset;
    // if true, the output of this node feeds an AnalyticNode
    private boolean isAnalyticSort;
    // if SortNode(TopNNode in BE) is followed by AnalyticNode with partition_exprs, this partition_exprs is
    // also added to TopNNode to hint that local shuffle operator is prepended to TopNNode in
    // order to eliminate merging operation in pipeline execution engine.
    private List<Expr> analyticPartitionExprs = Collections.emptyList();

    // info_.sortTupleSlotExprs_ substituted with the outputSmap_ for materialized slots in init().
    public List<Expr> resolvedTupleExprs;

    public void setIsAnalyticSort(boolean v) {
        isAnalyticSort = v;
    }

    public boolean isAnalyticSort() {
        return isAnalyticSort;
    }

    public List<Expr> getAnalyticPartitionExprs() {
        return this.analyticPartitionExprs;
    }

    public void setAnalyticPartitionExprs(List<Expr> exprs) {
        this.analyticPartitionExprs = exprs;
    }

    private DataPartition inputPartition;

    public void setInputPartition(DataPartition inputPartition) {
        this.inputPartition = inputPartition;
    }

    public DataPartition getInputPartition() {
        return inputPartition;
    }

    public SortNode(PlanNodeId id, PlanNode input, SortInfo info, boolean useTopN,
                    boolean isDefaultLimit, long offset) {
        super(id, useTopN ? "TOP-N" : "SORT");
        this.info = info;
        this.useTopN = useTopN;
        this.isDefaultLimit = isDefaultLimit;
        this.tupleIds.addAll(Lists.newArrayList(info.getSortTupleDescriptor().getId()));
        this.nullableTupleIds.addAll(input.getNullableTupleIds());
        this.children.add(input);
        this.offset = offset;
        Preconditions.checkArgument(info.getOrderingExprs().size() == info.getIsAscOrder().size());
    }

    /**
     * Clone 'inputSortNode' for distributed Top-N
     */
    public SortNode(PlanNodeId id, SortNode inputSortNode, PlanNode child) {
        super(id, inputSortNode, inputSortNode.useTopN ? "TOP-N" : "SORT");
        this.info = inputSortNode.info;
        this.useTopN = inputSortNode.useTopN;
        this.isDefaultLimit = inputSortNode.isDefaultLimit;
        this.children.add(child);
        this.offset = inputSortNode.offset;
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

    @Override
    public void getMaterializedIds(Analyzer analyzer, List<SlotId> ids) {
        super.getMaterializedIds(analyzer, ids);
        Expr.getIds(info.getOrderingExprs(), null, ids);
    }

    @Override
    protected void computeStats(Analyzer analyzer) {
        super.computeStats(analyzer);
        cardinality = getChild(0).cardinality;
        if (hasLimit()) {
            if (cardinality == -1) {
                cardinality = limit;
            } else {
                cardinality = Math.min(cardinality, limit);
            }
        }
        LOG.debug("stats Sort: cardinality=" + Long.toString(cardinality));
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
        Preconditions.checkState(tupleIds.size() == 1, "Incorrect size for tupleIds in SortNode");
        sortInfo.setSort_tuple_slot_exprs(Expr.treesToThrift(resolvedTupleExprs));

        msg.sort_node = new TSortNode(sortInfo, useTopN);
        msg.sort_node.setOffset(offset);

        if (info.getPartitionExprs() != null) {
            msg.sort_node.setPartition_exprs(Expr.treesToThrift(info.getPartitionExprs()));
        }
        // TODO(lingbin): remove blew codes, because it is duplicate with TSortInfo
        msg.sort_node.setOrdering_exprs(Expr.treesToThrift(info.getOrderingExprs()));
        msg.sort_node.setIs_asc_order(info.getIsAscOrder());
        msg.sort_node.setNulls_first(info.getNullsFirst());
        msg.sort_node.setAnalytic_partition_exprs(Expr.treesToThrift(analyticPartitionExprs));
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
    }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
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
        output.append(detailPrefix).append("offset: ").append(offset).append("\n");
        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return children.get(0).getNumInstances();
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
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }

    @Override
    public boolean canPushDownRuntimeFilter() {
        if (useTopN) {
            return false;
        }
        return true;
    }
}
