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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/ExchangeNode.java

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
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.SortInfo;
import com.starrocks.analysis.TupleId;
import com.starrocks.common.exception.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.thrift.TExchangeNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TNormalExchangeNode;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TNormalSortInfo;
import com.starrocks.thrift.TPartitionType;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TSortInfo;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Objects;

/**
 * Receiver side of a 1:n data stream. Logically, an ExchangeNode consumes the data
 * produced by its children. For each of the sending child nodes the actual data
 * transmission is performed by the DataStreamSink of the PlanFragment housing
 * that child node. Typically, an ExchangeNode only has a single sender child but,
 * e.g., for distributed union queries an ExchangeNode may have one sender child per
 * union operand.
 * <p>
 * If a (optional) SortInfo field is set, the ExchangeNode will merge its
 * inputs on the parameters specified in the SortInfo object. It is assumed that the
 * inputs are also sorted individually on the same SortInfo parameter.
 */
public class ExchangeNode extends PlanNode {
    // The parameters based on which sorted input streams are merged by this
    // exchange node. Null if this exchange does not merge sorted streams
    private SortInfo mergeInfo;

    // Offset after which the exchange begins returning rows. Currently valid
    // only if mergeInfo_ is non-null, i.e. this is a merging exchange node.
    private long offset;

    private TPartitionType partitionType;
    private DataPartition dataPartition;
    private DistributionSpec.DistributionType distributionType;
    // Specify the columns which need to send, work on CTE, and keep empty in other sense
    private List<Integer> receiveColumns;

    /**
     * Create ExchangeNode that consumes output of inputNode.
     * An ExchangeNode doesn't have an input node as a child, which is why we
     * need to compute the cardinality here.
     */
    public ExchangeNode(PlanNodeId id, PlanNode inputNode, DataPartition dataPartition) {
        super(id, inputNode, "EXCHANGE");
        offset = 0;
        children.add(inputNode);
        this.conjuncts = Lists.newArrayList();
        this.dataPartition = dataPartition;
        if (hasLimit()) {
            cardinality = Math.min(limit, inputNode.cardinality);
        } else {
            cardinality = inputNode.cardinality;
        }
        // Only apply the limit at the receiver if there are multiple senders.
        if (inputNode.getFragment().isPartitioned()) {
            if (inputNode instanceof SortNode) {
                SortNode sortNode = (SortNode) inputNode;
                if (Objects.equals(TopNType.ROW_NUMBER, sortNode.getTopNType()) &&
                        CollectionUtils.isEmpty(sortNode.getSortInfo().getPartitionExprs())) {
                    limit = inputNode.limit;
                } else {
                    unsetLimit();
                }
            } else {
                limit = inputNode.limit;
            }
        }
        computeTupleIds();
    }

    public ExchangeNode(PlanNodeId id, PlanNode inputNode, DistributionSpec.DistributionType type) {
        this(id, inputNode, DataPartition.UNPARTITIONED);
        distributionType = type;
    }

    public void setDataPartition(DataPartition dataPartition) {
        this.dataPartition = dataPartition;
    }

    public void setPartitionType(TPartitionType type) {
        partitionType = type;
    }

    public DistributionSpec.DistributionType getDistributionType() {
        return distributionType;
    }

    public boolean isMerge() {
        return mergeInfo != null;
    }

    public void setReceiveColumns(List<Integer> receiveColumns) {
        this.receiveColumns = receiveColumns;
    }

    public List<Integer> getReceiveColumns() {
        return receiveColumns;
    }

    @Override
    public final void computeTupleIds() {
        clearTupleIds();
        tupleIds.addAll(getChild(0).getTupleIds());
        nullableTupleIds.addAll(getChild(0).getNullableTupleIds());
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        Preconditions.checkState(conjuncts.isEmpty());
    }

    /**
     * Set the parameters used to merge sorted input streams. This can be called
     * after init().
     */
    public void setMergeInfo(SortInfo info, long offset) {
        this.mergeInfo = info;
        this.offset = offset;
        this.planNodeName = "MERGING-EXCHANGE";
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.EXCHANGE_NODE;
        msg.exchange_node = new TExchangeNode();
        for (TupleId tid : tupleIds) {
            msg.exchange_node.addToInput_row_tuples(tid.asInt());
        }
        if (mergeInfo != null) {
            TSortInfo sortInfo = new TSortInfo(
                    Expr.treesToThrift(mergeInfo.getOrderingExprs()), mergeInfo.getIsAscOrder(),
                    mergeInfo.getNullsFirst());
            msg.exchange_node.setSort_info(sortInfo);
            msg.exchange_node.setOffset(offset);
        }
        if (partitionType != null) {
            msg.exchange_node.setPartition_type(partitionType);
        }
        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        msg.exchange_node.setEnable_parallel_merge(sessionVariable.isEnableParallelMerge());
    }

    @Override
    protected String debugString() {
        ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.add("offset", offset);
        return helper.toString();
    }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        List<Expr> partitionExprs = dataPartition.getPartitionExprs();
        if (detailLevel == TExplainLevel.VERBOSE) {
            if (distributionType != null) {
                output.append(detailPrefix).append("distribution type: ")
                        .append(distributionType).append('\n');
            }
            if (partitionType != null) {
                output.append(detailPrefix).append("partition type: ")
                        .append(partitionType).append('\n');
            }
            if (CollectionUtils.isNotEmpty(partitionExprs)) {
                output.append(detailPrefix)
                        .append("partition exprs: ")
                        .append(getVerboseExplain(partitionExprs, detailLevel))
                        .append('\n');
            }
        }
        if (offset != 0) {
            output.append(detailPrefix)
                    .append("offset: ")
                    .append(offset)
                    .append('\n');
        }
        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return numInstances;
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }

    @Override
    public boolean pushDownRuntimeFilters(DescriptorTable descTbl, RuntimeFilterDescription description, Expr probeExpr,
                                          List<Expr> partitionByExprs) {
        if (!canPushDownRuntimeFilter()) {
            return false;
        }
        boolean accept = pushCrossExchange(descTbl, description, probeExpr, partitionByExprs);
        // Add the rf onto ExchangeNode if it can not be pushed down to Exchange's offsprings or
        // session variable runtime_filter_on_exchange_node is true(in default is false).
        boolean onExchangeNode = (!accept || ConnectContext.get().getSessionVariable().isRuntimeFilterOnExchangeNode());
        // UPDATE:
        // but in some complex query case, fragment delivery time will be much longer than expected.
        // since building runtime filter is very fast, we possibly will see a case that:
        // we are going to deliver global runtime filter to some nodes(fragment instances), but those
        // fragment instances are not ready yet, then global runtime filter does not apply at all.
        // the safest way to handle this case, is to put global runtime filter at the
        // boundary of fragment instance(exchange node).
        // we enable this only when:
        // - session variable enabled &
        // - this rf has been accepted by children nodes(global rf).
        if (probeExpr.isBoundByTupleIds(getTupleIds()) && description.canAcceptFilter(this)) {
            if (onExchangeNode || (description.isLocalApplicable() && description.inLocalFragmentInstance())) {
                description.addProbeExpr(id.asInt(), probeExpr);
                description.addPartitionByExprsIfNeeded(id.asInt(), probeExpr, partitionByExprs);
                probeRuntimeFilters.add(description);
                accept = true;
            }
        }
        return accept;
    }

    private boolean pushCrossExchange(DescriptorTable descTbl,
                                      RuntimeFilterDescription description, Expr probeExpr,
                                      List<Expr> partitionByExprs) {
        if (!description.canPushAcrossExchangeNode()) {
            return false;
        }

        boolean crossExchange = false;
        // TODO: remove this later when multi columns on grf is default on.
        // broadcast or only one RF, always can be cross exchange
        if (description.isBroadcastJoin() || description.getEqualCount() == 1) {
            crossExchange = true;
        } else if (description.getEqualCount() > 1 && partitionByExprs.size() == 1) {
            // RF nums > 1 and only partition by one column, only send the RF which RF's column equals partition column
            Expr pExpr = partitionByExprs.get(0);
            if (probeExpr instanceof SlotRef && pExpr instanceof SlotRef &&
                    ((SlotRef) probeExpr).getSlotId().asInt() == ((SlotRef) pExpr).getSlotId().asInt()) {
                crossExchange = true;
            }
        }

        if (!crossExchange) {
            // If partitionByExprs's size is 1 and it is not the probeExpr, don't push down it
            // because multi GRFs will cause performance decrease which multi GRFs will increase
            // scan's wait time.
            if (description.getEqualCount() > 1 && partitionByExprs.size() == 1) {
                return false;
            }
            if (!ConnectContext.get().getSessionVariable().isEnableMultiColumnsOnGlobbalRuntimeFilter()) {
                return false;
            }
        }

        boolean accept = false;
        description.enterExchangeNode();
        for (PlanNode node : children) {
            if (node.pushDownRuntimeFilters(descTbl, description, probeExpr, partitionByExprs)) {
                description.setHasRemoteTargets(true);
                accept = true;
            }
        }
        description.exitExchangeNode();
        return accept;
    }

    @Override
    public boolean canDoReplicatedJoin() {
        return false;
    }

    @Override
    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        TNormalExchangeNode exchangeNode = new TNormalExchangeNode();
        exchangeNode.setInput_row_tuples(normalizer.remapTupleIds(tupleIds));
        if (mergeInfo != null) {
            TNormalSortInfo sortInfo = new TNormalSortInfo();
            sortInfo.setOrdering_exprs(normalizer.normalizeOrderedExprs(mergeInfo.getOrderingExprs()));
            sortInfo.setIs_asc_order(mergeInfo.getIsAscOrder());
            sortInfo.setNulls_first(mergeInfo.getNullsFirst());
            exchangeNode.setSort_info(sortInfo);
        }
        exchangeNode.setOffset(offset);
        exchangeNode.setPartition_type(partitionType);
        planNode.setExchange_node(exchangeNode);
        planNode.setNode_type(TPlanNodeType.EXCHANGE_NODE);
        normalizeConjuncts(normalizer, planNode, conjuncts);
        super.toNormalForm(planNode, normalizer);
    }
}
