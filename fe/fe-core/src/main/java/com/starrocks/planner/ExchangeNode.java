// This file is made available under Elastic License 2.0.
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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SortInfo;
import com.starrocks.analysis.TupleId;
import com.starrocks.common.UserException;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.thrift.TExchangeNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TSortInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class ExchangeNode extends PlanNode {
    private static final Logger LOG = LogManager.getLogger(ExchangeNode.class);

    // The parameters based on which sorted input streams are merged by this
    // exchange node. Null if this exchange does not merge sorted streams
    private SortInfo mergeInfo;

    // Offset after which the exchange begins returning rows. Currently valid
    // only if mergeInfo_ is non-null, i.e. this is a merging exchange node.
    private long offset;

    private DistributionSpec.DistributionType distributionType;

    /**
     * Create ExchangeNode that consumes output of inputNode.
     * An ExchangeNode doesn't have an input node as a child, which is why we
     * need to compute the cardinality here.
     */
    public ExchangeNode(PlanNodeId id, PlanNode inputNode, boolean copyConjuncts) {
        super(id, inputNode, "EXCHANGE");
        offset = 0;
        children.add(inputNode);
        if (!copyConjuncts) {
            this.conjuncts = Lists.newArrayList();
        }
        if (hasLimit()) {
            cardinality = Math.min(limit, inputNode.cardinality);
        } else {
            cardinality = inputNode.cardinality;
        }
        // Only apply the limit at the receiver if there are multiple senders.
        if (inputNode.getFragment().isPartitioned()) {
            limit = inputNode.limit;
        }
        computeTupleIds();
    }

    public ExchangeNode(PlanNodeId id, PlanNode inputNode, boolean copyConjuncts,
                        DistributionSpec.DistributionType type) {
        this(id, inputNode, copyConjuncts);
        distributionType = type;
    }

    // For Test
    public ExchangeNode(PlanNodeId id, PlanNode inputNode) {
        super(id, inputNode, "EXCHANGE");
    }

    public DistributionSpec.DistributionType getDistributionType() {
        return distributionType;
    }

    @Override
    public final void computeTupleIds() {
        clearTupleIds();
        tupleIds.addAll(getChild(0).getTupleIds());
        tblRefIds.addAll(getChild(0).getTblRefIds());
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
        if (offset != 0) {
            return detailPrefix + "offset: " + offset + "\n";
        } else {
            return "";
        }
    }

    @Override
    public int getNumInstances() {
        return numInstances;
    }

    @Override
    public boolean isVectorized() {
        for (PlanNode node : getChildren()) {
            if (!node.isVectorized()) {
                return false;
            }
        }

        for (Expr expr : conjuncts) {
            if (!expr.isVectorized()) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }

    @Override
    public boolean pushDownRuntimeFilters(RuntimeFilterDescription description, Expr probeExpr) {
        boolean accept = false;
        if (description.canPushAcrossExchangeNode()) {
            description.enterExchangeNode();
            for (PlanNode node : children) {
                if (node.pushDownRuntimeFilters(description, probeExpr)) {
                    description.setHasRemoteTargets(true);
                    accept = true;
                }
            }
            description.exitExchangeNode();
        }

        // if this rf is generate by broadcast or co-location, and this rf is local
        // we'd better to put a rf at exchange node, because underneath scan node has to wait global rf.
        // maybe that scan node has to wait for a long time.
        if (probeExpr.isBoundByTupleIds(getTupleIds())) {
            if (description.isLocalApplicable() && description.inLocalFragmentInstance()) {
                description.addProbeExpr(id.asInt(), probeExpr);
                probeRuntimeFilters.add(description);
                accept = true;
            }
        }
        return accept;
    }

    @Override
    public boolean canDoReplicatedJoin() {
        return false;
    }

}
