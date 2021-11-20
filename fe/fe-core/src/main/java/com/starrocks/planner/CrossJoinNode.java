// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/CrossJoinNode.java

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
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableRef;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Cross join between left child and right child.
 */
// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class CrossJoinNode extends PlanNode {
    private static final Logger LOG = LogManager.getLogger(CrossJoinNode.class);

    // Default per-host memory requirement used if no valid stats are available.
    // TODO: Come up with a more useful heuristic (e.g., based on scanned partitions).
    private static final long DEFAULT_PER_HOST_MEM = 2L * 1024L * 1024L * 1024L;
    private final TableRef innerRef_;

    public CrossJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, TableRef innerRef) {
        super(id, "CROSS JOIN");
        innerRef_ = innerRef;
        tupleIds.addAll(outer.getTupleIds());
        tupleIds.addAll(inner.getTupleIds());
        children.add(outer);
        children.add(inner);

        // Inherits all the nullable tuple from the children
        // Mark tuples that form the "nullable" side of the outer join as nullable.
        nullableTupleIds.addAll(outer.getNullableTupleIds());
        nullableTupleIds.addAll(inner.getNullableTupleIds());
    }

    public TableRef getInnerRef() {
        return innerRef_;
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        super.computeStats(analyzer);
        if (getChild(0).cardinality == -1 || getChild(1).cardinality == -1) {
            cardinality = -1;
        } else {
            cardinality = getChild(0).cardinality * getChild(1).cardinality;
            if (computeSelectivity() != -1) {
                cardinality = Math.round(((double) cardinality) * computeSelectivity());
            }
        }
        if (cardinality < -1) {
            cardinality = Long.MAX_VALUE;
        }
        LOG.debug("stats CrossJoin: cardinality={}", Long.toString(cardinality));
    }

    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this).addValue(super.debugString()).toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.CROSS_JOIN_NODE;
    }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder().append(detailPrefix + "cross join:" + "\n");
        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix + "predicates: ").append(getExplainString(conjuncts) + "\n");
        } else {
            output.append(detailPrefix + "predicates is NULL.\n");
        }
        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return Math.max(children.get(0).getNumInstances(), children.get(1).getNumInstances());
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
}
