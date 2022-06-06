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

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableRef;
import com.starrocks.common.IdGenerator;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TCrossJoinNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Cross join between left child and right child.
 */
// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class NestLoopJoinNode extends JoinNode implements RuntimeFilterBuildNode {
    private static final Logger LOG = LogManager.getLogger(NestLoopJoinNode.class);

    // Default per-host memory requirement used if no valid stats are available.
    // TODO: Come up with a more useful heuristic (e.g., based on scanned partitions).
    private static final long DEFAULT_PER_HOST_MEM = 2L * 1024L * 1024L * 1024L;
    private final TableRef innerRef_;

    private final List<RuntimeFilterDescription> buildRuntimeFilters = Lists.newArrayList();

    public List<RuntimeFilterDescription> getBuildRuntimeFilters() {
        return buildRuntimeFilters;
    }

    public void clearBuildRuntimeFilters() {
        buildRuntimeFilters.removeIf(RuntimeFilterDescription::isHasRemoteTargets);
    }

    public NestLoopJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, TableRef innerRef,
                            JoinOperator joinOp,
                            List<Expr> joinConjuncts) {
        super("NESTLOOP JOIN", id, outer, inner, joinOp, Lists.newArrayList(), joinConjuncts);
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
    protected void toThrift(TPlanNode msg) {
        // TODO(mofei) change to nestloop join
        msg.node_type = TPlanNodeType.CROSS_JOIN_NODE;
        msg.cross_join_node = new TCrossJoinNode();
        if (!buildRuntimeFilters.isEmpty()) {
            msg.cross_join_node.setBuild_runtime_filters(
                    RuntimeFilterDescription.toThriftRuntimeFilterDescriptions(buildRuntimeFilters));
        }
    }

    @Override
    public void buildRuntimeFilters(IdGenerator<RuntimeFilterId> generator) {
        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        final List<Expr> conjuncts = this.getConjuncts();
        JoinNode.DistributionMode distributionMode = JoinNode.DistributionMode.BROADCAST;
        PlanNode buildStageNode = this.getChild(1);
        for (int i = 0; i < conjuncts.size(); i++) {
            Expr expr = conjuncts.get(i);
            // we only support BinaryPredicate to build RuntimeFilter
            if (expr.getChildren().size() == 2) {

                Expr left = expr.getChild(0);
                Expr right = expr.getChild(1);

                if (!(left instanceof SlotRef)) {
                    continue;
                }

                if (!right.isBoundByTupleIds(getChild(1).getTupleIds())) {
                    continue;
                }

                RuntimeFilterDescription rf = new RuntimeFilterDescription(sessionVariable);
                rf.setFilterId(generator.getNextId().asInt());
                rf.setBuildPlanNodeId(getId().asInt());
                rf.setExprOrder(i);
                rf.setJoinMode(distributionMode);
                rf.setBuildCardinality(buildStageNode.getCardinality());
                rf.setOnlyLocal(true);

                rf.setBuildExpr(right);
                boolean accept = getChild(0).pushDownRuntimeFilters(rf, left);
                if (accept) {
                    this.getBuildRuntimeFilters().add(rf);
                }
            }
        }
    }

}
