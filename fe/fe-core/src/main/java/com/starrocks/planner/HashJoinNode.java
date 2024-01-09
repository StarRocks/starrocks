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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/HashJoinNode.java

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

import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableRef;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TEqJoinCondition;
import com.starrocks.thrift.THashJoinNode;
import com.starrocks.thrift.TNormalHashJoinNode;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;

import java.util.ArrayList;
import java.util.List;

/**
 * Hash join between left child and right child.
 * The right child must be a leaf node, ie, can only materialize
 * a single input tuple.
 */
public class HashJoinNode extends JoinNode {
    public HashJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, TableRef innerRef,
                        List<Expr> eqJoinConjuncts, List<Expr> otherJoinConjuncts) {
        super("HASH JOIN", id, outer, inner, innerRef, eqJoinConjuncts, otherJoinConjuncts);
    }

    public HashJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, JoinOperator joinOp,
                        List<Expr> eqJoinConjuncts, List<Expr> otherJoinConjuncts) {
        super("HASH JOIN", id, outer, inner, joinOp, eqJoinConjuncts, otherJoinConjuncts);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HASH_JOIN_NODE;
        msg.hash_join_node = new THashJoinNode();
        msg.hash_join_node.join_op = joinOp.toThrift();
        msg.hash_join_node.distribution_mode = distrMode.toThrift();
        StringBuilder sqlJoinPredicatesBuilder = new StringBuilder();
        for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            TEqJoinCondition eqJoinCondition = new TEqJoinCondition(eqJoinPredicate.getChild(0).treeToThrift(),
                    eqJoinPredicate.getChild(1).treeToThrift());
            eqJoinCondition.setOpcode(eqJoinPredicate.getOp().getOpcode());
            msg.hash_join_node.addToEq_join_conjuncts(eqJoinCondition);
            if (sqlJoinPredicatesBuilder.length() > 0) {
                sqlJoinPredicatesBuilder.append(", ");
            }
            sqlJoinPredicatesBuilder.append(eqJoinPredicate.toSql());
        }
        for (Expr e : otherJoinConjuncts) {
            msg.hash_join_node.addToOther_join_conjuncts(e.treeToThrift());
            if (sqlJoinPredicatesBuilder.length() > 0) {
                sqlJoinPredicatesBuilder.append(", ");
            }
            sqlJoinPredicatesBuilder.append(e.toSql());
        }
        if (sqlJoinPredicatesBuilder.length() > 0) {
            msg.hash_join_node.setSql_join_predicates(sqlJoinPredicatesBuilder.toString());
        }
        if (!conjuncts.isEmpty()) {
            StringBuilder sqlPredicatesBuilder = new StringBuilder();
            for (Expr e : conjuncts) {
                if (sqlPredicatesBuilder.length() > 0) {
                    sqlPredicatesBuilder.append(", ");
                }
                sqlPredicatesBuilder.append(e.toSql());
            }
            if (sqlPredicatesBuilder.length() > 0) {
                msg.hash_join_node.setSql_predicates(sqlPredicatesBuilder.toString());
            }
        }
        msg.hash_join_node.setIs_push_down(isPushDown);
        if (innerRef != null) {
            msg.hash_join_node.setIs_rewritten_from_not_in(innerRef.isJoinRewrittenFromNotIn());
        }
        if (!buildRuntimeFilters.isEmpty()) {
            msg.hash_join_node.setBuild_runtime_filters(
                    RuntimeFilterDescription.toThriftRuntimeFilterDescriptions(buildRuntimeFilters));
        }
        msg.hash_join_node.setBuild_runtime_filters_from_planner(
                ConnectContext.get().getSessionVariable().getEnableGlobalRuntimeFilter());
        if (partitionExprs != null) {
            msg.hash_join_node.setPartition_exprs(Expr.treesToThrift(partitionExprs));
        }
        msg.setFilter_null_value_columns(filter_null_value_columns);

        if (outputSlots != null) {
            msg.hash_join_node.setOutput_columns(outputSlots);
        }

        if (getCanLocalShuffle()) {
            msg.hash_join_node.setInterpolate_passthrough(
                    ConnectContext.get().getSessionVariable().isHashJoinInterpolatePassthrough());
        }
        if (isOneMatchProbe()) {
            msg.hash_join_node.setIs_one_match_probe(true);
        }
    }

    @Override
    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        TNormalHashJoinNode hashJoinNode = new TNormalHashJoinNode();
        hashJoinNode.setJoin_op(getJoinOp().toThrift());
        hashJoinNode.setDistribution_mode(getDistrMode().toThrift());
        hashJoinNode.setEq_join_conjuncts(normalizer.normalizeExprs(new ArrayList<>(eqJoinConjuncts)));
        hashJoinNode.setOther_join_conjuncts(normalizer.normalizeExprs(otherJoinConjuncts));
        hashJoinNode.setIs_rewritten_from_not_in(innerRef != null && innerRef.isJoinRewrittenFromNotIn());
        hashJoinNode.setPartition_exprs(normalizer.normalizeOrderedExprs(partitionExprs));
        hashJoinNode.setOutput_columns(normalizer.remapIntegerSlotIds(outputSlots));
        planNode.setHash_join_node(hashJoinNode);
        planNode.setNode_type(TPlanNodeType.HASH_JOIN_NODE);
        normalizeConjuncts(normalizer, planNode, conjuncts);
    }

    @Override
    public void collectEquivRelation(FragmentNormalizer normalizer) {
        if (!joinOp.isSemiJoin() && !joinOp.isInnerJoin()) {
            return;
        }
        for (BinaryPredicate eq : eqJoinConjuncts) {
            if (!eq.getOp().equals(BinaryType.EQ)) {
                continue;
            }
            SlotId lhsSlotId = ((SlotRef) eq.getChild(0)).getSlotId();
            SlotId rhsSlotId = ((SlotRef) eq.getChild(1)).getSlotId();
            normalizer.getEquivRelation().union(lhsSlotId, rhsSlotId);
        }
    }

    @Override
    public boolean extractConjunctsToNormalize(FragmentNormalizer normalizer) {
        if (!joinOp.isInnerJoin() && joinOp.isSemiJoin()) {
            return false;
        }
        return super.extractConjunctsToNormalize(normalizer);
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        if (joinOp.isRightJoin() || joinOp.isFullOuterJoin()) {
            return false;
        }

        return getChildren().stream().allMatch(PlanNode::canUseRuntimeAdaptiveDop);
    }
}
