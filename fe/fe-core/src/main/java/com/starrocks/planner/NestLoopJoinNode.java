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

package com.starrocks.planner;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableRef;
import com.starrocks.common.IdGenerator;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TNestLoopJoinNode;
import com.starrocks.thrift.TNormalNestLoopJoinNode;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * NESTLOOP JOIN
 * Support all kinds of join type and join conjuncts
 */
public class NestLoopJoinNode extends JoinNode implements RuntimeFilterBuildNode {

    private static final Logger LOG = LogManager.getLogger(NestLoopJoinNode.class);

    public NestLoopJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, TableRef innerRef,
                            JoinOperator joinOp, List<Expr> eqJoinConjuncts, List<Expr> joinConjuncts) {
        super("NESTLOOP JOIN", id, outer, inner, joinOp, eqJoinConjuncts, joinConjuncts);
    }

    /**
     * Build the filter if inner table contains only one row, which is a common case for scalar subquery
     */
    @Override
    public void buildRuntimeFilters(IdGenerator<RuntimeFilterId> generator, DescriptorTable descTbl) {
        if (!joinOp.isInnerJoin() && !joinOp.isLeftSemiJoin() && !joinOp.isRightJoin() && !joinOp.isCrossJoin()) {
            return;
        }
        if (!ConnectContext.get().getSessionVariable().isEnableCrossJoinRuntimeFilter()) {
            return;
        }

        List<Expr> conjuncts = new ArrayList<>(otherJoinConjuncts);
        conjuncts.addAll(getConjuncts());
        for (int i = 0; i < conjuncts.size(); i++) {
            Expr expr = conjuncts.get(i);
            if (expr.getChildren().size() == 2) {
                Expr left = expr.getChild(0);
                Expr right = expr.getChild(1);
                if (canBuildFilter(expr, left, right)) {
                    pushDownCrossJoinFilter(generator, descTbl, left, right, i);
                } else if (canBuildFilter(expr, right, left)) {
                    pushDownCrossJoinFilter(generator, descTbl, right, left, i);
                }
            }
        }
    }

    // Only binary op could build a filter
    // And some special cases are not suitable for build a filter, such as NOT_EQ
    private boolean canBuildFilter(Expr joinExpr, Expr probeExpr, Expr buildExpr) {
        if (!(probeExpr instanceof SlotRef)) {
            return false;
        }
        PlanNode probeChild = getChild(0);
        PlanNode buildChild = getChild(1);
        return probeExpr.isBoundByTupleIds(probeChild.getTupleIds()) &&
                buildExpr.isBoundByTupleIds(buildChild.getTupleIds());
    }

    private void pushDownCrossJoinFilter(IdGenerator<RuntimeFilterId> generator, DescriptorTable descTbl,
                                         Expr probeExpr, Expr buildExpr, int idx) {
        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        PlanNode buildStageNode = this.getChild(1);
        RuntimeFilterDescription rf = new RuntimeFilterDescription(sessionVariable);
        rf.setFilterId(generator.getNextId().asInt());
        rf.setBuildPlanNodeId(getId().asInt());
        rf.setExprOrder(idx);
        rf.setJoinMode(DistributionMode.BROADCAST);
        rf.setBuildCardinality(buildStageNode.getCardinality());
        rf.setOnlyLocal(true);
        rf.setBuildExpr(buildExpr);

        if (getChild(0).pushDownRuntimeFilters(descTbl, rf, probeExpr, probePartitionByExprs)) {
            this.getBuildRuntimeFilters().add(rf);
        }
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        Preconditions.checkState(CollectionUtils.isEmpty(eqJoinConjuncts));
        Preconditions.checkState(!joinOp.isRightSemiAntiJoin());
        msg.node_type = TPlanNodeType.NESTLOOP_JOIN_NODE;
        msg.nestloop_join_node = new TNestLoopJoinNode();
        msg.nestloop_join_node.join_op = joinOp.toThrift();

        if (CollectionUtils.isNotEmpty(otherJoinConjuncts)) {
            for (Expr e : otherJoinConjuncts) {
                msg.nestloop_join_node.addToJoin_conjuncts(e.treeToThrift());
            }
            String sqlJoinPredicate = otherJoinConjuncts.stream().map(Expr::toSql).collect(Collectors.joining(","));
            msg.nestloop_join_node.setSql_join_conjuncts(sqlJoinPredicate);
        }

        if (!buildRuntimeFilters.isEmpty()) {
            msg.nestloop_join_node.setBuild_runtime_filters(
                    RuntimeFilterDescription.toThriftRuntimeFilterDescriptions(buildRuntimeFilters));
        }
    }

    @Override
    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        TNormalNestLoopJoinNode nlJoinNode = new TNormalNestLoopJoinNode();
        nlJoinNode.setJoin_op(getJoinOp().toThrift());
        nlJoinNode.setJoin_conjuncts(normalizer.normalizeExprs(otherJoinConjuncts));
        planNode.setNestloop_join_node(nlJoinNode);
        planNode.setNode_type(TPlanNodeType.NESTLOOP_JOIN_NODE);
        normalizeConjuncts(normalizer, planNode, conjuncts);
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return getChildren().stream().allMatch(PlanNode::canUseRuntimeAdaptiveDop);
    }
}
