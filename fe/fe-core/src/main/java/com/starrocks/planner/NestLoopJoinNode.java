// This file is made available under Elastic License 2.0.

package com.starrocks.planner;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableRef;
import com.starrocks.common.IdGenerator;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TNestLoopJoinNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * NESTLOOP JOIN
 *  TODO: support all kinds of join type
 */
public class NestLoopJoinNode extends JoinNode implements RuntimeFilterBuildNode {

    private static final Logger LOG = LogManager.getLogger(NestLoopJoinNode.class);

    public NestLoopJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, TableRef innerRef,
                            JoinOperator joinOp, List<Expr> eqJoinConjuncts, List<Expr> joinConjuncts) {
        super("NESTLOOP JOIN", id, outer, inner, joinOp, eqJoinConjuncts, joinConjuncts);
    }

    @Override
    public void buildRuntimeFilters(IdGenerator<RuntimeFilterId> generator) {
        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        PlanNode buildStageNode = this.getChild(1);
        List<Expr> conjuncts = new ArrayList<>(otherJoinConjuncts);
        conjuncts.addAll(getConjuncts());
        for (int i = 0; i < conjuncts.size(); i++) {
            Expr expr = conjuncts.get(i);
            if (canBuildFilter(expr)) {
                Expr left = expr.getChild(0);
                Expr right = expr.getChild(1);

                RuntimeFilterDescription rf = new RuntimeFilterDescription(sessionVariable);
                rf.setFilterId(generator.getNextId().asInt());
                rf.setBuildPlanNodeId(getId().asInt());
                rf.setExprOrder(i);
                rf.setJoinMode(DistributionMode.BROADCAST);
                rf.setBuildCardinality(buildStageNode.getCardinality());
                rf.setOnlyLocal(true);
                rf.setBuildExpr(right);

                if (getChild(0).pushDownRuntimeFilters(rf, left)) {
                    this.getBuildRuntimeFilters().add(rf);
                }
            }
        }
    }

    // Only binary op could build a filter
    // And some special cases are not suitable for build a filter, such as NOT_EQ
    private boolean canBuildFilter(Expr joinExpr) {
        if (joinExpr.getChildren().size() != 2) {
            return false;
        }
        Expr leftExpr = joinExpr.getChild(0);
        Expr rightExpr = joinExpr.getChild(1);
        PlanNode leftChild = getChild(0);
        PlanNode rightChild = getChild(1);

        if (!(leftExpr instanceof SlotRef)) {
            return false;
        }
        if (joinExpr instanceof BinaryPredicate && ((BinaryPredicate) joinExpr).getOp().isUnequivalence()) {
            return false;
        }
        if (!leftExpr.isBoundByTupleIds(leftChild.getTupleIds())) {
            return false;
        }
        return rightExpr.isBoundByTupleIds(rightChild.getTupleIds());
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        Preconditions.checkState(eqJoinConjuncts == null || eqJoinConjuncts.isEmpty());
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

}
