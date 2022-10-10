// This file is made available under Elastic License 2.0.

package com.starrocks.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableRef;
import com.starrocks.common.IdGenerator;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TExplainLevel;
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

    private final List<RuntimeFilterDescription> buildRuntimeFilters = Lists.newArrayList();

    public NestLoopJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, TableRef innerRef,
                            JoinOperator joinOp, List<Expr> eqJoinConjuncts, List<Expr> joinConjuncts) {
        super("NESTLOOP JOIN", id, outer, inner, joinOp, eqJoinConjuncts, joinConjuncts);
    }

    @Override
    public List<RuntimeFilterDescription> getBuildRuntimeFilters() {
        return buildRuntimeFilters;
    }

    @Override
    public void clearBuildRuntimeFilters() {
        buildRuntimeFilters.removeIf(RuntimeFilterDescription::isHasRemoteTargets);
    }

    @Override
    public void buildRuntimeFilters(IdGenerator<RuntimeFilterId> generator) {
        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        JoinNode.DistributionMode distributionMode = JoinNode.DistributionMode.BROADCAST;
        PlanNode buildStageNode = this.getChild(1);
        List<Expr> conjuncts = new ArrayList<>(otherJoinConjuncts);
        conjuncts.addAll(getConjuncts());
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

                if (getChild(0).pushDownRuntimeFilters(rf, left, probePartitionByExprs)) {
                    this.getBuildRuntimeFilters().add(rf);
                }
            }
        }
    }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        String explain = super.getNodeExplainString(detailPrefix, detailLevel);
        StringBuilder output = new StringBuilder(explain);
        if (!buildRuntimeFilters.isEmpty()) {
            output.append(detailPrefix).append("build runtime filters:\n");
            for (RuntimeFilterDescription rf : buildRuntimeFilters) {
                output.append(detailPrefix).append("- ").append(rf.toExplainString(-1)).append("\n");
            }
        }
        return output.toString();
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
