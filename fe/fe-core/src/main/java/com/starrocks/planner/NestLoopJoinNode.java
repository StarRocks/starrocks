// This file is made available under Elastic License 2.0.

package com.starrocks.planner;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.TableRef;
import com.starrocks.thrift.TNestLoopJoinNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

/**
 * NESTLOOP JOIN
 *  TODO: support all kinds of join type
 */
public class NestLoopJoinNode extends JoinNode {
    private static final Logger LOG = LogManager.getLogger(NestLoopJoinNode.class);

    public NestLoopJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, TableRef innerRef,
                            JoinOperator joinOp, List<Expr> eqJoinConjuncts, List<Expr> joinConjuncts) {
        super("NESTLOOP JOIN", id, outer, inner, joinOp, eqJoinConjuncts, joinConjuncts);

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
