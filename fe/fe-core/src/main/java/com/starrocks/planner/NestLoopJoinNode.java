// This file is made available under Elastic License 2.0.

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.TableRef;
import com.starrocks.thrift.TCrossJoinNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * NESTLOOP JOIN
 *  TODO: support all kinds of join type
 *  TODO: change the executor to nestloop-node
 */
public class NestLoopJoinNode extends JoinNode {
    private static final Logger LOG = LogManager.getLogger(NestLoopJoinNode.class);

    public NestLoopJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, TableRef innerRef,
                            JoinOperator joinOp,
                            List<Expr> joinConjuncts) {
        super("NESTLOOP JOIN", id, outer, inner, joinOp, Lists.newArrayList(), joinConjuncts);

    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.CROSS_JOIN_NODE;
        msg.cross_join_node = new TCrossJoinNode();
        if (!buildRuntimeFilters.isEmpty()) {
            msg.cross_join_node.setBuild_runtime_filters(
                    RuntimeFilterDescription.toThriftRuntimeFilterDescriptions(buildRuntimeFilters));
        }
    }

}
