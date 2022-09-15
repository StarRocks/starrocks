// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.planner.stream;

import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.TableRef;
import com.starrocks.planner.JoinNode;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.thrift.TEqJoinCondition;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TStreamJoinNode;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

public class StreamJoinNode extends JoinNode {

    public StreamJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, TableRef innerRef,
                          List<Expr> eqJoinConjuncts, List<Expr> otherJoinConjuncts) {
        super("StreamJoin", id, outer, inner, JoinOperator.INNER_JOIN, eqJoinConjuncts, otherJoinConjuncts);
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.STREAM_JOIN_NODE;
        msg.stream_join_node = new TStreamJoinNode();
        msg.stream_join_node.join_op = joinOp.toThrift();

        if (CollectionUtils.isNotEmpty(eqJoinConjuncts)) {
            for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
                TEqJoinCondition eqJoinCondition = new TEqJoinCondition(eqJoinPredicate.getChild(0).treeToThrift(),
                        eqJoinPredicate.getChild(1).treeToThrift());
                eqJoinCondition.setOpcode(eqJoinPredicate.getOp().getOpcode());
                msg.stream_join_node.addToEq_join_conjuncts(eqJoinCondition);
            }
        }
        if (CollectionUtils.isNotEmpty(otherJoinConjuncts)) {
            for (Expr e : otherJoinConjuncts) {
                msg.stream_join_node.addToOther_join_conjuncts(e.treeToThrift());
            }
            String sqlJoinPredicate = otherJoinConjuncts.stream().map(Expr::toSql).collect(Collectors.joining(","));
            msg.stream_join_node.setSql_join_predicates(sqlJoinPredicate);
        }

    }

    @Override
    public boolean canPushDownRuntimeFilter() {
        return false;
    }
}
