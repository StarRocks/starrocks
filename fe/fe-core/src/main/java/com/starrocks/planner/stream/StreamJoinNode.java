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


package com.starrocks.planner.stream;

import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.TableRef;
import com.starrocks.planner.JoinNode;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.sql.optimizer.operator.stream.IMTInfo;
import com.starrocks.thrift.TEqJoinCondition;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TStreamJoinNode;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;
import javax.ws.rs.NotSupportedException;

public class StreamJoinNode extends JoinNode {
    // TODO: support bi-stream join
    private IMTInfo rightIMT;

    public StreamJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, TableRef innerRef, List<Expr> eqJoinConjuncts,
                          List<Expr> otherJoinConjuncts) {
        super("StreamJoin", id, outer, inner, JoinOperator.INNER_JOIN, eqJoinConjuncts, otherJoinConjuncts);
    }

    public StreamJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, JoinOperator joinOp,
                          List<Expr> eqJoinConjuncts, List<Expr> otherJoinConjuncts) {
        super("StreamJoin", id, outer, inner, joinOp, eqJoinConjuncts, otherJoinConjuncts);
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
                TEqJoinCondition eqJoinCondition = new TEqJoinCondition(
                        eqJoinPredicate.getChild(0).treeToThrift(), eqJoinPredicate.getChild(1).treeToThrift());
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

    // TODO support bi-stream join
    public void setLeftIMT(IMTInfo imt) { throw new NotSupportedException("TODO"); }

    public void setRightIMT(IMTInfo imt) { this.rightIMT = imt; }

    @Override
    public boolean canPushDownRuntimeFilter() {
        return false;
    }

    @Override
    protected String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        String joinStr = super.getNodeExplainString(detailPrefix, detailLevel);
        joinStr += detailPrefix + "rhs_imt: ";
        if (rightIMT != null) {
            joinStr += rightIMT + "\n";
        } else {
            joinStr += "empty\n";
        }
        return joinStr;
    }
}
