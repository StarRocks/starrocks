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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TEqJoinCondition;
import com.starrocks.thrift.TMergeJoinNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;

import java.util.List;

/**
 * Hash join between left child and right child.
 * The right child must be a leaf node, ie, can only materialize
 * a single input tuple.
 */
public class MergeJoinNode extends JoinNode {
    public MergeJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, JoinOperator joinOp,
                         List<Expr> eqJoinConjuncts, List<Expr> otherJoinConjuncts) {
        super("MERGE JOIN", id, outer, inner, joinOp, eqJoinConjuncts, otherJoinConjuncts);
    }


    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.MERGE_JOIN_NODE;
        msg.merge_join_node = new TMergeJoinNode();
        msg.merge_join_node.join_op = joinOp.toThrift();
        msg.merge_join_node.distribution_mode = distrMode.toThrift();
        StringBuilder sqlJoinPredicatesBuilder = new StringBuilder();
        for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            TEqJoinCondition eqJoinCondition = new TEqJoinCondition(eqJoinPredicate.getChild(0).treeToThrift(),
                    eqJoinPredicate.getChild(1).treeToThrift());
            eqJoinCondition.setOpcode(eqJoinPredicate.getOp().getOpcode());
            msg.merge_join_node.addToEq_join_conjuncts(eqJoinCondition);
            if (sqlJoinPredicatesBuilder.length() > 0) {
                sqlJoinPredicatesBuilder.append(", ");
            }
            sqlJoinPredicatesBuilder.append(eqJoinPredicate.toSql());
        }
        for (Expr e : otherJoinConjuncts) {
            msg.merge_join_node.addToOther_join_conjuncts(e.treeToThrift());
            if (sqlJoinPredicatesBuilder.length() > 0) {
                sqlJoinPredicatesBuilder.append(", ");
            }
            sqlJoinPredicatesBuilder.append(e.toSql());
        }
        if (sqlJoinPredicatesBuilder.length() > 0) {
            msg.merge_join_node.setSql_join_predicates(sqlJoinPredicatesBuilder.toString());
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
                msg.merge_join_node.setSql_predicates(sqlPredicatesBuilder.toString());
            }
        }
        msg.merge_join_node.setIs_push_down(isPushDown);
        if (innerRef != null) {
            msg.merge_join_node.setIs_rewritten_from_not_in(innerRef.isJoinRewrittenFromNotIn());
        }
        if (!buildRuntimeFilters.isEmpty()) {
            msg.merge_join_node.setBuild_runtime_filters(
                    RuntimeFilterDescription.toThriftRuntimeFilterDescriptions(buildRuntimeFilters));
        }
        msg.merge_join_node.setBuild_runtime_filters_from_planner(
                ConnectContext.get().getSessionVariable().getEnableGlobalRuntimeFilter());
        if (partitionExprs != null) {
            msg.merge_join_node.setPartition_exprs(Expr.treesToThrift(partitionExprs));
        }
        msg.setFilter_null_value_columns(filter_null_value_columns);

        if (outputSlots != null) {
            msg.merge_join_node.setOutput_columns(outputSlots);
        }
    }

}
