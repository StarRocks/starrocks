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

package com.starrocks.qe.feedback.skeleton;

import com.starrocks.analysis.JoinOperator;
import com.starrocks.qe.feedback.NodeExecStats;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Objects;

public class JoinNode extends SkeletonNode {

    private JoinOperator joinType;

    private ScalarOperator onPredicate;

    private String joinHint;

    public JoinNode(OptExpression optExpression, NodeExecStats nodeExecStats, SkeletonNode parent) {
        super(optExpression, nodeExecStats, parent);
        PhysicalJoinOperator joinOperator = (PhysicalJoinOperator) optExpression.getOp();
        this.joinType = joinOperator.getJoinType();
        this.onPredicate = joinOperator.getOnPredicate();
        this.joinHint = joinOperator.getJoinHint();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), joinType, joinHint);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JoinNode joinNode = (JoinNode) o;
        return joinType == joinNode.joinType && Objects.equals(joinHint, joinNode.joinHint);
    }
}
