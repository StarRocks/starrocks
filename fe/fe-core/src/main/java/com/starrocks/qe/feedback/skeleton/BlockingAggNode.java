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

import com.google.common.collect.Sets;
import com.starrocks.qe.feedback.NodeExecStats;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Objects;

public class BlockingAggNode extends SkeletonNode {
    private final List<ColumnRefOperator> groupBys;

    public BlockingAggNode(OptExpression optExpression, NodeExecStats nodeExecStats, SkeletonNode parent) {
        super(optExpression, nodeExecStats, parent);
        groupBys = ((PhysicalHashAggregateOperator) optExpression.getOp()).getGroupBys();
    }

    public List<ColumnRefOperator> getGroupBys() {
        return groupBys;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), groupBys);
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

        BlockingAggNode that = (BlockingAggNode) o;
        return Objects.equals(Sets.newHashSet(groupBys), Sets.newHashSet(that.groupBys));
    }
}
